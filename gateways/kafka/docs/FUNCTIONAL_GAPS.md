# Kafka <-> Iggy functional gaps (Phase 1B)

This document tracks functional divergence between real Apache Kafka wire-protocol/broker
semantics and the Iggy-backed bridge (Phase 1B), for every API key and version range that
is currently in scope per [SCOPE.md](SCOPE.md):

- ApiVersions (key 18, v0-3)
- Metadata (key 3, v0-9)
- Produce (key 0, v3-9)
- Fetch (key 1, v4-12)
- ListOffsets (key 2, v1-6)
- CreateTopics (key 19, v2-5)

It complements [IGGY_LIMITATIONS.md](IGGY_LIMITATIONS.md). `IGGY_LIMITATIONS.md` tracks five
explicitly-deferred design items (L1-L5) that were already known when Phase 1B shipped. This
document tracks gaps found by a dedicated gap-analysis pass: every implemented API key/version
combination was checked against real Kafka client-visible behavior, with every finding
verified against current source at a specific `file:line`. Items here overlap with L1-L5 where
noted, but most are new.

All findings below were verified against source as of commit `113c46c10` (the "Initial
version" + review-fixes state). Line numbers will drift as the code changes; re-check before
acting on an item that looks stale.

Related docs:

- [IGGY_LIMITATIONS.md](IGGY_LIMITATIONS.md) - L1-L5 explicitly deferred design items
- [IGGY_CORE_SDK_GAPS.md](IGGY_CORE_SDK_GAPS.md) - gaps that require an Iggy core/SDK change,
  not fixable in the gateway alone
- [BRIDGE_MAPPING.md](BRIDGE_MAPPING.md) - current mapping and workarounds in production code
- [TODO_TASKS.md](TODO_TASKS.md) - actionable checklist derived from this document

---

## Summary - cross-cutting criticals (fix first)

These affect the success path for ordinary clients and should be addressed before the
per-API minor items below.

| ID | API/version | Problem | file:line | Impact |
|----|-------------|---------|-----------|--------|
| [C1](#c1---metadata-response-wire-format-is-malformed-on-the-bridge-path) | Metadata v0-9 | Bridge-path per-partition encoding is missing the leading `error_code:i16`; v9 uses `i32` instead of COMPACT_ARRAY varint for `replica_nodes`/`isr_nodes`/`offline_replicas` | `responses.rs:662-684` (v9), `705-727` (legacy) | Any Metadata response for an existing topic with partitions > 0 is byte-misaligned from the first partition onward. A real client decoder throws or disconnects. This is the **success path** - hit on every produce/fetch to an existing topic. |
| [C2](#c2---fetch-on-out-of-range-partition-can-panic-the-server) | Fetch v4-12 | `kafka_partition_index` only checks `>= 0`, never checks `< partitions_count`. Iggy core `ops.rs` does `local_partitions.get(namespace).expect(...)`, which panics on an out-of-range partition | `mapping.rs:69-71`, `core/server/src/streaming/partitions/ops.rs:71-73` | Fetch for a stale or non-existent partition index panics the shard/server. DoS via an ordinary Fetch request, with zero gateway-side guard. |
| [C3](#c3---one-iggy-message--one-kafka-recordbatch-breaks-per-record-offset-semantics) | Fetch v4-12 | One Iggy message maps to one Kafka `RecordBatch` (which can hold N records). `high_watermark` and offsets are tracked per-batch, but Kafka clients assume per-record | `iggy_bridge.rs` (`high_watermark_from_stats`), `handler.rs:105-161` | For any producer batching more than 1 record/batch (the Java default under load), `high_watermark` undercounts by roughly Nx. Consumer lag is wildly wrong and `seek()` to a record offset is impossible. Amplifies [L3](IGGY_LIMITATIONS.md#l3--recordbatch-offset-rewriting). |
| [C4](#c4---produce-acks0-still-gets-a-response-frame) | Produce v3-9 | `acks` is decoded but never used. A response is always sent, even for `acks=0` | `requests.rs:62`, never read in `handler.rs`/`iggy_bridge.rs` | A real broker sends **no response** for `acks=0`. The extra frame here desyncs the correlation-ID stream - every subsequent response on the connection is misread. |
| [C5](#c5---createtopics-on-an-existing-topic-returns-error_none-instead-of-topic_already_exists) | CreateTopics v2-5 | `StreamNameAlreadyExists`/`TopicNameAlreadyExists` are swallowed and mapped to `ERROR_NONE`, never `TOPIC_ALREADY_EXISTS` (36) | `iggy_bridge.rs:101-126`, `handler.rs:352-358` | Breaks the `TopicExistsException` idempotency-check contract used by Kafka Streams startup and provisioning scripts. The client believes it just created a topic that already existed. |
| [C6](#c6---metadata-never-auto-creates-unknown-topics) | Metadata v0-9 | `allow_auto_topic_creation` is decoded and discarded; an unknown topic always returns `UNKNOWN_TOPIC_OR_PARTITION`, never auto-creates | `requests.rs:486-488`, `handler.rs:301-315` | The default Kafka producer workflow (produce to a new topic with no explicit `createTopics` call) cannot work at all through this gateway. |
| [C7](#c7---listing-all-topics-always-returns-an-empty-list) | Metadata v0-9 | `MetadataTopicFilter::All` (list-all-topics) always returns 0 topics; no Iggy enumeration is performed | `handler.rs:291-294` | `AdminClient.listTopics()`-style flows see an "empty cluster" regardless of actual state. |

---

## Summary - Produce (key 0, v3-9)

| ID | Sev | Problem | file:line | Impact |
|----|-----|---------|-----------|--------|
| FG-PRD-01 | critical | Same as [C4](#c4---produce-acks0-still-gets-a-response-frame) | - | - |
| FG-PRD-02 | major | `transactional_id` is rejected with `INVALID_REQUEST` (42); a real transactional producer fails earlier at `InitProducerId` (22), which is not in `SUPPORTED_RANGES` at all | `handler.rs:117-122` | `initTransactions()` fails fast with `UnsupportedVersionException`; the Produce-level check is mostly dead code. |
| FG-PRD-03 | major | No `MESSAGE_TOO_LARGE` (10). A frame over 8 MiB triggers `FrameTooLarge`, dropping the entire TCP connection with no per-partition error | `server.rs:328-374`; error code 10 is not defined in `api.rs:40-54` | A producer with `batch.size`/`max.request.size` > 8 MiB gets a connection drop and an endless reconnect/retry loop (the size never resolves). |
| FG-PRD-04 | major | No idempotent-producer (KIP-98) dedup - Iggy has no `producer_id`/`epoch`/`sequence` concept | `core/common/src/traits/message_client.rs:44-50` | `enable.idempotence=true` (the Java default) gives zero duplicate protection; combined with [C4](#c4---produce-acks0-still-gets-a-response-frame) and [L4](IGGY_LIMITATIONS.md#l4--produce-ack-toctou-offset-inference-races) retries, real duplicates can occur. |
| FG-PRD-05 | major | No CRC-32C validation on produce - a corrupted batch is accepted and stored, and only fails later at consume time | no CRC handling anywhere in `gateways/kafka/src` | Error detection is shifted from produce-time to consume-time, which can poison the consume loop at a fixed offset forever. |
| FG-PRD-06 | minor | An empty `topics[]` request produces a fabricated 1-topic/1-partition response (`name=""`) instead of an empty response | `responses.rs:34-43` | A client that checks `response.topics.len() == request.topics.len()` sees a mismatch. |
| FG-PRD-07 | minor | `records = Some(empty Bytes)` triggers Iggy's `InvalidMessagePayloadLength`, unmapped, surfacing as `UNKNOWN_SERVER_ERROR` (-1) instead of `CORRUPT_MESSAGE` (2) | `iggy_bridge.rs:163-167`, `handler.rs:364-386` | A client-side bug is masked as a server error, with the wrong retry semantics. |
| FG-PRD-08 | minor | `records = None` returns `INVALID_REQUEST` (42); a real broker would use `CORRUPT_MESSAGE` (2) | `handler.rs:129-134` | Wrong retriability classification (edge case; conformant clients never send a null records field). |
| FG-PRD-09 | minor | `log_start_offset` (v5+) is hardcoded to `0` in the Produce response, not threaded from the real value used on Fetch | `responses.rs:473-475` | Currently correct (no retention exists yet), but will be wrong once retention lands. |
| FG-PRD-10 | minor | v8+ `record_errors`/`error_message` are correctly emitted empty, but can structurally never be populated (no per-record parsing) | `responses.rs:86-94,476-484` | KIP-467 per-record granularity is unavailable; errors are all-or-nothing per partition. |
| FG-PRD-11 | minor | A partition index `< -1` (e.g. `-2`) is silently coerced to partition 0 via `unwrap_or(0)`, and the response echoes partition 0 as success | `mapping.rs:55-61,69-71` | Silent redirect to the wrong partition with no error (fuzzing / buggy-client edge case). |
| FG-PRD-12 | minor | `concat_record_batches` does a naive byte concat, assuming every Iggy-message payload is exactly one well-formed `RecordBatch` with a correct `batchLength` (true today, but unverified) | `responses.rs:748-761` | Latent - breaks if any batch's length header is ever wrong or truncated. |
| FG-PRD-13 | minor | Tombstones (null-value records) pass through fine, but Iggy has no compaction, so they are never purged | n/a (Iggy has no compaction concept) | `cleanup.policy=compact` topics never actually compact. |
| FG-PRD-14 | minor | A 0-record-count `RecordBatch` (valid header, 0 records) is accepted as 1 Iggy message and consumes 1 offset slot | `iggy_message.rs:170-172` (payload-empty check only) | Kafka record-count vs Iggy offset-count diverge by 1 per such batch (edge case). |

---

## Summary - Fetch (key 1, v4-12)

| ID | Sev | Problem | file:line | Impact |
|----|-----|---------|-----------|--------|
| FG-FET-01 | critical | Same as [C2](#c2---fetch-on-out-of-range-partition-can-panic-the-server) | - | - |
| FG-FET-02 | critical | Same as [C3](#c3---one-iggy-message--one-kafka-recordbatch-breaks-per-record-offset-semantics) | - | - |
| FG-FET-03 | major | `max_wait_ms`/`min_bytes` are decoded but never used. `poll_messages` is immediate-return-only; there is no server-side long-poll in Iggy core | `requests.rs:123-124,148-149` (never read in `handler.rs`); `core/server/src/streaming/partitions/ops.rs:78-84` | An idle consumer with `fetch.max.wait.ms=500` gets an instant empty response instead of a 500ms hold, causing busy-polling at 5-10x or more the expected request rate. |
| FG-FET-04 | major | `partition_max_bytes`/`max_bytes` are decoded but never used; the fetch is hardcoded to `MAX_FETCH_MESSAGE_COUNT=500` messages regardless, including when `max_bytes=0` (a metadata-only probe) | `mapping.rs:51`, `handler.rs:188-195` | The response can be thousands of times larger than the client's declared buffer; a `max_bytes=0` probe still returns full data. |
| FG-FET-05 | major | A negative `fetch_offset` is silently clamped to 0 via `.max(0)` instead of returning `OFFSET_OUT_OF_RANGE` (1) | `handler.rs:187` | A buggy or corrupted offset of -1 silently succeeds reading from offset 0, masking the bug as a full-history replay. |
| FG-FET-06 | major | `fetch_offset > high_watermark` returns an empty success from Iggy, never `OFFSET_OUT_OF_RANGE` (1) | `core/server/src/streaming/partitions/ops.rs:78-84` (no `OffsetOutOfRange` anywhere in core) | A consumer with a stale offset past the high watermark never gets the error that normally triggers `auto.offset.reset` recovery - it is stuck polling forever. |
| FG-FET-07 | minor | `session_id`/`session_epoch`/`forgotten_topics_data` (KIP-227) are discarded; the response always replies `session_id=0` (full-fetch) | `requests.rs:159-163,225-248`, `responses.rs:518` | Protocol-legal (clients fall back to full fetch), but every request must carry the full partition list - bandwidth overhead only. |
| FG-FET-08 | minor | An empty fetch result encodes `records=null`, not zero-length bytes (real brokers always send an empty array, never null) | `responses.rs:748-761,560-564` | Happens on every idle poll; mainstream clients tolerate null, but it deviates from real-broker wire output. |
| FG-FET-09 | minor | Iggy's own per-message `header.timestamp` is discarded entirely; Fetch returns only the producer-embedded `CreateTime`. `message.timestamp.type=LogAppendTime` is silently a no-op (not even present in the `CreatableTopic` struct) | `iggy_bridge.rs:48-52` (`FetchedMessage` has no timestamp field), `requests.rs:369-374` | Tooling that depends on `LogAppendTime` gets the producer's clock instead of the broker append time. |
| FG-FET-10 | minor | `log_start_offset` is computed as "the first offset returned by this poll", not the partition's true retained floor, and is hardcoded to 0 when the poll returns nothing | `iggy_bridge.rs:231` | Latent - correct only while nothing is ever deleted; wrong as soon as Iggy retention triggers. |
| FG-FET-11 | minor (latent) | `isolation_level` is decoded but unused - currently correct (no transactions, so LSO == HWM), but there is no plumbing if Iggy ever adds transactions | `requests.rs` decode, unused in `handler.rs` | No current impact; future-risk only. |

---

## Summary - ListOffsets (key 2, v1-6)

| ID | Sev | Problem | file:line | Impact |
|----|-----|---------|-----------|--------|
| FG-LO-01 | major | `timestamp=-2` (earliest) is hardcoded to `0`. Iggy does support segment retention/deletion (`core/server/src/shard/system/segments.rs::clean_topic_messages`), and the `Partition` struct has no `log_start_offset` field at all to report the real value | `iggy_bridge.rs:272`, `core/common/src/types/partition/mod.rs:32-44` | Once retention triggers: ListOffsets(-2) says "earliest=0", and Fetch from 0 is silently clamped to the real start (e.g. 5000) with no error - the client believes it read from 0 but actually skipped 5000 messages. |
| FG-LO-02 | major | An arbitrary-timestamp lookup with no match returns `BridgeError::UnsupportedTimestampSeek` -> `INVALID_REQUEST` (42). Real Kafka returns `(error=NONE, offset=-1)` for "nothing after this time yet" - a normal, heavily-used response (Streams/Connect time-seeks) | `iggy_bridge.rs:354-382`, `error.rs:31-35`, `handler.rs:367-369` | A time-based seek on a live/tailing topic (the steady-state case) returns an error instead of the `-1` sentinel clients expect and check for. |
| FG-LO-03 | minor | Unknown-partition/error responses set `offset=0`, not the Kafka convention of `-1` alongside the error code | `handler.rs:242-249,260-264` | Cosmetic for clients that branch on `error_code` first (most do). |
| FG-LO-04 | minor (latent) | `isolation_level` is decoded but unused - no current divergence (no transactions) | `requests.rs:297` | Future-risk only. |
| FG-LO-05 | minor | `current_leader_epoch` (v4+) is discarded; the response uses `leader_epoch=-1` - a correct sentinel, but inconsistent with Metadata's hardcoded `leader_epoch=0` for the same partition (see [FG-MD-06](#summary---metadata-key-3-v0-9)) | `requests.rs:325-327`, `responses.rs:617-618` | No hard failure, but an internal inconsistency across APIs. |

---

## Summary - Metadata (key 3, v0-9)

| ID | Sev | Problem | file:line | Impact |
|----|-----|---------|-----------|--------|
| FG-MD-01 | critical | Same as [C1](#c1---metadata-response-wire-format-is-malformed-on-the-bridge-path) | - | - |
| FG-MD-02 | major | Same as [C7](#c7---listing-all-topics-always-returns-an-empty-list) | - | - |
| FG-MD-03 | major | Same as [C6](#c6---metadata-never-auto-creates-unknown-topics) | - | - |
| FG-MD-04 | major | [L1](IGGY_LIMITATIONS.md#l1--metadata-encoder-deduplication) (encoder dedup) is also a correctness divergence, not just a maintenance issue: the stub path (`KAFKA_IGGY_BRIDGE=0`) returns parseable-but-always-"not found"; the bridge path returns malformed ([C1](#c1---metadata-response-wire-format-is-malformed-on-the-bridge-path)) for the same request/version once a topic has partitions | `api.rs:283-345` vs `responses.rs:637-734` | Toggling bridge mode changes "always wrong" to "unparseable" - there is no safe mode for real topics. |
| FG-MD-05 | minor | `leader=1` (matches the broker's `node_id=1`), but `replicas`/`isr` arrays are the literal value `0` - the leader is not a member of its own replica/ISR set (broker id 0 does not exist). Latent until [C1](#c1---metadata-response-wire-format-is-malformed-on-the-bridge-path) is fixed | `responses.rs:668-674,713-720`, see also `BRIDGE_MAPPING.md` "replica=0,isr=0" | Tooling that validates the "leader in ISR" invariant flags the partition as inconsistent. |
| FG-MD-06 | minor | `leader_epoch` (v7+) is hardcoded to `0`, vs ListOffsets' `-1` for the same partition - `0` says "real tracked epoch 0", `-1` says "not tracked", which is inconsistent | `responses.rs:672/717` vs ListOffsets `responses.rs:617-618` | Unlikely to cause a hard failure, but epoch-aware client logic gets contradictory signals across APIs. |
| FG-MD-07 | minor | `cluster_id` (v2+) is always `null` | `api.rs:297,321`, `responses.rs:658,698` | `AdminClient.describeCluster().clusterId()` / `KafkaStreams.clusterId()` return null; at least stable (no false cross-cluster mismatch). |

---

## Summary - CreateTopics (key 19, v2-5)

| ID | Sev | Problem | file:line | Impact |
|----|-----|---------|-----------|--------|
| FG-CT-01 | critical | Same as [C5](#c5---createtopics-on-an-existing-topic-returns-error_none-instead-of-topic_already_exists) | - | - |
| FG-CT-02 | major | `validate_only=true` returns success **before** the replication-factor check runs - a dry run says OK, but the real run then fails with `INVALID_REPLICATION_FACTOR` | `handler.rs:328-330,333` | `validateOnly(true)` gives a false positive, defeating its purpose. |
| FG-CT-03 | major | `replicaAssignment` (manual partition-to-broker layout, v0+) is fully decoded and discarded, with no mutual-exclusivity check against `num_partitions`/`replication_factor` | `requests.rs:399-418` | A client expecting a specific N-partition/broker layout silently gets `DEFAULT_TOPIC_PARTITIONS` (1) instead, with zero error. |
| FG-CT-04 | major | `configs` (`cleanup.policy`/`retention.ms`/`min.insync.replicas`, etc., v0+) is fully decoded and discarded - Iggy has no config-KV mechanism at all; the topic is hardcoded to `IggyExpiry::NeverExpire`/`MaxTopicSize::ServerDefault` | `requests.rs:420-435`, `iggy_bridge.rs:118-119` | `cleanup.policy=compact` / `retention.ms=X` are silently ignored and `ERROR_NONE` is returned - a durability/compaction expectation mismatch with no signal to the client. |
| FG-CT-05 | major | No Kafka topic-name validation (`[a-zA-Z0-9._-]+`, max 249 chars). Iggy's only check is length <= 255 with no charset rule. Names that are valid in Iggy but invalid in Kafka (spaces, `/`, unicode, 250-255 chars) are silently accepted; the empty-name edge case returns `INVALID_REQUEST` (42) instead of `INVALID_TOPIC_EXCEPTION` (17) | `requests.rs` decode, `core/common/src/types/identifier/mod.rs:172-184`, `handler.rs:367-369` | Topics can be created here that real Kafka tooling could never create or manage; wrong error code for the empty-name case. |
| FG-CT-06 | major | The v5 response echoes the **requested** `num_partitions`/`replication_factor` (e.g. `-1`/`-1`), not the actual resolved/persisted values | `handler.rs:361`, `responses.rs:355-356` vs `346-350` | `CreateTopicsResult.numPartitions()` (the Java v5+ API) returns the `-1` sentinel as the "actual" count, which is nonsensical downstream. |
| FG-CT-07 | major | In a multi-topic batch, a replication-factor violation on **any** topic produces a single global placeholder error response (`name=""`), even if earlier topics in the loop were already created in Iggy | `handler.rs:332-338`, `responses.rs:297-306` | An already-created topic is misreported as failed; client and broker state diverge. Overlaps [L2](IGGY_LIMITATIONS.md#l2--per-topic-createtopics-errors-on-partial-failure) item 4, replication-factor-specific case. |
| FG-CT-08 | minor | The v5 response `configs[]` (KIP-525 effective configs) is always an empty array | `responses.rs:354-358` | Incomplete versus spec, but not misleading (empty, not wrong). |
| FG-CT-09 | minor | `num_partitions=-1` resolves to 1, not the typical real-cluster default of 3 | `mapping.rs:32`, `handler.rs:346-350` | Surprises clients expecting a cluster-configured default (though it matches vanilla Kafka's out-of-the-box default of 1). |
| FG-CT-10 | minor | `error_message` is always `null`, even on error | `responses.rs:346-352` | `kafka-topics.sh` / AdminClient show no description, only the numeric code. |

---

## Summary - ApiVersions (key 18, v0-3)

| ID | Sev | Problem | file:line | Impact |
|----|-----|---------|-----------|--------|
| FG-AV-01 | minor | v3 omits all KIP-584/919 tagged fields (`SupportedFeatures`/`FinalizedFeatures`/`FinalizedFeaturesEpoch`/`ZkMigrationReady`) | `api.rs:234-267` | Wire-valid (empty tagged fields means "absent" is legal); feature-flag-gated client paths simply never see these advertised. |
| FG-AV-02 | minor | There is no `decode_api_versions_request`; v3 `client_software_name`/`client_software_version` (KIP-511) are never parsed | `api.rs:142-149`, no decoder in `requests.rs` | No client-identity telemetry. Harmless (the unused body cannot cause a decode bug), but an untested/uncovered path. |

---

## Detailed findings - cross-cutting criticals

### C1 - Metadata response wire format is malformed on the bridge path

**What exists today.** `encode_metadata_response_from_topics` (`responses.rs:636-734`) has two
branches: a v9-flexible branch (650-688) and a legacy v0-v8 branch (689-731). In both branches,
the per-partition success path writes six `i32` fields back to back -
`partition_index, leader_id, leader_epoch(v7+), replica_nodes, isr_nodes, offline_replicas(v5+)`
- with **no leading `error_code: i16`** (lines 668-675 and 713-720). The real Kafka
`MetadataResponsePartition` schema starts every partition entry with `error_code: i16`. The v9
branch additionally writes `replica_nodes`/`isr_nodes`/`offline_replicas` as `write_i32(0)`,
which is the legacy 4-byte array-length encoding; v9 is flexible and must encode these as
`COMPACT_ARRAY` (a varint length+1 prefix).

**Why this is a problem.** Every partition entry in the response is missing 2 bytes
(`error_code`) at the start, and in v9 the array encodings use the wrong width. From the first
partition onward, the rest of the response is byte-misaligned. A conformant client decoder will
either throw a deserialization error or read garbage into subsequent fields.

**Client-visible impact.** This is the success path: any Metadata response for a topic that
exists and has at least one partition. It fires on the normal "produce/fetch to an existing
topic" flow (clients call Metadata first to discover partition leaders). Real clients
(`kafka-python`, `librdkafka`, the Java client) will fail to parse this response or disconnect.

**Suggested fix.** This is the correctness half of [L1](IGGY_LIMITATIONS.md#l1--metadata-encoder-deduplication)
(metadata encoder dedup). When building the shared encoder:

1. Write `error_code: i16` as the first field of every partition entry, in both the legacy and
   flexible branches.
2. In the v9-flexible branch, encode `replica_nodes`, `isr_nodes`, and `offline_replicas` as
   `COMPACT_ARRAY` (varint `len + 1`, then `len` x `i32`), not `write_i32(0)`.
3. Add a round-trip test that decodes the encoder's own output with a real schema (or a
   `kafka-protocol`-crate decoder) for both a legacy and a flexible version, for a topic with
   >= 1 partition.

---

### C2 - Fetch on out-of-range partition can panic the server

**What exists today.** `kafka_partition_index` (`mapping.rs:69-71`) only rejects negative
partition indices via `u32::try_from(kafka_partition).ok()`. It does not check the index
against the topic's actual partition count. The Fetch handler (`handler.rs:176-195`) passes
whatever non-negative index it gets straight to `bridge.fetch_partition`. In Iggy core,
`core/server/src/streaming/partitions/ops.rs:71-73` looks up the partition via
`local_partitions.get(namespace).expect(...)`, which panics if the partition does not exist.

**Why this is a problem.** A Fetch request naming a partition index that is out of range for
the topic (e.g. partition 5 on a 1-partition topic) reaches the `.expect()` and panics the
shard/server thread.

**Client-visible impact.** Any client (malicious or simply stale - e.g. after a topic was
recreated with fewer partitions) sending an ordinary Fetch request for an out-of-range
partition can crash the server. This is a DoS via a normal, protocol-legal request with no
gateway-side guard at all.

**Suggested fix.**

1. Gateway-side mitigation (does not require an Iggy core change): before calling
   `bridge.fetch_partition`, validate `part.partition < partitions_count` using
   `bridge.topic_metadata(&topic.topic)` (the same call already used by the Metadata handler).
   If out of range, return `ERROR_UNKNOWN_TOPIC_OR_PARTITION` for that partition instead of
   calling into the bridge.
2. Longer-term, Iggy core's `ops.rs:71-73` should return a `Result`/`IggyError` instead of
   `.expect()`, so that out-of-range access can never panic regardless of caller. This is a
   core change outside gateway scope, but should be filed against `core/server`.

---

### C3 - One Iggy message = one Kafka RecordBatch breaks per-record offset semantics

**What exists today.** The bridge stores each Kafka `RecordBatch` (which can contain N records)
as a single opaque Iggy message (`handler.rs:105-161`). Iggy's offset accounting and
`high_watermark` (`iggy_bridge.rs`, `high_watermark_from_stats`) are therefore tracked in units
of "Iggy messages" = "Kafka batches", not "Kafka records".

**Why this is a problem.** Kafka clients - and the wire protocol itself - assume offsets and the
high watermark are counted in records. A producer using batching (the Java client's default
under any load) sends batches with more than one record each. The high watermark and any
offset returned by Fetch/ListOffsets/Produce reflect the batch count, not the record count.

**Client-visible impact.** For any producer batching N records per `RecordBatch`,
`high_watermark` undercounts the true record-offset position by roughly a factor of N.
Consumer lag metrics are wildly wrong, and `seek(TopicPartition, recordOffset)` to a specific
record offset is impossible - the offset the client computes from its own producer's
`RecordMetadata` does not correspond to the gateway's notion of offset. This amplifies
[L3](IGGY_LIMITATIONS.md#l3--recordbatch-offset-rewriting) (RecordBatch offset rewriting):
fixing L3's offset rewrite alone is not sufficient while the underlying storage unit is a whole
batch rather than a record.

**Suggested fix.** This is architectural and should be designed together with
[L3](IGGY_LIMITATIONS.md#l3--recordbatch-offset-rewriting) and
[L5](IGGY_LIMITATIONS.md#l5--future-batched-produce-and-base_offset). Two directions worth
evaluating:

- **Split on produce**: parse the incoming `RecordBatch` and store one Iggy message per Kafka
  record, with a 1:1 offset mapping. Requires a `RecordBatch` parser (handles compression
  codecs) and a corresponding re-batching step on Fetch to reconstruct `RecordBatch` framing
  for the client.
- **Per-batch record-count metadata**: keep one Iggy message per batch, but store the record
  count alongside it (e.g. in an `IggyMessage` header), and have the bridge compute
  record-accurate `high_watermark`/offsets by summing per-batch record counts rather than
  message counts. Cheaper than splitting, but still requires offset rewriting inside the batch
  on fetch (L3) for clients that read batch-internal offsets.

---

### C4 - Produce acks=0 still gets a response frame

**What exists today.** `decode_produce_request` parses `acks` into the request struct
(`requests.rs:62`), but nothing in `handler.rs` or `iggy_bridge.rs` ever reads it. The Produce
handler (`handler.rs:105-161`) always calls `encode_produce_response_from_topic_outcomes` (or
`encode_produce_error_response`) and returns a response `Bytes`, which `server.rs` then writes
to the socket.

**Why this is a problem.** Per the Kafka protocol, when `acks=0` the broker sends **no
response** for that request at all - not an empty response, literally zero bytes for that
correlation ID. The client does not expect a frame and does not consume one from the socket
buffer.

**Client-visible impact.** With `acks=0`, this gateway writes an extra response frame the
client is not expecting. The client's correlation-ID-based response demultiplexer reads this
frame as the response to the *next* request it sent, and every subsequent response on the
connection is misread from that point on. This silently corrupts the connection for any
producer configured with `acks=0`.

**Suggested fix.** In `handle_produce` (`handler.rs:105-161`), check `req.acks == 0` after a
successful decode. If so, perform the produce calls as normal (so messages are still written),
but return an empty `Bytes` (zero-length) from `handle_with_bridge`, and ensure `server.rs`'s
response-writing path treats a zero-length response for this API/version as "write nothing"
rather than "write a zero-length frame header". Verify the latter does not already write a
4-byte length prefix unconditionally - if it does, the dispatch loop needs a explicit
"no response" signal (e.g. `Option<Bytes>`) rather than relying on an empty `Bytes`.

---

### C5 - CreateTopics on an existing topic returns ERROR_NONE instead of TOPIC_ALREADY_EXISTS

**What exists today.** `ensure_stream_and_topic` (`iggy_bridge.rs:101-126`) treats
`StreamNameAlreadyExists` and `TopicNameAlreadyExists` as success (idempotent get-or-create).
`handle_create_topics` (`handler.rs:320-362`) calls this same method for every topic in the
request and only inspects the `Err` case for non-"already exists" errors
(`handler.rs:352-358`); an "already exists" outcome falls through to
`encode_create_topics_response` with `ERROR_NONE`.

**Why this is a problem.** Kafka's `CreateTopics` contract is that creating a topic that
already exists returns `TOPIC_ALREADY_EXISTS` (36) for that topic, **not** `ERROR_NONE`. This
is the basis of the common `TopicExistsException`-catch idempotency pattern used by Kafka
Streams application startup, Connect, and provisioning scripts ("try to create; if it already
exists, that's fine, but I need to know it already existed").

**Client-visible impact.** A client calling `CreateTopics` for a topic that already exists is
told `ERROR_NONE` ("created"), when in fact nothing changed. Code paths that branch on
`TopicExistsException` to mean "already provisioned, skip further setup" never take that
branch here - they proceed as if a fresh topic was just created.

**Suggested fix.** The idempotent `ensure_stream_and_topic` behavior is correct and needed for
[C6](#c6---metadata-never-auto-creates-unknown-topics) (Produce-driven auto-create, where
"already exists" really should be silent success). For the `CreateTopics` API specifically,
the handler needs to distinguish "I asked to create this and it already existed" from "I asked
to create this and it does not exist yet, so I created it":

1. Add a bridge method (or a return-value variant on `ensure_stream_and_topic`) that reports
   whether the topic was newly created or already existed, e.g.
   `enum EnsureOutcome { Created, AlreadyExists }`.
2. In `handle_create_topics`, map `AlreadyExists` to `ERROR_TOPIC_ALREADY_EXISTS` (36) for that
   topic's response entry, while [C6](#c6---metadata-never-auto-creates-unknown-topics)'s
   auto-create path continues to treat `AlreadyExists` as silent success.
3. This combines naturally with [L2](IGGY_LIMITATIONS.md#l2--per-topic-createtopics-errors-on-partial-failure)'s
   per-topic outcome struct.

---

### C6 - Metadata never auto-creates unknown topics

**What exists today.** `decode_metadata_topic_filter` parses `allow_auto_topic_creation`
(`requests.rs:486-488`), but `handle_metadata` (`handler.rs:301-315`) never reads it. For a
named topic that `bridge.topic_metadata` reports as not found (`Ok(None)`), the handler always
calls `metadata_unknown_topic`, which encodes `UNKNOWN_TOPIC_OR_PARTITION`.

**Why this is a problem.** The default Kafka producer workflow is: producer calls `Metadata`
for the target topic; if the topic does not exist and `allow_auto_topic_creation=true` (the
client default), the broker creates it with the cluster's default partition count and returns
its metadata in the same response. The producer then proceeds to `Produce` against that topic.

**Client-visible impact.** Through this gateway, a producer targeting a topic that has not been
explicitly created via `CreateTopics` gets `UNKNOWN_TOPIC_OR_PARTITION` from `Metadata` and
never gets to `Produce` at all. The single most common Kafka producer onboarding flow ("just
start producing, the topic will be created for you") does not work.

**Suggested fix.**

1. In `handle_metadata`, when `filter` names a topic and `bridge.topic_metadata` returns
   `Ok(None)` **and** `allow_auto_topic_creation` is true (the default when the field is
   absent on pre-v4 requests, per Kafka semantics), call
   `bridge.ensure_stream_and_topic(&name, DEFAULT_TOPIC_PARTITIONS)` and, on success, report the
   newly created topic's metadata instead of `metadata_unknown_topic`.
2. This call must use the idempotent "already exists is fine" path
   ([C5](#c5---createtopics-on-an-existing-topic-returns-error_none-instead-of-topic_already_exists)'s
   `AlreadyExists` variant should be treated as success here, unlike in `CreateTopics`).
3. When `allow_auto_topic_creation=false`, keep current behavior
   (`UNKNOWN_TOPIC_OR_PARTITION`).

---

### C7 - Listing all topics always returns an empty list

**What exists today.** `handle_metadata` (`handler.rs:291-294`) special-cases
`MetadataTopicFilter::All` (an empty/null `topics` array in the request, meaning "list every
topic") by immediately returning `encode_metadata_response_from_topics(version, broker, &[])` -
an empty topic list, with no call into the bridge.

**Why this is a problem.** `MetadataTopicFilter::All` is the wire representation of
`AdminClient.listTopics()` and similar "what topics exist" calls. Returning an empty list is
indistinguishable from "the cluster has no topics", regardless of actual state.

**Client-visible impact.** `AdminClient.listTopics()`, `kafka-topics.sh --list`, and any
discovery tooling that relies on a `Metadata` request with no named topics will report an empty
cluster even when topics exist and are fully functional via Produce/Fetch.

**Suggested fix.**

1. Add a bridge method that enumerates all Iggy streams/topics that map to Kafka topics (the
   Iggy SDK's stream/topic listing calls), returning a `Vec<MetadataTopicOutcome>`.
2. In `handle_metadata`, when `filter` is `MetadataTopicFilter::All`, call this method and pass
   its result to `encode_metadata_response_from_topics` instead of short-circuiting to `&[]`.
3. Be mindful of the 1:1 stream/topic naming convention (`mapping.rs:37-48`): only
   stream/topic pairs that follow the Kafka-topic naming convention should be reported (avoid
   leaking internal/non-Kafka streams if any exist).

---

## Confirmed NOT a gap (reference - do not re-investigate)

These were checked during the gap analysis and found to be correct as implemented.

**Produce:** empty-topics multi-topic/multi-partition looping is correct; `log_append_time=-1`
is correct; `throttle_time_ms=0` is correct; flexible v9 tagged-fields placement is correct;
key/value/headers are preserved via opaque passthrough.

**Fetch:** `replica_id` decode is correct; `rack_id`/`preferred_read_replica=-1` are correct (no
follower replicas); `diverging_epoch`/`current_leader`/`snapshot_id` (v12) are correctly
empty-tagged; `current_leader_epoch`/`last_fetched_epoch` discard is correct for a single
broker; `aborted_transactions` empty-array (not null) encoding is correct in both the legacy and
flexible branches.

**ListOffsets:** v0 `max_num_offsets` scoping is correct; v6 flexible tagged-fields/ordering is
correct; `AUTHORIZED_OPS_UNKNOWN = i32::MIN` is the correct KIP-430 sentinel.

**Metadata:** `controller_id=1` is consistent with the broker's `node_id=1`; `offline_replicas=[]`
is correct (a single always-online broker); `is_internal=false` is correct; the v10+ topic_id
decode branch is correctly dead (max supported version is 9).

**CreateTopics/ApiVersions:** the KAFKA-18659 librdkafka Produce-min=0 workaround is correctly
implemented (`api.rs:221-232`, tested); the ApiVersions response header is always v0,
correctly guarded (`header.rs:131-140`); ApiVersions(18) bridge-fallback dispatch is correct;
CreateTopics v2-5 decode/encode/dispatch coverage has no drift versus `SUPPORTED_RANGES`.

---

## Suggested fix order

1. **C1** - Metadata wire format. Likely breaks nearly every real client today; combine with
   the [L1](IGGY_LIMITATIONS.md#l1--metadata-encoder-deduplication) encoder-dedup refactor.
2. **C2** - Fetch out-of-range partition panic/DoS. Gateway-side bound check is low effort.
3. **C4** - Produce `acks=0` response desync. Self-contained handler change.
4. **C5** - CreateTopics `TOPIC_ALREADY_EXISTS`. Pairs naturally with
   [L2](IGGY_LIMITATIONS.md#l2--per-topic-createtopics-errors-on-partial-failure).
5. **C6 / C7** - Metadata auto-create and list-all-topics. Both unblock standard client
   onboarding flows.
6. **C3** - Per-record offset semantics. Largest effort; architectural, overlaps
   [L3](IGGY_LIMITATIONS.md#l3--recordbatch-offset-rewriting) and
   [L5](IGGY_LIMITATIONS.md#l5--future-batched-produce-and-base_offset).
7. Remaining per-API major/minor items, roughly in the order listed in each summary table.

---

## Updating this document

New gaps discovered during future work should be appended to the relevant per-API summary
table with the next available `FG-<API>-NN` ID, and a detailed section added if the item is
severe enough to warrant one (criticals/majors generally should; minors can stay
table-only). Mirror new items into [TODO_TASKS.md](TODO_TASKS.md).
