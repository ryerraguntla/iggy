# Kafka gateway - TODO tasks

Running task tracker for the Kafka gateway (Phase 1B and beyond). This file is appended to as
new follow-ups, bugs, or gaps are discovered - it is not a one-time snapshot.

Each item links to its detail in [FUNCTIONAL_GAPS.md](FUNCTIONAL_GAPS.md),
[IGGY_LIMITATIONS.md](IGGY_LIMITATIONS.md), or [IGGY_CORE_SDK_GAPS.md](IGGY_CORE_SDK_GAPS.md)
where one exists. Check an item off when the fix lands; do not delete completed items, move
them to the "Done" section at the bottom with the PR reference.

---

## Cross-cutting critical (fix first)

- [x] **C1** - Metadata response wire format is malformed on the bridge path (missing
  `error_code:i16` per partition; v9 array encoding wrong). [detail](FUNCTIONAL_GAPS.md#c1---metadata-response-wire-format-is-malformed-on-the-bridge-path)
- [ ] **C2** - Fetch on an out-of-range partition can panic the server (DoS via ordinary
  request). [detail](FUNCTIONAL_GAPS.md#c2---fetch-on-out-of-range-partition-can-panic-the-server)
- [ ] **C3** - One Iggy message = one Kafka RecordBatch breaks per-record offset/high-watermark
  semantics. [detail](FUNCTIONAL_GAPS.md#c3---one-iggy-message--one-kafka-recordbatch-breaks-per-record-offset-semantics)
- [ ] **C4** - Produce `acks=0` still gets a response frame, desyncing the connection.
  [detail](FUNCTIONAL_GAPS.md#c4---produce-acks0-still-gets-a-response-frame)
- [ ] **C5** - CreateTopics on an existing topic returns `ERROR_NONE` instead of
  `TOPIC_ALREADY_EXISTS`. [detail](FUNCTIONAL_GAPS.md#c5---createtopics-on-an-existing-topic-returns-error_none-instead-of-topic_already_exists)
- [ ] **C6** - Metadata never auto-creates unknown topics (`allow_auto_topic_creation`
  ignored). [detail](FUNCTIONAL_GAPS.md#c6---metadata-never-auto-creates-unknown-topics)
- [ ] **C7** - Listing all topics (`Metadata` with empty topic filter) always returns an empty
  list. [detail](FUNCTIONAL_GAPS.md#c7---listing-all-topics-always-returns-an-empty-list)

## IGGY_LIMITATIONS.md deferred items (L1-L5)

- [ ] **L1** - Metadata encoder dedup (`api.rs` <-> `responses.rs`).
  [detail](IGGY_LIMITATIONS.md#l1--metadata-encoder-deduplication)
- [ ] **L2** - Per-topic `CreateTopics` errors on partial failure.
  [detail](IGGY_LIMITATIONS.md#l2--per-topic-createtopics-errors-on-partial-failure)
- [ ] **L3** - `RecordBatch` offset rewriting on produce/fetch.
  [detail](IGGY_LIMITATIONS.md#l3--recordbatch-offset-rewriting)
- [ ] **L4** - Produce ack TOCTOU / offset inference races.
  [detail](IGGY_LIMITATIONS.md#l4--produce-ack-toctou-offset-inference-races)
- [ ] **L5** - Multi-message produce `base_offset` design note.
  [detail](IGGY_LIMITATIONS.md#l5--future-batched-produce-and-base_offset)

---

## Iggy core/SDK changes (required for 100% Kafka compatibility)

These cannot be fixed in the gateway alone - they need a change to `core/common` and/or
`core/server`. Detail in [IGGY_CORE_SDK_GAPS.md](IGGY_CORE_SDK_GAPS.md).

- [ ] SDK-01 (moderate) - `send_messages` must return assigned offsets.
  [detail](IGGY_CORE_SDK_GAPS.md#sdk-01---send_messages-must-return-assigned-offsets)
- [ ] SDK-02 (large) - Record-granular offset accounting (1 append = N Kafka records).
  [detail](IGGY_CORE_SDK_GAPS.md#sdk-02---record-granular-offset-accounting)
- [ ] SDK-03 (moderate) - `Partition` needs `log_start_offset`.
  [detail](IGGY_CORE_SDK_GAPS.md#sdk-03---partition-needs-log_start_offset)
- [ ] SDK-04 (small) - Out-of-range partition access must not panic.
  [detail](IGGY_CORE_SDK_GAPS.md#sdk-04---out-of-range-partition-access-must-not-panic)
- [ ] SDK-05 (moderate) - `poll_messages` must signal "offset beyond log end".
  [detail](IGGY_CORE_SDK_GAPS.md#sdk-05---poll_messages-must-signal-offset-beyond-log-end)
- [ ] SDK-06 (moderate) - Long-poll support (`max_wait_ms`/`min_bytes`).
  [detail](IGGY_CORE_SDK_GAPS.md#sdk-06---long-poll-support-max_wait_msmin_bytes)
- [ ] SDK-07 (large) - Idempotent producer (KIP-98: producer_id/epoch/sequence).
  [detail](IGGY_CORE_SDK_GAPS.md#sdk-07---idempotent-producer-kip-98)
- [ ] SDK-08 (large) - Transactional producer/consumer (full EOS).
  [detail](IGGY_CORE_SDK_GAPS.md#sdk-08---transactional-producerconsumer-full-eos)
- [ ] SDK-09 (large) - Compaction (`cleanup.policy=compact`).
  [detail](IGGY_CORE_SDK_GAPS.md#sdk-09---compaction-cleanuppolicycompact)
- [ ] SDK-10 (large) - Generic per-topic config store (KIP-525 DescribeConfigs/AlterConfigs).
  [detail](IGGY_CORE_SDK_GAPS.md#sdk-10---generic-per-topic-config-store-kip-525)
- [ ] SDK-11 (very large) - Multi-broker cluster model (replicas, rack-awareness, KIP-392).
  [detail](IGGY_CORE_SDK_GAPS.md#sdk-11---multi-broker-cluster-model-kip-392-etc)

---

## Produce (key 0, v3-9)

- [ ] FG-PRD-02 (major) - `transactional_id` rejected with wrong error code; `InitProducerId`
  (22) entirely unsupported. `handler.rs:117-122`
- [ ] FG-PRD-03 (major) - No `MESSAGE_TOO_LARGE` (10); >8 MiB frame drops the connection
  instead of a per-partition error. `server.rs:328-374`, `api.rs:40-54`
- [ ] FG-PRD-04 (major) - No idempotent-producer (KIP-98) dedup.
  `core/common/src/traits/message_client.rs:44-50`
- [ ] FG-PRD-05 (major) - No CRC-32C validation on produce; corruption surfaces at consume
  time instead of produce time.
- [ ] FG-PRD-06 (minor) - Empty `topics[]` request returns a fabricated 1-topic/1-partition
  response. `responses.rs:34-43`
- [ ] FG-PRD-07 (minor) - `records=Some(empty Bytes)` surfaces as `UNKNOWN_SERVER_ERROR`
  instead of `CORRUPT_MESSAGE`. `iggy_bridge.rs:163-167`, `handler.rs:364-386`
- [ ] FG-PRD-08 (minor) - `records=None` returns `INVALID_REQUEST` instead of
  `CORRUPT_MESSAGE`. `handler.rs:129-134`
- [ ] FG-PRD-09 (minor) - `log_start_offset` (v5+) hardcoded to 0; latent until retention
  exists. `responses.rs:473-475`
- [ ] FG-PRD-10 (minor) - v8+ `record_errors`/`error_message` structurally can never be
  populated (KIP-467). `responses.rs:86-94,476-484`
- [ ] FG-PRD-11 (minor) - Partition index `< -1` silently coerced to partition 0.
  `mapping.rs:55-61,69-71`
- [ ] FG-PRD-12 (minor) - `concat_record_batches` assumes well-formed `batchLength` per
  payload, unverified. `responses.rs:748-761`
- [ ] FG-PRD-13 (minor) - Tombstones never purged (no compaction in Iggy).
- [ ] FG-PRD-14 (minor) - 0-record-count `RecordBatch` consumes 1 offset slot.
  `iggy_message.rs:170-172`

## Fetch (key 1, v4-12)

- [ ] FG-FET-03 (major) - `max_wait_ms`/`min_bytes` ignored; no server-side long-poll, causes
  consumer busy-polling. `requests.rs:123-124,148-149`
- [ ] FG-FET-04 (major) - `partition_max_bytes`/`max_bytes` ignored; hardcoded 500-message
  fetch regardless of requested size (including `max_bytes=0` probes). `mapping.rs:51`,
  `handler.rs:188-195`
- [ ] FG-FET-05 (major) - Negative `fetch_offset` silently clamped to 0 instead of
  `OFFSET_OUT_OF_RANGE`. `handler.rs:187`
- [ ] FG-FET-06 (major) - `fetch_offset > high_watermark` never returns
  `OFFSET_OUT_OF_RANGE`; consumer stuck polling forever.
  `core/server/src/streaming/partitions/ops.rs:78-84`
- [ ] FG-FET-07 (minor) - KIP-227 incremental fetch sessions discarded; always full-fetch
  (`session_id=0`). `requests.rs:159-163,225-248`, `responses.rs:518`
- [ ] FG-FET-08 (minor) - Empty fetch result encodes `records=null` instead of empty array.
  `responses.rs:748-761,560-564`
- [ ] FG-FET-09 (minor) - Iggy per-message timestamp discarded; `LogAppendTime` is a silent
  no-op. `iggy_bridge.rs:48-52`, `requests.rs:369-374`
- [ ] FG-FET-10 (minor) - `log_start_offset` not the true retention floor; latent until
  retention exists. `iggy_bridge.rs:231`
- [ ] FG-FET-11 (minor, latent) - `isolation_level` decoded, unused; future-risk only if Iggy
  adds transactions.

## ListOffsets (key 2, v1-6)

- [ ] FG-LO-01 (major) - `timestamp=-2` (earliest) hardcoded to 0; wrong once retention
  triggers, with no error signal. `iggy_bridge.rs:272`,
  `core/common/src/types/partition/mod.rs:32-44`
- [ ] FG-LO-02 (major) - Arbitrary-timestamp lookup with no match returns `INVALID_REQUEST`
  instead of the `(NONE, -1)` sentinel Kafka clients expect. `iggy_bridge.rs:354-382`,
  `error.rs:31-35`, `handler.rs:367-369`
- [ ] FG-LO-03 (minor) - Unknown-partition/error responses set `offset=0` instead of `-1`.
  `handler.rs:242-249,260-264`
- [ ] FG-LO-04 (minor, latent) - `isolation_level` decoded, unused; future-risk only.
  `requests.rs:297`
- [ ] FG-LO-05 (minor) - `current_leader_epoch` (v4+) discarded; `leader_epoch=-1` inconsistent
  with Metadata's hardcoded `leader_epoch=0`. `requests.rs:325-327`, `responses.rs:617-618`

## Metadata (key 3, v0-9)

- [ ] FG-MD-04 (major) - L1 encoder dedup is also a correctness divergence: stub path vs
  bridge path diverge in failure mode for the same request. `api.rs:283-345`,
  `responses.rs:637-734`
- [ ] FG-MD-05 (minor) - `leader=1` not present in `replicas`/`isr` (`=0`); latent until C1
  fixed. `responses.rs:668-674,713-720`
- [ ] FG-MD-06 (minor) - `leader_epoch` hardcoded to 0, inconsistent with ListOffsets' `-1`
  for the same partition. `responses.rs:672/717` vs `responses.rs:617-618`
- [ ] FG-MD-07 (minor) - `cluster_id` (v2+) always `null`. `api.rs:297,321`,
  `responses.rs:658,698`

## CreateTopics (key 19, v2-5)

- [ ] FG-CT-02 (major) - `validate_only=true` returns success before the replication-factor
  check runs (false positive dry run). `handler.rs:328-330,333`
- [ ] FG-CT-03 (major) - `replicaAssignment` fully decoded and discarded, no
  mutual-exclusivity check. `requests.rs:399-418`
- [ ] FG-CT-04 (major) - `configs` (cleanup.policy/retention.ms/etc.) fully decoded and
  discarded, `ERROR_NONE` returned regardless. `requests.rs:420-435`, `iggy_bridge.rs:118-119`
- [ ] FG-CT-05 (major) - No Kafka topic-name validation; invalid names silently accepted,
  empty-name uses wrong error code. `requests.rs` decode,
  `core/common/src/types/identifier/mod.rs:172-184`, `handler.rs:367-369`
- [ ] FG-CT-06 (major) - v5 response echoes requested `num_partitions`/`replication_factor`
  (`-1`/`-1`) instead of resolved values. `handler.rs:361`, `responses.rs:346-356`
- [ ] FG-CT-07 (major) - Multi-topic batch: one topic's RF violation produces a single global
  placeholder error, misreporting already-created topics. `handler.rs:332-338`,
  `responses.rs:297-306`
- [ ] FG-CT-08 (minor) - v5 response `configs[]` (KIP-525) always empty. `responses.rs:354-358`
- [ ] FG-CT-09 (minor) - `num_partitions=-1` resolves to 1, not 3. `mapping.rs:32`,
  `handler.rs:346-350`
- [ ] FG-CT-10 (minor) - `error_message` always `null` on error. `responses.rs:346-352`

## ApiVersions (key 18, v0-3)

- [ ] FG-AV-01 (minor) - v3 omits KIP-584/919 tagged fields (SupportedFeatures etc.).
  `api.rs:234-267`
- [ ] FG-AV-02 (minor) - No `decode_api_versions_request`; v3 client software name/version
  (KIP-511) never parsed. `api.rs:142-149`

---

## Done

(Move completed items here with PR reference and date.)
