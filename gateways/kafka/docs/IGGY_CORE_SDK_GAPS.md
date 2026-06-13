# Iggy core/SDK gaps for full Kafka compatibility

[FUNCTIONAL_GAPS.md](FUNCTIONAL_GAPS.md) and [IGGY_LIMITATIONS.md](IGGY_LIMITATIONS.md) track
gaps fixable inside the Kafka gateway. This document tracks the subset of gaps that **cannot**
be fixed in the gateway alone - they require a change to `core/common` (the SDK trait/types
layer) and/or `core/server` (the broker implementation).

Each item below was checked against current `core/` source to confirm the limitation is real
(not just gateway-side under-use of an existing capability). Items that initially looked like
SDK gaps but turned out to already be supported are listed separately in
[Not actually an SDK gap](#not-actually-an-sdk-gap) so they are not duplicated here.

Related docs:

- [FUNCTIONAL_GAPS.md](FUNCTIONAL_GAPS.md) - gateway-side functional gap analysis (C1-C7,
  FG-*)
- [IGGY_LIMITATIONS.md](IGGY_LIMITATIONS.md) - explicitly deferred gateway design items (L1-L5)
- [TODO_TASKS.md](TODO_TASKS.md) - actionable checklist, includes these items under "Iggy
  core/SDK changes"

---

## Summary

| ID | Item | Effort | Fixes |
|----|------|--------|-------|
| SDK-01 | `send_messages` must return assigned offsets | moderate | L4, L5, prereq for SDK-02/L3 |
| SDK-02 | Record-granular offset accounting (1 append = N Kafka records) | large | C3 |
| SDK-03 | `Partition` needs `log_start_offset` | moderate | FG-LO-01, FG-FET-10, FG-PRD-09 |
| SDK-04 | Out-of-range partition access must not panic | small | C2 |
| SDK-05 | `poll_messages` must signal "offset beyond log end" | moderate | FG-FET-06 |
| SDK-06 | Long-poll support (`max_wait_ms`/`min_bytes`) | moderate | FG-FET-03 |
| SDK-07 | Idempotent producer (KIP-98: producer_id/epoch/sequence) | large | FG-PRD-04 |
| SDK-08 | Transactional producer/consumer (full EOS) | large | transactional_id support, isolation_level=READ_COMMITTED |
| SDK-09 | Compaction (`cleanup.policy=compact`) | large | FG-PRD-13 |
| SDK-10 | Generic per-topic config store (KIP-525 DescribeConfigs/AlterConfigs) | large | FG-CT-04, FG-CT-08 |
| SDK-11 | Multi-broker cluster model (replicas, rack-awareness, KIP-392) | very large | Phase 4 multi-broker metadata |

---

## SDK-01 - `send_messages` must return assigned offsets

**What exists today.** `MessageClient::send_messages` (`core/common/src/traits/message_client.rs:44-50`):

```rust
async fn send_messages(
    &self,
    stream_id: &Identifier,
    topic_id: &Identifier,
    partitioning: &Partitioning,
    messages: &mut [IggyMessage],
) -> Result<(), IggyError>;
```

No offset information is returned. The gateway infers the produce-ack offset by snapshotting
`get_topic` partition `current_offset` before and after the call (`iggy_bridge.rs`).

**What's needed.** Return `{ partition_id: u32, base_offset: u64, last_offset: u64 }` (or
per-message offsets) on success.

**Fixes.** [L4](IGGY_LIMITATIONS.md#l4--produce-ack-toctou-offset-inference-races) (removes the
before/after snapshot race), [L5](IGGY_LIMITATIONS.md#l5--future-batched-produce-and-base_offset)
(`base_offset` for multi-record batches becomes well-defined). Prerequisite for SDK-02 and
[L3](IGGY_LIMITATIONS.md#l3--recordbatch-offset-rewriting).

---

## SDK-02 - Record-granular offset accounting

**What exists today.** One `send_messages` call with one `IggyMessage` advances the partition
offset by 1, regardless of how many Kafka records that message's payload represents. The
gateway stores each Kafka `RecordBatch` (which can contain N records) as a single opaque
`IggyMessage` (`handler.rs:105-161`). `IggyMessageHeader` (`core/common/src/types/message/message_header.rs:37-46`)
has a `reserved: u64` field (offsets 56-64) that is currently unused.

**What's needed.** A way for the producer of an `IggyMessage` to declare "this payload
represents N logical records", and for core to:

1. Advance the partition offset and high watermark by N, not 1.
2. Return (via SDK-01) the `base_offset` for the batch, i.e. the offset assigned to the first
   of the N records.

The unused `reserved` field is a candidate location for `record_count: u32` without breaking
the on-disk message header layout (`IGGY_MESSAGE_HEADER_SIZE = 8+16+8+8+8+4+4+8`,
`message_header.rs:24`).

**Fixes.** [C3](FUNCTIONAL_GAPS.md#c3---one-iggy-message--one-kafka-recordbatch-breaks-per-record-offset-semantics) -
`high_watermark` and offsets currently undercount by ~Nx for any batching producer. This is
the largest single architectural item; [L3](IGGY_LIMITATIONS.md#l3--recordbatch-offset-rewriting)
(RecordBatch internal offset rewrite) still needs gateway-side work on top of this, but without
SDK-02 the rewrite has nothing correct to rewrite *to*.

---

## SDK-03 - `Partition` needs `log_start_offset`

**What exists today.** `Partition` (`core/common/src/types/partition/mod.rs:32-45`):

```rust
pub struct Partition {
    pub id: u32,
    pub created_at: IggyTimestamp,
    pub segments_count: u32,
    pub current_offset: u64,
    pub size: IggyByteSize,
    pub messages_count: u64,
}
```

No field represents the lowest retained offset. Segment cleanup
(`core/server/src/shard/system/segments.rs::clean_topic_messages`) deletes old segments but
nothing tracks/exposes the resulting floor.

**What's needed.** Add `log_start_offset: u64` to `Partition`, updated whenever
`clean_topic_messages` removes the lowest segment(s).

**Fixes.**
[FG-LO-01](FUNCTIONAL_GAPS.md#summary---listoffsets-key-2-v1-6) (ListOffsets(-2) hardcoded to
0), [FG-FET-10](FUNCTIONAL_GAPS.md#summary---fetch-key-1-v4-12) (Fetch `log_start_offset`
hardcoded to "first offset returned by this poll"),
[FG-PRD-09](FUNCTIONAL_GAPS.md#summary---produce-key-0-v3-9) (Produce response
`log_start_offset` hardcoded to 0). All three are latent/correct only because Iggy retention
isn't exercised yet by the gateway - this becomes load-bearing the moment retention is
configured.

---

## SDK-04 - Out-of-range partition access must not panic

**What exists today.** `core/server/src/streaming/partitions/ops.rs:71-73`:

```rust
let partition = local_partitions
    .get(&namespace)
    .expect("partition must exist for this namespace");
```

`kafka_partition_index` (`mapping.rs:69-71`) only rejects negative indices; it never checks
against the topic's actual partition count, so an out-of-range index reaches this `.expect()`.

**What's needed.** Return `Err(IggyError::PartitionNotFound(...))` instead of panicking when
`namespace` has no entry.

**Fixes.** [C2](FUNCTIONAL_GAPS.md#c2---fetch-on-out-of-range-partition-can-panic-the-server) -
currently a DoS via an ordinary Fetch request for a stale/non-existent partition. Note: the
gateway can (and should) add its own bound check using `topic_metadata` as a first line of
defense, but that does not make this core fix optional - any other caller of `ops.rs` hitting
an out-of-range partition has the same panic.

---

## SDK-05 - `poll_messages` must signal "offset beyond log end"

**What exists today.** `core/server/src/streaming/partitions/ops.rs:78-84` returns an empty
`PolledMessages` both when the consumer is caught up at the high watermark *and* when
`fetch_offset > high_watermark` (an invalid/stale offset). There is no `OffsetOutOfRange`
variant anywhere in `core`.

**What's needed.** `poll_messages` (or the `ops.rs` helper underneath it) must distinguish
these two cases - either via a distinct `Err(IggyError::OffsetOutOfRange)` or a flag on
`PolledMessages` the gateway can map to Kafka's `OFFSET_OUT_OF_RANGE` (1).

**Fixes.** [FG-FET-06](FUNCTIONAL_GAPS.md#summary---fetch-key-1-v4-12) - without this, a
consumer with a stale offset past the high watermark never receives the error that normally
triggers `auto.offset.reset`, and polls forever.

---

## SDK-06 - Long-poll support (`max_wait_ms`/`min_bytes`)

**What exists today.** `poll_messages` is immediate-return-only
(`core/server/src/streaming/partitions/ops.rs:78-84`). The gateway decodes Fetch's
`max_wait_ms`/`min_bytes` (`requests.rs:123-124,148-149`) but never reads them.

**What's needed.** Server-side support for "wait up to `max_wait_ms` for at least `min_bytes`
of new data, or return immediately if data is already available" - i.e. a hold-and-wake
mechanism on the partition's append path.

**Fixes.** [FG-FET-03](FUNCTIONAL_GAPS.md#summary---fetch-key-1-v4-12) - without this, an idle
consumer with `fetch.max.wait.ms=500` gets an instant empty response and busy-polls at 5-10x+
the expected request rate.

---

## SDK-07 - Idempotent producer (KIP-98)

**What exists today.** No `producer_id`, `producer_epoch`, or per-partition sequence number
concept anywhere in `core/common` or `core/server`. `transactional_id` is rejected outright in
the gateway (`handler.rs:117-122`), and `InitProducerId` (API key 22) is not in
`SUPPORTED_RANGES`.

**What's needed.** Producer registration (an `InitProducerId`-equivalent allocating
`producer_id`/`epoch`), and per-partition sequence-number tracking with a dedup window so a
retried append with a previously-seen `(producer_id, epoch, sequence)` is recognized as a
duplicate and not re-applied.

**Fixes.** [FG-PRD-04](FUNCTIONAL_GAPS.md#summary---produce-key-0-v3-9) -
`enable.idempotence=true` is the Java client default; without this, combined with
[C4](FUNCTIONAL_GAPS.md#c4---produce-acks0-still-gets-a-response-frame)/
[L4](IGGY_LIMITATIONS.md#l4--produce-ack-toctou-offset-inference-races) retries, real
duplicates can land.

---

## SDK-08 - Transactional producer/consumer (full EOS)

**What exists today.** Nothing. `transactional_id` is rejected
(`handler.rs:117-122`), `isolation_level` is decoded and unused everywhere it appears
(Fetch `requests.rs`, ListOffsets `requests.rs:297`), and there is no concept of control
batches/transaction markers.

**What's needed.** `InitProducerId`, `AddPartitionsToTxn`, `AddOffsetsToTxn`, `EndTxn` API
support; transaction-coordinator state; control-batch markers written to the log on
commit/abort; `isolation_level=READ_COMMITTED` filtering on Fetch (skip records from
uncommitted/aborted transactions).

**Fixes.** Enables transactional producers (Kafka Streams exactly-once, transactional
sinks/sources) end to end. This is the largest single feature gap for clients that require
EOS; everything else in this document is usable without it.

---

## SDK-09 - Compaction (`cleanup.policy=compact`)

**What exists today.** Iggy has no compaction concept - segments are append-only and only
removed wholesale by retention (`clean_topic_messages`). Tombstone records (null-value Kafka
records) pass through and are stored like any other message
([FG-PRD-13](FUNCTIONAL_GAPS.md#summary---produce-key-0-v3-9)).

**What's needed.** A background process that rewrites segments to retain only the latest
record per key (plus tombstone-retention-window handling), analogous to Kafka's log cleaner.

**Fixes.** [FG-PRD-13](FUNCTIONAL_GAPS.md#summary---produce-key-0-v3-9) and the `configs`
half of [FG-CT-04](FUNCTIONAL_GAPS.md#summary---createtopics-key-19-v2-5) -
`cleanup.policy=compact` topics (commonly used for KTable-backed changelog topics in Kafka
Streams) never actually compact.

---

## SDK-10 - Generic per-topic config store (KIP-525)

**What exists today.** Topic creation hardcodes `IggyExpiry::NeverExpire` and
`MaxTopicSize::ServerDefault` (`iggy_bridge.rs:118-119`). `CreateTopics`'
`configs` field (`cleanup.policy`, `retention.ms`, `retention.bytes`,
`min.insync.replicas`, `segment.bytes`, etc.) is fully decoded and discarded
(`requests.rs:420-435`).

**What's needed.** A persisted key-value config map per topic in `core`, with:

1. `create_topic`/`update_topic` accepting arbitrary config entries.
2. A read-back API so the gateway can echo "effective configs" on `CreateTopics` v5
   ([FG-CT-08](FUNCTIONAL_GAPS.md#summary---createtopics-key-19-v2-5)) and eventually implement
   `DescribeConfigs`/`AlterConfigs` (currently out of `SUPPORTED_RANGES` entirely, but required
   for "100%").

**Fixes.** [FG-CT-04](FUNCTIONAL_GAPS.md#summary---createtopics-key-19-v2-5),
[FG-CT-08](FUNCTIONAL_GAPS.md#summary---createtopics-key-19-v2-5). `retention.ms`/`retention.bytes`
also feed SDK-03's `log_start_offset` semantics once retention is config-driven rather than
server-default.

---

## SDK-11 - Multi-broker cluster model (KIP-392 etc.)

**What exists today.** Single hardcoded broker (`node_id=1`, `controller_id=1`), no replicas,
no rack metadata. `replicas`/`isr` arrays in Metadata are literal `0`
([FG-MD-05](FUNCTIONAL_GAPS.md#summary---metadata-key-3-v0-9)).

**What's needed.** Iggy clustering/replication: multiple brokers, partition replica
assignment, leader election, ISR tracking, rack-aware replica placement for follower-fetch
(KIP-392).

**Fixes.** Phase 4 multi-broker metadata. Clients that specifically exercise
multi-broker-aware paths (replica fetching, rack-aware consumers, `kafka-reassign-partitions`)
cannot be supported until this exists. Largest item in this document by scope; likely a
multi-quarter effort independent of the Kafka gateway.

---

## Not actually an SDK gap

Checked while compiling this list - these initially looked like SDK limitations but the
underlying capability already exists. The fix is gateway-only; see
[FUNCTIONAL_GAPS.md](FUNCTIONAL_GAPS.md) for the corresponding item.

- **List all streams/topics (C7).** `StreamClient::get_streams()` (`core/common/src/traits/stream_client.rs:32`)
  and `TopicClient::get_topics()` (`core/common/src/traits/topic_client.rs:39`) already exist.
  `handle_metadata`'s `MetadataTopicFilter::All` branch just needs to call them instead of
  short-circuiting to an empty list.
- **`TOPIC_ALREADY_EXISTS` (C5).** `IggyError::StreamNameAlreadyExists`/`TopicNameAlreadyExists`
  are already distinct error variants returned by `create_stream`/`create_topic`. The gateway
  just maps both to success unconditionally; `CreateTopics` needs to map them to
  `ERROR_TOPIC_ALREADY_EXISTS` (36) instead, while the auto-create path (C6) keeps treating
  them as success.
- **`LogAppendTime` / per-message timestamp (FG-FET-09).** `IggyMessageHeader` already carries
  both `timestamp` (broker-assigned, `message_header.rs:41`) and `origin_timestamp`
  (producer-supplied, `message_header.rs:42`). The gateway's `FetchedMessage`
  (`iggy_bridge.rs:48-52`) just doesn't surface `timestamp` to the Fetch response encoder yet.

---

## Updating this document

New items go in the summary table with the next `SDK-NN` ID, plus a detail section. Mirror
into [TODO_TASKS.md](TODO_TASKS.md) under "Iggy core/SDK changes". If an item turns out to
already be supported, move it to [Not actually an SDK gap](#not-actually-an-sdk-gap) rather
than deleting it - that section exists to stop the same false-positive being re-investigated.
