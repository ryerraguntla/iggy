# Iggy limitations & explicit follow-ups (Kafka gateway)

Phase 1B documents known gaps between Kafka client expectations and what the Iggy bridge can deliver today. This file is the **authoritative tracker** for work that is intentionally deferred—not bugs filed by oversight.

Related docs:

- [BRIDGE_MAPPING.md](BRIDGE_MAPPING.md) — current mapping and workarounds in production code
- [IMPLEMENTATION_CHECKLIST.md](IMPLEMENTATION_CHECKLIST.md) — review gate for new bridge PRs

---

## Summary

| ID | Item | Phase 1B status | Blocker |
|----|------|-----------------|---------|
| L1 | Metadata encoder dedup (`api.rs` ↔ `responses.rs`) | **Not done** | Refactor only; no Iggy server change |
| L2 | Per-topic `CreateTopics` errors on partial failure | **Partial** | Handler short-circuits; encoder supports per-topic codes for partition sentinels only |
| L3 | `RecordBatch` offset rewriting on produce | **Not done** | Needs batch parser/rewriter; ideally `send_messages` returns assigned offset |
| L4 | Produce ack TOCTOU / offset inference races | **Acknowledged** | Needs `send_messages` response; see below |
| L5 | Multi-message produce `base_offset` | **Design note** | `last_message_offset` ≠ batch `base_offset` |

---

## L4 — Produce ack TOCTOU (offset inference races)

### Status

**Not fixable in the gateway alone** while `send_messages` returns `Result<(), IggyError>`. Documented and tracked; mitigated only by SDK/server returning assigned partition + offset in the send response.

### Balanced produce (`partition == -1`)

`detect_written_partition` (`iggy_bridge.rs`) snapshots every partition's `current_offset` from `get_topic` **before** `send_messages`, then again **after**. It returns the **first** partition index where `after > before`.

**Failure mode:** If another producer (any TCP client, same topic) appends between this call's before/after snapshots, it may increment a **lower-indexed** partition first. This request then reports **that other writer's** partition and offset in its Produce response — wrong `base_offset` for the Kafka client that just sent.

**Client impact:** Idempotent/sequence producers may mis-assign sequence numbers; retries can deduplicate incorrectly or duplicate if the client treats the wrong offset as its own.

### Explicit partition produce

After `send_messages`, the bridge reads `current_offset` via `get_topic` (same RTT class as before, no journal read). If another writer appends to the **same partition** between send and the post-send read, this request's `base_offset` can be **stale-high** (reports the other message's offset). Partition index remains correct.

### Produce ack unknown → broker error

`BridgeError::ProduceAckUnknown` maps to Kafka `ERROR_UNKNOWN_SERVER_ERROR` (-1). This can occur when the **write succeeded** but inference failed (empty `messages_count` after send, or no partition delta detected). Clients may retry → **duplicate messages**. No standard Kafka error means "persisted but ack lost"; mapping is intentional with tradeoff documented in `error.rs`.

### Full fix

1. Extend `send_messages` response: `{ partition_id, offsets: [u64] }` per batch.
2. Drop before/after snapshot and post-send `get_topic` for ack purposes.
3. Optional: idempotent producer support (Phase 3).

---

## L5 — Future batched produce and `base_offset`

`last_message_offset_from_stats` / `current_offset` after send returns the **last** written offset, not Kafka's `base_offset` for a multi-record batch.

Any future batched produce that reuses this helper for `base_offset` without adjustment will be **off by (N−1)**. A batch of N records needs `base_offset = last_offset - (N - 1)` (or a dedicated SDK field). Design note only — Phase 1B sends one `IggyMessage` per Produce partition entry.

---

## L1 — Metadata encoder deduplication

### What exists today

Metadata responses are encoded in **two separate places**:

| Code path | File | Function | Used when |
|-----------|------|----------|-----------|
| Stub (#3421) | `src/protocol/api.rs` | `encode_metadata_response` | `KAFKA_IGGY_BRIDGE=0`, unsupported versions, stub handler |
| Bridge (Phase 1B) | `src/protocol/responses.rs` | `encode_metadata_response_from_topics` | Iggy-backed handler with real topic topology |

Both implementations hand-roll the same Kafka Metadata response wire layout:

- `throttle_time_ms` (v3+)
- Brokers array (single advertised broker: `node_id=1`, host, port, rack)
- `cluster_id` (v2+), `controller_id` (v1+)
- Topics array with per-topic `error_code`, name, `is_internal` (v1+), partitions array, `topic_authorized_operations` (v8+)
- `cluster_authorized_operations` (v8+)
- Flexible encoding branch (v9+: compact strings, varints, tagged fields) vs legacy branch (v0–v8)

The stub path emits placeholder topics (`"unknown-topic"`, empty partitions, `ERROR_UNKNOWN_TOPIC_OR_PARTITION`). The bridge path emits real names, partition counts from `get_topic`, and broker/replica stubs (`leader=1`, `replica=0`, `isr=0`).

### Why this is a problem

1. **Wire drift** — Any fix for metadata versions (e.g. v10 topic UUID), broker fields, or authorized-operations sentinels must be applied twice.
2. **Test duplication** — Golden/stub tests exercise `api.rs`; bridge integration tests exercise `responses.rs`; neither fully covers the shared matrix.
3. **Review burden** — Checklist §6 and bridge PRs cannot assume a single encoder is the source of truth.

### What “done” looks like

Introduce a shared module (e.g. `src/protocol/metadata_response.rs`) with:

```text
MetadataResponseEncoder::new(api_version, broker)
  .with_topics(topics: &[MetadataTopicEntry])
  .encode() -> Bytes

MetadataTopicEntry:
  - Stub { name: placeholder, error: UNKNOWN_TOPIC_OR_PARTITION, partitions: [] }
  - Bridge { outcome: MetadataTopicOutcome }  // real topology from Iggy
```

- `api.rs` and `handler.rs` both call the shared encoder.
- Version gates (`flexible`, `authorized_operations`, partition record shape) live in **one** place.
- Unit tests: one table-driven suite over `(api_version, entry_kind)`.

### Scope / non-goals (L1)

- Does **not** require Iggy server or SDK changes.
- Does **not** implement Metadata v10 topic UUID lookup (max supported version is still 9 in the version firewall).
- Does **not** add multi-broker cluster metadata (Phase 4).

### Suggested PR

Single refactor PR, no behavior change; run existing metadata stub + bridge integration tests.

---

## L2 — Per-topic `CreateTopics` errors on partial failure

### What exists today

**Per-topic partition validation (done in Phase 1B):**

- `num_partitions == -1` → create with `DEFAULT_TOPIC_PARTITIONS` (1); response `error_code = ERROR_NONE`.
- `num_partitions <= 0` (and not `-1`) → **no** Iggy create; response per-topic `ERROR_INVALID_PARTITIONS` via `encode_create_topics_response_inner`.

**Iggy failure handling (not done):**

In `src/handler.rs`, `handle_create_topics` loops topics but **short-circuits on the first Iggy error**:

```text
for topic in req.topics {
    ...
    if let Err(e) = bridge.ensure_stream_and_topic(...).await {
        return encode_create_topics_error_response(version, bridge_error_code(&e));
    }
}
return encode_create_topics_response(version, &req);
```

`encode_create_topics_error_response` builds a **single placeholder topic** with one global `topic_error` applied to all entries—not a faithful multi-topic response.

### Kafka client expectation

`CreateTopics` response (v2+) returns an array parallel to the request: each creatable topic has its own `error_code` (and v5+ partition/replication echo). A request creating `topic-a` and `topic-b` where `topic-a` succeeds and `topic-b` hits a broker error should return:

- `topic-a` → `ERROR_NONE`
- `topic-b` → `ERROR_UNKNOWN_SERVER_ERROR` (or mapped Iggy code)

Clients such as `kafka-topics.sh` and admin APIs rely on per-topic codes for partial success.

### Partial `ensure_stream_and_topic` (documented, not fixed per-topic)

`src/bridge/iggy_bridge.rs`:

1. `create_stream(kafka_topic)` — idempotent on `StreamNameAlreadyExists`
2. `create_topic(...)` — idempotent on `TopicNameAlreadyExists`

If step 1 succeeds and step 2 fails with a **non**-already-exists error (e.g. invalid partitions, permission, disk), an **orphan stream** (no topic) can remain until a later retry succeeds. Today:

- The handler returns a **global** error response (see above).
- The client cannot tell which topic failed vs which already committed.
- A retry may see `StreamNameAlreadyExists` and proceed to `create_topic` (self-heal), but the first response was still wrong for multi-topic batches.

### What “done” looks like

1. **`CreateTopicOutcome` struct** — `{ name, error_code, num_partitions, replication_factor }` per request topic.
2. **Handler loop** — never `return` early on single-topic Iggy failure; record outcome and continue.
3. **`encode_create_topics_response_from_outcomes(version, &[CreateTopicOutcome])`** — replaces blanket `encode_create_topics_error_response` for bridge path.
4. **Replication factor** — today invalid RF aborts entire request; decide whether to keep global abort or per-topic `INVALID_REPLICATION_FACTOR` (Kafka brokers vary; per-topic is safer).
5. **Tests** — two-topic create: first succeeds, second invalid name / Iggy error → mixed per-topic codes in one response.

### Scope / non-goals (L2)

- Does **not** require Iggy API changes; only gateway response shaping.
- Does **not** add Kafka transaction or ACL create semantics.
- Orphan stream cleanup (delete empty stream on topic create failure) is optional hardening—not required for correct Kafka wire behavior.

### Suggested PR

Behavior change PR; update [MANUAL_TESTING.md](MANUAL_TESTING.md) with multi-topic create partial-failure scenario.

---

## L3 — `RecordBatch` offset rewriting

### What exists today

**Produce path** (`src/bridge/iggy_bridge.rs`, `src/handler.rs`):

1. Kafka client sends a serialized **`RecordBatch`** in the Produce request (`requests.rs`: `records: Option<Bytes>`).
2. Bridge stores those bytes **verbatim** as `IggyMessage` payload (`send_messages`).
3. Produce **response** returns Iggy-assigned `base_offset` and partition (with SDK workaround; see [BRIDGE_MAPPING.md](BRIDGE_MAPPING.md)).

**Fetch path:**

1. Bridge polls Iggy messages and concatenates opaque payloads into the Fetch `records` field.
2. Bytes inside the batch still contain the **producer’s** `baseOffset` / `lastOffsetDelta` from encode time—not the Iggy log offset.

### Why this is a problem

Kafka consumers often:

- Advance position using **response metadata** (Fetch high watermark, ListOffsets)—works with the bridge.
- Parse **inside** the `RecordBatch` for offset monotonicity, idempotence, or transactional semantics—**breaks** when:
  - Multiple producers write to the same topic/partition
  - A consumer compares batch-internal offsets to broker offsets
  - Tools decode batches assuming `baseOffset` matches the log

Produce ack offset (Iggy) and batch-internal offset (producer) can diverge from the first message on a partition.

### Iggy / SDK dependency

| Layer | Today | Needed for L3 |
|-------|-------|----------------|
| `MessageClient::send_messages` | `Result<(), IggyError>` | Returning `{ partition_id, offsets[] }` removes produce ack race; **does not** fix fetch without rewrite |
| `IggyMessage` header | Has `offset` after persist | Could expose offset to bridge without second roundtrip |
| Payload | Opaque bytes | Bridge must parse/mutate Kafka batch format |

Relevant trait today (`core/common/src/traits/message_client.rs`):

```rust
async fn send_messages(...) -> Result<(), IggyError>;
```

Until the response includes assigned offsets, Phase 1B uses `get_topic` partition `current_offset` diff or `poll_messages(Last)`—documented in `iggy_bridge.rs` and [BRIDGE_MAPPING.md](BRIDGE_MAPPING.md).

### What “done” looks like

**Minimum (gateway-only):**

1. On produce, after `send_messages` and once Iggy offset is known:
   - Parse `RecordBatch` (magic `0x2`, attributes, `baseOffset`, `lastOffsetDelta`, timestamps, records, CRC).
   - Set `baseOffset` to Iggy-assigned offset; recompute batch CRC.
   - Store rewritten bytes in Iggy.
2. On fetch, returned batches already match log offsets.

**Implementation options:**

| Option | Pros | Cons |
|--------|------|------|
| A. Rewrite in gateway on produce | No server change | Need robust batch parser; all compression codecs (gzip, snappy, lz4, zstd) |
| B. Store raw batch + Iggy offset in message header | Fetch can rebuild or expose offset separately | Kafka clients still expect batch format; may need fetch-time rewrite anyway |
| C. Server stores “Kafka batch view” | Single source of truth | Requires Iggy feature design |

Likely dependency: reuse or add a `RecordBatch` parser (e.g. align with `gateways/kafka/tools/kafka-tool` encoder types, or `kafka-protocol` crate).

### Compression and format caveats

- Batches may use compression (attribute bits). Rewriting `baseOffset` requires decompress → patch → recompress or patch only uncompressed batches initially.
- Magic v1 vs v2, timestamp type, and control batches (transactions) are out of Phase 1B scope; transactional produce is already rejected.

### Scope / non-goals (L3)

- Phase 1B **documents** opaque storage; does **not** rewrite.
- Fixing only produce ack (SDK returning offset) is **necessary but not sufficient** for consumers that read batch-internal offsets on fetch.
- Idempotent producer / transactional batches — Phase 3+.

### Suggested PR sequence

1. **Iggy/SDK** — extend `send_messages` response with partition + offsets (server + binary protocol + SDK).
2. **Gateway** — use response for `ProduceAck` (drop before/after `get_topic` race for balanced produce).
3. **Gateway** — RecordBatch rewrite module + tests with golden batches (uncompressed first, then compressed).

---

## Related limitations (not L1–L3 but same doc set)

These are workarounds in Phase 1B, not the three explicit follow-ups above. Details live in [BRIDGE_MAPPING.md](BRIDGE_MAPPING.md):

| Topic | Workaround | Full fix |
|-------|------------|----------|
| `send_messages` → `()` | `get_topic` stats for hwm/ack; TOCTOU races (L4) | SDK + server response (feeds L3/L4) |
| Single `Arc<IggyClient>` | One TCP connection, mutex-serialized SDK | Connection pool |
| `poll_messages(..., None, ...)` → partition 0 | Always pass `Some(partition)` | N/A for bridge |
| Timestamp `ListOffsets` | `PollingStrategy::timestamp`; error 42 if no match | Depends on Iggy message timestamps |
| Multi-broker metadata | Single broker stub | Phase 4 |

---

## Suggested implementation order

1. **L1** — Metadata encoder dedup (low risk, reduces future bug surface).
2. **SDK `send_messages` offset** — unblocks reliable `ProduceAck` (L4) and simplifies L3.
3. **L2** — Per-topic `CreateTopics` outcomes (admin UX, no parser needed).
4. **L3** — RecordBatch rewrite (largest effort; depends on parser + compression).

---

## Updating this document

When closing an item:

1. Mark the summary table **Done** with PR link.
2. Move detailed section to a short “Resolved in PR #…” note or delete resolved prose.
3. Update [BRIDGE_MAPPING.md](BRIDGE_MAPPING.md) and checklist §4/§6 if behavior changes.
