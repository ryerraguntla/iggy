# Kafka ↔ Iggy bridge mapping (Phase 1B)

Implements [Discussion #3253](https://github.com/apache/iggy/discussions/3253) Phase 1 Iggy integration for producer/fetch without consumer groups.

## Naming

| Kafka | Iggy |
|-------|------|
| Topic name | Stream name **and** topic name (1:1, same string) |
| Partition index | Partition index (both **0-based**) |
| RecordBatch bytes | `IggyMessage` payload (opaque) |

## Produce

- `partition == -1` → `Partitioning::balanced()`; response includes the **actual** assigned partition and Iggy offset
- `partition >= 0` → `Partitioning::partition_id(partition)`
- Transactional produce (`transactional_id` set) → `INVALID_REQUEST` (Phase 3)

### Produce offset workaround (Iggy SDK gap)

`send_messages` returns `()` — no partition or offset in the response. The bridge infers offset via `get_topic` partition stats (`current_offset`, `messages_count`):

1. **Explicit partition**: `current_offset` after send (empty partition → `ProduceAckUnknown`).
2. **Balanced (`-1`)**: snapshot all partition `current_offset` values before/after send; first index where `after > before` wins.

Concurrent producers on the same topic can race; offsets may be wrong under heavy contention until Iggy returns assigned partition IDs from `send_messages`.

## Fetch / ListOffsets

- Fetch polls one partition via `poll_messages(..., Some(partition), PollingStrategy::offset(offset), ...)` with up to `MAX_FETCH_MESSAGE_COUNT` (500) messages — `partition_max_bytes` is a Kafka byte hint, not used as a message count.
- `high_watermark` and `log_start_offset` come from Iggy per partition (not hardcoded `0`).
- ListOffsets `timestamp = -1` (latest) → high watermark; `-2` (earliest) → `0`
- Other timestamps → `PollingStrategy::timestamp` (Kafka ms → Iggy µs); empty/unsupported → `ERROR_INVALID_REQUEST` (42)
- High watermark / latest offset → `get_topic` stats (`hwm = current_offset + 1` when `messages_count > 0`, else `0`) — not `poll_messages(Last)`
- Record payloads are returned as opaque bytes in the Fetch `records` field

### RecordBatch / consumer offset limitation

Stored payloads are **opaque** Kafka `RecordBatch` bytes. Producer `baseOffset` inside the batch is **not** rewritten to Iggy offsets. Consumer offset bookkeeping that depends on embedded batch offsets will break when multiple producers write to the same topic. Phase 1B documents this; fixing it requires RecordBatch rewriting on produce.

## CreateTopics / Metadata

- `CreateTopics` → `ensure_stream_and_topic(name, num_partitions)` (idempotent)
- `num_partitions == -1` → `DEFAULT_TOPIC_PARTITIONS` (1)
- `num_partitions <= 0` (not `-1`) → per-topic `INVALID_PARTITIONS`, no create
- If `create_stream` succeeds but `create_topic` fails, an empty stream may remain until a retry succeeds
- `Metadata` with explicit topic names → `get_topic` for partition count; unknown topics → error `3`
- Replication factor must be `1` (single Iggy broker)

## Iggy capability limits (flag in PRs)

See [IGGY_LIMITATIONS.md](IGGY_LIMITATIONS.md) for explicit follow-ups (metadata encoder dedup, per-topic CreateTopics errors, RecordBatch rewrite) and implementation order.

| Limit | Impact |
|-------|--------|
| `send_messages` → `()` | Offset inference via extra roundtrip; race under concurrent producers |
| Single `Arc<IggyClient>` / TCP mutex | Throughput ceiling on one connection |
| `poll_messages(..., None, ...)` | Server resolves to partition 0 only — bridge always passes `Some(partition)` |
| No RecordBatch rewrite | Fetch offsets in batch bytes ≠ Iggy offsets |
| Timestamp ListOffsets | Depends on Iggy message timestamps; may return unsupported if no match |

## Configuration

| Variable | Default | Purpose |
|----------|---------|---------|
| `IGGY_TCP_ADDR` | `127.0.0.1:8090` | Iggy TCP endpoint |
| `IGGY_USERNAME` / `IGGY_PASSWORD` | `iggy` / `iggy` | Login credentials |
| `KAFKA_IGGY_BRIDGE` | enabled | Set `0` or `false` for stub-only mode (#3421) |
