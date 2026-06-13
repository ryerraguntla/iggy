# Kafka gateway — implementation checklist

Mandatory review gate for **every** bridge PR (Phase 1B+). Copy the relevant sections into agent prompts.

Reference: [Discussion #3253](https://github.com/apache/iggy/discussions/3253), [BRIDGE_MAPPING.md](BRIDGE_MAPPING.md).

---

## 1. Kafka array parity (MUST)

Produce, Fetch, and ListOffsets requests contain **topic arrays**. For each:

- [ ] Process **every** topic entry (side effects match request).
- [ ] Response encodes **the same number of topics** with matching names/partition indices.
- [ ] Never use `topics.first()` only unless explicitly rejecting multi-topic up front.
- [ ] Test: 2-topic Produce / Fetch / ListOffsets in one request.

Reject pattern (if not implementing multi-topic): return top-level or per-topic `INVALID_REQUEST` — never silent drop.

---

## 2. Sentinel values (MUST)

| Sentinel | API | Required behavior |
|----------|-----|-------------------|
| `partition == -1` | Produce | `Partitioning::balanced()`; response includes **actual** assigned partition + offset |
| `num_partitions == -1` | CreateTopics | Map to `DEFAULT_TOPIC_PARTITIONS`, create topic |
| `num_partitions <= 0` (not -1) | CreateTopics | Per-topic `INVALID_PARTITIONS`, **no** create |
| `timestamp == -1` | ListOffsets | Latest offset (high watermark) |
| `timestamp == -2` | ListOffsets | Earliest offset (`0` or first available) |
| Other timestamps | ListOffsets | `PollingStrategy::timestamp` **or** explicit unsupported error — **never** `Ok(0)` |

---

## 3. Offset truth (MUST)

- [ ] Produce `base_offset` = Iggy-assigned message offset on the **correct partition**.
- [ ] Do **not** use `high_watermark(None)` — Iggy treats `None` as partition 0 only.
- [ ] SDK `send_messages` returns `()` today — use `get_topic` partition `current_offset` diff or `poll_messages(Last)` on the target partition. Document race under concurrent producers.
- [ ] Fetch `high_watermark` / `log_start_offset` per partition from Iggy, not hardcoded `0`.
- [ ] **RecordBatch limitation**: stored payloads keep producer `baseOffset`; not rewritten to Iggy offsets. Document in BRIDGE_MAPPING.md (Phase 1B); real fix needs batch rewriting.

---

## 4. Iggy capability gaps (flag in PR)

| Gap | Workaround today | Tracking |
|-----|------------------|----------|
| `send_messages` no partition/offset in response | `get_topic` before/after or `poll_messages(Last)` | [metadata STM TODO](https://github.com/apache/iggy) — serialize assigned partition IDs |
| Single `Arc<IggyClient>` / TCP mutex | Document throughput ceiling | Connection pool follow-up |
| RecordBatch offset rewrite | Document single-producer/fresh-partition assumption | Phase 2+ |
| Multi-broker Kafka metadata | Single broker stub | Phase 4 |

---

## 5. Units and constants (MUST)

- [ ] Name magic numbers (`MAX_FETCH_MESSAGE_COUNT`, etc.).
- [ ] `partition_max_bytes` is a **byte budget hint** — document remap to message **count** cap (not bytes).
- [ ] No `.expect()` on values from network/Iggy — use `map_err` / safe defaults.

---

## 6. Failure modes (MUST document)

- [ ] `ensure_stream_and_topic`: stream may exist without topic if second step fails; self-heals on retry — document orphan stream.
- [ ] `KAFKA_IGGY_BRIDGE`: only `0`/`false` disable; warn on unrecognized values.
- [ ] Partial CreateTopics: per-topic errors in response, not blanket `ERROR_NONE`.

---

## 7. Tests required per change

- [ ] Multi-topic Produce → all topics in response.
- [ ] Produce `partition=-1` on multi-partition topic → response partition matches written partition.
- [ ] CreateTopics `num_partitions=-1` → topic exists in Iggy.
- [ ] Fetch/ListOffsets multi-topic.
- [ ] ListOffsets non-(-1/-2) timestamp → error or correct timestamp seek.
- [ ] No unused `pub` test helpers.

---

## 8. Self-review before PR (agent MUST output)

1. Trace each Kafka array field: request → side effect → response entry.
2. List every `Ok(ERROR_NONE)` where Iggy state might differ.
3. List hot-path double roundtrips to Iggy.
4. List documented limitations added to BRIDGE_MAPPING.md.

---

## 9. Code hygiene

- [ ] Re-export cohesive bridge types at crate root (`FetchedMessage`, `ProduceAck`, …).
- [ ] No dead code in `tests/common/`.
- [ ] Clippy clean: `cargo clippy -p iggy_gateway_kafka --all-targets -- -D warnings`.
- [ ] `cargo test -p iggy_gateway_kafka` (build `iggy-server` for bridge integration tests).
