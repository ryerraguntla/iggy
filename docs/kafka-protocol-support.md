# Kafka Protocol Support for Iggy

## Overview

Iggy optionally exposes a TCP listener that speaks the [Kafka wire protocol](https://kafka.apache.org/protocol.html). When enabled, standard Kafka clients — `librdkafka`, `kafka-go`, `franz-go`, Kafka Streams, etc. — connect to Iggy on a dedicated port (default `9092`) **without any client-side changes**. Iggy translates every Kafka request into native shard operations.

This feature is implemented as a **TCP Listener approach** — a second TCP port running inside the same Iggy binary, sharing all shard infrastructure (storage, consumer groups, authentication) with the native Iggy protocol.

---

## Architecture

```
Kafka Client (librdkafka, kafka-go, …)
        │  TCP :9092 — Kafka wire protocol
        ▼
┌──────────────────────────────────────────────────────────┐
│                   Iggy Server Process                    │
│                                                          │
│   TCP :8090 (Iggy native)   TCP :9092 (Kafka protocol)  │
│          │                         │                     │
│          └──────────────┬──────────┘                     │
│                         ▼                                │
│               IggyShard (per CPU core)                   │
│            shard.append_messages()                       │
│            shard.poll_messages()                         │
│            shard.store_consumer_offset()                 │
│            shard.login_user()                            │
└──────────────────────────────────────────────────────────┘
```

### Key design decisions

| Property | Detail |
|---|---|
| **In-process** | Kafka listener runs inside `iggy-server`, not a separate proxy |
| **SO_REUSEPORT** | One listener per shard thread — same kernel load-balancing as the native TCP port |
| **Shared state** | Kafka and native clients share the same streams, topics, and offsets |
| **Authentication** | SASL/PLAIN maps directly to Iggy username/password |
| **No ZooKeeper / KRaft** | Iggy is the broker and the metadata store |

---

## Topic Mapping

| Kafka concept | Iggy concept |
|---|---|
| Kafka broker | `IggyShard` (single-node) |
| Kafka topic `"orders"` | Iggy stream `kafka` → topic `orders` |
| Kafka partition ID (0-indexed) | Iggy partition ID (1-indexed, +1 on all operations) |
| Kafka consumer group | Iggy consumer group |
| Kafka offset | Iggy offset (direct 1:1) |
| SASL PLAIN credentials | Iggy username / password |

The Iggy stream that acts as the Kafka namespace is configured via `kafka.kafka_stream` (default `"kafka"`). This stream **must exist** before Kafka clients try to produce or consume.

```bash
# Create the kafka stream via the Iggy CLI before starting clients:
iggy stream create kafka
```

---

## Configuration

Add the following to `core/server/config.toml` (or set the corresponding env vars):

```toml
[kafka]
enabled       = true
address       = "0.0.0.0:9092"
kafka_stream  = "kafka"

[kafka.socket]
override_defaults = false
recv_buffer_size  = "100 KB"
send_buffer_size  = "100 KB"
keepalive         = false
nodelay           = true
```

### Environment variables

All fields support the `IGGY_KAFKA_` prefix pattern used by the rest of the server config:

```
IGGY_KAFKA_ENABLED=true
IGGY_KAFKA_ADDRESS=0.0.0.0:9092
IGGY_KAFKA_KAFKA_STREAM=kafka
```

---

## Supported Kafka APIs

| API key | Name | Notes |
|---|---|---|
| 0 | `Produce` | ⚠️ Message body conversion (RecordBatch→Iggy) is a TODO |
| 1 | `Fetch` | ⚠️ Response serialization (Iggy→RecordBatch) is a TODO |
| 2 | `ListOffsets` | Earliest / latest timestamps supported |
| 3 | `Metadata` | Full topic + partition layout |
| 8 | `OffsetCommit` | Persists via `shard.store_consumer_offset()` |
| 9 | `OffsetFetch` | Reads via `shard.get_consumer_offset()` |
| 10 | `FindCoordinator` | Always returns self (single-node) |
| 11 | `JoinGroup` | Single-member groups supported |
| 12 | `Heartbeat` | Always ACKs |
| 13 | `LeaveGroup` | Clears session state |
| 14 | `SyncGroup` | Returns empty assignment (client-driven) |
| 17 | `SaslHandshake` | `PLAIN` mechanism only |
| 18 | `ApiVersions` | Full version negotiation |
| 19 | `CreateTopics` | Creates Iggy topics in the kafka stream |
| 36 | `SaslAuthenticate` | `PLAIN` → `shard.login_user()` |

### Flexible encoding (KIP-482)

All APIs at or above their flexible-encoding version threshold use compact arrays and tagged fields. The version thresholds match the official Kafka specification; see `protocol/request.rs:is_flexible()`.

---

## Kafka Wire Protocol Framing

### Request

```
┌──────────────────┬──────────────┬──────────────────┬──────────────────┬──────────────┐
│ frame_length:i32 │ api_key: i16 │ api_version: i16 │ correlation_id:  │ client_id:   │
│  (big-endian)    │   (BE)       │   (BE)           │      i32 (BE)    │ nullable_str │
└──────────────────┴──────────────┴──────────────────┴──────────────────┴──────────────┘
  [tagged_fields if flexible]  [API-specific payload …]
```

### Response

```
┌──────────────────┬──────────────────┬───────────────────────┬────────────────────┐
│ frame_length:i32 │ correlation_id:  │ [tagged_fields        │ API-specific body  │
│  (big-endian)    │      i32 (BE)    │  if flexible]         │ …                  │
└──────────────────┴──────────────────┴───────────────────────┴────────────────────┘
```

---

## Module Layout

```
core/server/src/kafka/
├── mod.rs                  Module root, COMPONENT constant
├── kafka_server.rs         Parse config, call listener
├── kafka_listener.rs       SO_REUSEPORT TCP accept loop
├── connection_handler.rs   Per-connection read → dispatch → write loop
├── session.rs              KafkaSession (auth state, group membership)
├── error.rs                KafkaErrorCode enum, iggy_to_kafka_error()
├── protocol/
│   ├── mod.rs
│   ├── types.rs            read_*/write_* primitives (BE, varint, compact)
│   ├── request.rs          RequestHeader, api_key constants, is_flexible()
│   └── response.rs         frame_response() — wrap body with length + correlation_id
└── handlers/
    ├── mod.rs
    ├── api_versions.rs     API key 18
    ├── sasl.rs             API keys 17, 36
    ├── metadata.rs         API keys 3, 19
    ├── produce.rs          API key 0
    ├── fetch.rs            API key 1
    ├── list_offsets.rs     API key 2
    ├── offset_commit.rs    API key 8
    ├── offset_fetch.rs     API key 9
    ├── find_coordinator.rs API key 10
    ├── join_group.rs       API key 11
    ├── heartbeat.rs        API key 12
    ├── leave_group.rs      API key 13
    └── sync_group.rs       API key 14
```

The continuous task lives at:

```
core/server/src/shard/tasks/continuous/kafka_server.rs
```

---

## Implementation Phases

### Phase 1 — Protocol bootstrap (complete)
Bootstrap the listener, session lifecycle, SASL authentication, and topic metadata.  Validates that a Kafka client can connect, authenticate, discover topics, and have its connection managed correctly.

**Done:** `ApiVersions`, `SaslHandshake`, `SaslAuthenticate`, `Metadata`, `CreateTopics`, `FindCoordinator`, `Heartbeat`, `ListOffsets`, `OffsetCommit`, `OffsetFetch`, `JoinGroup`, `SyncGroup`, `LeaveGroup`.

### Phase 2 — Message I/O (TODO)
Wire up actual message produce and consume by:
1. Converting Kafka `RecordBatch` bytes → `IggyMessagesBatchMut` in `handlers/produce.rs`.
2. Serializing `IggyMessagesBatchSet` → Kafka `RecordBatch` bytes in `handlers/fetch.rs`.
3. Computing CRC32C on outbound batches.

### Phase 3 — Hardening
- TLS support (reuse `tcp_tls_listener.rs` pattern).
- Compression: gzip, snappy, lz4, zstd (RecordBatch attributes bits 0–2).
- Multi-member consumer groups with cooperative partition assignment.
- Exactly-once semantics via producer_id / producer_epoch.

---

## SASL Authentication Flow

```
Client → SaslHandshake("PLAIN")  →  Server: OK, enabled=[PLAIN]
Client → SaslAuthenticate(\0username\0password)
       →  shard.login_user(username, password, session)
       →  session.set_user_id(user.id)
       →  kafka_session.authenticated = true
Client → [any subsequent API request]  ← now allowed
```

All requests except `ApiVersions`, `SaslHandshake`, and `SaslAuthenticate` return `CLUSTER_AUTHORIZATION_FAILED (31)` until authentication completes.

---

## Error Code Mapping

| Iggy error | Kafka error code |
|---|---|
| `TopicIdNotFound` / `StreamIdNotFound` | `UNKNOWN_TOPIC_OR_PARTITION (3)` |
| `TopicNameAlreadyExists` | `TOPIC_ALREADY_EXISTS (36)` |
| `Unauthenticated` / `Unauthorized` | `CLUSTER_AUTHORIZATION_FAILED (31)` |
| `InvalidCredentials` | `SASL_AUTHENTICATION_FAILED (58)` |
| Any other | `NETWORK_EXCEPTION (13)` |

---

## Running with Kafka clients

```bash
# 1. Start Iggy with Kafka enabled (kafka.enabled = true in config.toml)
cargo run -p iggy-server

# 2. Create the kafka stream
iggy stream create kafka

# 3. Point a Kafka client at localhost:9092
kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic orders

kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic orders \
    --from-beginning
```

> **Note:** Until Phase 2 is complete, `Produce` and `Fetch` return stub responses that acknowledge the protocol correctly but do not yet write or read actual messages.
