/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//! Kafka protocol support for Iggy.
//!
//! This module implements a Kafka-wire-protocol-compatible TCP listener that
//! runs **inside the Iggy server process**.  Kafka clients connect on a dedicated
//! port (default 9092) and interact with Iggy streams/topics transparently.
//!
//! ## Architecture decision: in-process embedding
//!
//! This listener runs on the same shard threads as the rest of Iggy and calls
//! shard methods directly — no extra network hop, no separate auth round-trip.
//! The alternative (standalone proxy process using the Iggy SDK over TCP) is
//! simpler to deploy but adds latency and a second failure domain.
//! See `iggy_supporting_kafka` repo for the standalone reference implementation.
//!
//! ## Topic / stream mapping
//!
//! All Kafka topics are mapped into a single Iggy stream whose name is set by
//! `KafkaConfig::kafka_stream` (default `"kafka"`).
//! Kafka topic `"orders"` → Iggy stream `"kafka"` + topic `"orders"`.
//! The stream must exist before any Kafka clients connect; it is NOT auto-created
//! on first produce.  Use `CreateTopics` or pre-create via the Iggy API.
//!
//! ## Module layout
//!
//! | Module | Purpose |
//! |---|---|
//! | `kafka_server` | Top-level spawn: parse config, call listener |
//! | `kafka_listener` | SO_REUSEPORT TCP accept loop |
//! | `connection_handler` | Per-connection read → dispatch → write loop |
//! | `session` | Kafka-specific per-connection state |
//! | `error` | Kafka error codes + IggyError mapping |
//! | `protocol` | Wire-format primitives, request header, response framing |
//! | `handlers` | One module per Kafka API key |
//!
//! ## Implementation status (Phase 1 — wire protocol skeleton)
//!
//! | API | Status | Notes |
//! |---|---|---|
//! | ApiVersions (18) | Working | |
//! | SaslHandshake (17) + SaslAuthenticate (36) | Working | PLAIN only |
//! | Metadata (3) + CreateTopics (19) | Working | Single-broker topology |
//! | Produce (0) | **STUB** | Parses wire format, returns fake success; messages NOT stored in Iggy |
//! | Fetch (1) | **STUB** | Calls `poll_messages` but returns empty records — Kafka consumers receive no messages |
//! | ListOffsets (2) | Working | Reads partition stats from Iggy metadata |
//! | FindCoordinator (10) | Working | Returns self as coordinator |
//! | JoinGroup (11) | Partial | Basic group formation; no rebalance protocol |
//! | SyncGroup (14) | Partial | Assigns all partitions to the single joining member |
//! | Heartbeat (12) | Working | Always responds OK |
//! | LeaveGroup (13) | Working | |
//! | OffsetCommit (8) | Working | Persists via `shard.store_consumer_offset()` |
//! | OffsetFetch (9) | Working | Reads via `shard.get_consumer_offset()` |
//!
//! **Critical gaps**:
//! - Produce is a stub: RecordBatch → IggyMessage parsing not implemented.
//!   See `handlers/produce.rs` TODO. Next step: port the `parse_record_batch`
//!   implementation from branch `feat/kafka_protocol_support_4_iggy`.
//! - Fetch returns empty records: RecordBatch serialization not implemented.
//!   See `handlers/fetch.rs` for the full implementation guide.

pub mod connection_handler;
pub mod error;
pub mod handlers;
pub mod kafka_listener;
pub mod kafka_server;
pub mod protocol;
pub mod session;

pub const COMPONENT: &str = "KAFKA";
