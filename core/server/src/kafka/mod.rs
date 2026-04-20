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
//! runs inside the Iggy server process.  Kafka clients connect on a dedicated
//! port (default 9092) and interact with Iggy streams/topics transparently.
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

pub mod connection_handler;
pub mod error;
pub mod handlers;
pub mod kafka_listener;
pub mod kafka_server;
pub mod protocol;
pub mod session;

pub const COMPONENT: &str = "KAFKA";
