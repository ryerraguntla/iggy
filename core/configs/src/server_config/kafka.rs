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

use configs::ConfigEnv;
use serde::{Deserialize, Serialize};

/// Configuration for the Kafka-protocol-compatible TCP listener.
///
/// When enabled, Iggy exposes a separate TCP port that speaks the Kafka wire
/// protocol. Kafka clients (librdkafka, kafka-go, franz-go, etc.) connect to
/// this port without any changes; Iggy translates every request into native
/// shard operations.
///
/// Topic mapping: a Kafka topic named "orders" maps to the Iggy stream
/// specified by `kafka_stream` (default "kafka") and the Iggy topic "orders".
#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct KafkaConfig {
    /// Whether the Kafka-protocol listener is active.
    pub enabled: bool,

    /// Network address and port for the Kafka listener.
    /// Format: "HOST:PORT". Example: "0.0.0.0:9092".
    pub address: String,

    /// Name of the Iggy stream that acts as the Kafka namespace.
    /// All Kafka topics are mapped to topics inside this stream.
    pub kafka_stream: String,

    /// Low-level socket configuration for the Kafka listener.
    pub socket: KafkaSocketConfig,
}

/// Socket-level tuning for the Kafka listener.
#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct KafkaSocketConfig {
    /// When false, OS defaults are used for all socket options below.
    pub override_defaults: bool,

    /// SO_RCVBUF: maximum receive buffer size (e.g. "256 KB").
    pub recv_buffer_size: String,

    /// SO_SNDBUF: maximum send buffer size (e.g. "256 KB").
    pub send_buffer_size: String,

    /// SO_KEEPALIVE: periodically probe the connection to detect dead peers.
    pub keepalive: bool,

    /// TCP_NODELAY: disable Nagle algorithm for lower latency at the cost of
    /// slightly higher packet count.
    pub nodelay: bool,
}
