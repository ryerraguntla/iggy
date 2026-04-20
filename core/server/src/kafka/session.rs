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

use std::net::SocketAddr;

/// Per-connection Kafka state that supplements the generic Iggy `Session`.
///
/// Iggy's `Session` tracks the client_id and user_id after authentication.
/// `KafkaSession` additionally tracks Kafka-protocol-specific state:
/// SASL handshake completion, the Kafka client_id string from request headers,
/// and any active consumer group membership.
#[derive(Debug)]
pub struct KafkaSession {
    /// Iggy client_id assigned at TCP accept time.
    pub client_id: u32,

    /// Remote address of the connected Kafka client.
    pub address: SocketAddr,

    /// Set to `true` after a successful `SaslAuthenticate` exchange.
    /// Requests other than `ApiVersions`, `SaslHandshake`, and
    /// `SaslAuthenticate` are rejected with `CLUSTER_AUTHORIZATION_FAILED`
    /// until this is set.
    pub authenticated: bool,

    /// The `client_id` string from the Kafka request header, set on the
    /// first request received on this connection.
    pub kafka_client_id: Option<String>,

    /// The consumer `group_id` this connection joined via `JoinGroup`.
    /// `None` until `JoinGroup` succeeds.
    pub group_id: Option<String>,

    /// The `member_id` assigned to this connection within its consumer group.
    /// `None` until `JoinGroup` succeeds.
    pub member_id: Option<String>,

    /// The generation counter of the consumer group. Starts at -1
    /// (no group) and is incremented on each successful `JoinGroup`.
    pub generation_id: i32,
}

impl KafkaSession {
    pub fn new(client_id: u32, address: SocketAddr) -> Self {
        Self {
            client_id,
            address,
            authenticated: false,
            kafka_client_id: None,
            group_id: None,
            member_id: None,
            generation_id: -1,
        }
    }

    /// Returns `true` when this connection has joined a consumer group.
    pub fn in_consumer_group(&self) -> bool {
        self.group_id.is_some()
    }

    /// Clear consumer group state when the client leaves.
    pub fn leave_group(&mut self) {
        self.group_id = None;
        self.member_id = None;
        self.generation_id = -1;
    }
}
