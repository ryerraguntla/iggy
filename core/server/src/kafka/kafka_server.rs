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

//! Top-level Kafka server spawn function.
//!
//! Parses the configured address from `shard.config.kafka.address` and
//! delegates to `kafka_listener::start`.  This is the entry point called
//! by the continuous task spawned in `shard/tasks/continuous/kafka_server.rs`.

use crate::kafka::kafka_listener;
use crate::shard::IggyShard;
use crate::shard::task_registry::ShutdownToken;
use iggy_common::IggyError;
use std::net::SocketAddr;
use std::rc::Rc;
use tracing::info;

pub async fn spawn_kafka_server(
    shard: Rc<IggyShard>,
    shutdown: ShutdownToken,
) -> Result<(), IggyError> {
    let addr: SocketAddr = shard
        .config
        .kafka
        .address
        .parse()
        .expect("Failed to parse Kafka address from config");

    info!("Initializing Iggy Kafka protocol server…");
    kafka_listener::start("Iggy Kafka", addr, shard, shutdown).await
}
