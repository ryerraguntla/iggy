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

//! Kafka TCP listener — SO_REUSEPORT accept loop.
//!
//! This module mirrors `tcp_listener.rs` exactly, substituting the
//! Kafka connection handler and using `kafka_bound_address` instead of
//! `tcp_bound_address` for the cross-shard address broadcast.
//!
//! ## Multi-shard binding sequence
//!
//! 1. Shard 0 binds the configured address (e.g. `0.0.0.0:9092`).
//! 2. Shard 0 stores the resolved address in `kafka_bound_address` and
//!    broadcasts a `ShardEvent::AddressBound { Kafka, addr }` to all peers.
//! 3. Shards 1…N spin-wait on `kafka_bound_address`, then bind the same
//!    address with `SO_REUSEPORT` — the kernel load-balances connections.

use crate::kafka::connection_handler::handle_kafka_connection;
use crate::kafka::session::KafkaSession;
use crate::shard::IggyShard;
use crate::shard::task_registry::{ShutdownToken, TaskRegistry};
use crate::shard::transmission::event::ShardEvent;
use crate::tcp::tcp_listener::cleanup_connection;
use compio::net::{SocketOpts, TcpListener};
use err_trail::ErrContext;
use futures::FutureExt;
use iggy_common::{IggyError, SenderKind, TransportProtocol};
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;
use tracing::{error, info};

async fn create_listener(addr: SocketAddr) -> Result<TcpListener, std::io::Error> {
    let opts = SocketOpts::new().reuse_port(true);
    TcpListener::bind_with_options(addr, &opts).await
}

/// Bind the Kafka listener on this shard and enter the accept loop.
///
/// If `shard.id != 0` and the port is 0, this function polls
/// `kafka_bound_address` until shard 0 has announced the resolved port.
pub async fn start(
    server_name: &'static str,
    mut addr: SocketAddr,
    shard: Rc<IggyShard>,
    shutdown: ShutdownToken,
) -> Result<(), IggyError> {
    if shard.id != 0 && addr.port() == 0 {
        info!("Shard {} waiting for Kafka address from shard 0…", shard.id);
        loop {
            if let Some(bound_addr) = shard.kafka_bound_address.get() {
                addr = bound_addr;
                info!("Shard {} received Kafka address: {}", shard.id, addr);
                break;
            }
            compio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    let listener = create_listener(addr)
        .await
        .map_err(|_| IggyError::CannotBindToSocket(addr.to_string()))
        .error(|err: &IggyError| {
            format!("Failed to bind {server_name} to {addr}: {err}")
        })?;

    let actual_addr = listener.local_addr().map_err(|e| {
        error!("Failed to read local address: {e}");
        IggyError::CannotBindToSocket(addr.to_string())
    })?;
    info!("{server_name} listening on {actual_addr:?}");

    if shard.id == 0 {
        shard.kafka_bound_address.set(Some(actual_addr));

        if addr.port() == 0 {
            // Notify the config writer that the port is now known.
            let _ = shard.config_writer_notify.try_send(());

            // Tell every other shard which port to SO_REUSEPORT onto.
            let event = ShardEvent::AddressBound {
                protocol: TransportProtocol::Kafka,
                address: actual_addr,
            };
            shard.broadcast_event_to_all_shards(event).await?;
        }
    }

    accept_loop(server_name, listener, shard, shutdown).await
}

async fn accept_loop(
    server_name: &'static str,
    listener: TcpListener,
    shard: Rc<IggyShard>,
    shutdown: ShutdownToken,
) -> Result<(), IggyError> {
    loop {
        let shard = shard.clone();
        let accept_future = listener.accept();

        futures::select! {
            _ = shutdown.wait().fuse() => {
                info!("{server_name} received shutdown — no longer accepting connections.");
                break;
            }
            result = accept_future.fuse() => {
                match result {
                    Ok((stream, address)) => {
                        if shard.is_shutting_down() {
                            info!("Rejecting Kafka connection from {address} during shutdown.");
                            continue;
                        }

                        let shard_clone = shard.clone();
                        info!("Accepted new Kafka connection from {address}");

                        let session = shard_clone.add_client(&address, TransportProtocol::Kafka);
                        let client_id = session.client_id;
                        info!("Created Kafka session {} for {address}", session);

                        let mut sender = SenderKind::get_tcp_sender(stream);
                        let conn_stop_receiver = shard.task_registry.add_connection(client_id);
                        let kafka_session = KafkaSession::new(client_id, address);

                        let shard_for_conn = shard_clone.clone();
                        let registry = shard.task_registry.clone();
                        let registry_clone = registry.clone();

                        registry.spawn_connection(async move {
                            handle_kafka_connection(
                                kafka_session,
                                session,
                                &mut sender,
                                &shard_for_conn,
                                conn_stop_receiver,
                            )
                            .await;
                            cleanup_connection(
                                &mut sender,
                                client_id,
                                address,
                                &registry_clone,
                                &shard_for_conn,
                            )
                            .await;
                        });
                    }
                    Err(error) => error!("{server_name}: failed to accept socket: {error}"),
                }
            }
        }
    }
    Ok(())
}
