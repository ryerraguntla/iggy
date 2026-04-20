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

//! Per-connection Kafka protocol read → dispatch → write loop.
//!
//! Each accepted TCP connection runs this loop on a single shard thread.
//! The loop:
//!
//! 1. Reads a 4-byte big-endian frame length.
//! 2. Reads the remaining `frame_len` bytes (header + API payload).
//! 3. Peeks `api_key` and `api_version` to decide encoding (flexible vs classic).
//! 4. Parses the request header (correlation_id, client_id).
//! 5. Enforces authentication before any non-SASL request.
//! 6. Dispatches to the appropriate handler.
//! 7. Wraps the response body in a Kafka frame and writes it back.
//!
//! The loop exits (and the caller cleans up the connection) on:
//! - A stop signal from the task registry.
//! - Any I/O error or unexpected EOF.
//! - `CLUSTER_AUTHORIZATION_FAILED` when the client keeps sending unauthenticated requests.

use crate::kafka::error::KafkaErrorCode;
use crate::kafka::handlers;
use crate::kafka::protocol::request::{self, RequestHeader, api_key, api_key_name};
use crate::kafka::protocol::response::frame_response;
use crate::kafka::session::KafkaSession;
use crate::shard::IggyShard;
use crate::streaming::session::Session;
use async_channel::Receiver;
use bytes::BytesMut;
use futures::FutureExt;
use iggy_common::SenderKind;
use std::rc::Rc;
use tracing::{debug, warn};

pub async fn handle_kafka_connection(
    mut kafka_session: KafkaSession,
    session: Session,
    sender: &mut SenderKind,
    shard: &Rc<IggyShard>,
    stop_receiver: Receiver<()>,
) {
    loop {
        // ── Step 1: read the 4-byte frame length ────────────────────────────
        let len_buf = BytesMut::with_capacity(4);
        let (result, len_buf) = futures::select! {
            _ = stop_receiver.recv().fuse() => {
                debug!(
                    "Kafka connection stop signal for client {}",
                    session.client_id
                );
                return;
            }
            result = sender.read(len_buf).fuse() => result,
        };

        if result.is_err() {
            return; // client disconnected or I/O error
        }

        if len_buf.len() < 4 {
            return;
        }
        let frame_len = i32::from_be_bytes(len_buf[0..4].try_into().unwrap());
        if frame_len <= 0 || frame_len > 100 * 1024 * 1024 {
            warn!(
                "Kafka client {} sent invalid frame length {frame_len}",
                session.client_id
            );
            return;
        }

        // ── Step 2: read the rest of the frame ──────────────────────────────
        let payload_buf = BytesMut::with_capacity(frame_len as usize);
        let (result, payload_buf) = sender.read(payload_buf).await;
        if result.is_err() {
            return;
        }

        if payload_buf.len() < 4 {
            return;
        }

        // ── Step 3: peek api_key + api_version ──────────────────────────────
        let api_key_val = i16::from_be_bytes([payload_buf[0], payload_buf[1]]);
        let api_version = i16::from_be_bytes([payload_buf[2], payload_buf[3]]);
        let flexible = request::is_flexible(api_key_val, api_version);

        // ── Step 4: parse request header ────────────────────────────────────
        let mut payload_bytes = payload_buf.freeze();
        let header = RequestHeader::parse(&mut payload_bytes, flexible);
        let correlation_id = header.correlation_id;

        if kafka_session.kafka_client_id.is_none() {
            kafka_session.kafka_client_id = header.client_id.clone();
        }

        debug!(
            "Kafka client {} api_key={} ({}) version={}",
            session.client_id,
            api_key_val,
            api_key_name(api_key_val),
            api_version
        );

        // ── Step 5: authentication gate ─────────────────────────────────────
        if !kafka_session.authenticated
            && !matches!(
                api_key_val,
                api_key::API_VERSIONS | api_key::SASL_HANDSHAKE | api_key::SASL_AUTHENTICATE
            )
        {
            let body = not_authenticated_body(api_key_val, flexible);
            let frame = frame_response(correlation_id, &body, flexible);
            let frame_frozen = frame.freeze();
            let _ = sender.write_raw(&frame_frozen).await;
            continue;
        }

        // ── Step 6: dispatch ─────────────────────────────────────────────────
        let body: Vec<u8> = match api_key_val {
            api_key::API_VERSIONS => {
                handlers::api_versions::handle(api_version, &payload_bytes, flexible)
            }
            api_key::SASL_HANDSHAKE => {
                handlers::sasl::handle_handshake(api_version, &payload_bytes, flexible)
            }
            api_key::SASL_AUTHENTICATE => handlers::sasl::handle_authenticate(
                api_version,
                &payload_bytes,
                flexible,
                &mut kafka_session,
                shard,
                &session,
            ),
            api_key::METADATA => {
                handlers::metadata::handle(api_version, &payload_bytes, flexible, shard, &session)
                    .await
            }
            api_key::CREATE_TOPICS => {
                handlers::metadata::handle_create_topics(
                    api_version,
                    &payload_bytes,
                    flexible,
                    shard,
                    &session,
                )
                .await
            }
            api_key::PRODUCE => {
                handlers::produce::handle(
                    api_version,
                    &payload_bytes,
                    flexible,
                    shard,
                    &session,
                    &kafka_session,
                )
                .await
            }
            api_key::FETCH => {
                handlers::fetch::handle(
                    api_version,
                    &payload_bytes,
                    flexible,
                    shard,
                    &session,
                    &kafka_session,
                )
                .await
            }
            api_key::LIST_OFFSETS => {
                handlers::list_offsets::handle(
                    api_version,
                    &payload_bytes,
                    flexible,
                    shard,
                    &session,
                )
                .await
            }
            api_key::OFFSET_COMMIT => {
                handlers::offset_commit::handle(
                    api_version,
                    &payload_bytes,
                    flexible,
                    shard,
                    &session,
                )
                .await
            }
            api_key::OFFSET_FETCH => {
                handlers::offset_fetch::handle(
                    api_version,
                    &payload_bytes,
                    flexible,
                    shard,
                    &session,
                )
                .await
            }
            api_key::FIND_COORDINATOR => {
                handlers::find_coordinator::handle(api_version, &payload_bytes, flexible, shard)
                    .await
            }
            api_key::JOIN_GROUP => {
                handlers::join_group::handle(
                    api_version,
                    &payload_bytes,
                    flexible,
                    shard,
                    &session,
                    &mut kafka_session,
                )
                .await
            }
            api_key::HEARTBEAT => {
                handlers::heartbeat::handle(api_version, &payload_bytes, flexible)
            }
            api_key::LEAVE_GROUP => {
                handlers::leave_group::handle(
                    api_version,
                    &payload_bytes,
                    flexible,
                    &mut kafka_session,
                )
                .await
            }
            api_key::SYNC_GROUP => {
                handlers::sync_group::handle(api_version, &payload_bytes, flexible).await
            }
            _ => {
                warn!(
                    "Kafka client {} sent unsupported API key {}",
                    session.client_id, api_key_val
                );
                unsupported_version_body(flexible)
            }
        };

        // ── Step 7: write the framed response ────────────────────────────────
        let frame = frame_response(correlation_id, &body, flexible);
        let frame_bytes = frame.freeze();
        if sender.write_raw(&frame_bytes).await.is_err() {
            return;
        }
    }
}

fn not_authenticated_body(_api_key: i16, flexible: bool) -> Vec<u8> {
    use crate::kafka::protocol::types::{write_empty_tagged_fields, write_i16, write_i32};
    let mut buf = BytesMut::new();
    write_i32(&mut buf, 0); // throttle_time_ms
    write_i16(&mut buf, KafkaErrorCode::ClusterAuthorizationFailed as i16);
    if flexible {
        write_empty_tagged_fields(&mut buf);
    }
    buf.freeze().to_vec()
}

fn unsupported_version_body(flexible: bool) -> Vec<u8> {
    use crate::kafka::protocol::types::{write_empty_tagged_fields, write_i16, write_i32};
    let mut buf = BytesMut::new();
    write_i32(&mut buf, 0);
    write_i16(&mut buf, KafkaErrorCode::UnsupportedVersion as i16);
    if flexible {
        write_empty_tagged_fields(&mut buf);
    }
    buf.freeze().to_vec()
}
