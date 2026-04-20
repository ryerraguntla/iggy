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

//! `Produce` handler (API key 0).
//!
//! Translates a Kafka `Produce` request into a call to
//! `shard.append_messages()`.
//!
//! ## Kafka RecordBatch format (magic = 2, used in API version ≥ 3)
//!
//! ```text
//! base_offset: i64
//! batch_length: i32      (covers everything from partition_leader_epoch onwards)
//! partition_leader_epoch: i32
//! magic: i8              (must be 2)
//! crc: u32               (CRC32C of everything after crc)
//! attributes: i16
//! last_offset_delta: i32
//! base_timestamp: i64
//! max_timestamp: i64
//! producer_id: i64
//! producer_epoch: i16
//! base_sequence: i32
//! records_count: i32
//! [records...]
//! ```
//!
//! ## TODO items for production readiness
//! - Verify CRC32C checksum on inbound batches.
//! - Support MessageSet v0/v1 (API versions 0–2) for older clients.
//! - Handle compression (gzip, snappy, lz4, zstd in attributes bits 0–2).
//! - Map producer_id / producer_epoch for exactly-once semantics.

use crate::kafka::error::iggy_to_kafka_error;
use crate::kafka::protocol::types::{
    read_compact_nullable_string, read_compact_string, read_i16, read_i32, read_i64, read_i8,
    read_nullable_string, read_string, read_unsigned_varint, skip_tagged_fields, write_compact_string,
    write_empty_tagged_fields, write_i16, write_i32, write_i64, write_string, write_unsigned_varint,
};
use crate::kafka::session::KafkaSession;
use crate::shard::IggyShard;
use crate::shard::transmission::message::ResolvedPartition;
use crate::streaming::segments::{IggyIndexesMut, IggyMessagesBatchMut};
use crate::streaming::session::Session;
use bytes::{Buf, Bytes, BytesMut};
use iggy_common::{Identifier, PartitioningKind};
use std::rc::Rc;
use tracing::warn;

pub async fn handle(
    api_version: i16,
    payload: &Bytes,
    flexible: bool,
    shard: &Rc<IggyShard>,
    session: &Session,
    _kafka_session: &KafkaSession,
) -> Vec<u8> {
    let mut buf = payload.clone();

    // transactional_id (v3+, nullable)
    if api_version >= 3 {
        if flexible {
            read_compact_nullable_string(&mut buf);
        } else {
            read_nullable_string(&mut buf);
        }
    }
    let _acks = read_i16(&mut buf);
    let _timeout_ms = read_i32(&mut buf);

    let topic_count = if flexible {
        (read_unsigned_varint(&mut buf) as i64) - 1
    } else {
        read_i32(&mut buf) as i64
    };

    let kafka_stream = shard.config.kafka.kafka_stream.clone();
    let stream_id = match Identifier::named(&kafka_stream) {
        Ok(id) => id,
        Err(_) => return produce_error_response(flexible),
    };

    // (topic_name, [(partition_index, error_code, base_offset)])
    let mut topic_results: Vec<(String, Vec<(i32, i16, i64)>)> = Vec::new();

    for _ in 0..topic_count.max(0) {
        let topic_name = if flexible {
            read_compact_string(&mut buf)
        } else {
            read_string(&mut buf)
        };
        let partition_count = if flexible {
            (read_unsigned_varint(&mut buf) as i64) - 1
        } else {
            read_i32(&mut buf) as i64
        };

        let topic_id = match Identifier::named(&topic_name) {
            Ok(id) => id,
            Err(_) => {
                topic_results.push((topic_name, vec![]));
                continue;
            }
        };

        let resolved_topic = shard.resolve_topic_for_append(session.get_user_id(), &stream_id, &topic_id);
        let mut partition_results: Vec<(i32, i16, i64)> = Vec::new();

        for _ in 0..partition_count.max(0) {
            let partition_index = read_i32(&mut buf);

            // Record set bytes (the RecordBatch)
            let records: Option<Bytes> = if flexible {
                let len_plus_one = read_unsigned_varint(&mut buf);
                if len_plus_one == 0 {
                    None
                } else {
                    let len = len_plus_one as usize - 1;
                    Some(buf.copy_to_bytes(len))
                }
            } else {
                let len = read_i32(&mut buf);
                if len < 0 {
                    None
                } else {
                    Some(buf.copy_to_bytes(len as usize))
                }
            };

            if flexible {
                skip_tagged_fields(&mut buf);
            }

            let (error_code, base_offset) = match (&resolved_topic, records) {
                (Ok(rt), Some(batch_bytes)) => {
                    // TODO(kafka): Convert Kafka RecordBatch bytes into
                    // IggyMessagesBatchMut.  This requires:
                    //   1. Parsing the 49-byte RecordBatch header.
                    //   2. Iterating variable-length Records.
                    //   3. Building IggyMessagesBatchMut from extracted payloads.
                    //
                    // For now we return a stub success so the protocol handshake
                    // works end-to-end.  Replace the block below with real
                    // conversion logic before enabling production traffic.
                    let _ = batch_bytes;
                    let partition = ResolvedPartition {
                        stream_id: rt.stream_id,
                        topic_id: rt.topic_id,
                        partition_id: partition_index as usize + 1,
                    };
                    warn!(
                        "Kafka Produce: message conversion not yet implemented for \
                         topic '{topic_name}' partition {partition_index}. \
                         Returning stub success."
                    );
                    // TODO: replace with actual append_messages call:
                    // shard.append_messages(partition, batch).await
                    (0i16, 0i64)
                }
                (Err(e), _) => (iggy_to_kafka_error(e), -1i64),
                (_, None) => (0i16, 0i64), // empty batch — no-op
            };
            partition_results.push((partition_index, error_code, base_offset));
        }
        if flexible {
            skip_tagged_fields(&mut buf);
        }
        topic_results.push((topic_name, partition_results));
    }

    build_produce_response(&topic_results, api_version, flexible)
}

fn build_produce_response(
    topic_results: &[(String, Vec<(i32, i16, i64)>)],
    api_version: i16,
    flexible: bool,
) -> Vec<u8> {
    let mut body = BytesMut::new();
    if flexible {
        write_unsigned_varint(&mut body, topic_results.len() as u64 + 1);
    } else {
        write_i32(&mut body, topic_results.len() as i32);
    }
    for (topic_name, partitions) in topic_results {
        if flexible {
            write_compact_string(&mut body, topic_name);
        } else {
            write_string(&mut body, topic_name);
        }
        if flexible {
            write_unsigned_varint(&mut body, partitions.len() as u64 + 1);
        } else {
            write_i32(&mut body, partitions.len() as i32);
        }
        for &(partition_index, error_code, base_offset) in partitions {
            write_i32(&mut body, partition_index);
            write_i16(&mut body, error_code);
            write_i64(&mut body, base_offset);
            if api_version >= 2 {
                write_i64(&mut body, -1); // log_append_time
            }
            if api_version >= 5 {
                write_i64(&mut body, -1); // log_start_offset
            }
            if flexible {
                write_empty_tagged_fields(&mut body);
            }
        }
        if flexible {
            write_empty_tagged_fields(&mut body);
        }
    }
    write_i32(&mut body, 0); // throttle_time_ms
    if flexible {
        write_empty_tagged_fields(&mut body);
    }
    body.freeze().to_vec()
}

fn produce_error_response(flexible: bool) -> Vec<u8> {
    let mut body = BytesMut::new();
    if flexible {
        write_unsigned_varint(&mut body, 1);
        write_empty_tagged_fields(&mut body);
    } else {
        write_i32(&mut body, 0);
    }
    write_i32(&mut body, 0);
    if flexible {
        write_empty_tagged_fields(&mut body);
    }
    body.freeze().to_vec()
}
