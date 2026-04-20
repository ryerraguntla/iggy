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

//! `Fetch` handler (API key 1).
//!
//! Translates a Kafka `Fetch` request into a call to `shard.poll_messages()`
//! and re-encodes the result as a Kafka `RecordBatch`.
//!
//! ## TODO items for production readiness
//! - Encode polled `IggyMessagesBatchSet` as valid Kafka RecordBatch bytes,
//!   including the 49-byte header and per-record variable-length encoding.
//! - Support `max_wait_ms` / `min_bytes` long-polling semantics.
//! - Handle compression attributes on outbound batches.
//! - Compute correct CRC32C on each RecordBatch.

use crate::kafka::error::iggy_to_kafka_error;
use crate::kafka::protocol::types::{
    read_compact_string, read_i8, read_i32, read_i64, read_string, read_unsigned_varint,
    skip_tagged_fields, write_compact_string, write_empty_tagged_fields, write_i16, write_i32,
    write_i64, write_string, write_unsigned_varint,
};
use crate::kafka::session::KafkaSession;
use crate::shard::IggyShard;
use crate::shard::system::messages::PollingArgs;
use crate::streaming::session::Session;
use bytes::{Bytes, BytesMut};
use iggy_common::{Consumer, ConsumerKind, Identifier, PollingStrategy};
use std::rc::Rc;
use tracing::warn;

type PartitionFetchResult = (i32, i16, i64, Vec<u8>);
type TopicFetchResults = Vec<(String, Vec<PartitionFetchResult>)>;

pub async fn handle(
    api_version: i16,
    payload: &Bytes,
    flexible: bool,
    shard: &Rc<IggyShard>,
    session: &Session,
    kafka_session: &KafkaSession,
) -> Vec<u8> {
    let mut buf = payload.clone();

    let _replica_id = read_i32(&mut buf);
    let _max_wait_ms = read_i32(&mut buf);
    let _min_bytes = read_i32(&mut buf);
    if api_version >= 3 {
        let _max_bytes = read_i32(&mut buf);
    }
    if api_version >= 4 {
        let _isolation_level = read_i8(&mut buf);
    }
    if api_version >= 7 {
        let _session_id = read_i32(&mut buf);
        let _session_epoch = read_i32(&mut buf);
    }

    let topic_count = if flexible {
        (read_unsigned_varint(&mut buf) as i64) - 1
    } else {
        read_i32(&mut buf) as i64
    };

    let kafka_stream = shard.config.kafka.kafka_stream.clone();
    let stream_id = match Identifier::named(&kafka_stream) {
        Ok(id) => id,
        Err(_) => return empty_fetch_response(api_version, flexible),
    };

    let consumer = Consumer {
        kind: if kafka_session.group_id.is_some() {
            ConsumerKind::ConsumerGroup
        } else {
            ConsumerKind::Consumer
        },
        id: if let Some(gid) = &kafka_session.group_id {
            Identifier::named(gid)
                .unwrap_or_else(|_| Identifier::numeric(kafka_session.client_id).unwrap())
        } else {
            Identifier::numeric(kafka_session.client_id).unwrap()
        },
    };

    // (topic_name, [(partition_index, error_code, high_watermark, records_bytes)])
    let mut topic_results: TopicFetchResults = Vec::new();

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

        let resolved_topic =
            shard.resolve_topic_for_poll(session.get_user_id(), &stream_id, &topic_id);
        let mut partition_results: Vec<PartitionFetchResult> = Vec::new();

        for _ in 0..partition_count.max(0) {
            let partition_index = read_i32(&mut buf);
            if api_version >= 9 {
                let _current_leader_epoch = read_i32(&mut buf);
            }
            let fetch_offset = read_i64(&mut buf);
            if api_version >= 5 {
                let _log_start_offset = read_i64(&mut buf);
            }
            let partition_max_bytes = read_i32(&mut buf);
            if flexible {
                skip_tagged_fields(&mut buf);
            }

            let (error_code, high_watermark, records) = match &resolved_topic {
                Ok(rt) => {
                    let strategy = PollingStrategy::offset(fetch_offset as u64);
                    let count = ((partition_max_bytes as u32) / 1024).clamp(1, 1000);
                    let args = PollingArgs::new(strategy, count, false);

                    match shard
                        .poll_messages(
                            session.client_id,
                            *rt,
                            consumer.clone(),
                            Some(partition_index as u32 + 1),
                            args,
                        )
                        .await
                    {
                        Ok((_meta, _batch)) => {
                            // TODO(kafka): Convert IggyMessagesBatchSet to Kafka
                            // RecordBatch bytes.  The RecordBatch encoding requires:
                            //   1. A 49-byte header (base_offset, crc, attributes, etc.)
                            //   2. Per-record variable-length encoding with zigzag varints.
                            //   3. CRC32C computation over the whole batch.
                            //
                            // For now return empty records so Fetch works end-to-end
                            // without crashing the connection.
                            warn!(
                                "Kafka Fetch: RecordBatch serialization not yet \
                                 implemented for topic '{topic_name}' partition \
                                 {partition_index}. Returning empty batch."
                            );
                            (0i16, fetch_offset, vec![])
                        }
                        Err(e) => (iggy_to_kafka_error(&e), -1i64, vec![]),
                    }
                }
                Err(e) => (iggy_to_kafka_error(e), -1i64, vec![]),
            };
            partition_results.push((partition_index, error_code, high_watermark, records));
        }
        if flexible {
            skip_tagged_fields(&mut buf);
        }
        topic_results.push((topic_name, partition_results));
    }

    build_fetch_response(&topic_results, api_version, flexible)
}

fn build_fetch_response(
    topic_results: &[(String, Vec<PartitionFetchResult>)],
    api_version: i16,
    flexible: bool,
) -> Vec<u8> {
    let mut body = BytesMut::new();
    write_i32(&mut body, 0); // throttle_time_ms
    if api_version >= 7 {
        write_i16(&mut body, 0); // error_code
        write_i32(&mut body, 0); // session_id
    }

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
        for (partition_index, error_code, high_watermark, records) in partitions {
            write_i32(&mut body, *partition_index);
            write_i16(&mut body, *error_code);
            write_i64(&mut body, *high_watermark);
            if api_version >= 4 {
                write_i64(&mut body, -1); // last_stable_offset
            }
            if api_version >= 5 {
                write_i64(&mut body, 0); // log_start_offset
            }
            if api_version >= 4 {
                // aborted_transactions array (empty)
                if flexible {
                    write_unsigned_varint(&mut body, 1);
                } else {
                    write_i32(&mut body, 0);
                }
            }
            if api_version >= 11 {
                write_i32(&mut body, -1); // preferred_read_replica
            }
            // records (bytes field)
            if flexible {
                write_unsigned_varint(&mut body, records.len() as u64 + 1);
            } else {
                write_i32(&mut body, records.len() as i32);
            }
            body.extend_from_slice(records);
            if flexible {
                write_empty_tagged_fields(&mut body);
            }
        }
        if flexible {
            write_empty_tagged_fields(&mut body);
        }
    }
    if flexible {
        write_empty_tagged_fields(&mut body);
    }
    body.freeze().to_vec()
}

fn empty_fetch_response(api_version: i16, flexible: bool) -> Vec<u8> {
    build_fetch_response(&[], api_version, flexible)
}
