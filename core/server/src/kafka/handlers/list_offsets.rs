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

//! `ListOffsets` handler (API key 2).
//!
//! Clients use this to discover the earliest (`timestamp = -2`) or latest
//! (`timestamp = -1`) offset for a partition.  We resolve the Iggy topic and
//! read partition stats from the metadata.
//!
//! Special timestamp values:
//! - `-2` → `EARLIEST_TIMESTAMP`: return the smallest available offset.
//! - `-1` → `LATEST_TIMESTAMP`: return the next offset to be appended.

use crate::kafka::error::iggy_to_kafka_error;
use crate::kafka::protocol::types::{
    read_compact_string, read_i32, read_i64, read_i8, read_string, read_unsigned_varint,
    skip_tagged_fields, write_compact_string, write_empty_tagged_fields, write_i16, write_i32,
    write_i64, write_string, write_unsigned_varint,
};
use crate::shard::IggyShard;
use crate::streaming::session::Session;
use bytes::{Bytes, BytesMut};
use iggy_common::Identifier;
use std::rc::Rc;

const EARLIEST_TIMESTAMP: i64 = -2;
const LATEST_TIMESTAMP: i64 = -1;

pub async fn handle(
    api_version: i16,
    payload: &Bytes,
    flexible: bool,
    shard: &Rc<IggyShard>,
    session: &Session,
) -> Vec<u8> {
    let mut buf = payload.clone();

    let _replica_id = read_i32(&mut buf);
    let _isolation_level = if api_version >= 2 { read_i8(&mut buf) } else { 0 };

    let topic_count = if flexible {
        (read_unsigned_varint(&mut buf) as i64) - 1
    } else {
        read_i32(&mut buf) as i64
    };

    let kafka_stream = shard.config.kafka.kafka_stream.clone();
    let stream_id = match Identifier::named(&kafka_stream) {
        Ok(id) => id,
        Err(_) => return error_response(flexible),
    };

    // Vec of (topic_name, Vec<(partition_index, error_code, offset)>)
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
                // Skip remaining partitions and record error
                for _ in 0..partition_count.max(0) {
                    if flexible {
                        skip_tagged_fields(&mut buf);
                    }
                }
                topic_results.push((topic_name, vec![]));
                continue;
            }
        };

        let resolved = shard.resolve_topic(&stream_id, &topic_id);
        let mut partition_results: Vec<(i32, i16, i64)> = Vec::new();

        for _ in 0..partition_count.max(0) {
            let partition_index = read_i32(&mut buf);
            if api_version == 0 {
                let _max_num_offsets = read_i32(&mut buf);
            }
            let timestamp = read_i64(&mut buf);
            if flexible {
                skip_tagged_fields(&mut buf);
            }

            let (error_code, offset) = match &resolved {
                Ok(rt) => {
                    let partition_id = partition_index as usize + 1; // Iggy is 1-indexed
                    match shard
                        .metadata
                        .get_partition_stats_by_ids(rt.stream_id, rt.topic_id, partition_id)
                    {
                        Some(stats) => {
                            let offset = if timestamp == EARLIEST_TIMESTAMP {
                                0i64 // earliest always starts at 0
                            } else {
                                stats.current_offset() as i64 + 1 // next offset to append
                            };
                            (0i16, offset)
                        }
                        None => (iggy_to_kafka_error(&iggy_common::IggyError::PartitionNotFound(
                            partition_id,
                            topic_id.clone(),
                            stream_id.clone(),
                        )), -1i64),
                    }
                }
                Err(e) => (iggy_to_kafka_error(e), -1i64),
            };
            partition_results.push((partition_index, error_code, offset));
        }
        if flexible {
            skip_tagged_fields(&mut buf);
        }
        topic_results.push((topic_name, partition_results));
    }

    // Build response
    let mut body = BytesMut::new();
    write_i32(&mut body, 0); // throttle_time_ms

    if flexible {
        write_unsigned_varint(&mut body, topic_results.len() as u64 + 1);
    } else {
        write_i32(&mut body, topic_results.len() as i32);
    }
    for (topic_name, partitions) in &topic_results {
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
        for &(partition_index, error_code, offset) in partitions {
            write_i32(&mut body, partition_index);
            write_i16(&mut body, error_code);
            if api_version >= 1 {
                write_i64(&mut body, -1); // timestamp (not tracked)
            }
            write_i64(&mut body, offset);
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

fn error_response(flexible: bool) -> Vec<u8> {
    let mut body = BytesMut::new();
    write_i32(&mut body, 0);
    if flexible {
        write_unsigned_varint(&mut body, 1);
        write_empty_tagged_fields(&mut body);
    } else {
        write_i32(&mut body, 0);
    }
    body.freeze().to_vec()
}
