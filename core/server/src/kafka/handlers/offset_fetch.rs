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

//! `OffsetFetch` handler (API key 9).
//!
//! Returns the last committed offset for each partition in a consumer group.
//! Returns -1 when no offset has been committed yet (Kafka convention).

use crate::kafka::error::iggy_to_kafka_error;
use crate::kafka::protocol::types::{
    read_compact_string, read_i32, read_string, read_unsigned_varint, skip_tagged_fields,
    write_compact_string, write_empty_tagged_fields, write_i16, write_i32, write_i64,
    write_nullable_string, write_string, write_unsigned_varint,
};
use crate::shard::IggyShard;
use crate::streaming::session::Session;
use bytes::{Bytes, BytesMut};
use iggy_common::{Consumer, ConsumerKind, Identifier};
use std::rc::Rc;

pub async fn handle(
    _api_version: i16,
    payload: &Bytes,
    flexible: bool,
    shard: &Rc<IggyShard>,
    session: &Session,
) -> Vec<u8> {
    let mut buf = payload.clone();

    let group_id = if flexible {
        read_compact_string(&mut buf)
    } else {
        read_string(&mut buf)
    };
    let topic_count = if flexible {
        (read_unsigned_varint(&mut buf) as i64) - 1
    } else {
        read_i32(&mut buf) as i64
    };

    let kafka_stream = shard.config.kafka.kafka_stream.clone();
    let stream_id = match Identifier::named(&kafka_stream) {
        Ok(id) => id,
        Err(_) => return empty_response(flexible),
    };
    let consumer = Consumer {
        kind: ConsumerKind::ConsumerGroup,
        id: match Identifier::named(&group_id) {
            Ok(id) => id,
            Err(_) => return empty_response(flexible),
        },
    };

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

        let resolved = shard.resolve_topic(&stream_id, &topic_id);
        let mut partitions: Vec<(i32, i16, i64)> = Vec::new();

        for _ in 0..partition_count.max(0) {
            let partition_index = read_i32(&mut buf);
            if flexible {
                skip_tagged_fields(&mut buf);
            }

            let (error_code, committed_offset) = match &resolved {
                Ok(rt) => {
                    match shard
                        .get_consumer_offset(
                            session.client_id,
                            consumer.clone(),
                            *rt,
                            Some(partition_index as u32 + 1),
                        )
                        .await
                    {
                        Ok(Some(info)) => (0i16, info.stored_offset as i64),
                        Ok(None) => (0i16, -1i64), // no committed offset yet
                        Err(e) => (iggy_to_kafka_error(&e), -1i64),
                    }
                }
                Err(e) => (iggy_to_kafka_error(e), -1i64),
            };
            partitions.push((partition_index, error_code, committed_offset));
        }
        if flexible {
            skip_tagged_fields(&mut buf);
        }
        topic_results.push((topic_name, partitions));
    }

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
        for &(partition_index, error_code, committed_offset) in partitions {
            write_i32(&mut body, partition_index);
            write_i64(&mut body, committed_offset);
            write_nullable_string(&mut body, None); // metadata
            write_i16(&mut body, error_code);
            if flexible {
                write_empty_tagged_fields(&mut body);
            }
        }
        if flexible {
            write_empty_tagged_fields(&mut body);
        }
    }
    write_i16(&mut body, 0); // top-level error_code
    if flexible {
        write_empty_tagged_fields(&mut body);
    }
    body.freeze().to_vec()
}

fn empty_response(flexible: bool) -> Vec<u8> {
    let mut body = BytesMut::new();
    write_i32(&mut body, 0);
    if flexible {
        write_unsigned_varint(&mut body, 1);
    } else {
        write_i32(&mut body, 0);
    }
    write_i16(&mut body, 0);
    if flexible {
        write_empty_tagged_fields(&mut body);
    }
    body.freeze().to_vec()
}
