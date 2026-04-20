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

//! `OffsetCommit` handler (API key 8).
//!
//! Persists consumer group offsets via `shard.store_consumer_offset()`.
//! Maps Kafka partition offsets (0-indexed) to Iggy partition IDs (1-indexed).

use crate::kafka::error::iggy_to_kafka_error;
use crate::kafka::protocol::types::{
    read_compact_nullable_string, read_compact_string, read_i32, read_i64, read_nullable_string,
    read_string, read_unsigned_varint, skip_tagged_fields, write_compact_string,
    write_empty_tagged_fields, write_i16, write_i32, write_string, write_unsigned_varint,
};
use crate::shard::IggyShard;
use crate::streaming::session::Session;
use bytes::{Bytes, BytesMut};
use iggy_common::{Consumer, ConsumerKind, Identifier};
use std::rc::Rc;

pub async fn handle(
    api_version: i16,
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
    if api_version >= 1 {
        let _generation_id = read_i32(&mut buf);
    }
    if api_version >= 1 {
        if flexible {
            read_compact_nullable_string(&mut buf);
        } else {
            read_nullable_string(&mut buf);
        }
    }
    if api_version >= 7 {
        if flexible {
            read_compact_nullable_string(&mut buf);
        } else {
            read_nullable_string(&mut buf);
        }
    }

    let topic_count = if flexible {
        (read_unsigned_varint(&mut buf) as i64) - 1
    } else {
        read_i32(&mut buf) as i64
    };

    let kafka_stream = shard.config.kafka.kafka_stream.clone();
    let stream_id = match Identifier::named(&kafka_stream) {
        Ok(id) => id,
        Err(_) => return empty_ok_response(flexible),
    };

    let consumer = Consumer {
        kind: ConsumerKind::ConsumerGroup,
        id: match Identifier::named(&group_id) {
            Ok(id) => id,
            Err(_) => return empty_ok_response(flexible),
        },
    };

    let mut topic_results: Vec<(String, Vec<(i32, i16)>)> = Vec::new();

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

        let resolved = shard.resolve_topic_for_store_consumer_offset(
            session.get_user_id(),
            &stream_id,
            &topic_id,
        );

        let mut partition_results: Vec<(i32, i16)> = Vec::new();
        for _ in 0..partition_count.max(0) {
            let partition_index = read_i32(&mut buf);
            let committed_offset = read_i64(&mut buf);
            if api_version >= 6 {
                let _leader_epoch = read_i32(&mut buf);
            }
            if api_version == 1 {
                let _commit_timestamp = read_i64(&mut buf);
            }
            if flexible {
                read_compact_nullable_string(&mut buf); // metadata
                skip_tagged_fields(&mut buf);
            } else {
                read_nullable_string(&mut buf); // metadata
            }

            let error_code = match &resolved {
                Ok(rt) => {
                    match shard
                        .store_consumer_offset(
                            session.client_id,
                            consumer.clone(),
                            *rt,
                            Some(partition_index as u32 + 1),
                            committed_offset as u64,
                        )
                        .await
                    {
                        Ok(_) => 0i16,
                        Err(e) => iggy_to_kafka_error(&e),
                    }
                }
                Err(e) => iggy_to_kafka_error(e),
            };
            partition_results.push((partition_index, error_code));
        }
        if flexible {
            skip_tagged_fields(&mut buf);
        }
        topic_results.push((topic_name, partition_results));
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
        for &(partition_index, error_code) in partitions {
            write_i32(&mut body, partition_index);
            write_i16(&mut body, error_code);
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

fn empty_ok_response(flexible: bool) -> Vec<u8> {
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
