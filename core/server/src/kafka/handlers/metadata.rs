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

//! `Metadata` (API key 3) and `CreateTopics` (API key 19) handlers.
//!
//! `Metadata` is the first request after `ApiVersions` and SASL.  It tells
//! the client which brokers exist and what partitions each topic has.  We
//! always advertise ourselves as the single-broker cluster (node_id = shard.id).
//!
//! `CreateTopics` maps Kafka topic creation to `shard.create_topic()` inside
//! the stream named by `kafka_config.kafka_stream`.

use crate::kafka::error::{KafkaErrorCode, iggy_to_kafka_error};
use crate::kafka::protocol::types::{
    read_compact_string, read_i16, read_i32, read_string, read_unsigned_varint, skip_tagged_fields,
    write_compact_nullable_string, write_compact_string, write_empty_tagged_fields, write_i8,
    write_i16, write_i32, write_nullable_string, write_string, write_unsigned_varint,
};
use crate::shard::IggyShard;
use crate::streaming::session::Session;
use bytes::{Bytes, BytesMut};
use iggy_common::{CompressionAlgorithm, IggyExpiry, MaxTopicSize};
use std::rc::Rc;
use tracing::debug;

/// Build a `Metadata` response for the requested topics.
///
/// - Presents `shard.id` as the only broker (`node_id`).
/// - Resolves each topic from the Kafka stream in Iggy metadata.
/// - Unknown topics get `UNKNOWN_TOPIC_OR_PARTITION` in their entry.
pub async fn handle(
    _api_version: i16,
    payload: &Bytes,
    flexible: bool,
    shard: &Rc<IggyShard>,
    session: &Session,
) -> Vec<u8> {
    let mut buf = payload.clone();

    // Parse requested topics (null/empty array means "all topics").
    let topic_count = if flexible {
        (read_unsigned_varint(&mut buf) as i64) - 1
    } else {
        read_i32(&mut buf) as i64
    };

    let mut requested: Vec<String> = Vec::new();
    for _ in 0..topic_count.max(0) {
        let name = if flexible {
            read_compact_string(&mut buf)
        } else {
            read_string(&mut buf)
        };
        requested.push(name);
        if flexible {
            skip_tagged_fields(&mut buf);
        }
    }
    if flexible {
        skip_tagged_fields(&mut buf); // request-level tagged fields
    }

    let kafka_stream = &shard.config.kafka.kafka_stream;
    let node_id = shard.id as i32;
    let bound_addr = shard
        .kafka_bound_address
        .get()
        .map(|a| a.to_string())
        .unwrap_or_else(|| shard.config.kafka.address.clone());
    let host = bound_addr
        .split(':')
        .next()
        .unwrap_or("127.0.0.1")
        .to_string();
    let port: i32 = bound_addr
        .split(':')
        .next_back()
        .and_then(|p| p.parse().ok())
        .unwrap_or(9092);

    // Collect topic metadata: (name, partition_count, error_code)
    let topics_meta = collect_topic_metadata(shard, session, &requested, kafka_stream).await;

    let mut body = BytesMut::new();
    write_i32(&mut body, 0); // throttle_time_ms

    // Brokers array — single broker (ourselves)
    if flexible {
        write_unsigned_varint(&mut body, 2); // 1 element compact length
    } else {
        write_i32(&mut body, 1);
    }
    write_i32(&mut body, node_id);
    if flexible {
        write_compact_string(&mut body, &host);
    } else {
        write_string(&mut body, &host);
    }
    write_i32(&mut body, port);
    write_nullable_string(&mut body, None); // rack (nullable)
    if flexible {
        write_empty_tagged_fields(&mut body);
    }

    // cluster_id (nullable string)
    write_nullable_string(&mut body, Some("iggy-kafka-bridge"));
    // controller_id
    write_i32(&mut body, node_id);

    // Topics array
    if flexible {
        write_unsigned_varint(&mut body, topics_meta.len() as u64 + 1);
    } else {
        write_i32(&mut body, topics_meta.len() as i32);
    }
    for (topic_name, partition_count, error_code) in &topics_meta {
        write_i16(&mut body, *error_code);
        if flexible {
            write_compact_string(&mut body, topic_name);
        } else {
            write_string(&mut body, topic_name);
        }
        write_i8(&mut body, 0); // is_internal = false

        // Partitions array
        let pc = *partition_count as u32;
        if flexible {
            write_unsigned_varint(&mut body, pc as u64 + 1);
        } else {
            write_i32(&mut body, pc as i32);
        }
        for p in 0..pc {
            write_i16(&mut body, 0); // partition error_code
            write_i32(&mut body, p as i32); // partition_index
            write_i32(&mut body, node_id); // leader_id
            write_i32(&mut body, 0); // leader_epoch
            // replica_nodes [node_id]
            if flexible {
                write_unsigned_varint(&mut body, 2);
            } else {
                write_i32(&mut body, 1);
            }
            write_i32(&mut body, node_id);
            // isr_nodes [node_id]
            if flexible {
                write_unsigned_varint(&mut body, 2);
            } else {
                write_i32(&mut body, 1);
            }
            write_i32(&mut body, node_id);
            // offline_replicas []
            if flexible {
                write_unsigned_varint(&mut body, 1);
            } else {
                write_i32(&mut body, 0);
            }
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

/// Returns `(topic_name, partition_count, kafka_error_code)` for each topic.
async fn collect_topic_metadata(
    shard: &IggyShard,
    session: &Session,
    requested: &[String],
    kafka_stream: &str,
) -> Vec<(String, usize, i16)> {
    use iggy_common::Identifier;

    let stream_id = match Identifier::named(kafka_stream) {
        Ok(id) => id,
        Err(_) => return vec![],
    };

    let user_id = session.get_user_id();

    let topic_names: Vec<String> = if requested.is_empty() {
        // Return all topics in the kafka stream
        match shard.metadata.query_topics(user_id, &stream_id) {
            Ok(Some(topics)) => topics.iter().map(|t| t.name.to_string()).collect(),
            _ => return vec![],
        }
    } else {
        requested.to_vec()
    };

    let mut result = Vec::with_capacity(topic_names.len());
    for name in topic_names {
        let topic_id = match iggy_common::Identifier::named(&name) {
            Ok(id) => id,
            Err(_) => {
                result.push((name, 0, KafkaErrorCode::UnknownTopicOrPartition as i16));
                continue;
            }
        };
        match shard.resolve_topic(&stream_id, &topic_id) {
            Ok(resolved) => {
                let count = shard
                    .metadata
                    .partitions_count(resolved.stream_id, resolved.topic_id);
                result.push((name, count, 0i16));
            }
            Err(e) => {
                debug!("Kafka Metadata: topic '{name}' not found: {e}");
                result.push((name, 0, iggy_to_kafka_error(&e)));
            }
        }
    }
    result
}

/// `CreateTopics` — create Iggy topics in the kafka stream on demand.
pub async fn handle_create_topics(
    _api_version: i16,
    payload: &Bytes,
    flexible: bool,
    shard: &Rc<IggyShard>,
    _session: &Session,
) -> Vec<u8> {
    use iggy_common::Identifier;

    let mut buf = payload.clone();
    let topic_count = if flexible {
        (read_unsigned_varint(&mut buf) as i64) - 1
    } else {
        read_i32(&mut buf) as i64
    };

    let kafka_stream = &shard.config.kafka.kafka_stream;
    let stream_id_str = kafka_stream.clone();

    let mut results: Vec<(String, i16)> = Vec::new();

    for _ in 0..topic_count.max(0) {
        let name = if flexible {
            read_compact_string(&mut buf)
        } else {
            read_string(&mut buf)
        };
        let _num_partitions = read_i32(&mut buf);
        let _replication_factor = read_i16(&mut buf);
        // Skip assignments and configs arrays
        let assign_count = if flexible {
            (read_unsigned_varint(&mut buf) as i64) - 1
        } else {
            read_i32(&mut buf) as i64
        };
        for _ in 0..assign_count.max(0) {
            if flexible {
                skip_tagged_fields(&mut buf);
            }
        }
        let config_count = if flexible {
            (read_unsigned_varint(&mut buf) as i64) - 1
        } else {
            read_i32(&mut buf) as i64
        };
        for _ in 0..config_count.max(0) {
            if flexible {
                skip_tagged_fields(&mut buf);
            }
        }
        if flexible {
            skip_tagged_fields(&mut buf);
        }

        let stream_id = match Identifier::named(&stream_id_str) {
            Ok(id) => id,
            Err(_) => {
                results.push((name, KafkaErrorCode::UnknownTopicOrPartition as i16));
                continue;
            }
        };

        let resolved_stream = match shard.resolve_stream(&stream_id) {
            Ok(s) => s,
            Err(e) => {
                results.push((name, iggy_to_kafka_error(&e)));
                continue;
            }
        };

        let error_code = match shard
            .create_topic(
                resolved_stream,
                name.clone(),
                IggyExpiry::NeverExpire,
                CompressionAlgorithm::None,
                MaxTopicSize::ServerDefault,
                None,
            )
            .await
        {
            Ok(_) => {
                // Create partitions for the new topic
                // TODO: call create_partitions with num_partitions
                0i16
            }
            Err(e) => iggy_to_kafka_error(&e),
        };
        results.push((name, error_code));
    }

    let mut body = BytesMut::new();
    write_i32(&mut body, 0); // throttle_time_ms
    if flexible {
        write_unsigned_varint(&mut body, results.len() as u64 + 1);
    } else {
        write_i32(&mut body, results.len() as i32);
    }
    for (name, err) in &results {
        if flexible {
            write_compact_string(&mut body, name);
        } else {
            write_string(&mut body, name);
        }
        write_i16(&mut body, *err);
        write_compact_nullable_string(&mut body, None); // error_message
        if flexible {
            write_empty_tagged_fields(&mut body);
        }
    }
    if flexible {
        write_empty_tagged_fields(&mut body);
    }
    body.freeze().to_vec()
}
