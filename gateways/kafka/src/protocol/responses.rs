// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Kafka response encoders (stub implementations).

#![allow(clippy::pedantic)]

use crate::bridge::mapping::KAFKA_PARTITIONS_USE_DEFAULT;
use crate::protocol::api::{
    ERROR_INVALID_PARTITIONS, ERROR_NONE, ERROR_UNKNOWN_TOPIC_OR_PARTITION,
};
use crate::protocol::codec::Encoder;
use crate::protocol::requests::{
    CreateTopicsRequest, FetchRequest, ListOffsetsRequest, ProducePartitionData, ProduceRequest,
    ProduceTopicData,
};
use bytes::{BufMut, Bytes, BytesMut};

/// Well-formed Produce response with a single placeholder topic/partition.
pub fn encode_produce_error_response(version: i16, error_code: i16) -> Bytes {
    let topics = vec![ProduceTopicData {
        topic: String::new(), // TODO topic name will be populated in the end to end functional completion
        partitions: vec![ProducePartitionData {
            partition: 0,
            records: None,
        }],
    }];
    encode_produce_response_inner(version, &topics, error_code)
}

pub fn encode_produce_response(version: i16, req: &ProduceRequest) -> Bytes {
    encode_produce_response_inner(version, &req.topics, ERROR_NONE)
}

fn encode_produce_response_inner(
    version: i16,
    topics: &[ProduceTopicData],
    partition_error: i16,
) -> Bytes {
    let flexible = version >= 9;
    let mut e = Encoder::with_capacity(512);

    if flexible {
        e.write_varint((topics.len() + 1) as u64);
    } else {
        e.write_i32(i32::try_from(topics.len()).expect("topic count bounded"));
    }

    for topic in topics {
        if flexible {
            e.write_compact_nullable_string(Some(&topic.topic));
        } else {
            e.write_nullable_string_unchecked(Some(&topic.topic));
        }

        if flexible {
            e.write_varint((topic.partitions.len() + 1) as u64);
        } else {
            e.write_i32(i32::try_from(topic.partitions.len()).expect("partition count bounded"));
        }

        for p in &topic.partitions {
            e.write_i32(p.partition);
            e.write_i16(partition_error);
            e.write_i64(0);
            if version >= 2 {
                e.write_i64(-1);
            }
            if version >= 5 {
                e.write_i64(0);
            }
            if version >= 8 {
                if flexible {
                    e.write_varint(1);
                    e.write_compact_nullable_string(None);
                } else {
                    e.write_i32(0);
                    e.write_nullable_string_unchecked(None);
                }
            }
            if flexible {
                e.write_empty_tagged_fields();
            }
        }

        if flexible {
            e.write_empty_tagged_fields();
        }
    }

    if version >= 1 {
        e.write_i32(0);
    }
    if flexible {
        e.write_empty_tagged_fields();
    }

    e.freeze()
}

/// Well-formed Fetch response. Uses top-level `error_code` at v7+, or a single
/// placeholder topic/partition with per-partition `error_code` below v7.
pub fn encode_fetch_error_response(version: i16, error_code: i16) -> Bytes {
    use crate::protocol::requests::{FetchPartition, FetchTopic};

    if version >= 7 {
        return encode_fetch_response_inner(version, &[], Some(error_code), error_code);
    }

    let topics = vec![FetchTopic {
        topic: String::new(),
        partitions: vec![FetchPartition {
            partition: 0,
            fetch_offset: 0,
            partition_max_bytes: 1,
        }],
    }];
    encode_fetch_response_inner(version, &topics, Some(ERROR_NONE), error_code)
}

pub fn encode_fetch_response(version: i16, req: &FetchRequest) -> Bytes {
    encode_fetch_response_inner(version, &req.topics, Some(ERROR_NONE), ERROR_NONE)
}

fn encode_fetch_response_inner(
    version: i16,
    topics: &[crate::protocol::requests::FetchTopic],
    top_level_error: Option<i16>,
    partition_error: i16,
) -> Bytes {
    let flexible = version >= 12;
    let mut e = Encoder::with_capacity(512);

    if version >= 1 {
        e.write_i32(0);
    }
    if version >= 7 {
        e.write_i16(top_level_error.unwrap_or(ERROR_NONE));
        e.write_i32(0);
    }

    if flexible {
        e.write_varint((topics.len() + 1) as u64);
    } else {
        e.write_i32(i32::try_from(topics.len()).expect("topic count bounded"));
    }

    for topic in topics {
        if flexible {
            e.write_compact_nullable_string(Some(&topic.topic));
        } else {
            e.write_nullable_string_unchecked(Some(&topic.topic));
        }

        if flexible {
            e.write_varint((topic.partitions.len() + 1) as u64);
        } else {
            e.write_i32(i32::try_from(topic.partitions.len()).expect("partition count bounded"));
        }

        for partition in &topic.partitions {
            e.write_i32(partition.partition);
            e.write_i16(partition_error);
            e.write_i64(0); // high_watermark
            if version >= 4 {
                e.write_i64(0); // last_stable_offset
            }
            if version >= 5 {
                e.write_i64(0); // log_start_offset
            }
            if version >= 4 {
                if flexible {
                    e.write_varint(1); // empty aborted_transactions
                } else {
                    e.write_i32(0); // empty aborted_transactions
                }
            }
            if version >= 11 {
                e.write_i32(-1); // preferred_read_replica
            }
            if flexible {
                e.write_compact_nullable_bytes(None);
            } else {
                e.write_null_bytes();
            }
            if flexible {
                e.write_empty_tagged_fields();
            }
        }

        if flexible {
            e.write_empty_tagged_fields();
        }
    }

    if flexible {
        e.write_empty_tagged_fields();
    }

    e.freeze()
}

/// Well-formed ListOffsets response with a single placeholder topic/partition.
pub fn encode_list_offsets_error_response(version: i16, error_code: i16) -> Bytes {
    use crate::protocol::requests::{ListOffsetsPartition, ListOffsetsTopic};

    let topics = vec![ListOffsetsTopic {
        topic: String::new(),
        partitions: vec![ListOffsetsPartition {
            partition: 0,
            timestamp: -1,
        }],
    }];
    encode_list_offsets_response_inner(version, &topics, error_code)
}

pub fn encode_list_offsets_response(version: i16, req: &ListOffsetsRequest) -> Bytes {
    encode_list_offsets_response_inner(version, &req.topics, ERROR_NONE)
}

fn encode_list_offsets_response_inner(
    version: i16,
    topics: &[crate::protocol::requests::ListOffsetsTopic],
    partition_error: i16,
) -> Bytes {
    let flexible = version >= 6;
    let mut e = Encoder::with_capacity(256);

    if version >= 2 {
        e.write_i32(0);
    }

    if flexible {
        e.write_varint((topics.len() + 1) as u64);
    } else {
        e.write_i32(i32::try_from(topics.len()).expect("topic count bounded"));
    }

    for topic in topics {
        if flexible {
            e.write_compact_nullable_string(Some(&topic.topic));
        } else {
            e.write_nullable_string_unchecked(Some(&topic.topic));
        }

        if flexible {
            e.write_varint((topic.partitions.len() + 1) as u64);
        } else {
            e.write_i32(i32::try_from(topic.partitions.len()).expect("partition count bounded"));
        }

        for partition in &topic.partitions {
            e.write_i32(partition.partition);
            e.write_i16(partition_error);

            let offset = 0i64;
            if version >= 1 {
                e.write_i64(-1); // -1 = timestamp not available (Kafka sentinel)
            }
            e.write_i64(offset);
            if version >= 4 {
                e.write_i32(-1);
            }
            if flexible {
                e.write_empty_tagged_fields();
            }
        }

        if flexible {
            e.write_empty_tagged_fields();
        }
    }

    if flexible {
        e.write_empty_tagged_fields();
    }

    e.freeze()
}

/// Well-formed CreateTopics response with a single placeholder topic.
pub fn encode_create_topics_error_response(version: i16, error_code: i16) -> Bytes {
    use crate::protocol::requests::CreatableTopic;

    let topics = vec![CreatableTopic {
        name: String::new(),
        num_partitions: 1,
        replication_factor: 1,
    }];
    encode_create_topics_response_inner(version, &topics, error_code)
}

pub fn encode_create_topics_response(version: i16, req: &CreateTopicsRequest) -> Bytes {
    encode_create_topics_response_inner(version, &req.topics, ERROR_NONE)
}

fn encode_create_topics_response_inner(
    version: i16,
    topics: &[crate::protocol::requests::CreatableTopic],
    topic_error: i16,
) -> Bytes {
    let flexible = version >= 5;
    let mut e = Encoder::with_capacity(256);

    if version >= 2 {
        e.write_i32(0);
    }

    if flexible {
        e.write_varint((topics.len() + 1) as u64);
    } else {
        e.write_i32(i32::try_from(topics.len()).expect("topic count bounded"));
    }

    for topic in topics {
        if flexible {
            e.write_compact_nullable_string(Some(&topic.name));
        } else {
            e.write_nullable_string_unchecked(Some(&topic.name));
        }

        let error_code = if topic_error != ERROR_NONE {
            topic_error
        } else if topic.num_partitions <= 0 && topic.num_partitions != KAFKA_PARTITIONS_USE_DEFAULT {
            ERROR_INVALID_PARTITIONS
        } else {
            ERROR_NONE
        };
        e.write_i16(error_code);

        if version >= 1 {
            if flexible {
                e.write_compact_nullable_string(None);
            } else {
                e.write_nullable_string_unchecked(None);
            }
        }

        if version >= 5 {
            e.write_i32(topic.num_partitions);
            e.write_i16(topic.replication_factor);
            e.write_varint(1);
        }

        if flexible {
            e.write_empty_tagged_fields();
        }
    }

    if flexible {
        e.write_empty_tagged_fields();
    }

    e.freeze()
}

// ── Bridge-backed responses (Phase 1B) ───────────────────────────────────────

/// Per-partition Produce outcome from the Iggy bridge.
#[derive(Debug, Clone, Copy)]
pub struct ProducePartitionOutcome {
    pub partition: i32,
    pub error_code: i16,
    pub base_offset: i64,
}

/// Per-partition Fetch outcome from the Iggy bridge.
#[derive(Debug, Clone)]
pub struct FetchPartitionOutcome {
    pub partition: i32,
    pub error_code: i16,
    pub high_watermark: i64,
    pub log_start_offset: i64,
    pub records: Option<Bytes>,
}

/// Per-partition ListOffsets outcome from the Iggy bridge.
#[derive(Debug, Clone, Copy)]
pub struct ListOffsetsPartitionOutcome {
    pub partition: i32,
    pub error_code: i16,
    pub offset: i64,
}

/// Metadata topic entry backed by Iggy state.
#[derive(Debug, Clone)]
pub struct MetadataTopicOutcome {
    pub name: String,
    pub error_code: i16,
    pub partitions_count: u32,
}

/// Advertised broker id returned in Metadata partition entries.
const METADATA_BROKER_NODE_ID: i32 = 1;

/// Sentinel when the gateway does not track leader epochs.
const METADATA_LEADER_EPOCH: i32 = -1;

/// Single-broker cluster: leader is the only replica and ISR member.
const METADATA_SINGLE_BROKER_REPLICA: [i32; 1] = [METADATA_BROKER_NODE_ID];

/// Encode one successful `MetadataResponsePartition` (Kafka keys 3, v0–v9 in scope).
fn write_metadata_partition(e: &mut Encoder, api_version: i16, partition_index: u32) {
    let partition_index =
        i32::try_from(partition_index).expect("metadata partition index fits i32");
    let flexible = api_version >= 9;

    e.write_i16(ERROR_NONE);
    e.write_i32(partition_index);
    e.write_i32(METADATA_BROKER_NODE_ID);

    if api_version >= 7 {
        e.write_i32(METADATA_LEADER_EPOCH);
    }

    if flexible {
        e.write_compact_i32_array(&METADATA_SINGLE_BROKER_REPLICA);
        e.write_compact_i32_array(&METADATA_SINGLE_BROKER_REPLICA);
        if api_version >= 5 {
            e.write_compact_i32_array(&[]);
        }
        e.write_empty_tagged_fields();
    } else {
        e.write_legacy_i32_array(&METADATA_SINGLE_BROKER_REPLICA);
        e.write_legacy_i32_array(&METADATA_SINGLE_BROKER_REPLICA);
        if api_version >= 5 {
            e.write_legacy_i32_array(&[]);
        }
    }
}

/// Encode a Produce response for one or more topics.
pub fn encode_produce_response_from_topic_outcomes(
    version: i16,
    topics: &[(String, Vec<ProducePartitionOutcome>)],
) -> Bytes {
    let produce_topics: Vec<ProduceTopicData> = topics
        .iter()
        .map(|(name, outcomes)| ProduceTopicData {
            topic: name.clone(),
            partitions: outcomes
                .iter()
                .map(|o| ProducePartitionData {
                    partition: o.partition,
                    records: None,
                })
                .collect(),
        })
        .collect();
    let flat_outcomes: Vec<ProducePartitionOutcome> =
        topics.iter().flat_map(|(_, o)| o.iter().copied()).collect();
    encode_produce_response_with_offsets(version, &produce_topics, &flat_outcomes)
}

fn encode_produce_response_with_offsets(
    version: i16,
    topics: &[ProduceTopicData],
    outcomes: &[ProducePartitionOutcome],
) -> Bytes {
    let flexible = version >= 9;
    let mut e = Encoder::with_capacity(512);
    let mut outcome_idx = 0;

    if flexible {
        e.write_varint((topics.len() + 1) as u64);
    } else {
        e.write_i32(i32::try_from(topics.len()).expect("topic count bounded"));
    }

    for topic in topics {
        if flexible {
            e.write_compact_nullable_string(Some(&topic.topic));
        } else {
            let _ = e.write_nullable_string(Some(&topic.topic));
        }

        if flexible {
            e.write_varint((topic.partitions.len() + 1) as u64);
        } else {
            e.write_i32(i32::try_from(topic.partitions.len()).expect("partition count bounded"));
        }

        for p in &topic.partitions {
            let outcome = outcomes.get(outcome_idx).copied().unwrap_or(ProducePartitionOutcome {
                partition: p.partition,
                error_code: ERROR_NONE,
                base_offset: 0,
            });
            outcome_idx += 1;

            e.write_i32(p.partition);
            e.write_i16(outcome.error_code);
            e.write_i64(outcome.base_offset);
            if version >= 2 {
                e.write_i64(-1);
            }
            if version >= 5 {
                e.write_i64(0);
            }
            if version >= 8 {
                if flexible {
                    e.write_varint(1);
                    e.write_compact_nullable_string(None);
                } else {
                    e.write_i32(0);
                    let _ = e.write_nullable_string(None);
                }
            }
            if flexible {
                e.write_empty_tagged_fields();
            }
        }

        if flexible {
            e.write_empty_tagged_fields();
        }
    }

    if version >= 1 {
        e.write_i32(0);
    }
    if flexible {
        e.write_empty_tagged_fields();
    }

    e.freeze()
}

/// Encode a Fetch response for one or more topics.
pub fn encode_fetch_response_from_topic_outcomes(
    version: i16,
    topics: &[(String, Vec<FetchPartitionOutcome>)],
) -> Bytes {
    let flexible = version >= 12;
    let mut e = Encoder::with_capacity(1024);

    if version >= 1 {
        e.write_i32(0);
    }
    if version >= 7 {
        e.write_i16(ERROR_NONE);
        e.write_i32(0);
    }

    if flexible {
        e.write_varint((topics.len() + 1) as u64);
    } else {
        e.write_i32(i32::try_from(topics.len()).unwrap_or(i32::MAX));
    }

    for (topic, outcomes) in topics {
        if flexible {
            e.write_compact_nullable_string(Some(topic));
        } else {
            let _ = e.write_nullable_string(Some(topic));
        }

        if flexible {
            e.write_varint((outcomes.len() + 1) as u64);
        } else {
            e.write_i32(i32::try_from(outcomes.len()).unwrap_or(i32::MAX));
        }

        for outcome in outcomes {
            e.write_i32(outcome.partition);
            e.write_i16(outcome.error_code);
            e.write_i64(outcome.high_watermark);
            if version >= 4 {
                e.write_i64(outcome.high_watermark);
            }
            if version >= 5 {
                e.write_i64(outcome.log_start_offset);
            }
            if version >= 4 {
                if flexible {
                    e.write_varint(1);
                } else {
                    e.write_i32(0);
                }
            }
            if version >= 11 {
                e.write_i32(-1);
            }
            if flexible {
                e.write_compact_nullable_bytes(outcome.records.as_deref());
            } else {
                let _ = e.write_nullable_bytes(outcome.records.as_deref());
            }
            if flexible {
                e.write_empty_tagged_fields();
            }
        }

        if flexible {
            e.write_empty_tagged_fields();
        }
    }

    if flexible {
        e.write_empty_tagged_fields();
    }

    e.freeze()
}

/// Encode a ListOffsets response for one or more topics.
pub fn encode_list_offsets_response_from_topic_outcomes(
    version: i16,
    topics: &[(String, Vec<ListOffsetsPartitionOutcome>)],
) -> Bytes {
    let flexible = version >= 6;
    let mut e = Encoder::with_capacity(256);

    if version >= 2 {
        e.write_i32(0);
    }

    if flexible {
        e.write_varint((topics.len() + 1) as u64);
    } else {
        e.write_i32(i32::try_from(topics.len()).unwrap_or(i32::MAX));
    }

    for (topic, outcomes) in topics {
        if flexible {
            e.write_compact_nullable_string(Some(topic));
            e.write_varint((outcomes.len() + 1) as u64);
        } else {
            let _ = e.write_nullable_string(Some(topic));
            e.write_i32(i32::try_from(outcomes.len()).unwrap_or(i32::MAX));
        }

        for outcome in outcomes {
            e.write_i32(outcome.partition);
            e.write_i16(outcome.error_code);
            if version >= 1 {
                e.write_i64(-1);
            }
            e.write_i64(outcome.offset);
            if version >= 4 {
                e.write_i32(-1);
            }
            if flexible {
                e.write_empty_tagged_fields();
            }
        }

        if flexible {
            e.write_empty_tagged_fields();
        }
    }

    if flexible {
        e.write_empty_tagged_fields();
    }

    e.freeze()
}

/// Encode Metadata response with topic topology from Iggy.
pub fn encode_metadata_response_from_topics(
    api_version: i16,
    broker: &crate::protocol::api::BrokerAdvertise,
    topics: &[MetadataTopicOutcome],
) -> Bytes {
    const AUTHORIZED_OPS_UNKNOWN: i32 = i32::MIN;
    let flexible = api_version >= 9;
    let mut e = Encoder::with_capacity(512);

    if api_version >= 3 {
        e.write_i32(0);
    }

    if flexible {
        e.write_varint(2);
        e.write_i32(1);
        e.write_compact_nullable_string(Some(&broker.host));
        e.write_i32(broker.port);
        e.write_compact_nullable_string(None);
        e.write_empty_tagged_fields();

        e.write_compact_nullable_string(None);
        e.write_i32(1);

        e.write_varint((topics.len() + 1) as u64);
        for topic in topics {
            e.write_i16(topic.error_code);
            e.write_compact_nullable_string(Some(&topic.name));
            e.write_bool(false);
            if topic.error_code == ERROR_NONE {
                e.write_varint((topic.partitions_count as u64) + 1);
                for p in 0..topic.partitions_count {
                    write_metadata_partition(&mut e, api_version, p);
                }
            } else {
                e.write_varint(1);
            }
            if api_version >= 8 {
                e.write_i32(AUTHORIZED_OPS_UNKNOWN);
            }
            e.write_empty_tagged_fields();
        }
        if api_version >= 8 {
            e.write_i32(AUTHORIZED_OPS_UNKNOWN);
        }
        e.write_empty_tagged_fields();
    } else {
        e.write_i32(1);
        e.write_i32(1);
        let _ = e.write_nullable_string(Some(&broker.host));
        e.write_i32(broker.port);
        if api_version >= 1 {
            let _ = e.write_nullable_string(None);
        }
        if api_version >= 2 {
            let _ = e.write_nullable_string(None);
        }
        if api_version >= 1 {
            e.write_i32(1);
        }

        e.write_i32(i32::try_from(topics.len()).expect("topic count bounded"));
        for topic in topics {
            e.write_i16(topic.error_code);
            let _ = e.write_nullable_string(Some(&topic.name));
            if api_version >= 1 {
                e.write_bool(false);
            }
            if topic.error_code == ERROR_NONE {
                e.write_i32(i32::try_from(topic.partitions_count).expect("partition count"));
                for p in 0..topic.partitions_count {
                    write_metadata_partition(&mut e, api_version, p);
                }
            } else {
                e.write_i32(0);
            }
            if api_version >= 8 {
                e.write_i32(AUTHORIZED_OPS_UNKNOWN);
            }
        }
        if api_version >= 8 {
            e.write_i32(AUTHORIZED_OPS_UNKNOWN);
        }
    }

    e.freeze()
}

/// Build a single unknown-topic metadata outcome (stub compatibility).
#[must_use]
pub fn metadata_unknown_topic(name: &str) -> MetadataTopicOutcome {
    MetadataTopicOutcome {
        name: name.to_string(),
        error_code: ERROR_UNKNOWN_TOPIC_OR_PARTITION,
        partitions_count: 0,
    }
}

/// Concatenate opaque record payloads for a Fetch `records` field.
#[must_use]
pub fn concat_record_batches(payloads: &[Bytes]) -> Option<Bytes> {
    if payloads.is_empty() {
        return None;
    }
    if payloads.len() == 1 {
        return Some(payloads[0].clone());
    }
    let capacity = payloads.iter().map(Bytes::len).sum();
    let mut buf = BytesMut::with_capacity(capacity);
    for p in payloads {
        buf.put_slice(p);
    }
    Some(buf.freeze())
}
