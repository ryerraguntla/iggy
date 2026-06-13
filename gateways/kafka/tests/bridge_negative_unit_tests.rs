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

//! Non-happy-path unit tests for bridge encoders, mapping, and protocol edge cases.

use bytes::Bytes;

use iggy_gateway_kafka::bridge::mapping::{
    KAFKA_PARTITIONS_USE_DEFAULT, kafka_partition_index, kafka_topic_identifier,
    stream_and_topic_ids,
};
use iggy_gateway_kafka::protocol::api::{
    ERROR_INVALID_PARTITIONS, ERROR_INVALID_REQUEST, ERROR_NONE, ERROR_UNKNOWN_TOPIC_OR_PARTITION,
};
use iggy_gateway_kafka::protocol::codec::Encoder;
use iggy_gateway_kafka::protocol::requests::{
    CreatableTopic, CreateTopicsRequest, decode_create_topics_request, decode_produce_request,
};
use iggy_gateway_kafka::protocol::responses::{
    FetchPartitionOutcome, ListOffsetsPartitionOutcome, ProducePartitionOutcome,
    encode_create_topics_response, encode_fetch_response_from_topic_outcomes,
    encode_list_offsets_response_from_topic_outcomes, encode_metadata_response_from_topics,
    encode_produce_response_from_topic_outcomes, metadata_unknown_topic,
};
use iggy_gateway_kafka::protocol::api::BrokerAdvertise;

// ── Mapping / identifier negatives ───────────────────────────────────────────

#[test]
fn kafka_partition_index_rejects_unassigned_for_fetch() {
    assert_eq!(kafka_partition_index(-1), None);
}

#[test]
fn kafka_partition_index_rejects_large_negative() {
    assert_eq!(kafka_partition_index(-99), None);
}

#[test]
fn kafka_topic_identifier_rejects_empty_name() {
    assert!(kafka_topic_identifier("").is_none());
}

#[test]
fn stream_and_topic_ids_rejects_empty_name() {
    assert!(stream_and_topic_ids("").is_none());
}

// ── CreateTopics encoder negatives ───────────────────────────────────────────

#[test]
fn create_topics_response_zero_partitions_is_invalid_partitions() {
    let req = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: "t".to_string(),
            num_partitions: 0,
            replication_factor: 1,
        }],
        timeout_ms: 5_000,
        validate_only: false,
    };
    let body = encode_create_topics_response(2, &req);
    let mut d = iggy_gateway_kafka::protocol::codec::Decoder::new(body);
    d.read_i32().unwrap();
    d.read_i32().unwrap();
    d.read_nullable_string().unwrap();
    assert_eq!(d.read_i16().unwrap(), ERROR_INVALID_PARTITIONS);
}

#[test]
fn create_topics_response_minus_one_partitions_is_ok() {
    let req = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: "t".to_string(),
            num_partitions: KAFKA_PARTITIONS_USE_DEFAULT,
            replication_factor: 1,
        }],
        timeout_ms: 5_000,
        validate_only: false,
    };
    let body = encode_create_topics_response(2, &req);
    let mut d = iggy_gateway_kafka::protocol::codec::Decoder::new(body);
    d.read_i32().unwrap();
    d.read_i32().unwrap();
    d.read_nullable_string().unwrap();
    assert_eq!(d.read_i16().unwrap(), ERROR_NONE);
}

#[test]
fn create_topics_response_negative_partitions_below_minus_one_is_invalid() {
    let req = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: "t".to_string(),
            num_partitions: -2,
            replication_factor: 1,
        }],
        timeout_ms: 5_000,
        validate_only: false,
    };
    let body = encode_create_topics_response(2, &req);
    let mut d = iggy_gateway_kafka::protocol::codec::Decoder::new(body);
    d.read_i32().unwrap();
    d.read_i32().unwrap();
    d.read_nullable_string().unwrap();
    assert_eq!(d.read_i16().unwrap(), ERROR_INVALID_PARTITIONS);
}

// ── Bridge response encoder error propagation ────────────────────────────────

#[test]
fn produce_response_encoder_propagates_partition_error() {
    let body = encode_produce_response_from_topic_outcomes(
        3,
        &[(
            "t".to_string(),
            vec![ProducePartitionOutcome {
                partition: 0,
                error_code: ERROR_INVALID_REQUEST,
                base_offset: 0,
            }],
        )],
    );
    let mut d = iggy_gateway_kafka::protocol::codec::Decoder::new(body);
    d.read_i32().unwrap();
    d.read_nullable_string().unwrap();
    d.read_i32().unwrap();
    d.read_i32().unwrap();
    assert_eq!(d.read_i16().unwrap(), ERROR_INVALID_REQUEST);
}

#[test]
fn fetch_response_encoder_propagates_unknown_topic_error() {
    let body = encode_fetch_response_from_topic_outcomes(
        4,
        &[(
            "missing".to_string(),
            vec![FetchPartitionOutcome {
                partition: 0,
                error_code: ERROR_UNKNOWN_TOPIC_OR_PARTITION,
                high_watermark: 0,
                log_start_offset: 0,
                records: None,
            }],
        )],
    );
    let mut d = iggy_gateway_kafka::protocol::codec::Decoder::new(body);
    d.read_i32().unwrap();
    d.read_i32().unwrap();
    d.read_nullable_string().unwrap();
    d.read_i32().unwrap();
    d.read_i32().unwrap();
    assert_eq!(d.read_i16().unwrap(), ERROR_UNKNOWN_TOPIC_OR_PARTITION);
}

#[test]
fn list_offsets_response_encoder_propagates_invalid_request() {
    let body = encode_list_offsets_response_from_topic_outcomes(
        1,
        &[(
            "t".to_string(),
            vec![ListOffsetsPartitionOutcome {
                partition: 0,
                error_code: ERROR_INVALID_REQUEST,
                offset: 0,
            }],
        )],
    );
    let mut d = iggy_gateway_kafka::protocol::codec::Decoder::new(body);
    d.read_i32().unwrap();
    d.read_nullable_string().unwrap();
    d.read_i32().unwrap();
    d.read_i32().unwrap();
    assert_eq!(d.read_i16().unwrap(), ERROR_INVALID_REQUEST);
}

#[test]
fn multi_topic_produce_response_mixed_errors() {
    let body = encode_produce_response_from_topic_outcomes(
        3,
        &[
            (
                "ok".to_string(),
                vec![ProducePartitionOutcome {
                    partition: 0,
                    error_code: ERROR_NONE,
                    base_offset: 1,
                }],
            ),
            (
                "bad".to_string(),
                vec![ProducePartitionOutcome {
                    partition: 0,
                    error_code: ERROR_INVALID_REQUEST,
                    base_offset: 0,
                }],
            ),
        ],
    );
    let mut d = iggy_gateway_kafka::protocol::codec::Decoder::new(body);
    assert_eq!(d.read_i32().unwrap(), 2);
    d.read_nullable_string().unwrap();
    d.read_i32().unwrap();
    d.read_i32().unwrap();
    assert_eq!(d.read_i16().unwrap(), ERROR_NONE);
    d.read_i64().unwrap();
    d.read_i64().unwrap();
    d.read_nullable_string().unwrap();
    d.read_i32().unwrap();
    d.read_i32().unwrap();
    assert_eq!(d.read_i16().unwrap(), ERROR_INVALID_REQUEST);
}

// ── Decode negatives (malformed / truncated bodies) ──────────────────────────

#[test]
fn produce_v3_truncated_body_returns_decode_error() {
    let mut enc = Encoder::with_capacity(8);
    enc.write_nullable_string(None).unwrap();
    enc.write_i16(1);
    // missing timeout, topics, etc.
    assert!(decode_produce_request(3, enc.freeze()).is_err());
}

#[test]
fn create_topics_v2_truncated_body_returns_decode_error() {
    let mut enc = Encoder::with_capacity(4);
    enc.write_i32(1); // topics count but no topic entry
    assert!(decode_create_topics_request(2, enc.freeze()).is_err());
}

#[test]
fn produce_v3_null_topic_name_returns_decode_error() {
    let mut enc = Encoder::with_capacity(32);
    enc.write_nullable_string(None).unwrap();
    enc.write_i16(1);
    enc.write_i32(30_000);
    enc.write_i32(1); // topics
    enc.write_nullable_string(None).unwrap(); // null topic name
    assert!(decode_produce_request(3, enc.freeze()).is_err());
}

#[test]
fn produce_error_response_encoder_sets_top_level_partition_error() {
    let body =
        iggy_gateway_kafka::protocol::responses::encode_produce_error_response(3, ERROR_INVALID_REQUEST);
    let err = parse_produce_error_body(&body);
    assert_eq!(err, ERROR_INVALID_REQUEST);
}

#[test]
fn metadata_encoder_unknown_topic_uses_error_3() {
    let body = encode_metadata_response_from_topics(
        4,
        &BrokerAdvertise::default(),
        &[metadata_unknown_topic("missing")],
    );
    let mut d = iggy_gateway_kafka::protocol::codec::Decoder::new(body);
    d.read_i32().unwrap(); // throttle
    let _ = d.read_i32().unwrap(); // brokers
    d.read_i32().unwrap();
    d.read_nullable_string().unwrap();
    d.read_i32().unwrap();
    d.read_nullable_string().unwrap();
    d.read_nullable_string().unwrap();
    d.read_i32().unwrap();
    assert_eq!(d.read_i32().unwrap(), 1); // topics
    assert_eq!(d.read_i16().unwrap(), ERROR_UNKNOWN_TOPIC_OR_PARTITION);
    assert_eq!(
        d.read_nullable_string().unwrap().unwrap(),
        "missing"
    );
    d.read_bool().unwrap(); // is_internal (v1+)
    assert_eq!(d.read_i32().unwrap(), 0); // empty partitions
}

fn parse_produce_error_body(body: &Bytes) -> i16 {
    let mut d = iggy_gateway_kafka::protocol::codec::Decoder::new(body.clone());
    d.read_i32().unwrap();
    d.read_nullable_string().unwrap();
    d.read_i32().unwrap();
    d.read_i32().unwrap();
    d.read_i16().unwrap()
}
