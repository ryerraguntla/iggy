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

//! Unit tests for metadata decode and bridge response encoders.

use bytes::Bytes;

use iggy_gateway_kafka::protocol::codec::Encoder;
use iggy_gateway_kafka::protocol::requests::{
    MetadataTopicFilter, decode_metadata_topic_filter,
};
use iggy_gateway_kafka::protocol::responses::{
    FetchPartitionOutcome, ListOffsetsPartitionOutcome, ProducePartitionOutcome,
    concat_record_batches, encode_fetch_response_from_topic_outcomes,
    encode_list_offsets_response_from_topic_outcomes,
    encode_produce_response_from_topic_outcomes,
};

#[test]
fn metadata_v4_decodes_named_topic() {
    let mut enc = Encoder::with_capacity(32);
    enc.write_i32(1);
    enc.write_nullable_string(Some("my-topic")).unwrap();
    enc.write_bool(true);

    let filter = decode_metadata_topic_filter(4, enc.freeze()).unwrap();
    assert_eq!(
        filter,
        MetadataTopicFilter::Named(vec!["my-topic".to_string()])
    );
}

#[test]
fn metadata_v1_null_topics_means_all() {
    let mut enc = Encoder::with_capacity(8);
    enc.write_i32(-1);

    let filter = decode_metadata_topic_filter(1, enc.freeze()).unwrap();
    assert_eq!(filter, MetadataTopicFilter::All);
}

#[test]
fn produce_response_encoder_sets_base_offset() {
    let body = encode_produce_response_from_topic_outcomes(
        3,
        &[(
            "t".to_string(),
            vec![ProducePartitionOutcome {
                partition: 0,
                error_code: 0,
                base_offset: 7,
            }],
        )],
    );
    let mut d = iggy_gateway_kafka::protocol::codec::Decoder::new(body);
    d.read_i32().unwrap();
    d.read_nullable_string().unwrap();
    d.read_i32().unwrap();
    d.read_i32().unwrap();
    assert_eq!(d.read_i16().unwrap(), 0);
    assert_eq!(d.read_i64().unwrap(), 7);
}

#[test]
fn fetch_response_encoder_includes_records() {
    let records = Bytes::from_static(b"record-batch-bytes");
    let body = encode_fetch_response_from_topic_outcomes(
        4,
        &[(
            "t".to_string(),
            vec![FetchPartitionOutcome {
                partition: 0,
                error_code: 0,
                high_watermark: 1,
                log_start_offset: 0,
                records: Some(records.clone()),
            }],
        )],
    );
    let mut d = iggy_gateway_kafka::protocol::codec::Decoder::new(body);
    d.read_i32().unwrap(); // throttle (fetch v1+)
    d.read_i32().unwrap(); // topics
    d.read_nullable_string().unwrap();
    d.read_i32().unwrap(); // partitions
    d.read_i32().unwrap(); // partition index
    d.read_i16().unwrap();
    d.read_i64().unwrap(); // high_watermark
    d.read_i64().unwrap(); // last_stable_offset (v4+, not log_start — that's v5+)
    d.read_i32().unwrap(); // aborted_transactions (v4+)
    let got = d.read_nullable_bytes().unwrap().expect("records");
    assert_eq!(got, records);
}

#[test]
fn list_offsets_encoder_writes_offset() {
    let body = encode_list_offsets_response_from_topic_outcomes(
        1,
        &[(
            "t".to_string(),
            vec![ListOffsetsPartitionOutcome {
                partition: 0,
                error_code: 0,
                offset: 42,
            }],
        )],
    );
    let mut d = iggy_gateway_kafka::protocol::codec::Decoder::new(body);
    assert_eq!(d.read_i32().unwrap(), 1); // topics (v1: no throttle prefix)
    d.read_nullable_string().unwrap();
    assert_eq!(d.read_i32().unwrap(), 1); // partitions
    d.read_i32().unwrap(); // partition index
    d.read_i16().unwrap();
    d.read_i64().unwrap(); // timestamp sentinel
    assert_eq!(d.read_i64().unwrap(), 42);
}

#[test]
fn multi_topic_produce_response_encodes_both_topics() {
    let body = encode_produce_response_from_topic_outcomes(
        3,
        &[
            (
                "a".to_string(),
                vec![ProducePartitionOutcome {
                    partition: 0,
                    error_code: 0,
                    base_offset: 1,
                }],
            ),
            (
                "b".to_string(),
                vec![ProducePartitionOutcome {
                    partition: 0,
                    error_code: 0,
                    base_offset: 2,
                }],
            ),
        ],
    );
    let mut d = iggy_gateway_kafka::protocol::codec::Decoder::new(body);
    assert_eq!(d.read_i32().unwrap(), 2); // topics
    assert_eq!(d.read_nullable_string().unwrap().unwrap(), "a");
    assert_eq!(d.read_i32().unwrap(), 1); // partitions
    d.read_i32().unwrap(); // partition index
    d.read_i16().unwrap(); // error_code
    d.read_i64().unwrap(); // base_offset
    d.read_i64().unwrap(); // log_append_time_ms (v2+)
    assert_eq!(d.read_nullable_string().unwrap().unwrap(), "b");
}

#[test]
fn concat_record_batches_single_and_multi() {
    let a = Bytes::from_static(b"a");
    assert_eq!(
        concat_record_batches(&[
            Bytes::from_static(b"a"),
            Bytes::from_static(b"b"),
        ])
        .unwrap(),
        Bytes::from_static(b"ab")
    );
    assert_eq!(concat_record_batches(std::slice::from_ref(&a)).unwrap(), a);
    assert!(concat_record_batches(&[]).is_none());
}
