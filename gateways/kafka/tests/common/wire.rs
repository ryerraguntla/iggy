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

//! Kafka request body builders and response parsers for bridge integration tests.
#![allow(dead_code, clippy::doc_markdown)]

use bytes::Bytes;

use iggy_gateway_kafka::protocol::codec::{Decoder, Encoder};

/// CreateTopics request body (v2).
pub fn create_topics_v2_body(
    name: &str,
    num_partitions: i32,
    replication_factor: i16,
) -> Bytes {
    let mut e = Encoder::with_capacity(64);
    e.write_i32(1);
    e.write_nullable_string(Some(name)).expect("topic name");
    e.write_i32(num_partitions);
    e.write_i16(replication_factor);
    e.write_i32(0); // assignments
    e.write_i32(0); // configs
    e.write_i32(5_000); // timeout_ms
    e.write_bool(false); // validate_only
    e.freeze()
}

/// Metadata request body (v4) with explicit topic names.
pub fn metadata_v4_body(topic: &str) -> Bytes {
    let mut e = Encoder::with_capacity(32);
    e.write_i32(1);
    e.write_nullable_string(Some(topic)).expect("topic name");
    e.write_bool(true); // allow_auto_topic_creation
    e.freeze()
}

/// ListOffsets request body (v1).
pub fn list_offsets_v1_body(topic: &str, partition: i32, timestamp: i64) -> Bytes {
    let mut e = Encoder::with_capacity(48);
    e.write_i32(-1); // replica_id
    e.write_i32(1); // topics
    e.write_nullable_string(Some(topic)).expect("topic");
    e.write_i32(1); // partitions
    e.write_i32(partition);
    e.write_i64(timestamp);
    e.freeze()
}

/// Fetch request body (v4).
pub fn fetch_v4_body(topic: &str, partition: i32, fetch_offset: i64) -> Bytes {
    let mut e = Encoder::with_capacity(64);
    e.write_i32(-1); // replica_id
    e.write_i32(500); // max_wait_ms
    e.write_i32(1); // min_bytes
    e.write_i32(1_048_576); // max_bytes (v3+)
    e.write_i8(0); // isolation_level (v4+)
    e.write_i32(1); // topics
    e.write_nullable_string(Some(topic)).expect("topic");
    e.write_i32(1); // partitions
    e.write_i32(partition);
    e.write_i64(fetch_offset);
    e.write_i32(1_048_576); // partition_max_bytes
    e.freeze()
}

/// Produce request body (v3).
pub fn produce_v3_body(
    topic: &str,
    partition: i32,
    transactional_id: Option<&str>,
    records: Option<&[u8]>,
) -> Bytes {
    let mut e = Encoder::with_capacity(128);
    e.write_nullable_string(transactional_id)
        .expect("transactional_id");
    e.write_i16(1); // acks
    e.write_i32(30_000); // timeout_ms
    e.write_i32(1); // topics
    e.write_nullable_string(Some(topic)).expect("topic");
    e.write_i32(1); // partitions
    e.write_i32(partition);
    e.write_nullable_bytes(records).expect("records");
    e.freeze()
}

/// First per-partition `error_code` in a Produce v3 response (throttle is trailing on v1+).
pub fn parse_produce_v3_partition_error(body: &Bytes) -> i16 {
    let mut d = Decoder::new(body.clone());
    d.read_i32().unwrap(); // topics array length
    d.read_nullable_string().unwrap();
    d.read_i32().unwrap(); // partitions array length
    d.read_i32().unwrap(); // partition index
    d.read_i16().unwrap()
}

/// Top-level or first-partition error in a Produce error-only response.
pub fn parse_produce_error_response(body: &Bytes) -> i16 {
    let mut d = Decoder::new(body.clone());
    if d.remaining() >= 2 {
        // May be throttle + topics or direct partition error depending on encoder path.
        let peek = d.read_i32().unwrap();
        if peek == 0 || peek == 1 {
            // throttle or topic count — continue parsing
            if peek == 0 {
                d.read_i32().unwrap();
            }
            let _ = d.read_nullable_string().unwrap();
            d.read_i32().unwrap();
            d.read_i32().unwrap();
            return d.read_i16().unwrap();
        }
        // rewind-ish: treat first i32 as topic count
        let mut d2 = Decoder::new(body.clone());
        d2.read_i32().unwrap();
        let _ = d2.read_nullable_string().unwrap();
        d2.read_i32().unwrap();
        d2.read_i32().unwrap();
        return d2.read_i16().unwrap();
    }
    d.read_i16().unwrap()
}

/// First per-partition `error_code` in a Fetch v4 response.
pub fn parse_fetch_v4_partition_error(body: &Bytes) -> i16 {
    let mut d = Decoder::new(body.clone());
    d.read_i32().unwrap(); // throttle (v1+)
    d.read_i32().unwrap(); // topics
    d.read_nullable_string().unwrap();
    d.read_i32().unwrap(); // partitions
    d.read_i32().unwrap(); // partition index
    d.read_i16().unwrap()
}

/// First per-partition `error_code` in a ListOffsets v1 response.
pub fn parse_list_offsets_v1_partition_error(body: &Bytes) -> i16 {
    let mut d = Decoder::new(body.clone());
    d.read_i32().unwrap(); // topics (no throttle on v1)
    d.read_nullable_string().unwrap();
    d.read_i32().unwrap(); // partitions
    d.read_i32().unwrap(); // partition index
    d.read_i16().unwrap()
}

/// First topic `error_code` in a CreateTopics v2 response.
pub fn parse_create_topics_v2_topic_error(body: &Bytes) -> i16 {
    let mut d = Decoder::new(body.clone());
    d.read_i32().unwrap(); // throttle (v2+)
    d.read_i32().unwrap(); // topics
    d.read_nullable_string().unwrap();
    d.read_i16().unwrap()
}

/// Metadata v4 first topic `error_code`.
pub fn parse_metadata_v4_topic_error(body: &Bytes) -> i16 {
    let mut m = Decoder::new(body.clone());
    m.read_i32().unwrap(); // throttle
    assert_eq!(m.read_i32().unwrap(), 1); // brokers
    m.read_i32().unwrap();
    m.read_nullable_string().unwrap();
    m.read_i32().unwrap();
    m.read_nullable_string().unwrap();
    m.read_nullable_string().unwrap();
    m.read_i32().unwrap();
    assert_eq!(m.read_i32().unwrap(), 1); // topics
    m.read_i16().unwrap()
}
