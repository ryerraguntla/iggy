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

//! Metadata bridge encoder round-trip against `kafka-protocol` (C1 regression).

use iggy_gateway_kafka::protocol::api::BrokerAdvertise;
use iggy_gateway_kafka::protocol::responses::{
    MetadataTopicOutcome, encode_metadata_response_from_topics,
};
use kafka_protocol::messages::{BrokerId, MetadataResponse};
use kafka_protocol::protocol::Decodable;

fn decode_metadata(body: bytes::Bytes, api_version: i16) -> MetadataResponse {
    let mut buf = body;
    MetadataResponse::decode(&mut buf, api_version).expect("metadata response must decode")
}

fn topic_with_partitions(name: &str, partitions_count: u32) -> MetadataTopicOutcome {
    MetadataTopicOutcome {
        name: name.to_string(),
        error_code: 0,
        partitions_count,
    }
}

#[test]
fn bridge_metadata_v4_partitions_roundtrip_kafka_protocol() {
    let body = encode_metadata_response_from_topics(
        4,
        &BrokerAdvertise::default(),
        &[topic_with_partitions("orders", 3)],
    );
    let resp = decode_metadata(body, 4);
    assert_eq!(resp.topics.len(), 1);
    assert_eq!(
        resp.topics[0].name.as_ref().map(|n| n.0.as_str()),
        Some("orders")
    );
    assert_eq!(resp.topics[0].partitions.len(), 3);

    for (idx, part) in resp.topics[0].partitions.iter().enumerate() {
        assert_eq!(part.error_code, 0, "partition {idx} error_code");
        assert_eq!(part.partition_index, i32::try_from(idx).unwrap());
        assert_eq!(part.leader_id, BrokerId(1));
        assert_eq!(part.replica_nodes, vec![BrokerId(1)]);
        assert_eq!(part.isr_nodes, vec![BrokerId(1)]);
    }
}

#[test]
fn bridge_metadata_v9_partitions_roundtrip_kafka_protocol() {
    let body = encode_metadata_response_from_topics(
        9,
        &BrokerAdvertise::default(),
        &[topic_with_partitions("events", 1)],
    );
    let resp = decode_metadata(body, 9);
    assert_eq!(resp.topics.len(), 1);
    let part = &resp.topics[0].partitions[0];
    assert_eq!(part.error_code, 0);
    assert_eq!(part.partition_index, 0);
    assert_eq!(part.leader_id, BrokerId(1));
    assert_eq!(part.leader_epoch, -1);
    assert_eq!(part.replica_nodes, vec![BrokerId(1)]);
    assert_eq!(part.isr_nodes, vec![BrokerId(1)]);
    assert!(part.offline_replicas.is_empty());
}
