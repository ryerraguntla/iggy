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

//! Iggy-backed bridge **negative-path** integration tests (wire + handler errors).

#[path = "common/server.rs"]
mod server;
#[path = "common/tcp.rs"]
mod tcp;
#[path = "common/wire.rs"]
mod wire;

use std::sync::Arc;

use bytes::Bytes;
use serial_test::serial;

use iggy_gateway_kafka::protocol::api::{
    API_KEY_CREATE_TOPICS, API_KEY_FETCH, API_KEY_LIST_OFFSETS, API_KEY_METADATA, API_KEY_PRODUCE,
    ERROR_INVALID_PARTITIONS, ERROR_INVALID_REQUEST, ERROR_INVALID_REPLICATION_FACTOR, ERROR_NONE,
    ERROR_UNKNOWN_TOPIC_OR_PARTITION,
};
use iggy_gateway_kafka::{IggyBridge, IggyBridgeConfig};

use server::{iggy::IggyTestServer, spawn_test_server_with_bridge};
use tcp::round_trip;
use wire::{
    create_topics_v2_body, fetch_v4_body, list_offsets_v1_body, metadata_v4_body, produce_v3_body,
    parse_create_topics_v2_topic_error, parse_fetch_v4_partition_error,
    parse_list_offsets_v1_partition_error, parse_metadata_v4_topic_error,
    parse_produce_v3_partition_error,
};

async fn iggy_bridge_from_server(server: &IggyTestServer) -> Arc<IggyBridge> {
    let config = IggyBridgeConfig {
        server_address: server.tcp_addr().to_string(),
        ..IggyBridgeConfig::default()
    };
    Arc::new(
        IggyBridge::connect(&config)
            .await
            .expect("bridge connect"),
    )
}

async fn with_bridge_server<F, Fut>(test: F)
where
    F: FnOnce(std::net::SocketAddr, Arc<IggyBridge>) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let iggy = IggyTestServer::start().await;
    let bridge = iggy_bridge_from_server(&iggy).await;
    let (kafka_addr, _shutdown) = spawn_test_server_with_bridge(bridge.clone()).await;
    test(kafka_addr, bridge).await;
    iggy.stop();
}

// ── Metadata ─────────────────────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn bridge_metadata_unknown_topic_returns_error_3() {
    with_bridge_server(|addr, _| async move {
        let body = metadata_v4_body("no-such-topic-neg");
        let (_, resp) = round_trip(addr, API_KEY_METADATA, 4, 1, &body).await;
        assert_eq!(
            parse_metadata_v4_topic_error(&resp),
            ERROR_UNKNOWN_TOPIC_OR_PARTITION
        );
    })
    .await;
}

#[tokio::test]
#[serial]
async fn bridge_metadata_known_vs_unknown_topics_sequential() {
    with_bridge_server(|addr, bridge| async move {
        bridge
            .ensure_stream_and_topic("known-meta-neg", 1)
            .await
            .expect("create");

        let known_resp =
            round_trip(addr, API_KEY_METADATA, 4, 2, &metadata_v4_body("known-meta-neg")).await;
        assert_eq!(
            parse_metadata_v4_topic_error(&known_resp.1),
            ERROR_NONE
        );

        let unknown_resp = round_trip(
            addr,
            API_KEY_METADATA,
            4,
            3,
            &metadata_v4_body("unknown-meta-neg"),
        )
        .await;
        assert_eq!(
            parse_metadata_v4_topic_error(&unknown_resp.1),
            ERROR_UNKNOWN_TOPIC_OR_PARTITION
        );
    })
    .await;
}

// ── CreateTopics ─────────────────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn bridge_create_topics_zero_partitions_returns_invalid_partitions() {
    with_bridge_server(|addr, _| async move {
        let body = create_topics_v2_body("zero-part-topic", 0, 1);
        let (_, resp) = round_trip(addr, API_KEY_CREATE_TOPICS, 2, 10, &body).await;
        assert_eq!(
            parse_create_topics_v2_topic_error(&resp),
            ERROR_INVALID_PARTITIONS
        );
    })
    .await;
}

#[tokio::test]
#[serial]
async fn bridge_create_topics_minus_one_partitions_succeeds() {
    with_bridge_server(|addr, bridge| async move {
        let body = create_topics_v2_body("default-part-topic", -1, 1);
        let (_, resp) = round_trip(addr, API_KEY_CREATE_TOPICS, 2, 11, &body).await;
        assert_eq!(parse_create_topics_v2_topic_error(&resp), ERROR_NONE);

        let meta = bridge.topic_metadata("default-part-topic").await.unwrap();
        assert!(meta.is_some());
    })
    .await;
}

#[tokio::test]
#[serial]
async fn bridge_create_topics_invalid_replication_factor_aborts_request() {
    with_bridge_server(|addr, _| async move {
        let body = create_topics_v2_body("bad-rf-topic", 1, 3);
        let (_, resp) = round_trip(addr, API_KEY_CREATE_TOPICS, 2, 12, &body).await;
        assert_eq!(
            parse_create_topics_v2_topic_error(&resp),
            ERROR_INVALID_REPLICATION_FACTOR
        );
    })
    .await;
}

#[tokio::test]
#[serial]
async fn bridge_create_topics_zero_partitions_does_not_create_topic() {
    with_bridge_server(|addr, bridge| async move {
        let body = create_topics_v2_body("never-created-topic", 0, 1);
        round_trip(addr, API_KEY_CREATE_TOPICS, 2, 13, &body).await;
        assert!(bridge.topic_metadata("never-created-topic").await.unwrap().is_none());
    })
    .await;
}

// ── Produce ──────────────────────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn bridge_produce_null_records_returns_invalid_request() {
    with_bridge_server(|addr, _| async move {
        let body = produce_v3_body("prod-neg-topic", 0, None, None);
        let (_, resp) = round_trip(addr, API_KEY_PRODUCE, 3, 20, &body).await;
        assert_eq!(
            parse_produce_v3_partition_error(&resp),
            ERROR_INVALID_REQUEST
        );
    })
    .await;
}

#[tokio::test]
#[serial]
async fn bridge_produce_transactional_id_returns_invalid_request() {
    with_bridge_server(|addr, _| async move {
        let body = produce_v3_body(
            "prod-neg-topic",
            0,
            Some("txn-1"),
            Some(b"payload"),
        );
        let (_, resp) = round_trip(addr, API_KEY_PRODUCE, 3, 21, &body).await;
        // Top-level produce error response — first partition error in body.
        let mut d = iggy_gateway_kafka::protocol::codec::Decoder::new(resp);
        d.read_i32().unwrap();
        d.read_nullable_string().unwrap();
        d.read_i32().unwrap();
        d.read_i32().unwrap();
        assert_eq!(d.read_i16().unwrap(), ERROR_INVALID_REQUEST);
    })
    .await;
}

#[tokio::test]
#[serial]
async fn bridge_produce_truncated_body_returns_invalid_request() {
    with_bridge_server(|addr, _| async move {
        let truncated = Bytes::from_static(&[0, 0, 0, 1, 0]); // incomplete produce v3
        let (_, resp) = round_trip(addr, API_KEY_PRODUCE, 3, 22, &truncated).await;
        let mut d = iggy_gateway_kafka::protocol::codec::Decoder::new(resp);
        d.read_i32().unwrap();
        d.read_nullable_string().unwrap();
        d.read_i32().unwrap();
        d.read_i32().unwrap();
        assert_eq!(d.read_i16().unwrap(), ERROR_INVALID_REQUEST);
    })
    .await;
}

#[tokio::test]
#[serial]
async fn bridge_produce_unknown_topic_returns_unknown_topic_or_partition() {
    with_bridge_server(|addr, _| async move {
        let body = produce_v3_body("missing-produce-topic", 0, None, Some(b"x"));
        let (_, resp) = round_trip(addr, API_KEY_PRODUCE, 3, 23, &body).await;
        assert_eq!(
            parse_produce_v3_partition_error(&resp),
            ERROR_UNKNOWN_TOPIC_OR_PARTITION
        );
    })
    .await;
}

// ── Fetch ────────────────────────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn bridge_fetch_unknown_topic_returns_error_3() {
    with_bridge_server(|addr, _| async move {
        let body = fetch_v4_body("missing-fetch-topic", 0, 0);
        let (_, resp) = round_trip(addr, API_KEY_FETCH, 4, 30, &body).await;
        assert_eq!(
            parse_fetch_v4_partition_error(&resp),
            ERROR_UNKNOWN_TOPIC_OR_PARTITION
        );
    })
    .await;
}

#[tokio::test]
#[serial]
async fn bridge_fetch_negative_partition_returns_error_3() {
    with_bridge_server(|addr, bridge| async move {
        bridge
            .ensure_stream_and_topic("fetch-neg-part", 1)
            .await
            .expect("create");
        let body = fetch_v4_body("fetch-neg-part", -1, 0);
        let (_, resp) = round_trip(addr, API_KEY_FETCH, 4, 31, &body).await;
        assert_eq!(
            parse_fetch_v4_partition_error(&resp),
            ERROR_UNKNOWN_TOPIC_OR_PARTITION
        );
    })
    .await;
}

#[tokio::test]
#[serial]
async fn bridge_fetch_truncated_body_returns_invalid_request() {
    with_bridge_server(|addr, _| async move {
        let truncated = Bytes::from_static(&[0, 0, 0, 7]);
        let (_, resp) = round_trip(addr, API_KEY_FETCH, 4, 32, &truncated).await;
        assert_eq!(
            parse_fetch_v4_partition_error(&resp),
            ERROR_INVALID_REQUEST
        );
    })
    .await;
}

#[tokio::test]
#[serial]
async fn bridge_fetch_empty_topic_returns_no_records() {
    with_bridge_server(|addr, bridge| async move {
        bridge
            .ensure_stream_and_topic("empty-fetch-topic", 1)
            .await
            .expect("create");
        let body = fetch_v4_body("empty-fetch-topic", 0, 0);
        let (_, resp) = round_trip(addr, API_KEY_FETCH, 4, 33, &body).await;
        assert_eq!(parse_fetch_v4_partition_error(&resp), ERROR_NONE);
        let mut d = iggy_gateway_kafka::protocol::codec::Decoder::new(resp);
        d.read_i32().unwrap();
        d.read_i32().unwrap();
        d.read_nullable_string().unwrap();
        d.read_i32().unwrap();
        d.read_i32().unwrap();
        d.read_i16().unwrap();
        d.read_i64().unwrap(); // hwm
        d.read_i64().unwrap(); // last_stable (v4+)
        d.read_i32().unwrap(); // aborted txns
        assert!(d.read_nullable_bytes().unwrap().is_none());
    })
    .await;
}

// ── ListOffsets ──────────────────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn bridge_list_offsets_unknown_topic_returns_error_3() {
    with_bridge_server(|addr, _| async move {
        let body = list_offsets_v1_body("missing-offset-topic", 0, -1);
        let (_, resp) = round_trip(addr, API_KEY_LIST_OFFSETS, 1, 40, &body).await;
        assert_eq!(
            parse_list_offsets_v1_partition_error(&resp),
            ERROR_UNKNOWN_TOPIC_OR_PARTITION
        );
    })
    .await;
}

#[tokio::test]
#[serial]
async fn bridge_list_offsets_bad_timestamp_sentinel_returns_invalid_request() {
    with_bridge_server(|addr, bridge| async move {
        bridge
            .ensure_stream_and_topic("offset-ts-neg", 1)
            .await
            .expect("create");
        let body = list_offsets_v1_body("offset-ts-neg", 0, -3);
        let (_, resp) = round_trip(addr, API_KEY_LIST_OFFSETS, 1, 41, &body).await;
        assert_eq!(
            parse_list_offsets_v1_partition_error(&resp),
            ERROR_INVALID_REQUEST
        );
    })
    .await;
}

#[tokio::test]
#[serial]
async fn bridge_list_offsets_future_timestamp_no_match_returns_invalid_request() {
    with_bridge_server(|addr, bridge| async move {
        bridge
            .ensure_stream_and_topic("offset-future", 1)
            .await
            .expect("create");
        // Milliseconds far in the future — no messages at this timestamp.
        let body = list_offsets_v1_body("offset-future", 0, 4_102_444_800_000);
        let (_, resp) = round_trip(addr, API_KEY_LIST_OFFSETS, 1, 42, &body).await;
        assert_eq!(
            parse_list_offsets_v1_partition_error(&resp),
            ERROR_INVALID_REQUEST
        );
    })
    .await;
}

#[tokio::test]
#[serial]
async fn bridge_list_offsets_negative_partition_returns_error_3() {
    with_bridge_server(|addr, bridge| async move {
        bridge
            .ensure_stream_and_topic("offset-neg-part", 1)
            .await
            .expect("create");
        let body = list_offsets_v1_body("offset-neg-part", -1, -1);
        let (_, resp) = round_trip(addr, API_KEY_LIST_OFFSETS, 1, 43, &body).await;
        assert_eq!(
            parse_list_offsets_v1_partition_error(&resp),
            ERROR_UNKNOWN_TOPIC_OR_PARTITION
        );
    })
    .await;
}

#[tokio::test]
#[serial]
async fn bridge_list_offsets_earliest_on_empty_partition_returns_zero() {
    with_bridge_server(|addr, bridge| async move {
        bridge
            .ensure_stream_and_topic("offset-earliest-empty", 1)
            .await
            .expect("create");
        let body = list_offsets_v1_body("offset-earliest-empty", 0, -2);
        let (_, resp) = round_trip(addr, API_KEY_LIST_OFFSETS, 1, 44, &body).await;
        assert_eq!(parse_list_offsets_v1_partition_error(&resp), ERROR_NONE);
        let mut d = iggy_gateway_kafka::protocol::codec::Decoder::new(resp);
        d.read_i32().unwrap();
        d.read_nullable_string().unwrap();
        d.read_i32().unwrap();
        d.read_i32().unwrap();
        d.read_i16().unwrap();
        d.read_i64().unwrap(); // timestamp sentinel
        assert_eq!(d.read_i64().unwrap(), 0);
    })
    .await;
}
