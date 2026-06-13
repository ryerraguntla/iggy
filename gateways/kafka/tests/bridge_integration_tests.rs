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

//! Iggy-backed bridge integration tests (spawns `iggy-server` via test harness).

#[path = "common/server.rs"]
mod server;
#[path = "common/tcp.rs"]
mod tcp;

use std::sync::Arc;

use bytes::Bytes;
use integration::harness::{TestHarnessBuilder, TestServerConfig};

fn iggy_server_config() -> TestServerConfig {
    let mut config = TestServerConfig::default();
    if let Ok(path) = std::env::var("CARGO_BIN_EXE_iggy-server") {
        config.executable_path = Some(path);
    } else {
        let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let workspace_root = manifest
            .parent()
            .and_then(|p| p.parent())
            .expect("workspace root");
        let candidate = workspace_root.join("target/debug/iggy-server");
        assert!(
            candidate.exists(),
            "build iggy-server first: cargo build -p server --bin iggy-server"
        );
        config.executable_path = Some(candidate.display().to_string());
    }
    config
}

use iggy_gateway_kafka::protocol::api::{
    API_KEY_CREATE_TOPICS, API_KEY_LIST_OFFSETS, API_KEY_METADATA, ERROR_NONE,
    ERROR_UNKNOWN_TOPIC_OR_PARTITION,
};
use iggy_gateway_kafka::protocol::codec::{Decoder, Encoder};
use iggy_gateway_kafka::{IggyBridge, IggyBridgeConfig};

use server::spawn_test_server_with_bridge;
use tcp::round_trip;

fn parse_metadata_v4_topic_error(body: &Bytes) -> i16 {
    let mut m = Decoder::new(body.clone());
    m.read_i32().unwrap(); // throttle_time_ms
    assert_eq!(m.read_i32().unwrap(), 1); // brokers
    m.read_i32().unwrap(); // node_id
    m.read_nullable_string().unwrap(); // host
    m.read_i32().unwrap(); // port
    m.read_nullable_string().unwrap(); // rack (v1+)
    m.read_nullable_string().unwrap(); // cluster_id (v2+)
    m.read_i32().unwrap(); // controller_id (v1+)
    assert_eq!(m.read_i32().unwrap(), 1); // topics
    m.read_i16().unwrap() // first topic error_code
}

async fn iggy_bridge_from_harness(
    harness: &integration::harness::TestHarness,
) -> Arc<IggyBridge> {
    let tcp_addr = harness.server().tcp_addr().expect("iggy tcp addr");
    let config = IggyBridgeConfig {
        server_address: tcp_addr.to_string(),
        ..IggyBridgeConfig::default()
    };
    Arc::new(
        IggyBridge::connect(&config)
            .await
            .expect("bridge connect"),
    )
}

#[tokio::test]
async fn bridge_produce_fetch_roundtrip_on_iggy() {
    let mut harness = TestHarnessBuilder::default()
        .server(iggy_server_config())
        .build()
        .expect("harness");
    harness.start().await.expect("start iggy");

    let bridge = iggy_bridge_from_harness(&harness).await;
    bridge
        .ensure_stream_and_topic("bridge-e2e-topic", 1)
        .await
        .expect("create topic");

    let payload = Bytes::from_static(b"kafka-bridge-payload");
    let ack = bridge
        .produce("bridge-e2e-topic", 0, payload.clone())
        .await
        .expect("produce");
    assert_eq!(ack.partition, 0);
    assert!(ack.base_offset >= 0);

    let fetched = bridge
        .fetch_partition("bridge-e2e-topic", 0, 0, 10)
        .await
        .expect("fetch");
    assert_eq!(fetched.messages.len(), 1);
    assert_eq!(fetched.messages[0].payload, payload);

    harness.stop().await.expect("stop iggy");
}

#[tokio::test]
async fn bridge_kafka_wire_create_topics_and_metadata() {
    let mut harness = TestHarnessBuilder::default()
        .server(iggy_server_config())
        .build()
        .expect("harness");
    harness.start().await.expect("start iggy");

    let bridge = iggy_bridge_from_harness(&harness).await;
    let (kafka_addr, _shutdown) = spawn_test_server_with_bridge(bridge).await;

    let mut create_body = Encoder::with_capacity(64);
    create_body.write_i32(1);
    create_body
        .write_nullable_string(Some("wire-topic"))
        .expect("topic name");
    create_body.write_i32(1);
    create_body.write_i16(1);
    create_body.write_i32(0);
    create_body.write_i32(0);
    create_body.write_i32(5_000);
    create_body.write_bool(false);

    let (_, create_resp) = round_trip(
        kafka_addr,
        API_KEY_CREATE_TOPICS,
        2,
        1,
        &create_body.freeze(),
    )
    .await;
    let mut d = Decoder::new(create_resp);
    assert_eq!(d.read_i32().unwrap(), 0); // throttle_time_ms
    assert_eq!(d.read_i32().unwrap(), 1); // topics array length
    assert_eq!(
        d.read_nullable_string().unwrap().unwrap(),
        "wire-topic"
    );
    assert_eq!(d.read_i16().unwrap(), ERROR_NONE);

    let mut meta_body = Encoder::with_capacity(16);
    meta_body.write_i32(1);
    meta_body
        .write_nullable_string(Some("wire-topic"))
        .expect("topic name");
    meta_body.write_bool(true);

    let (_, meta_resp) =
        round_trip(kafka_addr, API_KEY_METADATA, 4, 2, &meta_body.freeze()).await;
    let topic_error = parse_metadata_v4_topic_error(&meta_resp);
    assert_eq!(topic_error, ERROR_NONE);

    harness.stop().await.expect("stop iggy");
}

#[tokio::test]
async fn bridge_kafka_wire_metadata_unknown_topic() {
    let mut harness = TestHarnessBuilder::default()
        .server(iggy_server_config())
        .build()
        .expect("harness");
    harness.start().await.expect("start iggy");

    let bridge = iggy_bridge_from_harness(&harness).await;
    let (kafka_addr, _shutdown) = spawn_test_server_with_bridge(bridge).await;

    let mut meta_body = Encoder::with_capacity(16);
    meta_body.write_i32(1);
    meta_body
        .write_nullable_string(Some("missing-topic"))
        .expect("topic name");
    meta_body.write_bool(true);

    let (_, meta_resp) =
        round_trip(kafka_addr, API_KEY_METADATA, 4, 3, &meta_body.freeze()).await;
    assert_eq!(
        parse_metadata_v4_topic_error(&meta_resp),
        ERROR_UNKNOWN_TOPIC_OR_PARTITION
    );

    harness.stop().await.expect("stop iggy");
}

#[tokio::test]
async fn bridge_list_offsets_latest_after_produce() {
    let mut harness = TestHarnessBuilder::default()
        .server(iggy_server_config())
        .build()
        .expect("harness");
    harness.start().await.expect("start iggy");

    let bridge = iggy_bridge_from_harness(&harness).await;
    let (kafka_addr, _shutdown) = spawn_test_server_with_bridge(bridge).await;

    let mut create_body = Encoder::with_capacity(64);
    create_body.write_i32(1);
    create_body
        .write_nullable_string(Some("offset-topic"))
        .expect("topic name");
    create_body.write_i32(1);
    create_body.write_i16(1);
    create_body.write_i32(0);
    create_body.write_i32(0);
    create_body.write_i32(5_000);
    create_body.write_bool(false);
    round_trip(
        kafka_addr,
        API_KEY_CREATE_TOPICS,
        2,
        10,
        &create_body.freeze(),
    )
    .await;

    let bridge_direct = iggy_bridge_from_harness(&harness).await;
    bridge_direct
        .produce("offset-topic", 0, Bytes::from_static(b"x"))
        .await
        .expect("produce");

    let mut list_body = Encoder::with_capacity(32);
    list_body.write_i32(-1); // replica_id
    list_body.write_i32(1); // topics array length
    list_body
        .write_nullable_string(Some("offset-topic"))
        .expect("topic");
    list_body.write_i32(1); // partitions array length
    list_body.write_i32(0); // partition index
    list_body.write_i64(-1); // timestamp = latest

    let (_, list_resp) = round_trip(
        kafka_addr,
        API_KEY_LIST_OFFSETS,
        1,
        11,
        &list_body.freeze(),
    )
    .await;
    let mut d = Decoder::new(list_resp);
    assert_eq!(d.read_i32().unwrap(), 1); // topics (v1 has no throttle prefix)
    assert_eq!(
        d.read_nullable_string().unwrap().unwrap(),
        "offset-topic"
    );
    assert_eq!(d.read_i32().unwrap(), 1); // partitions
    assert_eq!(d.read_i32().unwrap(), 0); // partition index
    assert_eq!(d.read_i16().unwrap(), ERROR_NONE);
    d.read_i64().unwrap(); // timestamp unavailable sentinel
    assert!(d.read_i64().unwrap() >= 1); // latest offset

    harness.stop().await.expect("stop iggy");
}
