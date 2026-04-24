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

//! TDD test: Kafka wire-protocol produce → Iggy native poll.
//!
//! ## Message path traced by this test
//!
//! ```text
//! rskafka producer (SASL/PLAIN iggy:iggy)
//!   └─► TCP :kafka_port
//!         └─► kafka_listener.rs  (SO_REUSEPORT accept loop)
//!               └─► connection_handler.rs
//!                     ├─ ApiVersions  (no auth required)
//!                     ├─ SaslHandshake PLAIN
//!                     ├─ SaslAuthenticate → shard.login_user("iggy","iggy")
//!                     ├─ Metadata  → shard topology
//!                     └─ Produce   → produce.rs → IggyMessage write  ← Phase 2
//! Iggy SDK poll_messages("kafka", "kafka-tdd-topic")
//!   └─► asserts payload == "hello-from-kafka"
//! ```
//!
//! ## TDD Status
//!
//! | Phase | Component | Status |
//! |-------|-----------|--------|
//! | 1 | SASL handshake + auth | implemented |
//! | 1 | ApiVersions response | implemented |
//! | 1 | Metadata response | implemented |
//! | 2 | RecordBatch → IggyMessage in produce.rs | **NOT YET** — test drives this |
//!
//! The test will fail at the final `assert!(!polled.messages.is_empty(), …)` until
//! `produce.rs` parses the Kafka `RecordBatch` and calls `shard.append_messages(…)`.
//!
//! ## Authentication note
//!
//! The full end-to-end test sets `IGGY_KAFKA_REQUIRE_SASL=false` so that
//! rskafka 0.4 (which lacks built-in SASL support) can connect without
//! authentication.  When that flag is false the server auto-authenticates
//! every incoming Kafka connection as root — **never** use this in production.
//!
//! Run all Kafka tests (including the end-to-end one) with:
//! ```bash
//! cargo test -p integration kafka -- --nocapture
//! ```

use iggy::prelude::*;
use integration::harness::{TestHarness, TestServerConfig};
use rskafka::client::{ClientBuilder, partition::UnknownTopicHandling};
use rskafka::chrono::Utc;
use rskafka::record::Record;
use std::collections::HashMap;
use std::net::TcpListener as StdTcpListener;
use tokio::time::{Duration, sleep, timeout};
use tracing::info;

const KAFKA_STREAM: &str = "kafka";
const TEST_TOPIC: &str = "kafka-tdd-topic";
const TEST_PAYLOAD: &[u8] = b"hello-from-kafka";

/// Reserve a free OS port and immediately release it.
///
/// There is a small TOCTOU window between release and server bind.
/// This is acceptable for testing because the window is tiny (microseconds)
/// and the alternative (port 0 + config file parsing) requires more plumbing.
fn reserve_free_port() -> u16 {
    StdTcpListener::bind("127.0.0.1:0")
        .expect("Failed to bind port-finder socket")
        .local_addr()
        .expect("Failed to get local address")
        .port()
}

/// Wait until a TCP port accepts connections or the deadline is reached.
async fn wait_for_port(addr: &str, max_wait: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + max_wait;
    loop {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        sleep(Duration::from_millis(50)).await;
    }
}

/// End-to-end test: produce a message through the Kafka wire protocol and
/// verify it appears when polled via the native Iggy SDK.
///
/// Uses `require_sasl = false` so rskafka (which lacks built-in SASL support)
/// can connect without authentication.  The server auto-authenticates the
/// connection as root when that flag is set.
#[tokio::test]
async fn test_kafka_produce_appears_in_iggy() {
    let kafka_port = reserve_free_port();
    let kafka_addr = format!("127.0.0.1:{kafka_port}");

    info!("Starting Iggy server with Kafka listener on {kafka_addr}");

    let mut harness = TestHarness::builder()
        .server(
            TestServerConfig::builder()
                .quic_enabled(false)
                .websocket_enabled(false)
                .extra_envs(HashMap::from([
                    ("IGGY_KAFKA_ENABLED".to_string(), "true".to_string()),
                    ("IGGY_KAFKA_ADDRESS".to_string(), kafka_addr.clone()),
                    // Disable SASL requirement so rskafka 0.4 (no SASL support)
                    // can connect.  The server auto-authenticates as root.
                    // NEVER use this in production.
                    ("IGGY_KAFKA_REQUIRE_SASL".to_string(), "false".to_string()),
                ]))
                .build(),
        )
        .build()
        .expect("Failed to build harness");

    harness.start().await.expect("Failed to start harness");

    // ── Pre-create the Kafka stream and topic via native Iggy SDK ─────────
    // The Kafka listener maps Kafka topic "T" → Iggy stream "kafka" / topic "T".
    // The server does not auto-create streams, so we seed them here.
    let iggy = harness
        .tcp_root_client()
        .await
        .expect("Failed to create Iggy root client");

    iggy.create_stream(KAFKA_STREAM)
        .await
        .expect("Failed to create 'kafka' stream");

    iggy.create_topic(
        &Identifier::named(KAFKA_STREAM).unwrap(),
        TEST_TOPIC,
        1,
        Default::default(),
        None,
        IggyExpiry::NeverExpire,
        MaxTopicSize::ServerDefault,
    )
    .await
    .expect("Failed to create test topic");

    info!(
        "Pre-created stream='{}' topic='{}'",
        KAFKA_STREAM, TEST_TOPIC
    );

    // ── Wait for Kafka port to be ready ───────────────────────────────────
    assert!(
        wait_for_port(&kafka_addr, Duration::from_secs(10)).await,
        "Kafka port {kafka_port} did not become ready within 10 seconds"
    );
    info!("Kafka port {kafka_port} is accepting connections");

    // ── Connect via Kafka wire protocol (rskafka) ─────────────────────────
    // Flow: ApiVersions → Metadata → Produce
    // SASL is not needed because require_sasl=false auto-authenticates as root.
    info!("Connecting rskafka client to {kafka_addr}");

    let kafka_client = timeout(Duration::from_secs(15), async {
        ClientBuilder::new(vec![kafka_addr.clone()])
            .build()
            .await
    })
    .await
    .expect("rskafka connect timed out")
    .expect("Failed to build rskafka client — check SASL auth in connection_handler.rs");

    info!("rskafka connected — building partition client for '{TEST_TOPIC}' partition 0");

    let partition_client = kafka_client
        .partition_client(TEST_TOPIC, 0, UnknownTopicHandling::Retry)
        .await
        .expect("Failed to get rskafka partition client");

    // ── Produce one message via Kafka wire protocol ───────────────────────
    info!(
        "Producing '{}' via Kafka protocol",
        String::from_utf8_lossy(TEST_PAYLOAD)
    );

    let record = Record {
        key: None,
        value: Some(TEST_PAYLOAD.to_vec()),
        headers: Default::default(),
        timestamp: Utc::now(),
    };

    partition_client
        .produce(
            vec![record],
            rskafka::client::partition::Compression::NoCompression,
        )
        .await
        .expect(
            "Produce failed — either SASL auth is missing or produce.rs (Phase 2) \
             is not yet writing RecordBatch to the Iggy partition",
        );

    info!(
        "✓ Produced '{}' via Kafka wire protocol",
        String::from_utf8_lossy(TEST_PAYLOAD)
    );

    // Give the server a moment to persist the message
    sleep(Duration::from_millis(100)).await;

    // ── Verify the message arrived via native Iggy SDK ────────────────────
    let polled = iggy
        .poll_messages(
            &Identifier::named(KAFKA_STREAM).unwrap(),
            &Identifier::named(TEST_TOPIC).unwrap(),
            Some(0),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            10,
            false,
        )
        .await
        .expect("Failed to poll messages from Iggy");

    info!(
        "Polled {} message(s) from Iggy SDK",
        polled.messages.len()
    );

    // ── TDD RED assertion — drives Phase 2 implementation ─────────────────
    // This assertion fails until produce.rs converts the Kafka RecordBatch
    // into an IggyMessagesBatchMut and calls shard.append_messages(…).
    assert!(
        !polled.messages.is_empty(),
        "Phase 2 incomplete: Kafka Produce request received but no message stored in Iggy. \
         Implement RecordBatch → IggyMessagesBatchMut conversion in \
         core/server/src/kafka/handlers/produce.rs and call shard.append_messages(…)."
    );

    let payload = String::from_utf8_lossy(&polled.messages[0].payload);
    assert_eq!(
        payload.as_ref(),
        String::from_utf8_lossy(TEST_PAYLOAD).as_ref(),
        "Message payload mismatch: got '{payload}', expected '{}'",
        String::from_utf8_lossy(TEST_PAYLOAD)
    );

    info!("✓ End-to-end Kafka → Iggy flow verified!");
}

/// Smoke test: verify the Kafka listener accepts TCP connections and responds
/// to ApiVersions with a valid Kafka frame (correlation_id echo, no error).
///
/// This test does NOT require SASL and should PASS with the current implementation.
#[tokio::test]
async fn test_kafka_listener_responds_to_api_versions() {
    let kafka_port = reserve_free_port();
    let kafka_addr = format!("127.0.0.1:{kafka_port}");

    let mut harness = TestHarness::builder()
        .server(
            TestServerConfig::builder()
                .quic_enabled(false)
                .websocket_enabled(false)
                .extra_envs(HashMap::from([
                    ("IGGY_KAFKA_ENABLED".to_string(), "true".to_string()),
                    ("IGGY_KAFKA_ADDRESS".to_string(), kafka_addr.clone()),
                ]))
                .build(),
        )
        .build()
        .expect("Failed to build harness");

    harness.start().await.expect("Failed to start harness");

    assert!(
        wait_for_port(&kafka_addr, Duration::from_secs(10)).await,
        "Kafka port {kafka_port} did not become ready"
    );

    // Raw Kafka ApiVersions v3 request (flexible encoding)
    // Frame layout: [4-byte length][api_key=18][api_version=3][correlation_id=1][flexible header][body]
    let mut frame_body: Vec<u8> = Vec::new();
    // api_key (int16): 18 = ApiVersions
    frame_body.extend_from_slice(&18i16.to_be_bytes());
    // api_version (int16): 3
    frame_body.extend_from_slice(&3i16.to_be_bytes());
    // correlation_id (int32): 1
    frame_body.extend_from_slice(&1i32.to_be_bytes());
    // client_id: compact nullable string (varint len+1=0 means null)
    frame_body.push(0); // null client_id (varint 0 = null compact string)
    // tagged fields: 0 fields
    frame_body.push(0);
    // body: client_software_name (compact string "rskafka-test")
    let name = b"rskafka-test";
    frame_body.push((name.len() + 1) as u8); // compact string length+1
    frame_body.extend_from_slice(name);
    // client_software_version (compact string "0.1")
    let version = b"0.1";
    frame_body.push((version.len() + 1) as u8);
    frame_body.extend_from_slice(version);
    // tagged fields: 0
    frame_body.push(0);

    let frame_len = frame_body.len() as i32;
    let mut wire: Vec<u8> = frame_len.to_be_bytes().to_vec();
    wire.extend_from_slice(&frame_body);

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(&kafka_addr)
        .await
        .expect("Failed to connect raw TCP to Kafka port");

    stream.write_all(&wire).await.expect("Failed to write ApiVersions");

    // Read response: 4-byte length, then body
    let mut resp_len_buf = [0u8; 4];
    timeout(Duration::from_secs(5), stream.read_exact(&mut resp_len_buf))
        .await
        .expect("Timed out reading ApiVersions response length")
        .expect("Failed to read response length");

    let resp_len = i32::from_be_bytes(resp_len_buf);
    assert!(
        resp_len > 0 && resp_len < 65536,
        "Unexpected response frame length: {resp_len}"
    );

    let mut resp_body = vec![0u8; resp_len as usize];
    timeout(Duration::from_secs(5), stream.read_exact(&mut resp_body))
        .await
        .expect("Timed out reading ApiVersions response body")
        .expect("Failed to read response body");

    // Flexible ApiVersions response: correlation_id (int32) + error_code (int16)
    assert!(resp_body.len() >= 6, "Response too short: {} bytes", resp_body.len());

    let resp_correlation_id = i32::from_be_bytes(resp_body[0..4].try_into().unwrap());
    let error_code = i16::from_be_bytes(resp_body[4..6].try_into().unwrap());

    assert_eq!(
        resp_correlation_id, 1,
        "Correlation ID mismatch: expected 1, got {resp_correlation_id}"
    );
    assert_eq!(
        error_code, 0,
        "ApiVersions returned error code {error_code}: expected 0 (success)"
    );

    info!(
        "✓ Kafka listener responded to ApiVersions: correlation_id={resp_correlation_id}, \
         error_code={error_code}, response_len={resp_len} bytes"
    );
}
