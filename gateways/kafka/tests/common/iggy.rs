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

//! Minimal `iggy-server` launcher for Kafka bridge integration tests.
//!
//! The shared integration harness waits for `runtime/current_config.toml`.
//! With pre-reserved TCP ports and all transports enabled that write can race
//! or never arrive under parallel load. Bridge tests only need TCP, so this
//! helper disables the other listeners, uses a single shard, and probes TCP
//! readiness directly instead of the config file.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use iggy::prelude::{DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME};
use tokio::net::TcpStream;

const STARTUP_TIMEOUT: Duration = Duration::from_secs(60);

pub struct IggyTestServer {
    child: Child,
    tcp_addr: SocketAddr,
    data_path: PathBuf,
}

impl IggyTestServer {
    pub async fn start() -> Self {
        let workspace_root = workspace_root();
        let data_path = unique_data_dir(&workspace_root);
        std::fs::create_dir_all(&data_path).expect("create iggy data dir");

        let tcp_addr = reserve_ephemeral_port()
            .await
            .expect("reserve ephemeral tcp port");

        let mut command = Command::new(iggy_server_binary());
        command
            .current_dir(&workspace_root)
            .env("IGGY_SYSTEM_PATH", &data_path)
            .env("IGGY_TCP_ADDRESS", tcp_addr.to_string())
            .env("IGGY_QUIC_ENABLED", "false")
            .env("IGGY_HTTP_ENABLED", "false")
            .env("IGGY_WEBSOCKET_ENABLED", "false")
            .env("IGGY_SYSTEM_SHARDING_CPU_ALLOCATION", "0..1")
            .env("IGGY_SHARD_RUNTIME_CAPACITY", "256")
            .env("IGGY_ROOT_USERNAME", DEFAULT_ROOT_USERNAME)
            .env("IGGY_ROOT_PASSWORD", DEFAULT_ROOT_PASSWORD)
            .stdout(Stdio::null())
            .stderr(Stdio::null());

        let child = command.spawn().expect("spawn iggy-server");

        let mut server = Self {
            child,
            tcp_addr,
            data_path,
        };
        server
            .wait_for_tcp_ready()
            .await
            .expect("wait for iggy tcp listener");
        server
    }

    pub fn tcp_addr(&self) -> SocketAddr {
        self.tcp_addr
    }

    pub fn stop(mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    async fn wait_for_tcp_ready(&mut self) -> Result<(), String> {
        let deadline = Instant::now() + STARTUP_TIMEOUT;

        loop {
            if let Ok(Some(status)) = self.child.try_wait() {
                return Err(format!("iggy-server exited early with status: {status}"));
            }

            if TcpStream::connect(self.tcp_addr).await.is_ok() {
                return Ok(());
            }

            if Instant::now() >= deadline {
                let _ = self.child.kill();
                return Err(format!(
                    "iggy-server tcp listener not ready within {}s (address: {})",
                    STARTUP_TIMEOUT.as_secs(),
                    self.tcp_addr
                ));
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

impl Drop for IggyTestServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

async fn reserve_ephemeral_port() -> std::io::Result<SocketAddr> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    drop(listener);
    Ok(addr)
}

fn workspace_root() -> PathBuf {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root")
        .to_path_buf()
}

fn iggy_server_binary() -> PathBuf {
    if let Ok(path) = std::env::var("CARGO_BIN_EXE_iggy-server") {
        return PathBuf::from(path);
    }

    let candidate = workspace_root().join("target/debug/iggy-server");
    assert!(
        candidate.exists(),
        "build iggy-server first: cargo build -p server --bin iggy-server"
    );
    candidate
}

fn unique_data_dir(workspace_root: &Path) -> PathBuf {
    let test_name = std::thread::current()
        .name()
        .unwrap_or("kafka_bridge_test")
        .replace("::", "_");
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_nanos();
    workspace_root
        .join("test_logs")
        .join(format!("{test_name}_{suffix}"))
}
