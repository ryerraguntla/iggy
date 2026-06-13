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

//! Iggy backend connection settings (`IGGY_TCP_ADDR`, credentials).

use iggy::prelude::{DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME};

/// Connection settings for the Iggy TCP backend.
#[derive(Debug, Clone)]
pub struct IggyBridgeConfig {
    pub server_address: String,
    pub username: String,
    pub password: String,
}

impl Default for IggyBridgeConfig {
    fn default() -> Self {
        Self {
            server_address: "127.0.0.1:8090".to_string(),
            username: DEFAULT_ROOT_USERNAME.to_string(),
            password: DEFAULT_ROOT_PASSWORD.to_string(),
        }
    }
}

impl IggyBridgeConfig {
    /// Load from environment variables.
    ///
    /// | Variable | Default |
    /// |----------|---------|
    /// | `IGGY_TCP_ADDR` | `127.0.0.1:8090` |
    /// | `IGGY_USERNAME` | root |
    /// | `IGGY_PASSWORD` | root |
    #[must_use]
    pub fn from_env() -> Self {
        let mut config = Self::default();
        if let Ok(addr) = std::env::var("IGGY_TCP_ADDR") {
            config.server_address = addr;
        }
        if let Ok(user) = std::env::var("IGGY_USERNAME") {
            config.username = user;
        }
        if let Ok(pass) = std::env::var("IGGY_PASSWORD") {
            config.password = pass;
        }
        config
    }
}
