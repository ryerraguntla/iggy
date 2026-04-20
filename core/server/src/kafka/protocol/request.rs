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

//! Kafka request header parsing and API key constants.
//!
//! Every Kafka request begins with a 4-byte big-endian frame length, followed
//! by the request header and then the API-specific payload.  The connection
//! handler reads the frame length separately; this module parses everything
//! after the length field.
//!
//! ## Frame layout (after the 4-byte length prefix)
//!
//! ```text
//! ┌──────────────┬──────────────────┬──────────────────┬──────────────┐
//! │ api_key: i16 │ api_version: i16 │ correlation_id:  │ client_id:   │
//! │              │                  │       i32        │ nullable_str │
//! └──────────────┴──────────────────┴──────────────────┴──────────────┘
//!   [tagged_fields if flexible]  [API-specific payload ...]
//! ```

use super::types::{
    read_compact_nullable_string, read_i16, read_i32, read_nullable_string, skip_tagged_fields,
};
use bytes::Bytes;

/// Parsed Kafka request header (versions 0–2; v2 adds tagged fields).
#[derive(Debug)]
pub struct RequestHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    /// The Kafka `client.id` string from the producer/consumer configuration.
    pub client_id: Option<String>,
}

impl RequestHeader {
    /// Parse from a buffer that starts immediately after the 4-byte frame
    /// length.  `flexible` must be determined by peeking `api_key` and
    /// `api_version` before calling this (see [`is_flexible`]).
    pub fn parse(buf: &mut Bytes, flexible: bool) -> Self {
        let api_key = read_i16(buf);
        let api_version = read_i16(buf);
        let correlation_id = read_i32(buf);
        let client_id = if flexible {
            read_compact_nullable_string(buf)
        } else {
            read_nullable_string(buf)
        };
        if flexible {
            skip_tagged_fields(buf); // header tagged fields (always empty for now)
        }
        RequestHeader {
            api_key,
            api_version,
            correlation_id,
            client_id,
        }
    }
}

/// Kafka API key constants for the subset supported in this implementation.
pub mod api_key {
    pub const PRODUCE: i16 = 0;
    pub const FETCH: i16 = 1;
    pub const LIST_OFFSETS: i16 = 2;
    pub const METADATA: i16 = 3;
    pub const OFFSET_COMMIT: i16 = 8;
    pub const OFFSET_FETCH: i16 = 9;
    pub const FIND_COORDINATOR: i16 = 10;
    pub const JOIN_GROUP: i16 = 11;
    pub const HEARTBEAT: i16 = 12;
    pub const LEAVE_GROUP: i16 = 13;
    pub const SYNC_GROUP: i16 = 14;
    pub const SASL_HANDSHAKE: i16 = 17;
    pub const API_VERSIONS: i16 = 18;
    pub const CREATE_TOPICS: i16 = 19;
    pub const SASL_AUTHENTICATE: i16 = 36;
}

/// Returns whether a given `(api_key, api_version)` pair uses the flexible
/// (compact-array) encoding introduced in KIP-482.
///
/// The crossover version differs per API; the values here match the official
/// Kafka protocol specification.
pub fn is_flexible(api_key: i16, api_version: i16) -> bool {
    match api_key {
        api_key::API_VERSIONS => api_version >= 3,
        api_key::METADATA => api_version >= 9,
        api_key::PRODUCE => api_version >= 9,
        api_key::FETCH => api_version >= 12,
        api_key::LIST_OFFSETS => api_version >= 6,
        api_key::OFFSET_COMMIT => api_version >= 8,
        api_key::OFFSET_FETCH => api_version >= 6,
        api_key::FIND_COORDINATOR => api_version >= 3,
        api_key::JOIN_GROUP => api_version >= 6,
        api_key::HEARTBEAT => api_version >= 4,
        api_key::LEAVE_GROUP => api_version >= 4,
        api_key::SYNC_GROUP => api_version >= 4,
        api_key::SASL_HANDSHAKE => false,
        api_key::SASL_AUTHENTICATE => api_version >= 2,
        api_key::CREATE_TOPICS => api_version >= 5,
        _ => false,
    }
}

/// Human-readable name for an API key, used in log messages.
pub fn api_key_name(key: i16) -> &'static str {
    match key {
        api_key::PRODUCE => "Produce",
        api_key::FETCH => "Fetch",
        api_key::LIST_OFFSETS => "ListOffsets",
        api_key::METADATA => "Metadata",
        api_key::OFFSET_COMMIT => "OffsetCommit",
        api_key::OFFSET_FETCH => "OffsetFetch",
        api_key::FIND_COORDINATOR => "FindCoordinator",
        api_key::JOIN_GROUP => "JoinGroup",
        api_key::HEARTBEAT => "Heartbeat",
        api_key::LEAVE_GROUP => "LeaveGroup",
        api_key::SYNC_GROUP => "SyncGroup",
        api_key::SASL_HANDSHAKE => "SaslHandshake",
        api_key::API_VERSIONS => "ApiVersions",
        api_key::CREATE_TOPICS => "CreateTopics",
        api_key::SASL_AUTHENTICATE => "SaslAuthenticate",
        _ => "Unknown",
    }
}
