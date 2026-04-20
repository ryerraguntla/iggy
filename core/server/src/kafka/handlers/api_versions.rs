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

//! `ApiVersions` handler (API key 18).
//!
//! Clients send this request before any other to discover which API keys the
//! broker supports and what version range each key accepts.  The response
//! drives the version negotiation that determines whether flexible encoding
//! is used for subsequent requests.

use crate::kafka::protocol::request::api_key;
use crate::kafka::protocol::types::{
    write_empty_tagged_fields, write_i16, write_i32, write_unsigned_varint,
};
use bytes::{Bytes, BytesMut};

/// (api_key, min_version, max_version) for every API this bridge supports.
const SUPPORTED_APIS: &[(i16, i16, i16)] = &[
    (api_key::PRODUCE, 0, 9),
    (api_key::FETCH, 0, 12),
    (api_key::LIST_OFFSETS, 0, 6),
    (api_key::METADATA, 0, 9),
    (api_key::OFFSET_COMMIT, 0, 8),
    (api_key::OFFSET_FETCH, 0, 6),
    (api_key::FIND_COORDINATOR, 0, 3),
    (api_key::JOIN_GROUP, 0, 6),
    (api_key::HEARTBEAT, 0, 4),
    (api_key::LEAVE_GROUP, 0, 4),
    (api_key::SYNC_GROUP, 0, 4),
    (api_key::SASL_HANDSHAKE, 0, 1),
    (api_key::API_VERSIONS, 0, 3),
    (api_key::CREATE_TOPICS, 0, 5),
    (api_key::SASL_AUTHENTICATE, 0, 2),
];

/// Build an `ApiVersions` response body.
///
/// The payload layout (classic encoding):
/// ```text
/// error_code: i16
/// [api_key: i16, min_version: i16, max_version: i16] × N  (i32-length array)
/// throttle_time_ms: i32
/// ```
///
/// Flexible encoding wraps arrays in compact (varint) form and appends a
/// tagged-fields byte at the end of each struct.
pub fn handle(_api_version: i16, _payload: &Bytes, flexible: bool) -> Vec<u8> {
    let mut body = BytesMut::new();

    write_i16(&mut body, 0); // error_code = NONE

    if flexible {
        // compact array: length+1 as unsigned varint
        write_unsigned_varint(&mut body, SUPPORTED_APIS.len() as u64 + 1);
    } else {
        write_i32(&mut body, SUPPORTED_APIS.len() as i32);
    }

    for &(key, min_v, max_v) in SUPPORTED_APIS {
        write_i16(&mut body, key);
        write_i16(&mut body, min_v);
        write_i16(&mut body, max_v);
        if flexible {
            write_empty_tagged_fields(&mut body);
        }
    }

    write_i32(&mut body, 0); // throttle_time_ms
    if flexible {
        write_empty_tagged_fields(&mut body);
    }

    body.freeze().to_vec()
}
