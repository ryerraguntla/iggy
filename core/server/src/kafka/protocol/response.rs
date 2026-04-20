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

//! Kafka response framing.
//!
//! Every Kafka response is prefixed with a 4-byte big-endian frame length,
//! followed by a response header and the API-specific body.
//!
//! ## Response frame layout
//!
//! ```text
//! ┌──────────────────┬──────────────────────────┬───────────────────┐
//! │ frame_length:i32 │ correlation_id: i32      │ [tagged_fields    │
//! │  (excl. itself)  │                          │  if flexible]     │
//! └──────────────────┴──────────────────────────┴───────────────────┘
//!   [API-specific body ...]
//! ```

use super::types::{write_empty_tagged_fields, write_i32};
use bytes::BytesMut;

/// Wrap a pre-built response body with the standard Kafka frame header.
///
/// `correlation_id` must match the value from the corresponding request header.
/// `flexible` controls whether the response header includes an empty
/// tagged-fields byte (required by KIP-482 flexible encoding).
///
/// Returns the complete framed response ready to be written to the TCP stream.
pub fn frame_response(correlation_id: i32, body: &[u8], flexible: bool) -> BytesMut {
    // Response header: correlation_id (4 bytes) + optional tagged fields (1 byte).
    let header_len: usize = 4 + if flexible { 1 } else { 0 };
    let total_payload = header_len + body.len();

    let mut buf = BytesMut::with_capacity(4 + total_payload);
    write_i32(&mut buf, total_payload as i32); // outer frame length
    write_i32(&mut buf, correlation_id);
    if flexible {
        write_empty_tagged_fields(&mut buf);
    }
    buf.extend_from_slice(body);
    buf
}
