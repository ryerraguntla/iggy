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

//! `SyncGroup` handler (API key 14).
//!
//! After `JoinGroup`, the leader sends partition assignments for all members.
//! In the current single-member model we simply echo an empty assignment back,
//! which tells the client to use whatever assignment it computed itself.

use crate::kafka::protocol::types::{
    write_bytes, write_compact_bytes, write_empty_tagged_fields, write_i16, write_i32,
};
use bytes::{Bytes, BytesMut};

pub async fn handle(
    _api_version: i16,
    _payload: &Bytes,
    flexible: bool,
) -> Vec<u8> {
    let mut body = BytesMut::new();
    write_i32(&mut body, 0); // throttle_time_ms
    write_i16(&mut body, 0); // error_code = NONE
    // assignment bytes
    if flexible {
        write_compact_bytes(&mut body, Some(&[]));
        write_empty_tagged_fields(&mut body);
    } else {
        write_bytes(&mut body, Some(&[]));
    }
    body.freeze().to_vec()
}
