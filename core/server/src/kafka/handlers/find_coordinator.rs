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

//! `FindCoordinator` handler (API key 10).
//!
//! Clients call this before `JoinGroup` to find the group coordinator.
//! Because Iggy runs as a single-node broker in this configuration, we
//! always return ourselves as the coordinator.

use crate::kafka::protocol::types::{
    write_compact_string, write_empty_tagged_fields, write_i16, write_i32, write_nullable_string,
    write_string,
};
use crate::shard::IggyShard;
use bytes::{Bytes, BytesMut};
use std::rc::Rc;

pub async fn handle(
    _api_version: i16,
    _payload: &Bytes,
    flexible: bool,
    shard: &Rc<IggyShard>,
) -> Vec<u8> {
    let bound_addr = shard
        .kafka_bound_address
        .get()
        .map(|a| a.to_string())
        .unwrap_or_else(|| shard.config.kafka.address.clone());

    let host = bound_addr
        .split(':')
        .next()
        .unwrap_or("127.0.0.1")
        .to_string();
    let port: i32 = bound_addr
        .split(':')
        .next_back()
        .and_then(|p| p.parse().ok())
        .unwrap_or(9092);
    let node_id = shard.id as i32;

    let mut body = BytesMut::new();
    write_i32(&mut body, 0); // throttle_time_ms
    write_i16(&mut body, 0); // error_code = NONE
    write_nullable_string(&mut body, None); // error_message

    write_i32(&mut body, node_id);
    if flexible {
        write_compact_string(&mut body, &host);
    } else {
        write_string(&mut body, &host);
    }
    write_i32(&mut body, port);

    if flexible {
        write_empty_tagged_fields(&mut body);
    }

    body.freeze().to_vec()
}
