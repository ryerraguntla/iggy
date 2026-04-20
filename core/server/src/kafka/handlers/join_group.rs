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

//! `JoinGroup` handler (API key 11).
//!
//! The client joins (or creates) a consumer group.  We assign a deterministic
//! `member_id` derived from the Iggy `client_id` and always elect this
//! connection as the group leader (single-member groups are the initial
//! supported topology).
//!
//! The resulting `group_id` and `member_id` are stored on `KafkaSession` so
//! that `Heartbeat`, `SyncGroup`, and `LeaveGroup` can reference them.

use crate::kafka::protocol::types::{
    read_bytes, read_compact_bytes, read_compact_nullable_string, read_compact_string,
    read_i32, read_nullable_string, read_string, read_unsigned_varint, skip_tagged_fields,
    write_bytes, write_compact_bytes, write_compact_nullable_string, write_compact_string,
    write_empty_tagged_fields, write_i16, write_i32, write_nullable_string, write_string,
    write_unsigned_varint,
};
use crate::kafka::session::KafkaSession;
use crate::shard::IggyShard;
use crate::streaming::session::Session;
use bytes::{Bytes, BytesMut};
use std::rc::Rc;
use uuid::Uuid;

pub async fn handle(
    api_version: i16,
    payload: &Bytes,
    flexible: bool,
    _shard: &Rc<IggyShard>,
    session: &Session,
    kafka_session: &mut KafkaSession,
) -> Vec<u8> {
    let mut buf = payload.clone();

    let group_id = if flexible {
        read_compact_string(&mut buf)
    } else {
        read_string(&mut buf)
    };
    let _session_timeout_ms = read_i32(&mut buf);
    let _rebalance_timeout_ms = if api_version >= 1 {
        read_i32(&mut buf)
    } else {
        _session_timeout_ms
    };
    let member_id = if flexible {
        read_compact_nullable_string(&mut buf).unwrap_or_default()
    } else {
        read_nullable_string(&mut buf).unwrap_or_default()
    };
    // group_instance_id (v5+)
    if api_version >= 5 {
        if flexible {
            read_compact_nullable_string(&mut buf);
        } else {
            read_nullable_string(&mut buf);
        }
    }
    let protocol_type = if flexible {
        read_compact_string(&mut buf)
    } else {
        read_string(&mut buf)
    };
    // Skip protocols array
    let proto_count = if flexible {
        (read_unsigned_varint(&mut buf) as i64) - 1
    } else {
        read_i32(&mut buf) as i64
    };
    for _ in 0..proto_count.max(0) {
        if flexible {
            read_compact_string(&mut buf); // name
            read_compact_bytes(&mut buf); // metadata
            skip_tagged_fields(&mut buf);
        } else {
            read_string(&mut buf);
            read_bytes(&mut buf);
        }
    }
    if flexible {
        skip_tagged_fields(&mut buf);
    }

    let assigned_member_id = if member_id.is_empty() {
        format!("{}-{}", session.client_id, Uuid::new_v4())
    } else {
        member_id
    };

    kafka_session.group_id = Some(group_id.clone());
    kafka_session.member_id = Some(assigned_member_id.clone());
    kafka_session.generation_id = 1;

    let mut body = BytesMut::new();
    write_i32(&mut body, 0); // throttle_time_ms
    write_i16(&mut body, 0); // error_code = NONE
    write_i32(&mut body, kafka_session.generation_id);

    // protocol_type
    if flexible {
        write_compact_nullable_string(&mut body, Some(&protocol_type));
    } else {
        write_nullable_string(&mut body, Some(&protocol_type));
    }
    // protocol_name
    if flexible {
        write_compact_nullable_string(&mut body, None);
    } else {
        write_nullable_string(&mut body, None);
    }
    // leader (this connection is always leader)
    if flexible {
        write_compact_string(&mut body, &assigned_member_id);
    } else {
        write_string(&mut body, &assigned_member_id);
    }
    // member_id
    if flexible {
        write_compact_string(&mut body, &assigned_member_id);
    } else {
        write_string(&mut body, &assigned_member_id);
    }
    // members array — single member (ourselves)
    if flexible {
        write_unsigned_varint(&mut body, 2);
    } else {
        write_i32(&mut body, 1);
    }
    if flexible {
        write_compact_string(&mut body, &assigned_member_id);
        write_compact_nullable_string(&mut body, None); // group_instance_id
        write_compact_bytes(&mut body, Some(&[])); // metadata
        write_empty_tagged_fields(&mut body);
    } else {
        write_string(&mut body, &assigned_member_id);
        write_bytes(&mut body, Some(&[])); // metadata
    }

    if flexible {
        write_empty_tagged_fields(&mut body);
    }

    body.freeze().to_vec()
}
