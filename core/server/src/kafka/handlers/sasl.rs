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

//! SASL authentication handlers (API keys 17 and 36).
//!
//! The Kafka SASL handshake is a two-step process:
//!
//! 1. **`SaslHandshake` (key 17)** — the client declares which SASL mechanism
//!    it wants to use.  We support only `PLAIN`.
//!
//! 2. **`SaslAuthenticate` (key 36)** — the client sends the SASL token (for
//!    PLAIN: `\0username\0password`).  We call `shard.login_user()` and store
//!    the resulting user ID on the Iggy session so subsequent requests pass
//!    `ensure_authenticated`.

use crate::kafka::error::KafkaErrorCode;
use crate::kafka::protocol::types::{
    read_bytes, read_compact_bytes, read_compact_string, read_string,
    write_bytes, write_compact_bytes, write_compact_string, write_empty_tagged_fields, write_i16,
    write_i32, write_i64, write_nullable_string, write_string, write_unsigned_varint,
};
use crate::kafka::session::KafkaSession;
use crate::shard::IggyShard;
use crate::streaming::session::Session;
use bytes::{Bytes, BytesMut};
use std::rc::Rc;
use tracing::warn;

const SASL_PLAIN: &str = "PLAIN";

/// `SaslHandshake` — negotiate SASL mechanism.
///
/// Returns `UNSUPPORTED_SASL_MECHANISM` for anything other than `PLAIN`
/// and lists the supported mechanisms so the client can retry.
pub fn handle_handshake(_api_version: i16, payload: &Bytes, flexible: bool) -> Vec<u8> {
    let mut buf = payload.clone();
    let mechanism = if flexible {
        read_compact_string(&mut buf)
    } else {
        read_string(&mut buf)
    };

    let error_code: i16 = if mechanism == SASL_PLAIN {
        0
    } else {
        warn!("Kafka client requested unsupported SASL mechanism: {mechanism}");
        KafkaErrorCode::UnsupportedSaslMechanism as i16
    };

    let mut body = BytesMut::new();
    write_i16(&mut body, error_code);

    // enabled_mechanisms array
    if flexible {
        write_unsigned_varint(&mut body, 2); // 1 element + 1
        write_compact_string(&mut body, SASL_PLAIN);
    } else {
        write_i32(&mut body, 1);
        write_string(&mut body, SASL_PLAIN);
    }
    if flexible {
        write_empty_tagged_fields(&mut body);
    }

    body.freeze().to_vec()
}

/// `SaslAuthenticate` — verify credentials and establish the user session.
///
/// Parses the PLAIN token (`\0username\0password`), calls
/// `shard.login_user()`, and — on success — stores the user ID on the Iggy
/// session via `session.set_user_id()` and marks `kafka_session.authenticated`.
pub fn handle_authenticate(
    api_version: i16,
    payload: &Bytes,
    flexible: bool,
    kafka_session: &mut KafkaSession,
    shard: &Rc<IggyShard>,
    session: &Session,
) -> Vec<u8> {
    let mut buf = payload.clone();

    let auth_bytes: Bytes = if flexible {
        read_compact_bytes(&mut buf).unwrap_or_default()
    } else {
        read_bytes(&mut buf).unwrap_or_default()
    };

    // SASL PLAIN token format: NUL authzid NUL username NUL password
    let parts: Vec<&[u8]> = auth_bytes.split(|&b| b == 0).collect();

    let (error_code, error_msg) = if parts.len() >= 3 {
        let username = std::str::from_utf8(parts[1]).unwrap_or("");
        let password = std::str::from_utf8(parts[2]).unwrap_or("");

        match shard.login_user(username, password, Some(session)) {
            Ok(user) => {
                session.set_user_id(user.id);
                kafka_session.authenticated = true;
                (0i16, None)
            }
            Err(e) => {
                warn!(
                    "Kafka SASL/PLAIN authentication failed for '{}': {e}",
                    std::str::from_utf8(parts[1]).unwrap_or("?")
                );
                (
                    KafkaErrorCode::SaslAuthenticationFailed as i16,
                    Some("Authentication failed"),
                )
            }
        }
    } else {
        (
            KafkaErrorCode::SaslAuthenticationFailed as i16,
            Some("Malformed SASL token"),
        )
    };

    let mut body = BytesMut::new();
    write_i16(&mut body, error_code);

    // error_message (nullable string)
    write_nullable_string(&mut body, error_msg);

    // auth_bytes in response (empty for PLAIN)
    if flexible {
        write_compact_bytes(&mut body, Some(&[]));
    } else {
        write_bytes(&mut body, Some(&[]));
    }

    // session_lifetime_ms (added in v1)
    if api_version >= 1 {
        write_i64(&mut body, if error_code == 0 { i64::MAX } else { 0 });
    }

    if flexible {
        write_empty_tagged_fields(&mut body);
    }

    body.freeze().to_vec()
}
