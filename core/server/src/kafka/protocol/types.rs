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

//! Kafka wire-protocol primitive read/write helpers.
//!
//! All Kafka integers are big-endian. Two encoding families exist:
//!
//! * **Classic** (API versions < the flexible boundary): fixed-width integers,
//!   `i16`-length-prefixed strings, `i32`-length-prefixed byte arrays, and
//!   `i32`-length-prefixed arrays.
//!
//! * **Flexible** (API versions ≥ the flexible boundary): same integers, but
//!   strings and arrays use unsigned-varint length prefixes (compact encoding).
//!   Every struct also ends with a tagged-fields section (empty = `0x00`).
//!
//! These helpers abstract over both families so each handler can call the
//! right function based on the `flexible` flag set in `request::is_flexible`.

use bytes::{Buf, BufMut, Bytes, BytesMut};

// ─── Reading ────────────────────────────────────────────────────────────────

pub fn read_i8(buf: &mut Bytes) -> i8 {
    buf.get_i8()
}

pub fn read_i16(buf: &mut Bytes) -> i16 {
    buf.get_i16()
}

pub fn read_i32(buf: &mut Bytes) -> i32 {
    buf.get_i32()
}

pub fn read_i64(buf: &mut Bytes) -> i64 {
    buf.get_i64()
}

/// Kafka nullable string: `i16` byte-length (-1 = null), then UTF-8 bytes.
pub fn read_nullable_string(buf: &mut Bytes) -> Option<String> {
    let len = buf.get_i16();
    if len < 0 {
        return None;
    }
    let bytes = buf.copy_to_bytes(len as usize);
    String::from_utf8(bytes.to_vec()).ok()
}

/// Kafka non-nullable string (same encoding; treats null as empty string).
pub fn read_string(buf: &mut Bytes) -> String {
    read_nullable_string(buf).unwrap_or_default()
}

/// Kafka nullable bytes: `i32` byte-length (-1 = null), then raw bytes.
pub fn read_bytes(buf: &mut Bytes) -> Option<Bytes> {
    let len = buf.get_i32();
    if len < 0 {
        return None;
    }
    Some(buf.copy_to_bytes(len as usize))
}

/// Compact bytes: unsigned-varint `length + 1` (0 = null), then raw bytes.
pub fn read_compact_bytes(buf: &mut Bytes) -> Option<Bytes> {
    let len_plus_one = read_unsigned_varint(buf);
    if len_plus_one == 0 {
        return None;
    }
    let len = len_plus_one as usize - 1;
    Some(buf.copy_to_bytes(len))
}

/// Unsigned variable-length integer as used by compact encoding.
/// Each byte contributes 7 bits; the MSB signals continuation.
pub fn read_unsigned_varint(buf: &mut Bytes) -> u64 {
    let mut value: u64 = 0;
    let mut shift = 0u32;
    loop {
        if !buf.has_remaining() {
            break;
        }
        let byte = buf.get_u8();
        value |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    value
}

/// Compact (flexible) string: unsigned-varint `length + 1` (0 = null/empty),
/// then UTF-8 bytes.
pub fn read_compact_string(buf: &mut Bytes) -> String {
    let len_plus_one = read_unsigned_varint(buf);
    if len_plus_one == 0 {
        return String::new();
    }
    let len = len_plus_one as usize - 1;
    let bytes = buf.copy_to_bytes(len);
    String::from_utf8(bytes.to_vec()).unwrap_or_default()
}

/// Compact nullable string: 0 = null (returns None).
pub fn read_compact_nullable_string(buf: &mut Bytes) -> Option<String> {
    let len_plus_one = read_unsigned_varint(buf);
    if len_plus_one == 0 {
        return None;
    }
    let len = len_plus_one as usize - 1;
    let bytes = buf.copy_to_bytes(len);
    String::from_utf8(bytes.to_vec()).ok()
}

/// Skip the tagged-fields section at the end of a flexible struct.
/// Must be called after reading all known fields.
pub fn skip_tagged_fields(buf: &mut Bytes) {
    let count = read_unsigned_varint(buf);
    for _ in 0..count {
        read_unsigned_varint(buf); // tag number
        let size = read_unsigned_varint(buf) as usize;
        if buf.remaining() >= size {
            buf.advance(size);
        }
    }
}

// ─── Writing ────────────────────────────────────────────────────────────────

pub fn write_i8(buf: &mut BytesMut, v: i8) {
    buf.put_i8(v);
}

pub fn write_i16(buf: &mut BytesMut, v: i16) {
    buf.put_i16(v);
}

pub fn write_i32(buf: &mut BytesMut, v: i32) {
    buf.put_i32(v);
}

pub fn write_i64(buf: &mut BytesMut, v: i64) {
    buf.put_i64(v);
}

/// Write a Kafka nullable string (`i16` length prefix, -1 = null).
pub fn write_nullable_string(buf: &mut BytesMut, s: Option<&str>) {
    match s {
        None => buf.put_i16(-1),
        Some(s) => {
            buf.put_i16(s.len() as i16);
            buf.put_slice(s.as_bytes());
        }
    }
}

/// Write a Kafka non-nullable string.
pub fn write_string(buf: &mut BytesMut, s: &str) {
    write_nullable_string(buf, Some(s));
}

/// Write a compact (flexible) non-nullable string.
pub fn write_compact_string(buf: &mut BytesMut, s: &str) {
    write_unsigned_varint(buf, s.len() as u64 + 1);
    buf.put_slice(s.as_bytes());
}

/// Write a compact nullable string (0 = null).
pub fn write_compact_nullable_string(buf: &mut BytesMut, s: Option<&str>) {
    match s {
        None => write_unsigned_varint(buf, 0),
        Some(s) => {
            write_unsigned_varint(buf, s.len() as u64 + 1);
            buf.put_slice(s.as_bytes());
        }
    }
}

/// Write an unsigned variable-length integer.
pub fn write_unsigned_varint(buf: &mut BytesMut, mut v: u64) {
    loop {
        let byte = (v & 0x7F) as u8;
        v >>= 7;
        if v == 0 {
            buf.put_u8(byte);
            break;
        } else {
            buf.put_u8(byte | 0x80);
        }
    }
}

/// Write an empty tagged-fields section (a single `0x00` byte).
pub fn write_empty_tagged_fields(buf: &mut BytesMut) {
    write_unsigned_varint(buf, 0);
}

/// Write Kafka bytes field (`i32` length, -1 = null).
pub fn write_bytes(buf: &mut BytesMut, data: Option<&[u8]>) {
    match data {
        None => buf.put_i32(-1),
        Some(d) => {
            buf.put_i32(d.len() as i32);
            buf.put_slice(d);
        }
    }
}

/// Write a compact bytes field (unsigned-varint length+1, 0 = null).
pub fn write_compact_bytes(buf: &mut BytesMut, data: Option<&[u8]>) {
    match data {
        None => write_unsigned_varint(buf, 0),
        Some(d) => {
            write_unsigned_varint(buf, d.len() as u64 + 1);
            buf.put_slice(d);
        }
    }
}
