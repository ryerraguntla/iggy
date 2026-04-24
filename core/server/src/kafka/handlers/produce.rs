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

//! `Produce` handler (API key 0).
//!
//! Translates a Kafka `Produce` request into calls to `shard.append_messages()`.
//!
//! ## Kafka RecordBatch format (magic = 2, used in API version ≥ 3)
//!
//! ```text
//! base_offset:             i64   (8 bytes)
//! batch_length:            i32   (4 bytes — covers everything below)
//! partition_leader_epoch:  i32   (4 bytes)
//! magic:                   i8    (1 byte, must be 2)
//! crc:                     u32   (4 bytes, CRC32C)
//! attributes:              i16   (2 bytes)
//! last_offset_delta:       i32   (4 bytes)
//! base_timestamp:          i64   (8 bytes)
//! max_timestamp:           i64   (8 bytes)
//! producer_id:             i64   (8 bytes)
//! producer_epoch:          i16   (2 bytes)
//! base_sequence:           i32   (4 bytes)
//! records_count:           i32   (4 bytes)
//! [records...]
//! ```
//!
//! ## Each Record (zigzag-varint framing)
//!
//! ```text
//! length:           signed varint (zigzag, covers everything after this field)
//! attributes:       i8
//! timestamp_delta:  signed varint
//! offset_delta:     signed varint
//! key_length:       signed varint  (-1 = null key)
//! key:              bytes
//! value_length:     signed varint  (-1 = null value)
//! value:            bytes          → becomes IggyMessage payload
//! headers_count:    unsigned varint
//! [header: { key_len: unsigned varint, key_bytes, val_len: signed varint, val_bytes }]
//! ```
//!
//! ## TODO items for production readiness
//! - Verify CRC32C checksum on inbound batches.
//! - Support MessageSet v0/v1 (API versions 0–2) for older clients.
//! - Handle compression (gzip, snappy, lz4, zstd in attributes bits 0–2).
//! - Map producer_id / producer_epoch for exactly-once semantics.

use crate::kafka::error::iggy_to_kafka_error;
use crate::kafka::protocol::types::{
    read_compact_nullable_string, read_compact_string, read_i16, read_i32,
    read_nullable_string, read_string, read_unsigned_varint, skip_tagged_fields,
    write_compact_string, write_empty_tagged_fields, write_i16, write_i32, write_i64, write_string,
    write_unsigned_varint,
};
use crate::kafka::session::KafkaSession;
use crate::shard::IggyShard;
use crate::shard::transmission::message::ResolvedPartition;
use crate::streaming::segments::IggyMessagesBatchMut;
use crate::streaming::session::Session;
use bytes::{Buf, Bytes, BytesMut};
use iggy_common::{HeaderKey, HeaderValue, Identifier, IggyMessage, Sizeable};
use std::collections::HashMap;
use std::rc::Rc;
use tracing::{debug, warn};

type PartitionResult = (i32, i16, i64);
type TopicResults = Vec<(String, Vec<PartitionResult>)>;

pub async fn handle(
    api_version: i16,
    payload: &Bytes,
    flexible: bool,
    shard: &Rc<IggyShard>,
    session: &Session,
    _kafka_session: &KafkaSession,
) -> Vec<u8> {
    let mut buf = payload.clone();

    // transactional_id (v3+, nullable)
    if api_version >= 3 {
        if flexible {
            read_compact_nullable_string(&mut buf);
        } else {
            read_nullable_string(&mut buf);
        }
    }
    let _acks = read_i16(&mut buf);
    let _timeout_ms = read_i32(&mut buf);

    let topic_count = if flexible {
        (read_unsigned_varint(&mut buf) as i64) - 1
    } else {
        read_i32(&mut buf) as i64
    };

    let kafka_stream = shard.config.kafka.kafka_stream.clone();
    let stream_id = match Identifier::named(&kafka_stream) {
        Ok(id) => id,
        Err(_) => return produce_error_response(flexible),
    };

    let mut topic_results: TopicResults = Vec::new();

    for _ in 0..topic_count.max(0) {
        let topic_name = if flexible {
            read_compact_string(&mut buf)
        } else {
            read_string(&mut buf)
        };
        let partition_count = if flexible {
            (read_unsigned_varint(&mut buf) as i64) - 1
        } else {
            read_i32(&mut buf) as i64
        };

        let topic_id = match Identifier::named(&topic_name) {
            Ok(id) => id,
            Err(_) => {
                topic_results.push((topic_name, vec![]));
                continue;
            }
        };

        let resolved_topic =
            shard.resolve_topic_for_append(session.get_user_id(), &stream_id, &topic_id);
        let mut partition_results: Vec<PartitionResult> = Vec::new();

        for _ in 0..partition_count.max(0) {
            let partition_index = read_i32(&mut buf);

            // Record set bytes (the RecordBatch)
            let records: Option<Bytes> = if flexible {
                let len_plus_one = read_unsigned_varint(&mut buf);
                if len_plus_one == 0 {
                    None
                } else {
                    let len = len_plus_one as usize - 1;
                    Some(buf.copy_to_bytes(len))
                }
            } else {
                let len = read_i32(&mut buf);
                if len < 0 {
                    None
                } else {
                    Some(buf.copy_to_bytes(len as usize))
                }
            };

            if flexible {
                skip_tagged_fields(&mut buf);
            }

            let (error_code, base_offset) = match (&resolved_topic, records) {
                (Ok(rt), Some(batch_bytes)) => {
                    let messages = parse_record_batch(batch_bytes);
                    if messages.is_empty() {
                        (0i16, 0i64)
                    } else {
                        let total_size: u32 = messages
                            .iter()
                            .map(|m| m.get_size_bytes().as_bytes_u64() as u32)
                            .sum();
                        let batch = IggyMessagesBatchMut::from_messages(&messages, total_size);
                        let partition = ResolvedPartition {
                            stream_id: rt.stream_id,
                            topic_id: rt.topic_id,
                            partition_id: partition_index as usize + 1,
                        };
                        debug!(
                            "Kafka Produce: appending {} message(s) to \
                             stream={} topic={} partition={}",
                            messages.len(),
                            rt.stream_id,
                            rt.topic_id,
                            partition_index
                        );
                        match shard.append_messages(partition, batch).await {
                            Ok(()) => (0i16, 0i64),
                            Err(e) => {
                                warn!(
                                    "Kafka Produce: append_messages failed for \
                                     topic '{topic_name}' partition {partition_index}: {e}"
                                );
                                (iggy_to_kafka_error(&e), -1i64)
                            }
                        }
                    }
                }
                (Err(e), _) => (iggy_to_kafka_error(e), -1i64),
                (_, None) => (0i16, 0i64), // empty batch — no-op
            };
            partition_results.push((partition_index, error_code, base_offset));
        }
        if flexible {
            skip_tagged_fields(&mut buf);
        }
        topic_results.push((topic_name, partition_results));
    }

    build_produce_response(&topic_results, api_version, flexible)
}

// ─── RecordBatch parsing ─────────────────────────────────────────────────────

/// Parse a Kafka RecordBatch (magic = 2) and return individual `IggyMessage`s.
///
/// Records with a null or empty value are silently skipped because Iggy requires
/// a non-empty payload.
fn parse_record_batch(mut batch: Bytes) -> Vec<IggyMessage> {
    const BATCH_HEADER_SIZE: usize = 61;
    if batch.len() < BATCH_HEADER_SIZE {
        warn!("RecordBatch too short ({} bytes)", batch.len());
        return Vec::new();
    }

    // Skip batch header fields up to magic
    let _base_offset = batch.get_i64();
    let _batch_length = batch.get_i32();
    let _partition_leader_epoch = batch.get_i32();
    let magic = batch.get_i8();

    if magic != 2 {
        warn!(
            "Unsupported RecordBatch magic={magic}; only magic=2 is supported. \
             Older MessageSet format (v0/v1) is not yet implemented."
        );
        return Vec::new();
    }

    let _crc = batch.get_u32();
    let attributes = batch.get_i16();
    let _last_offset_delta = batch.get_i32();
    let _base_timestamp = batch.get_i64();
    let _max_timestamp = batch.get_i64();
    let _producer_id = batch.get_i64();
    let _producer_epoch = batch.get_i16();
    let _base_sequence = batch.get_i32();
    let num_records = batch.get_i32();

    // Compression is encoded in bits 0-2 of attributes.
    // Bit pattern: 0 = none, 1 = gzip, 2 = snappy, 3 = lz4, 4 = zstd
    let compression = attributes & 0x07;
    if compression != 0 {
        warn!(
            "Kafka RecordBatch uses compression={compression}; \
             decompression is not yet implemented — dropping batch."
        );
        return Vec::new();
    }

    let mut messages = Vec::with_capacity(num_records.max(0) as usize);
    for _ in 0..num_records.max(0) {
        if let Some(msg) = parse_record(&mut batch) {
            messages.push(msg);
        }
    }
    messages
}

/// Parse a single Kafka Record from within a RecordBatch.
fn parse_record(buf: &mut Bytes) -> Option<IggyMessage> {
    // Record length: zigzag varint, covering everything after this field
    let record_len = read_signed_varint(buf);
    if record_len <= 0 {
        return None;
    }
    if buf.remaining() < record_len as usize {
        warn!(
            "Record truncated: need {} bytes, only {} remaining",
            record_len,
            buf.remaining()
        );
        return None;
    }

    // Slice the record bytes so we don't accidentally consume beyond it
    let mut r = buf.copy_to_bytes(record_len as usize);

    let _attributes = r.get_i8();
    let _timestamp_delta = read_signed_varint(&mut r);
    let _offset_delta = read_signed_varint(&mut r);

    // Key (nullable)
    let key_len = read_signed_varint(&mut r);
    if key_len > 0 && r.remaining() >= key_len as usize {
        let _ = r.copy_to_bytes(key_len as usize); // skip key (not stored in Iggy)
    }

    // Value → IggyMessage payload
    let value_len = read_signed_varint(&mut r);
    let payload = if value_len > 0 && r.remaining() >= value_len as usize {
        r.copy_to_bytes(value_len as usize)
    } else {
        // Null or empty value — Iggy requires non-empty payload; skip
        return None;
    };

    // Headers → Iggy user headers
    let headers_count = read_unsigned_varint(&mut r) as i32;
    let mut user_headers: HashMap<HeaderKey, HeaderValue> = HashMap::new();
    for _ in 0..headers_count.max(0) {
        let hk_len = read_unsigned_varint(&mut r) as usize;
        if r.remaining() < hk_len {
            break;
        }
        let hk_bytes = r.copy_to_bytes(hk_len);

        let hv_len = read_signed_varint(&mut r);
        let hv_bytes = if hv_len >= 0 && r.remaining() >= hv_len as usize {
            r.copy_to_bytes(hv_len as usize)
        } else {
            break;
        };

        let key_str = String::from_utf8_lossy(&hk_bytes);
        if let (Ok(key), Ok(value)) = (
            HeaderKey::try_from(key_str.as_ref()),
            HeaderValue::try_from(hv_bytes.as_ref()),
        ) {
            user_headers.insert(key, value);
        }
    }

    let user_headers_opt = if user_headers.is_empty() {
        None
    } else {
        Some(user_headers)
    };

    IggyMessage::builder()
        .payload(payload)
        .maybe_user_headers(user_headers_opt)
        .build()
        .ok()
}

/// Decode a zigzag-encoded signed varint.
///
/// Kafka uses zigzag encoding for signed varints: the unsigned wire value `n`
/// represents the signed value `(n >> 1) ^ -(n & 1)`.
fn read_signed_varint(buf: &mut Bytes) -> i64 {
    let n = read_unsigned_varint(buf);
    ((n >> 1) as i64) ^ -((n & 1) as i64)
}

// ─── Response builders ───────────────────────────────────────────────────────

fn build_produce_response(
    topic_results: &[(String, Vec<PartitionResult>)],
    api_version: i16,
    flexible: bool,
) -> Vec<u8> {
    let mut body = BytesMut::new();
    if flexible {
        write_unsigned_varint(&mut body, topic_results.len() as u64 + 1);
    } else {
        write_i32(&mut body, topic_results.len() as i32);
    }
    for (topic_name, partitions) in topic_results {
        if flexible {
            write_compact_string(&mut body, topic_name);
        } else {
            write_string(&mut body, topic_name);
        }
        if flexible {
            write_unsigned_varint(&mut body, partitions.len() as u64 + 1);
        } else {
            write_i32(&mut body, partitions.len() as i32);
        }
        for &(partition_index, error_code, base_offset) in partitions {
            write_i32(&mut body, partition_index);
            write_i16(&mut body, error_code);
            write_i64(&mut body, base_offset);
            if api_version >= 2 {
                write_i64(&mut body, -1); // log_append_time
            }
            if api_version >= 5 {
                write_i64(&mut body, -1); // log_start_offset
            }
            if flexible {
                write_empty_tagged_fields(&mut body);
            }
        }
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

fn produce_error_response(flexible: bool) -> Vec<u8> {
    let mut body = BytesMut::new();
    if flexible {
        write_unsigned_varint(&mut body, 1);
        write_empty_tagged_fields(&mut body);
    } else {
        write_i32(&mut body, 0);
    }
    write_i32(&mut body, 0);
    if flexible {
        write_empty_tagged_fields(&mut body);
    }
    body.freeze().to_vec()
}
