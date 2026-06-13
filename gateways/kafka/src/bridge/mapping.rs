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

//! Kafka ↔ Iggy naming and partition mapping.

use iggy::prelude::{Identifier, Partitioning};

/// Kafka uses `-1` when the producer has no target partition (broker picks one).
pub const KAFKA_PARTITION_UNASSIGNED: i32 = -1;

/// `ListOffsets` sentinel for the earliest available offset.
pub const KAFKA_TIMESTAMP_EARLIEST: i64 = -2;

/// `ListOffsets` sentinel for the log end offset (high watermark).
pub const KAFKA_TIMESTAMP_LATEST: i64 = -1;

/// Default partition count when `CreateTopics` omits a positive value.
pub const DEFAULT_TOPIC_PARTITIONS: u32 = 1;

/// Kafka uses `-1` for `CreateTopics` "broker default" partition count.
pub const KAFKA_PARTITIONS_USE_DEFAULT: i32 = -1;

/// Kafka topic names map 1:1 to an Iggy stream and topic with the same name.
#[must_use]
pub fn kafka_topic_identifier(kafka_topic: &str) -> Option<Identifier> {
    Identifier::named(kafka_topic).ok()
}

/// Stream and topic identifiers for a Kafka topic (same underlying name).
#[must_use]
pub fn stream_and_topic_ids(kafka_topic: &str) -> Option<(Identifier, Identifier)> {
    let id = kafka_topic_identifier(kafka_topic)?;
    Some((id.clone(), id))
}

/// Maximum messages polled per Fetch partition (`partition_max_bytes` is a byte hint, not a count).
pub const MAX_FETCH_MESSAGE_COUNT: u32 = 500;

/// Produce partitioning: balanced when Kafka partition is unassigned, else fixed partition.
#[must_use]
pub fn partitioning_for_produce(kafka_partition: i32) -> Partitioning {
    if kafka_partition == KAFKA_PARTITION_UNASSIGNED {
        Partitioning::balanced()
    } else {
        Partitioning::partition_id(u32::try_from(kafka_partition).unwrap_or(0))
    }
}

/// Normalize a Kafka partition index for fetch/list-offset calls.
///
/// # Errors
///
/// Returns `None` when the partition index is negative (other than unassigned on produce).
#[must_use]
pub fn kafka_partition_index(kafka_partition: i32) -> Option<u32> {
    u32::try_from(kafka_partition).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unassigned_produce_uses_balanced() {
        assert!(matches!(
            partitioning_for_produce(KAFKA_PARTITION_UNASSIGNED),
            Partitioning { .. }
        ));
    }

    #[test]
    fn explicit_partition_uses_partition_id() {
        let p = partitioning_for_produce(2);
        assert_eq!(p.kind, iggy_common::PartitioningKind::PartitionId);
    }

    #[test]
    fn kafka_partition_index_rejects_negative() {
        assert_eq!(kafka_partition_index(-1), None);
        assert_eq!(kafka_partition_index(0), Some(0));
    }
}
