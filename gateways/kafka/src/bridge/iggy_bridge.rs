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

use bytes::Bytes;
use iggy::prelude::{
    Client, CompressionAlgorithm, Consumer, IggyClient, IggyClientBuilder, IggyError, IggyExpiry,
    IggyMessage, IggyTimestamp, MaxTopicSize, MessageClient, PollingStrategy, StreamClient,
    TopicClient, UserClient,
};
use tracing::debug;

use crate::bridge::config::IggyBridgeConfig;
use crate::bridge::mapping::{
    KAFKA_PARTITION_UNASSIGNED, KAFKA_TIMESTAMP_EARLIEST, KAFKA_TIMESTAMP_LATEST,
    partitioning_for_produce, stream_and_topic_ids,
};
use crate::error::{BridgeError, BridgeResult};

/// Metadata for a Kafka topic backed by Iggy.
#[derive(Debug, Clone, Copy)]
pub struct TopicMetadata {
    pub partitions_count: u32,
}

/// Acknowledgement for a successful Produce.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProduceAck {
    /// Partition that received the message (actual index, not `-1`).
    pub partition: i32,
    pub base_offset: i64,
}

/// One message returned from a partition fetch.
#[derive(Debug, Clone)]
pub struct FetchedMessage {
    pub offset: u64,
    pub payload: Bytes,
}

/// Result of fetching one partition.
#[derive(Debug, Clone)]
pub struct FetchPartitionResult {
    pub messages: Vec<FetchedMessage>,
    pub high_watermark: u64,
    pub log_start_offset: u64,
}

/// Async bridge to the Iggy broker (stream/topic mapping, produce, fetch, offsets).
pub struct IggyBridge {
    client: IggyClient,
}

impl IggyBridge {
    /// Connect and authenticate against Iggy using `config`.
    ///
    /// # Errors
    ///
    /// Returns [`BridgeError::Iggy`] when TCP connect, handshake, or login fails.
    pub async fn connect(config: &IggyBridgeConfig) -> BridgeResult<Self> {
        let client = IggyClientBuilder::new()
            .with_tcp()
            .with_server_address(config.server_address.clone())
            .build()
            .map_err(BridgeError::Iggy)?;
        client.connect().await.map_err(BridgeError::Iggy)?;
        client
            .login_user(&config.username, &config.password)
            .await
            .map_err(BridgeError::Iggy)?;
        debug!(addr = %config.server_address, "iggy bridge connected");
        Ok(Self { client })
    }

    /// Idempotently create the Iggy stream and topic backing a Kafka topic name.
    ///
    /// If `create_stream` succeeds but `create_topic` fails (non-already-exists), an empty stream
    /// may remain until a retry succeeds (`StreamNameAlreadyExists` on the next call).
    ///
    /// # Errors
    ///
    /// Returns [`BridgeError::InvalidTopicName`] or [`BridgeError::Iggy`] on create failures.
    pub async fn ensure_stream_and_topic(
        &self,
        kafka_topic: &str,
        partitions: u32,
    ) -> BridgeResult<()> {
        match self.client.create_stream(kafka_topic).await {
            Ok(_) => debug!(kafka_topic, "iggy stream created"),
            Err(IggyError::StreamNameAlreadyExists(_)) => {}
            Err(e) => return Err(BridgeError::Iggy(e)),
        }

        let (stream_id, topic_id) = stream_and_topic_ids(kafka_topic)
            .ok_or_else(|| BridgeError::InvalidTopicName(kafka_topic.to_string()))?;

        match self
            .client
            .create_topic(
                &stream_id,
                kafka_topic,
                partitions,
                CompressionAlgorithm::None,
                None,
                IggyExpiry::NeverExpire,
                MaxTopicSize::ServerDefault,
            )
            .await
        {
            Ok(_) => debug!(kafka_topic, partitions, "iggy topic created"),
            Err(IggyError::TopicNameAlreadyExists(_, _)) => {}
            Err(e) => return Err(BridgeError::Iggy(e)),
        }

        let _ = topic_id;
        Ok(())
    }

    /// Store opaque Kafka `RecordBatch` bytes in Iggy and return the assigned partition/offset.
    ///
    /// **Iggy SDK gap:** `send_messages` returns `()` — offset is inferred via `get_topic`
    /// partition `current_offset` diff (balanced) or `current_offset` on the target partition.
    ///
    /// **TOCTOU race:** `detect_written_partition` snapshots all partition `current_offset`
    /// values before/after send and returns the *first* index where `after > before`. Another
    /// producer (any client, same topic) writing between those snapshots can bump a lower-indexed
    /// partition so this response reports the other call's partition/offset. The explicit-partition
    /// path can return a stale-high offset if another writer appends to the same partition between
    /// `send_messages` and the post-send `get_topic`. See `IGGY_LIMITATIONS.md` (L4).
    ///
    /// # Errors
    ///
    /// Returns [`BridgeError::InvalidTopicName`], [`BridgeError::ProduceAckUnknown`], or
    /// [`BridgeError::Iggy`] when send or offset inference fails.
    pub async fn produce(
        &self,
        kafka_topic: &str,
        kafka_partition: i32,
        payload: Bytes,
    ) -> BridgeResult<ProduceAck> {
        let (stream_id, topic_id) = stream_and_topic_ids(kafka_topic)
            .ok_or_else(|| BridgeError::InvalidTopicName(kafka_topic.to_string()))?;

        let offsets_before = if kafka_partition == KAFKA_PARTITION_UNASSIGNED {
            Some(self.partition_current_offsets(kafka_topic).await?)
        } else {
            None
        };

        let partitioning = partitioning_for_produce(kafka_partition);
        let msg = IggyMessage::builder()
            .payload(payload)
            .build()
            .map_err(BridgeError::Iggy)?;

        self.client
            .send_messages(&stream_id, &topic_id, &partitioning, &mut [msg])
            .await
            .map_err(BridgeError::Iggy)?;

        if let Some(partition_id) = super::mapping::kafka_partition_index(kafka_partition) {
            let base_offset = self
                .last_message_offset(kafka_topic, partition_id)
                .await?;
            return Ok(ProduceAck {
                partition: kafka_partition,
                base_offset: i64::try_from(base_offset).unwrap_or(i64::MAX),
            });
        }

        let before = offsets_before.ok_or(BridgeError::ProduceAckUnknown)?;
        let after = self.partition_current_offsets(kafka_topic).await?;
        let (partition_id, base_offset) = detect_written_partition(&before, &after)?;
        Ok(ProduceAck {
            partition: i32::try_from(partition_id).unwrap_or(0),
            base_offset: i64::try_from(base_offset).unwrap_or(i64::MAX),
        })
    }

    /// Poll messages from one partition starting at `offset`, up to `max_count`.
    ///
    /// # Errors
    ///
    /// Returns [`BridgeError::InvalidTopicName`] or [`BridgeError::Iggy`] on poll failure.
    pub async fn fetch_partition(
        &self,
        kafka_topic: &str,
        partition: u32,
        offset: u64,
        max_count: u32,
    ) -> BridgeResult<FetchPartitionResult> {
        let (stream_id, topic_id) = stream_and_topic_ids(kafka_topic)
            .ok_or_else(|| BridgeError::InvalidTopicName(kafka_topic.to_string()))?;

        let polled = self
            .client
            .poll_messages(
                &stream_id,
                &topic_id,
                Some(partition),
                &Consumer::default(),
                &PollingStrategy::offset(offset),
                max_count,
                false,
            )
            .await
            .map_err(BridgeError::Iggy)?;

        let messages: Vec<FetchedMessage> = polled
            .messages
            .into_iter()
            .map(|m| FetchedMessage {
                offset: m.header.offset,
                payload: m.payload,
            })
            .collect();

        let log_start_offset = messages.first().map_or(0, |m| m.offset);
        let high_watermark = self.high_watermark(kafka_topic, partition).await?;

        Ok(FetchPartitionResult {
            messages,
            high_watermark,
            log_start_offset,
        })
    }

    /// Log end offset for one partition (always pass an explicit partition index).
    ///
    /// # Errors
    ///
    /// Returns [`BridgeError::InvalidTopicName`] or [`BridgeError::Iggy`] when metadata lookup fails.
    pub async fn high_watermark(
        &self,
        kafka_topic: &str,
        partition: u32,
    ) -> BridgeResult<u64> {
        let stats = self.partition_offset_and_count(kafka_topic, partition).await?;
        Ok(high_watermark_from_stats(stats))
    }

    /// Resolve a `ListOffsets` timestamp sentinel or explicit timestamp to an offset.
    ///
    /// # Errors
    ///
    /// Returns [`BridgeError::UnsupportedTimestampSeek`] for unknown sentinels, or other
    /// [`BridgeError`] variants when Iggy calls fail.
    pub async fn list_offset(
        &self,
        kafka_topic: &str,
        partition: u32,
        timestamp: i64,
    ) -> BridgeResult<i64> {
        match timestamp {
            KAFKA_TIMESTAMP_LATEST => {
                let hwm = self.high_watermark(kafka_topic, partition).await?;
                Ok(i64::try_from(hwm).unwrap_or(i64::MAX))
            }
            KAFKA_TIMESTAMP_EARLIEST => Ok(0),
            ts if ts >= 0 => {
                let timestamp_ms = u64::try_from(ts).map_err(|_| BridgeError::UnsupportedTimestampSeek)?;
                let offset = self
                    .offset_for_timestamp(kafka_topic, partition, timestamp_ms)
                    .await?;
                Ok(i64::try_from(offset).unwrap_or(i64::MAX))
            }
            _ => Err(BridgeError::UnsupportedTimestampSeek),
        }
    }

    /// Look up topic metadata when the stream/topic exists in Iggy.
    ///
    /// # Errors
    ///
    /// Returns [`BridgeError::Iggy`] for unexpected broker errors (missing topics yield `Ok(None)`).
    pub async fn topic_metadata(&self, kafka_topic: &str) -> BridgeResult<Option<TopicMetadata>> {
        let Some((stream_id, topic_id)) = stream_and_topic_ids(kafka_topic) else {
            return Ok(None);
        };

        match self.client.get_topic(&stream_id, &topic_id).await {
            Ok(Some(topic)) => Ok(Some(TopicMetadata {
                partitions_count: topic.partitions_count,
            })),
            Ok(None)
            | Err(
                IggyError::StreamIdNotFound(_)
                | IggyError::StreamNameNotFound(_)
                | IggyError::TopicIdNotFound(_, _)
                | IggyError::TopicNameNotFound(_, _),
            ) => Ok(None),
            Err(e) => Err(BridgeError::Iggy(e)),
        }
    }

    async fn partition_current_offsets(&self, kafka_topic: &str) -> BridgeResult<Vec<u64>> {
        let (stream_id, topic_id) = stream_and_topic_ids(kafka_topic)
            .ok_or_else(|| BridgeError::InvalidTopicName(kafka_topic.to_string()))?;
        let details = match self.client.get_topic(&stream_id, &topic_id).await {
            Ok(Some(topic)) => topic,
            Ok(None) => {
                return Err(BridgeError::Iggy(IggyError::TopicNameNotFound(
                    kafka_topic.to_string(),
                    kafka_topic.to_string(),
                )));
            }
            Err(e) => return Err(BridgeError::Iggy(e)),
        };
        Ok(details
            .partitions
            .iter()
            .map(|p| p.current_offset)
            .collect())
    }

    async fn last_message_offset(&self, kafka_topic: &str, partition: u32) -> BridgeResult<u64> {
        let stats = self.partition_offset_and_count(kafka_topic, partition).await?;
        last_message_offset_from_stats(stats)
    }

    async fn partition_offset_and_count(
        &self,
        kafka_topic: &str,
        partition: u32,
    ) -> BridgeResult<PartitionOffsetStats> {
        let (stream_id, topic_id) = stream_and_topic_ids(kafka_topic)
            .ok_or_else(|| BridgeError::InvalidTopicName(kafka_topic.to_string()))?;
        let details = match self.client.get_topic(&stream_id, &topic_id).await {
            Ok(Some(topic)) => topic,
            Ok(None) => {
                return Err(BridgeError::Iggy(IggyError::TopicNameNotFound(
                    kafka_topic.to_string(),
                    kafka_topic.to_string(),
                )));
            }
            Err(e) => return Err(BridgeError::Iggy(e)),
        };
        let idx = usize::try_from(partition).map_err(|_| BridgeError::ProduceAckUnknown)?;
        let part = details
            .partitions
            .get(idx)
            .ok_or(BridgeError::ProduceAckUnknown)?;
        Ok(PartitionOffsetStats {
            current_offset: part.current_offset,
            messages_count: part.messages_count,
        })
    }

    async fn offset_for_timestamp(
        &self,
        kafka_topic: &str,
        partition: u32,
        timestamp_ms: u64,
    ) -> BridgeResult<u64> {
        let (stream_id, topic_id) = stream_and_topic_ids(kafka_topic)
            .ok_or_else(|| BridgeError::InvalidTopicName(kafka_topic.to_string()))?;
        let polled = self
            .client
            .poll_messages(
                &stream_id,
                &topic_id,
                Some(partition),
                &Consumer::default(),
                &PollingStrategy::timestamp(IggyTimestamp::from(
                    timestamp_ms.saturating_mul(1_000),
                )),
                1,
                false,
            )
            .await
            .map_err(BridgeError::Iggy)?;
        polled
            .messages
            .first()
            .map(|m| m.header.offset)
            .ok_or(BridgeError::UnsupportedTimestampSeek)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PartitionOffsetStats {
    current_offset: u64,
    messages_count: u64,
}

/// Kafka log end offset from Iggy partition stats (`messages_count == 0` → empty log).
#[must_use]
const fn high_watermark_from_stats(stats: PartitionOffsetStats) -> u64 {
    if stats.messages_count == 0 {
        0
    } else {
        stats.current_offset.saturating_add(1)
    }
}

/// Last written offset for a single-message produce ack (`messages_count == 0` → unknown).
const fn last_message_offset_from_stats(stats: PartitionOffsetStats) -> BridgeResult<u64> {
    if stats.messages_count == 0 {
        Err(BridgeError::ProduceAckUnknown)
    } else {
        Ok(stats.current_offset)
    }
}

fn detect_written_partition(before: &[u64], after: &[u64]) -> BridgeResult<(u32, u64)> {
    for (idx, (prev, next)) in before.iter().zip(after.iter()).enumerate() {
        if next > prev {
            return Ok((
                u32::try_from(idx).map_err(|_| BridgeError::ProduceAckUnknown)?,
                *next,
            ));
        }
    }
    Err(BridgeError::ProduceAckUnknown)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_written_partition_finds_increase() {
        let (pid, off) = detect_written_partition(&[0, 5], &[0, 6]).unwrap();
        assert_eq!(pid, 1);
        assert_eq!(off, 6);
    }

    #[test]
    fn detect_written_partition_no_increase_errors() {
        assert!(detect_written_partition(&[0], &[0]).is_err());
    }

    #[test]
    fn high_watermark_from_stats_empty_partition() {
        assert_eq!(
            high_watermark_from_stats(PartitionOffsetStats {
                current_offset: 0,
                messages_count: 0,
            }),
            0
        );
    }

    #[test]
    fn high_watermark_from_stats_non_empty() {
        assert_eq!(
            high_watermark_from_stats(PartitionOffsetStats {
                current_offset: 7,
                messages_count: 8,
            }),
            8
        );
    }

    #[test]
    fn last_message_offset_from_stats_empty_errors() {
        assert!(last_message_offset_from_stats(PartitionOffsetStats {
            current_offset: 0,
            messages_count: 0,
        })
        .is_err());
    }

    #[test]
    fn last_message_offset_from_stats_returns_current_offset() {
        assert_eq!(
            last_message_offset_from_stats(PartitionOffsetStats {
                current_offset: 7,
                messages_count: 8,
            })
            .unwrap(),
            7
        );
    }
}
