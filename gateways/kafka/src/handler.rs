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

//! Request dispatch: stub handlers (#3421) or Iggy-backed handlers (Phase 1B).

use std::sync::Arc;

use bytes::Bytes;
use iggy::prelude::IggyError;
use tracing::warn;

use crate::bridge::iggy_bridge::IggyBridge;
use crate::bridge::mapping::{
    DEFAULT_TOPIC_PARTITIONS, KAFKA_PARTITIONS_USE_DEFAULT, MAX_FETCH_MESSAGE_COUNT,
    kafka_partition_index,
};
use crate::error::BridgeError;
use crate::protocol::api::{
    API_KEY_CREATE_TOPICS, API_KEY_FETCH, API_KEY_LIST_OFFSETS, API_KEY_METADATA, API_KEY_PRODUCE,
    BrokerAdvertise, ERROR_INVALID_PARTITIONS, ERROR_INVALID_REQUEST, ERROR_NONE,
    ERROR_UNKNOWN_SERVER_ERROR, ERROR_UNKNOWN_TOPIC_OR_PARTITION,
    handle_request, is_supported_version,
};
use crate::protocol::requests::{
    MetadataTopicFilter, decode_create_topics_request, decode_fetch_request,
    decode_list_offsets_request, decode_metadata_topic_filter, decode_produce_request,
};
use crate::protocol::responses::{
    FetchPartitionOutcome, ListOffsetsPartitionOutcome, MetadataTopicOutcome,
    ProducePartitionOutcome, concat_record_batches, encode_fetch_response_from_topic_outcomes,
    encode_list_offsets_response_from_topic_outcomes, encode_metadata_response_from_topics,
    encode_produce_response_from_topic_outcomes, metadata_unknown_topic,
};

/// Dispatches Kafka requests to stub or Iggy-backed handlers.
pub struct RequestHandler {
    broker: BrokerAdvertise,
    bridge: Option<Arc<IggyBridge>>,
}

impl RequestHandler {
    #[must_use]
    pub const fn stub(broker: BrokerAdvertise) -> Self {
        Self {
            broker,
            bridge: None,
        }
    }

    #[must_use]
    #[allow(clippy::missing_const_for_fn)] // `Arc` is not const-constructible
    pub fn with_bridge(broker: BrokerAdvertise, bridge: Arc<IggyBridge>) -> Self {
        Self {
            broker,
            bridge: Some(bridge),
        }
    }

    /// Handle one request body (after the request header has been stripped).
    pub async fn handle(&self, api_key: i16, api_version: i16, body: Bytes) -> Bytes {
        match &self.bridge {
            None => handle_request(api_key, api_version, body, &self.broker),
            Some(bridge) => {
                handle_with_bridge(bridge, &self.broker, api_key, api_version, body).await
            }
        }
    }
}

async fn handle_with_bridge(
    bridge: &IggyBridge,
    broker: &BrokerAdvertise,
    api_key: i16,
    api_version: i16,
    body: Bytes,
) -> Bytes {
    if !is_supported_version(api_key, api_version) {
        return handle_request(api_key, api_version, body, broker);
    }

    match api_key {
        API_KEY_PRODUCE => handle_produce(bridge, api_version, body).await,
        API_KEY_FETCH => handle_fetch(bridge, api_version, body).await,
        API_KEY_LIST_OFFSETS => handle_list_offsets(bridge, api_version, body).await,
        API_KEY_METADATA => handle_metadata(bridge, broker, api_version, body).await,
        API_KEY_CREATE_TOPICS => handle_create_topics(bridge, api_version, body).await,
        _ => handle_request(api_key, api_version, body, broker),
    }
}

async fn handle_produce(bridge: &IggyBridge, version: i16, body: Bytes) -> Bytes {
    let req = match decode_produce_request(version, body) {
        Ok(r) => r,
        Err(e) => {
            warn!("produce decode failed: {e}");
            return crate::protocol::responses::encode_produce_error_response(
                version,
                ERROR_INVALID_REQUEST,
            );
        }
    };

    if req.transactional_id.is_some() {
        return crate::protocol::responses::encode_produce_error_response(
            version,
            ERROR_INVALID_REQUEST,
        );
    }

    let mut response_topics = Vec::with_capacity(req.topics.len());

    for topic_data in &req.topics {
        let mut outcomes = Vec::with_capacity(topic_data.partitions.len());
        for part in &topic_data.partitions {
            let outcome = match &part.records {
                None => ProducePartitionOutcome {
                    partition: part.partition,
                    error_code: ERROR_INVALID_REQUEST,
                    base_offset: 0,
                },
                Some(records) => match bridge
                    .produce(&topic_data.topic, part.partition, records.clone())
                    .await
                {
                    Ok(ack) => ProducePartitionOutcome {
                        partition: ack.partition,
                        error_code: ERROR_NONE,
                        base_offset: ack.base_offset,
                    },
                    Err(e) => ProducePartitionOutcome {
                        partition: part.partition,
                        error_code: bridge_error_code(&e),
                        base_offset: 0,
                    },
                },
            };
            outcomes.push(outcome);
        }
        response_topics.push((topic_data.topic.clone(), outcomes));
    }

    if response_topics.is_empty() {
        crate::protocol::responses::encode_produce_error_response(version, ERROR_NONE)
    } else {
        encode_produce_response_from_topic_outcomes(version, &response_topics)
    }
}

async fn handle_fetch(bridge: &IggyBridge, version: i16, body: Bytes) -> Bytes {
    let Ok(req) = decode_fetch_request(version, body) else {
        return crate::protocol::responses::encode_fetch_error_response(
            version,
            ERROR_INVALID_REQUEST,
        );
    };

    let mut response_topics = Vec::with_capacity(req.topics.len());

    for topic in &req.topics {
        let mut outcomes = Vec::with_capacity(topic.partitions.len());
        for part in &topic.partitions {
            let Some(partition_id) = kafka_partition_index(part.partition) else {
                outcomes.push(FetchPartitionOutcome {
                    partition: part.partition,
                    error_code: ERROR_UNKNOWN_TOPIC_OR_PARTITION,
                    high_watermark: 0,
                    log_start_offset: 0,
                    records: None,
                });
                continue;
            };

            let fetch_offset = u64::try_from(part.fetch_offset.max(0)).unwrap_or(0);
            match bridge
                .fetch_partition(
                    &topic.topic,
                    partition_id,
                    fetch_offset,
                    MAX_FETCH_MESSAGE_COUNT,
                )
                .await
            {
                Ok(result) => {
                    let payloads: Vec<Bytes> =
                        result.messages.iter().map(|m| m.payload.clone()).collect();
                    outcomes.push(FetchPartitionOutcome {
                        partition: part.partition,
                        error_code: ERROR_NONE,
                        high_watermark: i64::try_from(result.high_watermark).unwrap_or(0),
                        log_start_offset: i64::try_from(result.log_start_offset).unwrap_or(0),
                        records: concat_record_batches(&payloads),
                    });
                }
                Err(e) => {
                    outcomes.push(FetchPartitionOutcome {
                        partition: part.partition,
                        error_code: bridge_error_code(&e),
                        high_watermark: 0,
                        log_start_offset: 0,
                        records: None,
                    });
                }
            }
        }
        response_topics.push((topic.topic.clone(), outcomes));
    }

    if response_topics.is_empty() {
        crate::protocol::responses::encode_fetch_error_response(version, ERROR_NONE)
    } else {
        encode_fetch_response_from_topic_outcomes(version, &response_topics)
    }
}

async fn handle_list_offsets(bridge: &IggyBridge, version: i16, body: Bytes) -> Bytes {
    let Ok(req) = decode_list_offsets_request(version, body) else {
        return crate::protocol::responses::encode_list_offsets_error_response(
            version,
            ERROR_INVALID_REQUEST,
        );
    };

    let mut response_topics = Vec::with_capacity(req.topics.len());

    for topic in &req.topics {
        let mut outcomes = Vec::with_capacity(topic.partitions.len());
        for part in &topic.partitions {
            let Some(partition_id) = kafka_partition_index(part.partition) else {
                outcomes.push(ListOffsetsPartitionOutcome {
                    partition: part.partition,
                    error_code: ERROR_UNKNOWN_TOPIC_OR_PARTITION,
                    offset: 0,
                });
                continue;
            };

            match bridge
                .list_offset(&topic.topic, partition_id, part.timestamp)
                .await
            {
                Ok(offset) => outcomes.push(ListOffsetsPartitionOutcome {
                    partition: part.partition,
                    error_code: ERROR_NONE,
                    offset,
                }),
                Err(e) => outcomes.push(ListOffsetsPartitionOutcome {
                    partition: part.partition,
                    error_code: bridge_error_code(&e),
                    offset: 0,
                }),
            }
        }
        response_topics.push((topic.topic.clone(), outcomes));
    }

    if response_topics.is_empty() {
        crate::protocol::responses::encode_list_offsets_error_response(version, ERROR_NONE)
    } else {
        encode_list_offsets_response_from_topic_outcomes(version, &response_topics)
    }
}

async fn handle_metadata(
    bridge: &IggyBridge,
    broker: &BrokerAdvertise,
    version: i16,
    body: Bytes,
) -> Bytes {
    let filter = match decode_metadata_topic_filter(version, body.clone()) {
        Ok(f) => f,
        Err(e) => {
            warn!("metadata decode failed: {e}");
            return handle_request(API_KEY_METADATA, version, body, broker);
        }
    };

    let topic_names = match &filter {
        MetadataTopicFilter::All => {
            return encode_metadata_response_from_topics(version, broker, &[]);
        }
        MetadataTopicFilter::Named(names) if names.is_empty() => {
            return encode_metadata_response_from_topics(version, broker, &[]);
        }
        MetadataTopicFilter::Named(names) => names.clone(),
    };

    let mut outcomes = Vec::with_capacity(topic_names.len());
    for name in topic_names {
        match bridge.topic_metadata(&name).await {
            Ok(Some(meta)) => outcomes.push(MetadataTopicOutcome {
                name,
                error_code: ERROR_NONE,
                partitions_count: meta.partitions_count,
            }),
            Ok(None) => outcomes.push(metadata_unknown_topic(&name)),
            Err(e) => {
                warn!(topic = %name, "metadata lookup failed: {e}");
                outcomes.push(metadata_unknown_topic(&name));
            }
        }
    }

    encode_metadata_response_from_topics(version, broker, &outcomes)
}

async fn handle_create_topics(bridge: &IggyBridge, version: i16, body: Bytes) -> Bytes {
    let Ok(req) = decode_create_topics_request(version, body) else {
        return crate::protocol::responses::encode_create_topics_error_response(
            version,
            ERROR_INVALID_REQUEST,
        );
    };

    if req.validate_only {
        return crate::protocol::responses::encode_create_topics_response(version, &req);
    }

    for topic in &req.topics {
        if topic.replication_factor != -1 && topic.replication_factor != 1 {
            return crate::protocol::responses::encode_create_topics_error_response(
                version,
                crate::protocol::api::ERROR_INVALID_REPLICATION_FACTOR,
            );
        }

        let should_create = topic.num_partitions == KAFKA_PARTITIONS_USE_DEFAULT
            || topic.num_partitions > 0;
        if !should_create {
            continue;
        }

        let partitions = if topic.num_partitions == KAFKA_PARTITIONS_USE_DEFAULT {
            DEFAULT_TOPIC_PARTITIONS
        } else {
            u32::try_from(topic.num_partitions).unwrap_or(DEFAULT_TOPIC_PARTITIONS)
        };

        if let Err(e) = bridge.ensure_stream_and_topic(&topic.name, partitions).await {
            warn!(topic = %topic.name, "create topic failed: {e}");
            return crate::protocol::responses::encode_create_topics_error_response(
                version,
                bridge_error_code(&e),
            );
        }
    }

    crate::protocol::responses::encode_create_topics_response(version, &req)
}

const fn bridge_error_code(err: &BridgeError) -> i16 {
    match err {
        // Invalid topic name or timestamp seek — not a message-format problem (see BridgeError doc).
        BridgeError::InvalidTopicName(_) | BridgeError::UnsupportedTimestampSeek => {
            ERROR_INVALID_REQUEST
        }
        // Write may have landed; see BridgeError::ProduceAckUnknown doc (L4).
        BridgeError::ProduceAckUnknown => ERROR_UNKNOWN_SERVER_ERROR,
        BridgeError::Iggy(e) => iggy_error_code(e),
    }
}

const fn iggy_error_code(err: &IggyError) -> i16 {
    match err {
        IggyError::StreamIdNotFound(_)
        | IggyError::StreamNameNotFound(_)
        | IggyError::TopicIdNotFound(_, _)
        | IggyError::TopicNameNotFound(_, _)
        | IggyError::PartitionNotFound(..) => ERROR_UNKNOWN_TOPIC_OR_PARTITION,
        IggyError::InvalidPartitionsCount => ERROR_INVALID_PARTITIONS,
        _ => ERROR_UNKNOWN_SERVER_ERROR,
    }
}
