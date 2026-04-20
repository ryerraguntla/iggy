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

use iggy_common::IggyError;

/// Kafka protocol error codes as defined in the Kafka specification.
/// Only codes used by the initially supported API set are listed here.
#[allow(dead_code)]
#[repr(i16)]
pub enum KafkaErrorCode {
    None = 0,
    UnknownTopicOrPartition = 3,
    NotLeaderOrFollower = 6,
    NetworkException = 13,
    GroupCoordinatorNotAvailable = 15,
    NotCoordinator = 16,
    TopicAlreadyExists = 36,
    UnsupportedSaslMechanism = 33,
    UnsupportedVersion = 35,
    ClusterAuthorizationFailed = 31,
    SaslAuthenticationFailed = 58,
    RebalanceInProgress = 27,
    UnknownMemberId = 25,
    IllegalGeneration = 22,
}

/// Map an `IggyError` to the closest Kafka error code.
pub fn iggy_to_kafka_error(err: &IggyError) -> i16 {
    match err {
        IggyError::TopicIdNotFound(..)
        | IggyError::TopicNameNotFound(..)
        | IggyError::StreamIdNotFound(..)
        | IggyError::StreamNameNotFound(..) => KafkaErrorCode::UnknownTopicOrPartition as i16,

        IggyError::TopicNameAlreadyExists(..) => KafkaErrorCode::TopicAlreadyExists as i16,

        IggyError::Unauthenticated | IggyError::Unauthorized => {
            KafkaErrorCode::ClusterAuthorizationFailed as i16
        }

        IggyError::InvalidCredentials => KafkaErrorCode::SaslAuthenticationFailed as i16,

        _ => KafkaErrorCode::NetworkException as i16,
    }
}
