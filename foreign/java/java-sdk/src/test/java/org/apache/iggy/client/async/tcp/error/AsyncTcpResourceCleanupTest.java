/*
 * Licensed to the Apache Software Foundation (ASF) under one
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

package org.apache.iggy.client.async.tcp.error;

import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.client.async.tcp.base.AsyncTcpTestBase;
import org.apache.iggy.client.async.tcp.mock.FaultyNettyServer;
import org.apache.iggy.exception.IggyNotConnectedException;
import org.apache.iggy.exception.IggyServerException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AsyncTcpResourceCleanupTest extends AsyncTcpTestBase {

    private FaultyNettyServer server;

    @BeforeEach
    void setUp() {
        server = new FaultyNettyServer();
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop();
            server = null;
        }
    }

    @Test
    void closeBeforeConnect_shouldCompleteNormally() {
        client = AsyncIggyTcpClient.builder().host(HOST).port(8090).build();

        assertThatCode(() -> client.close().get(5, TimeUnit.SECONDS)).doesNotThrowAnyException();
    }

    @Test
    void closeAfterConnect_shouldCompleteNormally() throws Exception {
        await(server.start());
        client = AsyncIggyTcpClient.builder().host(HOST).port(server.getPort()).build();
        await(client.connect());

        assertThatCode(() -> client.close().get(5, TimeUnit.SECONDS)).doesNotThrowAnyException();
    }

    @Test
    void closeIsIdempotent_threeCallsAllSucceed() throws Exception {
        await(server.start());
        client = AsyncIggyTcpClient.builder().host(HOST).port(server.getPort()).build();
        await(client.connect());

        assertThatCode(() -> client.close().get(5, TimeUnit.SECONDS)).doesNotThrowAnyException();
        assertThatCode(() -> client.close().get(5, TimeUnit.SECONDS)).doesNotThrowAnyException();
        assertThatCode(() -> client.close().get(5, TimeUnit.SECONDS)).doesNotThrowAnyException();
    }

    @Test
    void messagesAccessorAfterClose_shouldThrowIggyNotConnectedException() throws Exception {
        await(server.start());
        client = AsyncIggyTcpClient.builder().host(HOST).port(server.getPort()).build();
        await(client.connect());
        await(client.close());

        assertThatThrownBy(() -> client.messages()).isInstanceOf(IggyNotConnectedException.class);
    }

    @Test
    void streamsAccessorAfterClose_shouldThrowIggyNotConnectedException() throws Exception {
        await(server.start());
        client = AsyncIggyTcpClient.builder().host(HOST).port(server.getPort()).build();
        await(client.connect());
        await(client.close());

        assertThatThrownBy(() -> client.streams()).isInstanceOf(IggyNotConnectedException.class);
    }

    @Test
    void closeAfterServerError_shouldCompleteNormally() throws Exception {
        server.simulateServerError(1009, "Stream not found");
        await(server.start());

        client = AsyncIggyTcpClient.builder().host(HOST).port(server.getPort()).build();
        await(client.connect());

        assertCause(client.system().ping(), IggyServerException.class);
        assertThatCode(() -> client.close().get(5, TimeUnit.SECONDS)).doesNotThrowAnyException();
    }

    @Test
    void concurrentCloseAndSend_closeShouldAlwaysSucceed() throws Exception {
        await(server.start());

        client = AsyncIggyTcpClient.builder().host(HOST).port(server.getPort()).build();
        await(client.connect());

        CompletableFuture<String> pingFuture = client.system().ping();
        CompletableFuture<Void> closeFuture = client.close();

        assertThatCode(() -> closeFuture.get(5, TimeUnit.SECONDS)).doesNotThrowAnyException();
        assertThatCode(() -> pingFuture.handle((result, error) -> null).get(5, TimeUnit.SECONDS))
                .doesNotThrowAnyException();
    }
}
