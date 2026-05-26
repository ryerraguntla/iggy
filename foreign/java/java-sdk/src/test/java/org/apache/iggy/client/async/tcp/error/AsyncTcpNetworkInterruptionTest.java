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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AsyncTcpNetworkInterruptionTest extends AsyncTcpTestBase {

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
    void serverAcceptsButDoesNotRespond_pingShouldTimeout() throws Exception {
        server.simulateConnectionTimeout();
        await(server.start());

        client = AsyncIggyTcpClient.builder().host(HOST).port(server.getPort()).build();
        await(client.connect());

        CompletableFuture<String> pingFuture = client.system().ping();
        assertThatThrownBy(() -> pingFuture.get(5, TimeUnit.SECONDS)).isInstanceOf(TimeoutException.class);
    }

    @Test
    void serverSendsPartialFrame_requestShouldFail() throws Exception {
        server.simulatePartialFrame();
        await(server.start());

        client = AsyncIggyTcpClient.builder().host(HOST).port(server.getPort()).build();
        await(client.connect());

        CompletableFuture<String> pingFuture = client.system().ping();
        assertThatThrownBy(() -> pingFuture.get(5, TimeUnit.SECONDS)).isInstanceOf(Exception.class);
    }

    @Test
    void serverSendsMalformedResponse_requestShouldFail() throws Exception {
        server.simulateMalformedResponse();
        await(server.start());

        client = AsyncIggyTcpClient.builder().host(HOST).port(server.getPort()).build();
        await(client.connect());

        CompletableFuture<String> pingFuture = client.system().ping();
        assertThatThrownBy(() -> pingFuture.get(5, TimeUnit.SECONDS)).isInstanceOf(Exception.class);
    }

    @Test
    void serverCrashesDuringRequest_pendingRequestShouldFail() throws Exception {
        await(server.start());

        client = AsyncIggyTcpClient.builder().host(HOST).port(server.getPort()).build();
        await(client.connect());

        server.setAcceptButNotRespond();
        CompletableFuture<String> pingFuture = client.system().ping();

        server.stop();

        assertThatThrownBy(() -> pingFuture.get(5, TimeUnit.SECONDS)).isInstanceOf(Exception.class);
    }

    @Test
    void slowNetworkWithDelay_requestEventuallySucceeds() throws Exception {
        server.simulateSlowNetwork(Duration.ofMillis(300));
        await(server.start());

        client = AsyncIggyTcpClient.builder().host(HOST).port(server.getPort()).build();
        await(client.connect());

        CompletableFuture<String> pingFuture = client.system().ping();
        assertThat(await(pingFuture)).isEqualTo("pong");
    }
}
