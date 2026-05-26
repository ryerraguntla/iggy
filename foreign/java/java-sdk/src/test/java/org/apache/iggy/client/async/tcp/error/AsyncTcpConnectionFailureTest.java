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
import org.apache.iggy.exception.IggyConnectionException;
import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.junit.jupiter.api.Test;

import java.net.ConnectException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class AsyncTcpConnectionFailureTest extends AsyncTcpTestBase {

    @Test
    void connectToUnreachablePort_shouldFailWithConnectException() {
        client = AsyncIggyTcpClient.builder().host(HOST).port(19999).build();

        assertCause(client.connect(), ConnectException.class);
    }

    @Test
    void connectToInvalidHost_shouldFailWithUnknownHostException() {
        client = AsyncIggyTcpClient.builder()
                .host("this.host.absolutely.does.not.exist.invalid")
                .port(8090)
                .build();

        assertCause(client.connect(), UnknownHostException.class);
    }

    @Test
    void buildWithPortZero_shouldThrowIggyInvalidArgumentException() {
        assertThatThrownBy(() -> AsyncIggyTcpClient.builder().host("localhost").port(0).build())
                .isInstanceOf(IggyInvalidArgumentException.class);
    }

    @Test
    void buildWithNegativePort_shouldThrowIggyInvalidArgumentException() {
        assertThatThrownBy(() -> AsyncIggyTcpClient.builder().host("localhost").port(-1).build())
                .isInstanceOf(IggyInvalidArgumentException.class);
    }

    @Test
    void buildWithNullHost_shouldThrowIggyInvalidArgumentException() {
        assertThatThrownBy(() -> AsyncIggyTcpClient.builder().host(null).port(8090).build())
                .isInstanceOf(IggyInvalidArgumentException.class);
    }

    @Test
    void buildWithEmptyHost_shouldThrowIggyInvalidArgumentException() {
        assertThatThrownBy(() -> AsyncIggyTcpClient.builder().host("").port(8090).build())
                .isInstanceOf(IggyInvalidArgumentException.class);
    }

    @Test
    void twoConcurrentConnectionFailures_shouldBeIndependent() {
        AsyncIggyTcpClient firstClient = AsyncIggyTcpClient.builder().host(HOST).port(19998).build();
        AsyncIggyTcpClient secondClient = AsyncIggyTcpClient.builder().host(HOST).port(19999).build();

        try {
            assertCause(firstClient.connect(), ConnectException.class);
            assertCause(secondClient.connect(), ConnectException.class);
        } finally {
            firstClient.close().join();
            secondClient.close().join();
        }
    }

    @Test
    void connectWithShortConnectionTimeout_shouldFailWithIggyConnectionException() {
        client = AsyncIggyTcpClient.builder()
                .host("10.255.255.1")
                .port(8090)
                .connectionTimeout(Duration.ofMillis(100))
                .build();

        Throwable cause;
        try {
            client.connect().get(5, TimeUnit.SECONDS);
            throw new AssertionError("Expected connect() to fail");
        } catch (ExecutionException exception) {
            cause = exception.getCause();
        } catch (Exception exception) {
            throw new AssertionError(exception);
        }

        assumeTrue(
                cause instanceof IggyConnectionException,
                "Environment did not produce connect timeout (got "
                        + cause.getClass().getSimpleName() + ")");
        assertThat(cause).isInstanceOf(IggyConnectionException.class);
    }
}
