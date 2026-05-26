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

package org.apache.iggy.client.async.tcp.base;

import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.junit.jupiter.api.AfterEach;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class AsyncTcpTestBase {
    protected static final String HOST = "127.0.0.1";
    protected static final Duration TEST_TIMEOUT = Duration.ofSeconds(10);
    protected AsyncIggyTcpClient client;

    @AfterEach
    void baseCleanup() {
        if (client != null) {
            try {
                client.close().get(5, TimeUnit.SECONDS);
            } catch (Exception ignored) {
                // ignored
            }
            client = null;
        }
    }

    protected <T> T await(CompletableFuture<T> future) throws Exception {
        return future.get(TEST_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
    }

    protected void assertCause(CompletableFuture<?> future, Class<? extends Throwable> expectedCauseType) {
        assertThatThrownBy(() -> future.get(TEST_TIMEOUT.toSeconds(), TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOf(expectedCauseType);
    }
}
