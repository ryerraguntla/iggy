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

package org.apache.iggy.client.async.tcp.mock;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.IoEventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class MockNettyServer {
    private static final String HOST = "127.0.0.1";

    private volatile boolean acceptButNotRespond;
    private volatile Integer dropConnectionAfterBytes;
    private volatile Duration responseDelay = Duration.ZERO;
    private volatile int responseStatusCode;
    private volatile byte[] responsePayload = new byte[0];
    private volatile boolean sendMalformedResponse;

    private IoEventLoopGroup bossGroup;
    private IoEventLoopGroup workerGroup;
    private Channel serverChannel;
    private volatile int port = -1;

    public CompletableFuture<Void> start() {
        if (serverChannel != null && serverChannel.isActive()) {
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<Void> startFuture = new CompletableFuture<>();
        bossGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
        workerGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());

        ServerBootstrap bootstrap = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new RequestFrameDecoder());
                        ch.pipeline().addLast(new RequestHandler());
                    }
                });

        ChannelFuture bindFuture = bootstrap.bind(HOST, 0);
        bindFuture.addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                serverChannel = future.channel();
                port = ((InetSocketAddress) serverChannel.localAddress()).getPort();
                startFuture.complete(null);
            } else {
                startFuture.completeExceptionally(future.cause());
            }
        });

        return startFuture;
    }

    public void stop() {
        if (serverChannel != null) {
            serverChannel.close().syncUninterruptibly();
            serverChannel = null;
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully().syncUninterruptibly();
            workerGroup = null;
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully().syncUninterruptibly();
            bossGroup = null;
        }
        port = -1;
    }

    public int getPort() {
        if (port < 0) {
            throw new IllegalStateException("Server is not started");
        }
        return port;
    }

    public void setAcceptButNotRespond() {
        acceptButNotRespond = true;
        dropConnectionAfterBytes = null;
        sendMalformedResponse = false;
    }

    public void setDropConnectionAfterBytes(int bytes) {
        acceptButNotRespond = false;
        dropConnectionAfterBytes = bytes;
        sendMalformedResponse = false;
    }

    public void setResponseDelay(Duration delay) {
        responseDelay = delay == null ? Duration.ZERO : delay;
    }

    public void setResponse(int statusCode, byte[] payload) {
        acceptButNotRespond = false;
        sendMalformedResponse = false;
        responseStatusCode = statusCode;
        responsePayload = payload == null ? new byte[0] : Arrays.copyOf(payload, payload.length);
    }

    public void setSendMalformedResponse() {
        acceptButNotRespond = false;
        dropConnectionAfterBytes = null;
        sendMalformedResponse = true;
    }

    private ByteBuf buildResponse(ChannelHandlerContext ctx) {
        byte[] payloadCopy = responsePayload;
        ByteBuf response = ctx.alloc().buffer(8 + payloadCopy.length);
        response.writeIntLE(responseStatusCode);
        response.writeIntLE(payloadCopy.length);
        if (payloadCopy.length > 0) {
            response.writeBytes(payloadCopy);
        }
        return response;
    }

    private final class RequestHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            RequestFrame request = (RequestFrame) msg;
            Runnable sendResponse = () -> {
                if (acceptButNotRespond) {
                    return;
                }

                if (sendMalformedResponse) {
                    ByteBuf malformed = ctx.alloc().buffer(4);
                    malformed.writeIntLE(responseStatusCode);
                    ctx.writeAndFlush(malformed).addListener(ChannelFutureListener.CLOSE);
                    return;
                }

                ByteBuf response = buildResponse(ctx);
                Integer dropBytes = dropConnectionAfterBytes;
                if (dropBytes != null) {
                    int bytesToWrite = Math.min(Math.max(dropBytes, 0), response.readableBytes());
                    ByteBuf partialResponse = response.readRetainedSlice(bytesToWrite);
                    response.release();
                    ctx.writeAndFlush(partialResponse).addListener(ChannelFutureListener.CLOSE);
                    return;
                }

                ctx.writeAndFlush(response);
            };

            Duration currentDelay = responseDelay;
            if (!currentDelay.isZero() && !currentDelay.isNegative()) {
                ctx.channel().eventLoop().schedule(sendResponse, currentDelay.toMillis(), TimeUnit.MILLISECONDS);
            } else {
                sendResponse.run();
            }

            request.release();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
        }
    }

    private static final class RequestFrameDecoder extends ByteToMessageDecoder {

        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
            if (in.readableBytes() < 4) {
                return;
            }

            in.markReaderIndex();
            int frameLength = in.readIntLE();
            if (frameLength < 4) {
                ctx.close();
                return;
            }

            if (in.readableBytes() < frameLength) {
                in.resetReaderIndex();
                return;
            }

            int commandCode = in.readIntLE();
            int payloadLength = frameLength - 4;
            byte[] payload = new byte[payloadLength];
            if (payloadLength > 0) {
                in.readBytes(payload);
            }

            out.add(new RequestFrame(commandCode, payload));
        }
    }

    private static final class RequestFrame {
        private final int commandCode;
        private final byte[] payload;

        private RequestFrame(int commandCode, byte[] payload) {
            this.commandCode = commandCode;
            this.payload = payload;
        }

        private void release() {
            // no-op; method kept to make intent explicit for channelRead lifecycle
        }
    }
}
