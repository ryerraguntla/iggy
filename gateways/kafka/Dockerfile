# syntax=docker/dockerfile:1
FROM rust:1.85-bookworm AS builder
WORKDIR /app
COPY . .
RUN cargo build --release -p iggy_gateway_kafka --bin iggy-kafka-gateway

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/iggy-kafka-gateway /usr/local/bin/iggy-kafka-gateway
EXPOSE 9093
ENTRYPOINT ["iggy-kafka-gateway"]
