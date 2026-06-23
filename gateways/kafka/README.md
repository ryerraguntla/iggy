# Kafka gateway (`iggy-gateway-kafka`)

Kafka wire gateway for [apache/iggy#3421](https://github.com/apache/iggy/issues/3421) and [Discussion #3253](https://github.com/apache/iggy/discussions/3253) Phase 1:

- **Phase 1A** — TCP listener, version firewall, wire codecs (stub mode)
- **Phase 1B** — Iggy bridge: Produce, Fetch, ListOffsets, CreateTopics, Metadata backed by Iggy

## Run

Start Iggy (TCP `8090`), then the gateway:

```bash
# Terminal 1 — Iggy
cargo run -p server --bin iggy-server

# Terminal 2 — Kafka gateway (Iggy bridge enabled by default)
IGGY_TCP_ADDR=127.0.0.1:8090 cargo run -p iggy-gateway-kafka
```

Default Kafka bind: `127.0.0.1:9093`. Override with `KAFKA_BIND_ADDR`. Set `KAFKA_IGGY_BRIDGE=false` for stub-only mode (#3421).

Docker Compose stack: [`docker-compose.yml`](docker-compose.yml).

## Test

```bash
cargo test -p iggy-gateway-kafka
```

116 regression tests across 17 suites — see [docs/TEST_SUITE.md](docs/TEST_SUITE.md). Bridge integration tests require `cargo build -p server --bin iggy-server` first.

`decode_validation_tests` require wire fixtures under `tools/kafka-tool/kafka_messages/` (gitignored locally; CI generates them via `scripts/ci-wire-fixtures.sh`):

```bash
./gateways/kafka/scripts/ci-wire-fixtures.sh generate
cargo test -p iggy-gateway-kafka
./gateways/kafka/scripts/ci-wire-fixtures.sh cleanup   # optional
```

Or generate only the keys the tests need:

```bash
for key in 0 1 2 19; do
  cargo run -p kafka-message-gen -- generate \
    --output gateways/kafka/tools/kafka-tool/kafka_messages \
    --api-key "$key"
done
```

## Manual testing

Before check-in, run the procedure in [docs/MANUAL_TESTING.md](docs/MANUAL_TESTING.md) (smoke, version firewall, kcat, adversarial cases).

## Scoped APIs

See [docs/SCOPE.md](docs/SCOPE.md) for deliverables and [docs/BRIDGE_MAPPING.md](docs/BRIDGE_MAPPING.md) for Kafka↔Iggy mapping.

## Wire fixture tool

See [tools/kafka-tool/README.md](tools/kafka-tool/README.md).
