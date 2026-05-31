# AGENTS.md

## Cursor Cloud specific instructions

Apache Iggy is a Rust monorepo (`Cargo.toml` workspace under `core/`). The primary dev loop is **iggy-server** plus the **iggy** CLI; optional pieces include the SvelteKit **web** UI, foreign SDKs, BDD (Docker), and connectors.

### System dependencies (Linux)

The server links against **hwloc** via `hwlocality-sys`. On a fresh Ubuntu/Debian VM, install once (not in the VM update script):

```bash
sudo apt-get install -y libhwloc-dev pkg-config libudev-dev
```

This matches `.github/actions/utils/setup-rust-with-cache/action.yml`.

### Rust toolchain

`rust-toolchain.toml` pins **Rust 1.95.0** with `rustfmt` and `clippy`. `rustup` picks it up automatically in `/workspace`.

### Core commands

| Task | Command |
|------|---------|
| Build server + CLI | `cargo build --bin iggy-server --bin iggy` |
| Run server (dev creds) | `IGGY_SYSTEM_PATH=/tmp/iggy-dev-data cargo run --bin iggy-server -- --with-default-root-credentials` |
| CLI (TCP, default) | `cargo run --bin iggy -- -u iggy -p iggy …` |
| Unit tests (fast subset) | `cargo test -p iggy_binary_protocol -p consensus` |
| Format check | `cargo fmt --all -- --check` |
| Clippy (server + CLI) | `cargo clippy -p server -p iggy-cli -- -D warnings` |
| Convenience | `just server -- --with-default-root-credentials` (requires [just](https://github.com/casey/just)) |

**Credentials:** `--with-default-root-credentials` only applies on first start when the data directory is empty (`iggy` / `iggy`). Delete the data dir to reset.

**Ports (defaults):** TCP `8090`, HTTP `3000`, QUIC `8080`, WebSocket `8092`. Server binds to loopback in default config.

**CLI gotcha:** For a single-partition topic, use **partition ID `0`** for `message poll` (README examples sometimes use `1`).

### Web UI (`web/`)

Requires a running server with HTTP enabled (default when HTTP transport is on).

```bash
cd web && npm ci
PUBLIC_IGGY_API_URL=http://127.0.0.1:3000 npm run dev
```

Lint: `cd web && npm run lint`. See `web/README.md`.

### Docker / BDD

Root `docker compose up` builds and runs the server image. Full cross-SDK BDD needs Docker and `scripts/run-bdd-tests.sh` (see `bdd/README.md`). Not required for basic Rust + CLI development.

### Pre-commit

`.pre-commit-config.yaml` runs `cargo fmt`, `cargo clippy` (pre-push), markdownlint, and language-specific hooks. Install with `pre-commit install` if you want local hooks.
