cargo clean
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo build
cargo test
cargo machete
cargo sort --workspace
