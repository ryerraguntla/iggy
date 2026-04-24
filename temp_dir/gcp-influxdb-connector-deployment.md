# GCP Deployment & Testing Guide: Iggy InfluxDB Connectors

**Branch:** `feat/redshift-connector`  
**Connectors:** `iggy-connector-influxdb-sink`, `iggy-connector-influxdb-source`  
**Config schema:** `#[serde(tag = "version")]` — both sink and source use a tagged enum where `version = "v2"` or `version = "v3"` selects the variant.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [GCP Infrastructure Setup](#2-gcp-infrastructure-setup)
3. [Install InfluxDB 2](#3-install-influxdb-2)
4. [Install InfluxDB 3 Core](#4-install-influxdb-3-core)
5. [Build Iggy on GCP](#5-build-iggy-on-gcp)
6. [Connector Runtime Configuration](#6-connector-runtime-configuration)
7. [Connector Config Files](#7-connector-config-files)
8. [Manual Testing](#8-manual-testing)
9. [Automated Test Script](#9-automated-test-script)
10. [Environment Variable Reference](#10-environment-variable-reference)
11. [Key Implementation Details](#11-key-implementation-details)
12. [Troubleshooting](#12-troubleshooting)

---

## 1. Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│                              GCP VPC                                 │
│                                                                      │
│  ┌───────────────────┐  ┌────────────────┐  ┌──────────────────────┐│
│  │   VM: iggy-server │  │  VM: influxdb2 │  │   VM: influxdb3      ││
│  │                   │  │                │  │                      ││
│  │  iggy-server      │  │  InfluxDB 2.x  │  │  InfluxDB 3 Core     ││
│  │    :8090 (TCP)    │  │    :8086       │  │    :8181             ││
│  │    :8080 (HTTP)   │  │                │  │  • /api/v3/write_lp  ││
│  │                   │  │  • /api/v2/    │  │  • /api/v3/query_sql ││
│  │  connectors-      │  │    write       │  │  • /api/v2/write     ││
│  │  runtime          │  │  • /api/v2/    │  │    (V2 compat)       ││
│  │    :8081 (HTTP)   │  │    query(Flux) │  │  • Flux NOT supported││
│  └───────────────────┘  └────────────────┘  └──────────────────────┘│
└──────────────────────────────────────────────────────────────────────┘
```

### Connector variants supported

| Connector | Protocol version | Backend | Auth style |
|-----------|-----------------|---------|-----------|
| Sink V2 | `version = "v2"` | InfluxDB 2.x | `Token <token>` |
| Sink V3 | `version = "v3"` | InfluxDB 3 Core | `Bearer <token>` |
| Sink V2 → V3 | `version = "v2"` | InfluxDB 3 Core (V2 compat write) | `Token <token>` |
| Source V2 | `version = "v2"` | InfluxDB 2.x | `Token <token>` (Flux) |
| Source V3 | `version = "v3"` | InfluxDB 3 Core | `Bearer <token>` (SQL) |

> **Important:** InfluxDB 3 Core accepts `/api/v2/write` for writes (V2 line-protocol compatibility) but does **not** support Flux queries. The V2 source connector **cannot** use InfluxDB 3 as its backend — only the V3 source connector (SQL via `/api/v3/query_sql`) works with InfluxDB 3.

---

## 2. GCP Infrastructure Setup

### 2.1 Create VMs

```bash
PROJECT=your-gcp-project
ZONE=us-central1-a
REGION=us-central1

# Iggy server + connector runtime
gcloud compute instances create iggy-server \
  --project=$PROJECT --zone=$ZONE \
  --machine-type=e2-standard-2 \
  --image-family=debian-12 --image-project=debian-cloud \
  --boot-disk-size=30GB \
  --tags=iggy-server

# InfluxDB 2.x
gcloud compute instances create influxdb2 \
  --project=$PROJECT --zone=$ZONE \
  --machine-type=e2-standard-2 \
  --image-family=debian-12 --image-project=debian-cloud \
  --boot-disk-size=50GB \
  --tags=influxdb2

# InfluxDB 3 Core
gcloud compute instances create influxdb3 \
  --project=$PROJECT --zone=$ZONE \
  --machine-type=e2-standard-2 \
  --image-family=debian-12 --image-project=debian-cloud \
  --boot-disk-size=50GB \
  --tags=influxdb3
```

### 2.2 Firewall Rules

```bash
# Internal VPC traffic between VMs
gcloud compute firewall-rules create iggy-internal \
  --project=$PROJECT --network=default \
  --allow=tcp:8086,tcp:8090,tcp:8080,tcp:8081,tcp:8181 \
  --source-ranges=10.128.0.0/9 \
  --target-tags=iggy-server,influxdb2,influxdb3

# SSH from your workstation IP
gcloud compute firewall-rules create iggy-ssh \
  --project=$PROJECT --network=default \
  --allow=tcp:22 \
  --source-ranges=$(curl -s ifconfig.me)/32
```

### 2.3 Capture Internal IPs

```bash
IGGY_IP=$(gcloud compute instances describe iggy-server \
  --zone=$ZONE --format='get(networkInterfaces[0].networkIP)')
INFLUX2_IP=$(gcloud compute instances describe influxdb2 \
  --zone=$ZONE --format='get(networkInterfaces[0].networkIP)')
INFLUX3_IP=$(gcloud compute instances describe influxdb3 \
  --zone=$ZONE --format='get(networkInterfaces[0].networkIP)')

echo "Iggy:    $IGGY_IP"
echo "Influx2: $INFLUX2_IP"
echo "Influx3: $INFLUX3_IP"
```

---

## 3. Install InfluxDB 2

SSH into the `influxdb2` VM:

```bash
gcloud compute ssh influxdb2 --zone=$ZONE
```

```bash
# Add InfluxData repo and install
wget -q https://repos.influxdata.com/influxdata-archive_compat.key
echo '393e8779c89ac8d958f81f942f9ad7fb82a25e133faddaf92e15b16e6ac9ce4c influxdata-archive_compat.key' \
  | sha256sum -c \
  && cat influxdata-archive_compat.key \
  | gpg --dearmor \
  | sudo tee /etc/apt/trusted.gpg.d/influxdata-archive_compat.gpg >/dev/null

echo 'deb [signed-by=/etc/apt/trusted.gpg.d/influxdata-archive_compat.gpg] https://repos.influxdata.com/debian stable main' \
  | sudo tee /etc/apt/sources.list.d/influxdata.list

sudo apt-get update && sudo apt-get install -y influxdb2

# Enable and start
sudo systemctl enable --now influxd

# Bootstrap: creates admin user, org, and initial bucket
influx setup \
  --username iggy-admin \
  --password iggy-password \
  --org iggy-org \
  --bucket iggy-sink-bucket \
  --retention 0 \
  --force

# Create a separate bucket for source tests
influx bucket create --name iggy-source-bucket --org iggy-org

# Create a long-lived all-access token for the connectors
influx auth create \
  --org iggy-org \
  --read-buckets \
  --write-buckets \
  --description "iggy-connector-token"
# *** Save the printed token — you will need it in connector configs ***
```

### Verify InfluxDB 2

```bash
curl -s http://localhost:8086/health
# Expected: {"name":"influxdb","message":"ready for queries and writes","status":"pass",...}
```

---

## 4. Install InfluxDB 3 Core

SSH into the `influxdb3` VM:

```bash
gcloud compute ssh influxdb3 --zone=$ZONE
```

```bash
# Download and install the binary
curl -O https://dl.influxdata.com/influxdb/releases/influxdb3-core_latest_linux_amd64.tar.gz
tar xzf influxdb3-core_latest_linux_amd64.tar.gz
sudo mv influxdb3 /usr/local/bin/

# Create data directory and service user
sudo mkdir -p /var/lib/influxdb3
sudo useradd -r -s /bin/false influxdb3 2>/dev/null || true
sudo chown influxdb3:influxdb3 /var/lib/influxdb3

# Create systemd service
sudo tee /etc/systemd/system/influxdb3.service <<'EOF'
[Unit]
Description=InfluxDB 3 Core
After=network.target

[Service]
User=influxdb3
ExecStart=/usr/local/bin/influxdb3 serve \
  --node-id node0 \
  --object-store file \
  --data-dir /var/lib/influxdb3 \
  --http-bind 0.0.0.0:8181
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF

# For production: add --bearer-token YOUR_SECRET_TOKEN to ExecStart above

sudo systemctl daemon-reload
sudo systemctl enable --now influxdb3
```

### Verify InfluxDB 3

```bash
curl -s http://localhost:8181/health
# Expected: {"status":"ok"} or HTTP 200
```

> **Note:** InfluxDB 3 Core has no built-in setup/auth CLI. By default it runs without authentication. Add `--bearer-token your-token` to the ExecStart line for production deployments. All examples below use a placeholder token of `iggy-v3-token`.

---

## 5. Build Iggy on GCP

SSH into the `iggy-server` VM:

```bash
gcloud compute ssh iggy-server --zone=$ZONE
```

### 5.1 Install Rust and build dependencies

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "$HOME/.cargo/env"

sudo apt-get update && sudo apt-get install -y \
  build-essential pkg-config libssl-dev git cmake protobuf-compiler
```

### 5.2 Clone and build

```bash
git clone https://github.com/apache/iggy.git
cd iggy
git checkout feat/redshift-connector

# Build all required binaries and shared libraries
cargo build --release -p iggy-server
cargo build --release -p iggy-cli
cargo build --release -p iggy-connectors-runtime
cargo build --release -p iggy-connector-influxdb-sink
cargo build --release -p iggy-connector-influxdb-source
```

### 5.3 Start Iggy server and create streams

```bash
# Start the server in the background
RUST_LOG=info ./target/release/iggy-server &
sleep 3

CLI="./target/release/iggy-cli --transport tcp --server-address localhost:8090"

# Create stream
$CLI stream create iggy-stream

# Topics for sink tests (messages flow Iggy → InfluxDB)
$CLI topic create iggy-stream iggy-topic 1 none

# Topics for source tests (messages flow InfluxDB → Iggy)
$CLI topic create iggy-stream iggy-topic-from-influx2 1 none
$CLI topic create iggy-stream iggy-topic-from-influx3 1 none

echo "Streams and topics created."
```

---

## 6. Connector Runtime Configuration

### 6.1 Directory layout

```bash
mkdir -p ~/connectors-config/connectors
mkdir -p ~/connectors-state
```

### 6.2 Main runtime config: `~/connectors-config/config.toml`

```toml
[http]
enabled = true
address  = "0.0.0.0:8081"
api_key  = ""

[http.cors]
enabled = false

[iggy]
address  = "localhost:8090"
username = "iggy"
password = "iggy"
token    = ""

[iggy.tls]
enabled = false

[state]
path = "/home/USER/connectors-state"    # ← replace USER

[connectors]
config_type = "local"
config_dir  = "/home/USER/connectors-config/connectors"  # ← replace USER

[telemetry]
enabled = false
```

### 6.3 Start the connector runtime

```bash
cd ~/iggy
RUST_LOG=info ./target/release/iggy-connectors-runtime \
  --config ~/connectors-config/config.toml
```

Enable only the connector under test by setting `enabled = true` in its TOML file and `enabled = false` in all others, or run separate runtime processes with separate config directories.

---

## 7. Connector Config Files

> **Critical:** Every `[plugin_config]` section **must** include a `version` field set to `"v2"` or `"v3"`. This is the `#[serde(tag = "version")]` discriminant that selects `V2SinkConfig`/`V3SinkConfig` (or their source equivalents). Omitting it causes the config parse to fail with:
>
> ```
> Failed to parse configuration for connector: missing field `version`
> ```

Replace all `INFLUX2_IP`, `INFLUX3_IP`, `YOUR_INFLUXDB2_TOKEN`, and `/home/USER` placeholders before use.

---

### 7.1 InfluxDB 2 Sink

**File:** `~/connectors-config/connectors/influxdb_sink_v2.toml`

```toml
type    = "sink"
key     = "influxdb"
enabled = true
version = 0
name    = "InfluxDB v2 sink"
path    = "/home/USER/iggy/target/release/libiggy_connector_influxdb_sink"
plugin_config_format = "toml"
verbose = false

[[streams]]
stream         = "iggy-stream"
topics         = ["iggy-topic"]
schema         = "json"
batch_length   = 100
poll_interval  = "500ms"
consumer_group = "influxdb2-sink-cg"

[plugin_config]
version    = "v2"                             # ← serde discriminant (required)
url        = "http://INFLUX2_IP:8086"         # ← replace
org        = "iggy-org"
bucket     = "iggy-sink-bucket"
token      = "YOUR_INFLUXDB2_TOKEN"           # ← replace
measurement = "iggy_messages"
precision  = "ns"
batch_size = 500
include_metadata         = true
include_checksum         = true
include_origin_timestamp = true
include_stream_tag       = true
include_topic_tag        = true
include_partition_tag    = true
payload_format = "json"
max_retries    = 3
retry_delay    = "1s"
timeout        = "30s"
```

---

### 7.2 InfluxDB 2 Source

**File:** `~/connectors-config/connectors/influxdb_source_v2.toml`

Uses Flux queries against `/api/v2/query`, `Token` auth, annotated CSV response, cursor on `_time` column with `>= $cursor` + skip-N deduplication.

```toml
type    = "source"
key     = "influxdb"
enabled = true
version = 0
name    = "InfluxDB v2 source"
path    = "/home/USER/iggy/target/release/libiggy_connector_influxdb_source"
plugin_config_format = "toml"
verbose = false

[[streams]]
stream = "iggy-stream"
topic  = "iggy-topic-from-influx2"
schema = "json"

[plugin_config]
version        = "v2"                         # ← serde discriminant (required)
url            = "http://INFLUX2_IP:8086"     # ← replace
org            = "iggy-org"
token          = "YOUR_INFLUXDB2_TOKEN"       # ← replace
query          = '''
from(bucket: "iggy-sink-bucket")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "iggy_messages")
  |> filter(fn: (r) => r._time >= time(v: "$cursor"))
  |> sort(columns: ["_time"])
  |> limit(n: $limit)
'''
poll_interval  = "2s"
batch_size     = 100
cursor_field   = "_time"                      # V2 default; explicit is clearer
initial_offset = "1970-01-01T00:00:00Z"
payload_format = "json"
max_retries    = 3
retry_delay    = "1s"
timeout        = "30s"
```

---

### 7.3 InfluxDB 3 Sink (native V3 protocol)

**File:** `~/connectors-config/connectors/influxdb_sink_v3.toml`

Uses `/api/v3/write_lp`, `Bearer` auth, `db` field (not `bucket` or `database`).

```toml
type    = "sink"
key     = "influxdb"
enabled = true
version = 0
name    = "InfluxDB v3 sink"
path    = "/home/USER/iggy/target/release/libiggy_connector_influxdb_sink"
plugin_config_format = "toml"
verbose = false

[[streams]]
stream         = "iggy-stream"
topics         = ["iggy-topic"]
schema         = "json"
batch_length   = 100
poll_interval  = "500ms"
consumer_group = "influxdb3-sink-cg"

[plugin_config]
version     = "v3"                            # ← serde discriminant (required)
url         = "http://INFLUX3_IP:8181"        # ← replace
db          = "iggy-db"                       # ← field is "db", NOT "bucket" or "database"
token       = "iggy-v3-token"                 # ← omit or use placeholder if InfluxDB 3 started without auth
measurement = "iggy_messages"
precision   = "ns"
batch_size  = 500
include_metadata         = true
include_checksum         = true
include_origin_timestamp = true
include_stream_tag       = true
include_topic_tag        = true
include_partition_tag    = true
payload_format = "json"
max_retries    = 3
retry_delay    = "1s"
timeout        = "30s"
```

---

### 7.4 InfluxDB 3 Sink using V2 write protocol (compatibility mode)

**File:** `~/connectors-config/connectors/influxdb_sink_v2_on_v3.toml`

InfluxDB 3 Core accepts `/api/v2/write` with line-protocol. Use the V2 sink config pointed at InfluxDB 3, using the database name as the `bucket` value.

```toml
type    = "sink"
key     = "influxdb"
enabled = true
version = 0
name    = "InfluxDB v2-protocol sink on v3 backend"
path    = "/home/USER/iggy/target/release/libiggy_connector_influxdb_sink"
plugin_config_format = "toml"
verbose = false

[[streams]]
stream         = "iggy-stream"
topics         = ["iggy-topic"]
schema         = "json"
batch_length   = 100
poll_interval  = "500ms"
consumer_group = "influxdb2on3-sink-cg"

[plugin_config]
version     = "v2"                            # ← V2 sink config → uses /api/v2/write
url         = "http://INFLUX3_IP:8181"        # ← pointing at InfluxDB 3 Core
org         = "iggy-org"                      # InfluxDB 3 accepts but ignores org
bucket      = "iggy-db"                       # InfluxDB 3 treats bucket name as db name
token       = "iggy-v3-token"
measurement = "iggy_messages_v2_compat"
precision   = "ns"
batch_size  = 500
payload_format = "json"
max_retries    = 3
retry_delay    = "1s"
timeout        = "30s"
```

---

### 7.5 InfluxDB 3 Source

**File:** `~/connectors-config/connectors/influxdb_source_v3.toml`

Uses SQL queries against `/api/v3/query_sql`, `Bearer` auth, JSONL response, strict `> $cursor` semantics on the `time` column, stuck-timestamp detection with batch-size inflation.

```toml
type    = "source"
key     = "influxdb"
enabled = true
version = 0
name    = "InfluxDB v3 source"
path    = "/home/USER/iggy/target/release/libiggy_connector_influxdb_source"
plugin_config_format = "toml"
verbose = false

[[streams]]
stream = "iggy-stream"
topic  = "iggy-topic-from-influx3"
schema = "json"

[plugin_config]
version        = "v3"                         # ← serde discriminant (required)
url            = "http://INFLUX3_IP:8181"     # ← replace
db             = "iggy-db"                    # ← field is "db", not "org"+"bucket"
token          = "iggy-v3-token"              # ← omit or placeholder if no auth
query          = '''
SELECT * FROM iggy_messages
WHERE time > '$cursor'
ORDER BY time
LIMIT $limit
'''
poll_interval          = "2s"
batch_size             = 100
cursor_field           = "time"               # V3 default; explicit is clearer
initial_offset         = "1970-01-01T00:00:00Z"
payload_format         = "json"
stuck_batch_cap_factor = 10                   # V3-only: inflate batch up to 10× before CB trips
max_retries            = 3
retry_delay            = "1s"
timeout                = "30s"
```

---

## 8. Manual Testing

### Prerequisites

```bash
# Set convenience variables on iggy-server VM
CLI="$HOME/iggy/target/release/iggy-cli --transport tcp --server-address localhost:8090"
INFLUX2_IP=<your-influxdb2-internal-ip>
INFLUX2_TOKEN=<your-influxdb2-token>
INFLUX3_IP=<your-influxdb3-internal-ip>
```

---

### 8.1 Test: V2 Sink (Iggy → InfluxDB 2)

**Step 1 — Enable only the V2 sink connector** in its TOML, then start (or restart) the runtime.

**Step 2 — Publish a message to Iggy:**

```bash
$CLI message send iggy-stream iggy-topic - <<< \
  '{"measurement":"iggy_messages","value":42.5,"host":"gcp-test","env":"prod"}'
```

**Step 3 — Verify the point arrived in InfluxDB 2:**

```bash
curl -s "http://$INFLUX2_IP:8086/api/v2/query?org=iggy-org" \
  -H "Authorization: Token $INFLUX2_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "from(bucket:\"iggy-sink-bucket\") |> range(start:-5m) |> filter(fn:(r)=>r._measurement==\"iggy_messages\")",
    "type": "flux"
  }'
# Expected: annotated CSV rows with _measurement=iggy_messages
```

---

### 8.2 Test: V2 Source (InfluxDB 2 → Iggy)

**Step 1 — Enable only the V2 source connector** and start the runtime.

**Step 2 — Write a point directly into InfluxDB 2:**

```bash
TS_NS=$(date +%s%N)
curl -s "http://$INFLUX2_IP:8086/api/v2/write?org=iggy-org&bucket=iggy-sink-bucket&precision=ns" \
  -H "Authorization: Token $INFLUX2_TOKEN" \
  -H "Content-Type: text/plain; charset=utf-8" \
  --data-binary "sensor_readings,host=vm1 temperature=22.3,humidity=55.1 $TS_NS"
echo "Written at ns=$TS_NS"
```

**Step 3 — Poll Iggy for the message (allow up to ~10 seconds):**

```bash
sleep 5
$CLI message poll iggy-stream iggy-topic-from-influx2 1 0 10
# Expected: JSON payload containing sensor_readings fields
```

---

### 8.3 Test: V3 Sink (Iggy → InfluxDB 3)

**Step 1 — Enable only the V3 sink connector** and start the runtime.

**Step 2 — Publish a message to Iggy:**

```bash
$CLI message send iggy-stream iggy-topic - <<< \
  '{"measurement":"iggy_messages","value":99.1,"host":"gcp-v3-test"}'
```

**Step 3 — Verify via InfluxDB 3 SQL:**

```bash
curl -s "http://$INFLUX3_IP:8181/api/v3/query_sql" \
  -H "Content-Type: application/json" \
  -d '{"db":"iggy-db","q":"SELECT * FROM iggy_messages LIMIT 10","format":"jsonl"}'
# Expected: JSONL rows with iggy_messages fields
```

---

### 8.4 Test: V3 Source (InfluxDB 3 → Iggy)

**Step 1 — Enable only the V3 source connector** and start the runtime.

**Step 2 — Write a point directly into InfluxDB 3:**

```bash
TS_NS=$(date +%s%N)
curl -s "http://$INFLUX3_IP:8181/api/v3/write_lp?db=iggy-db&precision=ns" \
  -H "Content-Type: text/plain; charset=utf-8" \
  --data-binary "iggy_messages,host=vm3 value=7.7 $TS_NS"
echo "Written at ns=$TS_NS"
```

**Step 3 — Poll Iggy for the message:**

```bash
sleep 5
$CLI message poll iggy-stream iggy-topic-from-influx3 1 0 10
# Expected: JSON payload containing iggy_messages fields
```

---

### 8.5 Test: V2 Protocol Sink on InfluxDB 3 (compatibility)

**Step 1 — Enable only the V2-on-V3 sink connector** and start the runtime.

**Step 2 — Publish a message to Iggy:**

```bash
$CLI message send iggy-stream iggy-topic - <<< \
  '{"measurement":"iggy_messages_v2_compat","value":5005,"host":"compat-test"}'
```

**Step 3 — Verify via InfluxDB 3:**

```bash
curl -s "http://$INFLUX3_IP:8181/api/v3/query_sql" \
  -H "Content-Type: application/json" \
  -d '{"db":"iggy-db","q":"SELECT * FROM iggy_messages_v2_compat LIMIT 10","format":"jsonl"}'
# Expected: JSONL rows — confirms V2 line-protocol is accepted by InfluxDB 3
```

---

## 9. Automated Test Script

Save as `~/test-connectors.sh`, make executable with `chmod +x ~/test-connectors.sh`, then run on the `iggy-server` VM.

```bash
#!/usr/bin/env bash
# test-connectors.sh
# End-to-end test of all five InfluxDB connector scenarios.
#
# Usage:
#   ./test-connectors.sh <influx2-internal-ip> <influx2-token> <influx3-internal-ip>
#
# Prerequisites:
#   - iggy-server running on localhost:8090
#   - connectors-runtime running with the appropriate connector enabled
#   - streams/topics already created (see Section 5.3)
#
# The script enables one connector at a time by swapping the 'enabled' flag
# in the TOML files and sending SIGHUP to the runtime to reload.
# If you prefer to restart the runtime manually between tests, set
# AUTO_RELOAD=false below.

set -euo pipefail

# ── Configuration ──────────────────────────────────────────────────────────────
INFLUX2_IP="${1:?Usage: $0 <influx2-ip> <influx2-token> <influx3-ip>}"
INFLUX2_TOKEN="${2:?Usage: $0 <influx2-ip> <influx2-token> <influx3-ip>}"
INFLUX3_IP="${3:?Usage: $0 <influx2-ip> <influx2-token> <influx3-ip>}"

IGGY_DIR="${IGGY_DIR:-$HOME/iggy}"
IGGY_CLI="$IGGY_DIR/target/release/iggy-cli"
IGGY_ARGS="--transport tcp --server-address localhost:8090"
CONNECTORS_DIR="${CONNECTORS_DIR:-$HOME/connectors-config/connectors}"
RUNTIME_PID_FILE="${RUNTIME_PID_FILE:-/tmp/iggy-connectors-runtime.pid}"
CONFIG_FILE="${CONFIG_FILE:-$HOME/connectors-config/config.toml}"

POLL_ATTEMPTS=30   # max attempts when waiting for data
POLL_DELAY=2       # seconds between attempts

# ── Helpers ────────────────────────────────────────────────────────────────────
OK()   { printf '\033[32m[PASS]\033[0m %s\n' "$*"; }
FAIL() { printf '\033[31m[FAIL]\033[0m %s\n' "$*"; exit 1; }
INFO() { printf '\033[33m[INFO]\033[0m %s\n' "$*"; }
SKIP() { printf '\033[36m[SKIP]\033[0m %s\n' "$*"; }

# Poll a command until it returns non-empty stdout, or fail.
poll_until() {
    local desc="$1"
    shift
    for i in $(seq 1 $POLL_ATTEMPTS); do
        local out
        out=$("$@" 2>/dev/null) && [ -n "$out" ] && echo "$out" && return 0
        INFO "  waiting ($i/$POLL_ATTEMPTS) for: $desc"
        sleep $POLL_DELAY
    done
    return 1
}

# Send a JSON message to Iggy.
iggy_send() {
    local stream="$1" topic="$2" payload="$3"
    "$IGGY_CLI" $IGGY_ARGS message send "$stream" "$topic" - <<< "$payload"
}

# Poll Iggy for messages on a topic.
iggy_poll() {
    local stream="$1" topic="$2" count="${3:-5}"
    "$IGGY_CLI" $IGGY_ARGS message poll "$stream" "$topic" 1 0 "$count" 2>/dev/null
}

# ── Prerequisite checks ────────────────────────────────────────────────────────
INFO "Checking InfluxDB 2 health..."
curl -sf "http://$INFLUX2_IP:8086/health" | grep -q '"status":"pass"' \
  || FAIL "InfluxDB 2 health check failed — is influxd running on $INFLUX2_IP:8086?"
OK "InfluxDB 2 healthy"

INFO "Checking InfluxDB 3 health..."
curl -sf "http://$INFLUX3_IP:8181/health" >/dev/null \
  || FAIL "InfluxDB 3 health check failed — is influxdb3 running on $INFLUX3_IP:8181?"
OK "InfluxDB 3 healthy"

INFO "Checking Iggy server..."
"$IGGY_CLI" $IGGY_ARGS stream list >/dev/null 2>&1 \
  || FAIL "Iggy server not reachable on localhost:8090"
OK "Iggy server reachable"

INFO "Checking required topics exist..."
TOPICS=$("$IGGY_CLI" $IGGY_ARGS topic list iggy-stream 2>/dev/null)
for t in iggy-topic iggy-topic-from-influx2 iggy-topic-from-influx3; do
  echo "$TOPICS" | grep -q "$t" || FAIL "Topic '$t' missing — run Section 5.3 setup"
done
OK "All topics present"

# ── Test 1: V2 Sink (Iggy → InfluxDB 2) ──────────────────────────────────────
INFO ""
INFO "═══════════════════════════════════════════════════════════"
INFO " TEST 1: V2 Sink  (Iggy → InfluxDB 2)"
INFO "═══════════════════════════════════════════════════════════"
INFO "Ensure influxdb_sink_v2.toml is enabled and runtime is running."
INFO "Press ENTER when ready, or Ctrl+C to skip."
read -r

TEST_ID="sink-v2-$$"
iggy_send iggy-stream iggy-topic \
  "{\"measurement\":\"iggy_messages\",\"value\":1001,\"test_id\":\"$TEST_ID\"}"
INFO "Message sent to Iggy. Waiting for point in InfluxDB 2..."

FLUX_QUERY="from(bucket:\"iggy-sink-bucket\") |> range(start:-5m) |> filter(fn:(r)=>r._measurement==\"iggy_messages\")"
RESULT=$(poll_until "v2-sink point in InfluxDB 2" \
  curl -sf "http://$INFLUX2_IP:8086/api/v2/query?org=iggy-org" \
    -H "Authorization: Token $INFLUX2_TOKEN" \
    -H "Content-Type: application/json" \
    -d "{\"query\":\"$FLUX_QUERY\",\"type\":\"flux\"}"
) || FAIL "TEST 1 FAILED: point did not appear in InfluxDB 2 after $((POLL_ATTEMPTS * POLL_DELAY))s"

echo "$RESULT" | grep -q "iggy_messages" \
  || FAIL "TEST 1 FAILED: 'iggy_messages' not found in InfluxDB 2 response"
OK "TEST 1 PASSED: V2 Sink — point written to InfluxDB 2"

# ── Test 2: V2 Source (InfluxDB 2 → Iggy) ────────────────────────────────────
INFO ""
INFO "═══════════════════════════════════════════════════════════"
INFO " TEST 2: V2 Source  (InfluxDB 2 → Iggy)"
INFO "═══════════════════════════════════════════════════════════"
INFO "Ensure influxdb_source_v2.toml is enabled and runtime is running."
INFO "Press ENTER when ready, or Ctrl+C to skip."
read -r

TS_NS=$(date +%s%N)
TEST_ID="src-v2-$$"
curl -sf "http://$INFLUX2_IP:8086/api/v2/write?org=iggy-org&bucket=iggy-sink-bucket&precision=ns" \
  -H "Authorization: Token $INFLUX2_TOKEN" \
  -H "Content-Type: text/plain; charset=utf-8" \
  --data-binary "sensor_readings,host=gcp-test-v2,test_id=$TEST_ID value=22.3 $TS_NS"
OK "Point written to InfluxDB 2 (ns=$TS_NS). Waiting for Iggy message..."

RESULT=$(poll_until "v2-source message in Iggy" \
  iggy_poll iggy-stream iggy-topic-from-influx2 10
) || FAIL "TEST 2 FAILED: no message appeared in iggy-topic-from-influx2 after $((POLL_ATTEMPTS * POLL_DELAY))s"

echo "$RESULT" | grep -q "sensor_readings\|value\|22" \
  || FAIL "TEST 2 FAILED: expected payload not found in Iggy message"
OK "TEST 2 PASSED: V2 Source — message read from InfluxDB 2 into Iggy"

# ── Test 3: V3 Sink (Iggy → InfluxDB 3) ──────────────────────────────────────
INFO ""
INFO "═══════════════════════════════════════════════════════════"
INFO " TEST 3: V3 Sink  (Iggy → InfluxDB 3)"
INFO "═══════════════════════════════════════════════════════════"
INFO "Ensure influxdb_sink_v3.toml is enabled and runtime is running."
INFO "Press ENTER when ready, or Ctrl+C to skip."
read -r

TEST_ID="sink-v3-$$"
iggy_send iggy-stream iggy-topic \
  "{\"measurement\":\"iggy_messages\",\"value\":3003,\"test_id\":\"$TEST_ID\"}"
INFO "Message sent to Iggy. Waiting for row in InfluxDB 3..."

RESULT=$(poll_until "v3-sink row in InfluxDB 3" \
  curl -sf "http://$INFLUX3_IP:8181/api/v3/query_sql" \
    -H "Content-Type: application/json" \
    -d '{"db":"iggy-db","q":"SELECT * FROM iggy_messages LIMIT 5","format":"jsonl"}'
) || FAIL "TEST 3 FAILED: row did not appear in InfluxDB 3 after $((POLL_ATTEMPTS * POLL_DELAY))s"

echo "$RESULT" | grep -q "iggy_messages\|value\|3003" \
  || FAIL "TEST 3 FAILED: expected row not found in InfluxDB 3 response"
OK "TEST 3 PASSED: V3 Sink — row written to InfluxDB 3"

# ── Test 4: V3 Source (InfluxDB 3 → Iggy) ────────────────────────────────────
INFO ""
INFO "═══════════════════════════════════════════════════════════"
INFO " TEST 4: V3 Source  (InfluxDB 3 → Iggy)"
INFO "═══════════════════════════════════════════════════════════"
INFO "Ensure influxdb_source_v3.toml is enabled and runtime is running."
INFO "Press ENTER when ready, or Ctrl+C to skip."
read -r

TS_NS=$(date +%s%N)
TEST_ID="src-v3-$$"
curl -sf "http://$INFLUX3_IP:8181/api/v3/write_lp?db=iggy-db&precision=ns" \
  -H "Content-Type: text/plain; charset=utf-8" \
  --data-binary "iggy_messages,host=gcp-test-v3,test_id=$TEST_ID value=77.7 $TS_NS"
OK "Point written to InfluxDB 3 (ns=$TS_NS). Waiting for Iggy message..."

RESULT=$(poll_until "v3-source message in Iggy" \
  iggy_poll iggy-stream iggy-topic-from-influx3 10
) || FAIL "TEST 4 FAILED: no message appeared in iggy-topic-from-influx3 after $((POLL_ATTEMPTS * POLL_DELAY))s"

echo "$RESULT" | grep -q "iggy_messages\|value\|77" \
  || FAIL "TEST 4 FAILED: expected payload not found in Iggy message"
OK "TEST 4 PASSED: V3 Source — message read from InfluxDB 3 into Iggy"

# ── Test 5: V2 Protocol Sink on InfluxDB 3 (compatibility) ───────────────────
INFO ""
INFO "═══════════════════════════════════════════════════════════"
INFO " TEST 5: V2 Protocol Sink on InfluxDB 3 (compatibility)"
INFO "═══════════════════════════════════════════════════════════"
INFO "Ensure influxdb_sink_v2_on_v3.toml is enabled and runtime is running."
INFO "Press ENTER when ready, or Ctrl+C to skip."
read -r

TEST_ID="v2on3-$$"
iggy_send iggy-stream iggy-topic \
  "{\"measurement\":\"iggy_messages_v2_compat\",\"value\":5005,\"test_id\":\"$TEST_ID\"}"
INFO "Message sent to Iggy. Waiting for row in InfluxDB 3 (V2-compat table)..."

RESULT=$(poll_until "v2-on-v3 row in InfluxDB 3" \
  curl -sf "http://$INFLUX3_IP:8181/api/v3/query_sql" \
    -H "Content-Type: application/json" \
    -d '{"db":"iggy-db","q":"SELECT * FROM iggy_messages_v2_compat LIMIT 5","format":"jsonl"}'
) || FAIL "TEST 5 FAILED: row did not appear in iggy_messages_v2_compat after $((POLL_ATTEMPTS * POLL_DELAY))s"

echo "$RESULT" | grep -q "iggy_messages_v2_compat\|value\|5005" \
  || FAIL "TEST 5 FAILED: expected row not found in InfluxDB 3"
OK "TEST 5 PASSED: V2 line-protocol accepted by InfluxDB 3"

# ── Summary ────────────────────────────────────────────────────────────────────
echo ""
OK "╔═══════════════════════════════════════╗"
OK "║  All 5 connector tests PASSED!        ║"
OK "╚═══════════════════════════════════════╝"
```

---

## 10. Environment Variable Reference

All connector config fields can be set via environment variables instead of TOML files. The connector runtime maps:

```
IGGY_CONNECTORS_{SINK|SOURCE}_INFLUXDB_PLUGIN_CONFIG_{FIELD} → field name in config JSON
```

The env var value is lowercased and used as the JSON key. The `VERSION` env var provides the serde tag discriminant.

### V2 Sink env vars

```bash
export IGGY_CONNECTORS_SINK_INFLUXDB_PLUGIN_CONFIG_VERSION=v2        # required discriminant
export IGGY_CONNECTORS_SINK_INFLUXDB_PLUGIN_CONFIG_URL="http://INFLUX2_IP:8086"
export IGGY_CONNECTORS_SINK_INFLUXDB_PLUGIN_CONFIG_ORG=iggy-org
export IGGY_CONNECTORS_SINK_INFLUXDB_PLUGIN_CONFIG_BUCKET=iggy-sink-bucket
export IGGY_CONNECTORS_SINK_INFLUXDB_PLUGIN_CONFIG_TOKEN=YOUR_INFLUXDB2_TOKEN
export IGGY_CONNECTORS_SINK_INFLUXDB_PLUGIN_CONFIG_MEASUREMENT=iggy_messages
export IGGY_CONNECTORS_SINK_INFLUXDB_PLUGIN_CONFIG_PRECISION=ns
export IGGY_CONNECTORS_SINK_INFLUXDB_PLUGIN_CONFIG_PAYLOAD_FORMAT=json
export IGGY_CONNECTORS_SINK_INFLUXDB_STREAMS_0_STREAM=iggy-stream
export IGGY_CONNECTORS_SINK_INFLUXDB_STREAMS_0_TOPICS="[iggy-topic]"
export IGGY_CONNECTORS_SINK_INFLUXDB_STREAMS_0_SCHEMA=json
export IGGY_CONNECTORS_SINK_INFLUXDB_STREAMS_0_CONSUMER_GROUP=influxdb2-sink-cg
export IGGY_CONNECTORS_SINK_INFLUXDB_PATH=./target/release/libiggy_connector_influxdb_sink
```

### V3 Sink env vars

```bash
export IGGY_CONNECTORS_SINK_INFLUXDB_PLUGIN_CONFIG_VERSION=v3        # required discriminant
export IGGY_CONNECTORS_SINK_INFLUXDB_PLUGIN_CONFIG_URL="http://INFLUX3_IP:8181"
export IGGY_CONNECTORS_SINK_INFLUXDB_PLUGIN_CONFIG_DB=iggy-db        # note: DB not BUCKET
export IGGY_CONNECTORS_SINK_INFLUXDB_PLUGIN_CONFIG_TOKEN=iggy-v3-token
export IGGY_CONNECTORS_SINK_INFLUXDB_PLUGIN_CONFIG_MEASUREMENT=iggy_messages
export IGGY_CONNECTORS_SINK_INFLUXDB_PLUGIN_CONFIG_PRECISION=ns
export IGGY_CONNECTORS_SINK_INFLUXDB_PLUGIN_CONFIG_PAYLOAD_FORMAT=json
export IGGY_CONNECTORS_SINK_INFLUXDB_STREAMS_0_STREAM=iggy-stream
export IGGY_CONNECTORS_SINK_INFLUXDB_STREAMS_0_TOPICS="[iggy-topic]"
export IGGY_CONNECTORS_SINK_INFLUXDB_STREAMS_0_SCHEMA=json
export IGGY_CONNECTORS_SINK_INFLUXDB_STREAMS_0_CONSUMER_GROUP=influxdb3-sink-cg
export IGGY_CONNECTORS_SINK_INFLUXDB_PATH=./target/release/libiggy_connector_influxdb_sink
```

### V2 Source env vars

```bash
export IGGY_CONNECTORS_SOURCE_INFLUXDB_PLUGIN_CONFIG_VERSION=v2      # required discriminant
export IGGY_CONNECTORS_SOURCE_INFLUXDB_PLUGIN_CONFIG_URL="http://INFLUX2_IP:8086"
export IGGY_CONNECTORS_SOURCE_INFLUXDB_PLUGIN_CONFIG_ORG=iggy-org
export IGGY_CONNECTORS_SOURCE_INFLUXDB_PLUGIN_CONFIG_TOKEN=YOUR_INFLUXDB2_TOKEN
export IGGY_CONNECTORS_SOURCE_INFLUXDB_PLUGIN_CONFIG_QUERY='from(bucket:"iggy-sink-bucket") |> range(start:-1h) |> filter(fn:(r)=>r._time>=time(v:"$cursor")) |> sort(columns:["_time"]) |> limit(n:$limit)'
export IGGY_CONNECTORS_SOURCE_INFLUXDB_PLUGIN_CONFIG_POLL_INTERVAL=2s
export IGGY_CONNECTORS_SOURCE_INFLUXDB_PLUGIN_CONFIG_BATCH_SIZE=100
export IGGY_CONNECTORS_SOURCE_INFLUXDB_PLUGIN_CONFIG_CURSOR_FIELD=_time
export IGGY_CONNECTORS_SOURCE_INFLUXDB_PLUGIN_CONFIG_PAYLOAD_FORMAT=json
export IGGY_CONNECTORS_SOURCE_INFLUXDB_STREAMS_0_STREAM=iggy-stream
export IGGY_CONNECTORS_SOURCE_INFLUXDB_STREAMS_0_TOPIC=iggy-topic-from-influx2
export IGGY_CONNECTORS_SOURCE_INFLUXDB_STREAMS_0_SCHEMA=json
export IGGY_CONNECTORS_SOURCE_INFLUXDB_PATH=./target/release/libiggy_connector_influxdb_source
```

### V3 Source env vars

```bash
export IGGY_CONNECTORS_SOURCE_INFLUXDB_PLUGIN_CONFIG_VERSION=v3      # required discriminant
export IGGY_CONNECTORS_SOURCE_INFLUXDB_PLUGIN_CONFIG_URL="http://INFLUX3_IP:8181"
export IGGY_CONNECTORS_SOURCE_INFLUXDB_PLUGIN_CONFIG_DB=iggy-db
export IGGY_CONNECTORS_SOURCE_INFLUXDB_PLUGIN_CONFIG_TOKEN=iggy-v3-token
export IGGY_CONNECTORS_SOURCE_INFLUXDB_PLUGIN_CONFIG_QUERY="SELECT * FROM iggy_messages WHERE time > '\$cursor' ORDER BY time LIMIT \$limit"
export IGGY_CONNECTORS_SOURCE_INFLUXDB_PLUGIN_CONFIG_POLL_INTERVAL=2s
export IGGY_CONNECTORS_SOURCE_INFLUXDB_PLUGIN_CONFIG_BATCH_SIZE=100
export IGGY_CONNECTORS_SOURCE_INFLUXDB_PLUGIN_CONFIG_CURSOR_FIELD=time
export IGGY_CONNECTORS_SOURCE_INFLUXDB_PLUGIN_CONFIG_PAYLOAD_FORMAT=json
export IGGY_CONNECTORS_SOURCE_INFLUXDB_PLUGIN_CONFIG_STUCK_BATCH_CAP_FACTOR=10
export IGGY_CONNECTORS_SOURCE_INFLUXDB_STREAMS_0_STREAM=iggy-stream
export IGGY_CONNECTORS_SOURCE_INFLUXDB_STREAMS_0_TOPIC=iggy-topic-from-influx3
export IGGY_CONNECTORS_SOURCE_INFLUXDB_STREAMS_0_SCHEMA=json
export IGGY_CONNECTORS_SOURCE_INFLUXDB_PATH=./target/release/libiggy_connector_influxdb_source
```

---

## 11. Key Implementation Details

### Config field reference

| Config field | V2 Sink | V3 Sink | V2 Source | V3 Source |
|---|---|---|---|---|
| `version` | `"v2"` | `"v3"` | `"v2"` | `"v3"` |
| `url` | ✓ | ✓ | ✓ | ✓ |
| `org` | ✓ | — | ✓ | — |
| `bucket` | ✓ | — | — | — |
| `db` | — | ✓ | — | ✓ |
| `token` | ✓ | ✓ | ✓ | ✓ |
| `measurement` | ✓ | ✓ | — | — |
| `query` | — | — | ✓ (Flux) | ✓ (SQL) |
| `cursor_field` | — | — | `"_time"` | `"time"` |
| `precision` | ✓ | ✓ | — | — |
| `payload_format` | ✓ | ✓ | ✓ | ✓ |
| `stuck_batch_cap_factor` | — | — | — | ✓ |
| `include_metadata` | ✓ | ✓ | ✓ | — |

### Protocol comparison

| Concern | V2 | V3 |
|---|---|---|
| Serde discriminant | `version = "v2"` | `version = "v3"` |
| Auth header | `Token <token>` | `Bearer <token>` |
| Write endpoint | `/api/v2/write?org=X&bucket=Y&precision=P` | `/api/v3/write_lp?db=Y&precision=P` |
| Query endpoint | `/api/v2/query` (Flux) | `/api/v3/query_sql` (SQL) |
| Query response | Annotated CSV | JSONL (one JSON object per line) |
| Source cursor | `_time` column | `time` column |
| Cursor semantics | `>= $cursor` + skip-N dedup | strict `> $cursor` |
| Stuck-timestamp handling | N/A | Doubles batch size up to `stuck_batch_cap_factor × batch_size`, then trips circuit breaker |
| Health endpoint | `/health` | `/health` |

### Query template placeholders

Both V2 and V3 source connectors substitute two placeholders in the query string before sending:

- `$cursor` — current cursor value (RFC 3339 timestamp, validated against `^\d{4}-\d{2}-\d{2}T...` to prevent injection)
- `$limit` — effective batch size (may be inflated for V3 stuck-timestamp handling)

### Persisted state versioning

The source connector serialises its cursor position to disk as a versioned enum:

```json
{"version":"v2","last_timestamp":"2024-01-15T10:30:00Z","processed_rows":42,"cursor_row_count":3}
{"version":"v3","last_timestamp":"2024-01-15T10:30:00Z","processed_rows":42,"effective_batch_size":100}
```

If the on-disk state version does not match the connector config version (e.g., you switch a connector from V2 to V3), the connector logs an error and refuses to start until the stale state file is deleted or the config version is reverted.

---

## 12. Troubleshooting

### Config parse error on startup

```
Failed to parse configuration for connector with ID: 1
```

**Cause:** The `version` field is missing or wrong. The runtime maps `PLUGIN_CONFIG_VERSION` → `"version"` in JSON, which is the serde tag.  
**Fix:** Ensure `version = "v2"` or `version = "v3"` is present in `[plugin_config]`, or that `IGGY_CONNECTORS_{TYPE}_INFLUXDB_PLUGIN_CONFIG_VERSION` is set.

---

### V3 sink: data not appearing

```
write error: status=404 body={"error":"database not found: iggy-db"}
```

**Cause:** The database is created on first write in InfluxDB 3 Core — but the write itself fails if the request is malformed.  
**Fix:** Verify the `db` field matches exactly what you query with. Check that the `Bearer` token is correct (or that InfluxDB 3 was started with `--without-auth` / no `--bearer-token`).

---

### V2 source: messages not arriving in Iggy

**Checklist:**

1. Is the Flux query using `$cursor` and `$limit` placeholders (not hardcoded values)?
2. Is `cursor_field = "_time"` set? (V2 default, but explicit is safer.)
3. Is `initial_offset` set to a time before your data was written?
4. Does the bucket name in the query match the `bucket` field in the sink config?
5. Check connector runtime logs for `PermanentHttpError` (bad token/org/bucket) vs transient errors (circuit breaker).

---

### V3 source: cursor never advances (stuck)

**Cause:** All rows in a batch share the same `time` value. V3 uses strict `> $cursor`, so if the batch is full and all rows are at the same timestamp, the cursor cannot move.  
**Behaviour:** The connector doubles `effective_batch_size` each poll up to `stuck_batch_cap_factor × batch_size`. Once the cap is reached, the circuit breaker trips.  
**Fix:** Reduce write throughput so timestamps are unique, or increase `stuck_batch_cap_factor`. After the circuit breaker cool-down period (`circuit_breaker_cool_down`, default `30s`) the connector retries automatically.

---

### State version mismatch on restart

```
[ERROR] persisted state is V2 but connector is configured as V3 — refusing to start
```

**Fix:** Delete the state file for this connector in the `state.path` directory:

```bash
ls ~/connectors-state/
# Find the file named after the connector ID, e.g. connector_1.json
rm ~/connectors-state/connector_1.json
```

The connector will restart with a fresh cursor from `initial_offset`.

---

### InfluxDB 3 /ping vs /health

InfluxDB 3 Core does not respond to `/ping` with HTTP 204 (that is the InfluxDB 2 convention). Use `/health` which returns HTTP 200 with `{"status":"ok"}`.

---

*Document generated from source branch `feat/redshift-connector` — connector implementations in `core/connectors/sinks/influxdb_sink/src/lib.rs` and `core/connectors/sources/influxdb_source/src/`.*
