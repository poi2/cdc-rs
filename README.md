# cdc-rs

PostgreSQL logical replication (CDC) to stream outbox INSERT events to Cloud Pub/Sub.

## Architecture

```
AlloyDB / PostgreSQL (WAL)
  -> Logical Replication Slot (test_decoding)
  -> cdc-rs (poll -> decode -> filter INSERT -> publish)
  -> Cloud Pub/Sub
```

- `entity_id` as ordering key for message ordering
- peek -> publish -> confirm -> advance for at-least-once delivery
- Graceful shutdown on SIGTERM/SIGINT (K8s compatible)
- Automatic PostgreSQL reconnection with exponential backoff
- Pub/Sub publish retry with exponential backoff

## Setup

### Prerequisites

- Rust 1.88+
- PostgreSQL 15+ with `wal_level = logical`
- Google Cloud Pub/Sub (or emulator)

### Local Development

```bash
docker compose up -d
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/cdc_rs \
PUBSUB_TOPIC=my-topic \
cargo run
```

### Docker

```bash
docker build -t cdc-rs .
docker run -e DATABASE_URL=... -e PUBSUB_TOPIC=... cdc-rs
```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_URL` | PostgreSQL connection string | (required) |
| `PUBSUB_TOPIC` | Cloud Pub/Sub topic | (required) |
| `OUTBOX_TABLE` | Fully qualified outbox table name | `transactional_box.outbox` |
| `SLOT_NAME` | Replication slot name | `cdc_outbox_slot` |
| `PUBLICATION_NAME` | Publication name | `cdc_outbox_pub` |
| `POLL_INTERVAL_MS` | WAL polling interval (ms) | `100` |
| `MAX_CHANGES_PER_POLL` | Max WAL changes per poll | `1000` |
| `HEALTH_CHECK_PORT` | Health check HTTP port | `8080` |
| `RUST_LOG` | Log level filter | `info` |

## Pub/Sub Message Format

- **data**: `payload_binary` field (raw bytes, e.g. protobuf)
- **ordering_key**: `entity_id`
- **attributes**: `event_id`, `entity_id`, `event_name`, `occurred_at`

## Outbox Table Schema

```sql
CREATE TABLE transactional_box.outbox (
    event_id       UUID      NOT NULL PRIMARY KEY,
    entity_id      TEXT      NOT NULL,
    occurred_at    TIMESTAMP NOT NULL,
    event_name     TEXT      NOT NULL,
    payload        JSONB     NOT NULL,
    payload_binary BYTEA     NOT NULL DEFAULT ''::bytea
);
```

## Production Deployment Notes

### Publication Configuration

Create the publication with `INSERT` only to avoid unnecessary WAL transfer for UPDATE/DELETE operations:

```sql
CREATE PUBLICATION cdc_outbox_pub FOR TABLE transactional_box.outbox
    WITH (publish = 'insert');
```

If the publication was already created without this option, recreate it:

```sql
DROP PUBLICATION cdc_outbox_pub;
CREATE PUBLICATION cdc_outbox_pub FOR TABLE transactional_box.outbox
    WITH (publish = 'insert');
```

### Replication Slot Monitoring

**Replication slots retain WAL segments.** If cdc-rs stops consuming, WAL files accumulate and can fill the disk, potentially bringing down the entire database.

Monitor the slot lag:

```sql
SELECT
    slot_name,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag
FROM pg_replication_slots
WHERE slot_name = 'cdc_outbox_slot';
```

Set up alerts when lag exceeds a threshold (e.g. 1 GB). cdc-rs logs the slot lag every 60 seconds in its periodic stats output.

If cdc-rs is permanently decommissioned, **drop the replication slot** to stop WAL retention:

```sql
SELECT pg_drop_replication_slot('cdc_outbox_slot');
```

## Health Check

cdc-rs exposes an HTTP health check endpoint at `GET /` on the configured port (default: 8080). Returns `200 OK` when the process is running.

Kubernetes probe configuration:

```yaml
livenessProbe:
  httpGet:
    path: /
    port: 8080
  initialDelaySeconds: 5
  periodSeconds: 10
readinessProbe:
  httpGet:
    path: /
    port: 8080
  initialDelaySeconds: 5
  periodSeconds: 10
```
