# cdc-rs Implementation Plan

## Context

現在の outbox publisher は SELECT/UPDATE によるポーリング方式で、パフォーマンスの限界に達している。
PostgreSQL の論理レプリケーション (CDC) を利用して、outbox テーブルへの INSERT を WAL から直接読み取り、Cloud Pub/Sub に送信するプロセスを構築する。

- DB: AlloyDB (PostgreSQL v15 互換)
- wal2json は AlloyDB で利用不可 → **test_decoding** (組み込み) を使用
- 参考実装: `/Users/id/src/github.com/poi2/iggy_poc/apps/producer/src/main.rs`

---

## Architecture

```
AlloyDB (WAL)
  → Logical Replication Slot (test_decoding)
  → cdc-rs (poll → decode → filter INSERT only → publish)
  → Cloud Pub/Sub (outbox テーブルごとに異なる topic)
```

### Key Decisions

| 決定事項 | 選択 | 理由 |
|---------|------|------|
| Decoding plugin | **test_decoding** | AlloyDB で wal2json 利用不可。test_decoding は組み込みで確実に動作。iggy_poc の脆弱なパーサーではなく、状態機械ベースの堅牢なパーサーを実装 |
| Pub/Sub crate | **google-cloud-pubsub** | Rust で最も成熟した Pub/Sub クライアント。ordering_key, バッチ対応 |
| Ordering key | **entity_id** | エンティティ単位の順序保証 |
| Topic 構成 | **outbox テーブルごとに異なる topic** | TOML 設定ファイルで table → topic マッピングを定義 |
| Logging | **tracing** | 構造化ログ、レベル制御、本番運用向き |
| Slot advance 戦略 | **Pub/Sub 確認後に advance** | peek → publish → confirm → get (advance) で at-least-once を保証 |

---

## test_decoding パーサー設計

iggy_poc では `split_whitespace()` による素朴なパースだったが、jsonb/bytea/text のカラム値にスペースが含まれると破綻する。堅牢な状態機械パーサーを実装する。

### test_decoding 出力形式

```
table transactional_box.outbox: INSERT: event_id[uuid]:'550e8400-...' entity_id[text]:'order-123' occurred_at[timestamp without time zone]:'2024-01-15 10:30:00' event_name[text]:'OrderCreated' payload[jsonb]:'{"key": "value with spaces"}' payload_binary[bytea]:'\x'
```

### パースの課題と対策

| 課題 | 対策 |
|-----|------|
| jsonb/text 値にスペース含む | quoted value (`'...'`) を状態機械で正しく追跡 |
| 型名にスペース含む (e.g., `timestamp without time zone`) | `name[type]:` パターンの `[...]` 内をまず抽出 |
| シングルクォート内のエスケープ (`''`) | 連続 `''` をエスケープとして扱う |
| boolean/integer は非クォート | `:`の後が `'` で始まらない場合は次のスペースまでが値 |

---

## Configuration

TOML 設定ファイルで outbox テーブル → topic マッピングを定義。

```toml
# cdc-rs.toml

[postgres]
database_url = "postgresql://..."
slot_name = "cdc_outbox_slot"

[pubsub]
gcp_project_id = "my-project"

# outbox テーブル → topic マッピング
[[outbox]]
table = "transactional_box.outbox"
topic = "projects/my-project/topics/outbox-events"
publication_name = "cdc_outbox_pub"

[polling]
interval_ms = 100
max_changes_per_poll = 1000
```

---

## Project Structure

```
cdc-rs/
  Cargo.toml
  src/
    main.rs              # エントリーポイント、signal handling、run loop
    config.rs            # TOML 設定ファイル読み込み + CLI args
    postgres/
      mod.rs
      slot.rs            # Replication slot & publication の作成/確認
      wal_reader.rs      # ポーリングループ: peek → publish → advance
    decoder/
      mod.rs             # WalDecoder trait
      test_decoding.rs   # test_decoding パーサー (状態機械ベース)
    outbox.rs            # OutboxEvent struct、INSERT フィルタリング
    pubsub/
      mod.rs
      publisher.rs       # Cloud Pub/Sub publish (ordering key, retry)
      message.rs         # OutboxEvent → PubsubMessage 変換
    error.rs             # thiserror による型付きエラー
    shutdown.rs          # CancellationToken による graceful shutdown
  tests/
    decoder_test.rs      # test_decoding パーサーのユニットテスト
  docker-compose.yml     # PostgreSQL 15 (ローカル開発用)
  docs/
    ideation.md          # (既存)
```

---

## Dependencies

```toml
[dependencies]
tokio = { version = "1", features = ["full"] }
tokio-postgres = { version = "0.7", features = ["with-uuid-1", "with-serde_json-1"] }
google-cloud-pubsub = "0.30"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
uuid = { version = "1", features = ["v4", "serde"] }
chrono = { version = "0.4", features = ["serde"] }
clap = { version = "4", features = ["derive", "env"] }
toml = "0.8"
thiserror = "2"
anyhow = "1"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "json"] }
tokio-util = "0.7"
dotenvy = "0.15"
```

---

## Pub/Sub Message Structure

- **data**: outbox の `payload` フィールド (raw JSON bytes)
- **ordering_key**: `entity_id` の値
- **attributes**:
  - `event_id`, `entity_id`, `event_name`, `occurred_at`

---

## Core Flow: at-least-once guarantee

```
loop {
  1. pg_logical_slot_peek_changes() で WAL エントリ取得 (slot は進めない)
  2. test_decoding 出力をデコード → INSERT のみフィルタ → OutboxEvent に変換
  3. OutboxEvent → PubsubMessage に変換し publish, await confirmation
  4. 全 publish 確認後、pg_logical_slot_get_changes() で slot を advance
  5. エラー時は slot を advance せず、exponential backoff で retry
}
```

複数 outbox テーブルが設定されている場合、テーブルごとに独立した slot + polling loop を並行実行 (tokio::spawn)。

---

## Implementation Phases

### Phase 1: Project scaffolding + PostgreSQL connectivity
- `Cargo.toml` 依存関係のセットアップ
- `config.rs`: TOML 設定ファイル + CLI args (設定ファイルパスの指定)
- `postgres/slot.rs`: 接続、replication slot 作成 (test_decoding)、publication 作成
- `main.rs`: 接続して slot セットアップするだけの最小実装
- `docker-compose.yml`: PostgreSQL 15 (wal_level=logical)

### Phase 2: WAL reading + decoding
- `decoder/mod.rs`: `WalDecoder` trait 定義
- `decoder/test_decoding.rs`: 状態機械ベースの test_decoding パーサー
- `outbox.rs`: `OutboxEvent` struct、INSERT フィルタ
- `postgres/wal_reader.rs`: peek/get ポーリングループ
- ユニットテスト: パーサー (全カラム型: uuid, text, jsonb, bytea, boolean, integer, timestamp)

### Phase 3: Cloud Pub/Sub integration
- `pubsub/message.rs`: OutboxEvent → PubsubMessage 変換
- `pubsub/publisher.rs`: Pub/Sub クライアント初期化、publish + await confirmation
- フルパイプライン接続: peek → decode → filter → publish → advance
- テーブル → トピックのルーティング

### Phase 4: Reliability + operations
- `shutdown.rs`: SIGTERM/SIGINT で graceful shutdown (CancellationToken)
- Pub/Sub publish の exponential backoff retry
- PostgreSQL 再接続ロジック
- `tracing` による構造化ログ

### Phase 5: Integration testing + docs
- docker-compose で PostgreSQL の統合テスト
- INSERT/UPDATE フィルタリングの E2E テスト
- README.md 作成

---

## Key SQL Queries

```sql
-- Replication slot 作成
SELECT pg_create_logical_replication_slot('cdc_outbox_slot', 'test_decoding');

-- Publication 作成
CREATE PUBLICATION cdc_outbox_pub FOR TABLE transactional_box.outbox;

-- WAL エントリの読み取り (slot を進めない)
SELECT lsn::text, xid::text, data
FROM pg_logical_slot_peek_changes('cdc_outbox_slot', NULL, 1000);

-- slot の advance (publish 確認後)
SELECT lsn::text, xid::text, data
FROM pg_logical_slot_get_changes('cdc_outbox_slot', NULL, 1000);
```

---

## Verification

1. `docker compose up` で PostgreSQL 15 を起動
2. `cargo build` でビルド確認
3. `cargo test` でユニットテスト実行 (特に test_decoding パーサー)
4. outbox テーブルに INSERT し、ログで WAL デコード結果を確認
5. outbox テーブルに UPDATE し、フィルタされることを確認
6. Pub/Sub emulator (`gcloud beta emulators pubsub start`) で E2E 確認

---

## Reference Files

- `/Users/id/src/github.com/poi2/iggy_poc/apps/producer/src/main.rs` — WAL ポーリング、slot 管理の参考実装 (パーサーは流用せず、アーキテクチャのみ参考)
