# CDC-RS Performance Benchmark

PostgreSQL の論理レプリケーション (WAL) から outbox イベントを読み取り、デコードし、Cloud Pub/Sub へ publish するまでのスループットを計測した。

## Environment

| Item | Spec |
|------|------|
| Machine | Apple Silicon (macOS) |
| PostgreSQL | 15 (Docker container, `postgres:15`) |
| Pub/Sub | Emulator (`google-cloud-cli:emulators`, Docker) |
| WAL plugin | `test_decoding` (built-in) |
| Rust | edition 2024, release build (`--release`) |
| Payload size | Variable (100 / 300 / 1,000 bytes, `payload_binary` as raw bytes) |

PostgreSQL / Pub/Sub Emulator ともに Docker コンテナ内で実行。ベンチマークプロセスはホスト側から TCP (localhost) で接続。

## Methodology

1. outbox テーブルに N 行を一括 INSERT (`generate_series`)
2. 各アプローチで全行を処理する時間を計測
3. 3 カテゴリを計測:
   - **WAL read + decode のみ** (Polling / Streaming)
   - **Full pipeline** (WAL read + decode + Pub/Sub emulator publish)

### Approaches

| Approach | Description |
|----------|-------------|
| **Polling (batch=1,000)** | `pg_logical_slot_get_changes()` を batch=1,000 でループ呼び出し。現在の本番パターンに近い。 |
| **Polling (batch=10,000)** | 同上、batch=10,000。SQL 呼び出し回数を削減。 |
| **Streaming (`pg_recvlogical`)** | PostgreSQL 組み込みの `pg_recvlogical` で streaming replication protocol を使用。SQL レイヤーを介さず、WAL データが TCP ストリームとして push される。 |
| **Pipeline + Pub/Sub emulator** | Polling (batch=1,000) で WAL を読み取り、デコード後に Pub/Sub emulator へ publish するフルパイプライン。 |

## Results

### Payload size comparison (100K rows)

| Approach | 100 bytes | 300 bytes | 1,000 bytes |
|----------|-----------|-----------|-------------|
| Polling (batch=1,000) | 154,080 msgs/sec | 91,993 msgs/sec | 39,706 msgs/sec |
| Polling (batch=10,000) | 168,222 msgs/sec | 103,223 msgs/sec | 40,948 msgs/sec |
| Streaming (pg_recvlogical) | 205,805 msgs/sec | 142,042 msgs/sec | 72,960 msgs/sec |
| Pipeline + Pub/Sub emulator | 4,544 msgs/sec | 4,346 msgs/sec | 4,050 msgs/sec |

#### Payload size vs throughput (WAL read + decode)

```
msgs/sec
250K ┤
     │  ■ 100B
200K ┤  ■ Streaming
     │
150K ┤  ■ Polling     ● 300B
     │                ● Streaming
100K ┤                ● Polling     ▲ 1,000B
     │                              ▲ Streaming
 50K ┤                              ▲ Polling
     │
   0 ┤  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        100B          300B          1,000B
```

- 100B → 300B: **約 40% 低下** (Polling 154K → 92K)
- 300B → 1,000B: **約 57% 低下** (Polling 92K → 40K)
- **ペイロードサイズに対してほぼ線形にスループットが低下** (データ転送量が支配的)
- Pipeline (Pub/Sub emulator) は **サイズによらずほぼ一定** (~4,000-4,500 msgs/sec) — RPC レイテンシが支配的でペイロードサイズは無関係

### 1B messages estimated (300 bytes payload)

| Approach | Estimated Time |
|----------|---------------|
| Polling (WAL read + decode only) | **3.0 hours** |
| Streaming (WAL read + decode only) | **2.0 hours** |
| Pipeline + Pub/Sub emulator | **63.9 hours** |

## Analysis

### Payload size impact

- **WAL read + decode はペイロードサイズに対してほぼ線形に低下する**
  - 100B → 1,000B (10x) で Polling は 154K → 40K (3.9x 低下)、Streaming は 206K → 73K (2.8x 低下)
  - ボトルネックは WAL データの転送量 + test_decoding のテキストエンコーディング (bytea は hex エンコードで 2x に膨張)
- **Pipeline (Pub/Sub emulator) はペイロードサイズに対してほぼ不変** (4,544 → 4,050、11% 低下)
  - RPC round-trip レイテンシが支配的で、メッセージサイズの影響は微小
- **本番環境** (300B 相当): WAL read + decode が ~100K msgs/sec、Pub/Sub publish が十分高速であれば WAL read がボトルネック

### WAL read + decode (Polling vs Streaming)

- batch size を 1,000 → 10,000 に増やすと **~12% 改善** (300B: 92K → 103K)
- Streaming は Polling より **約 1.4x 高速** (300B: 142K vs 103K msgs/sec)
  - SQL parse/execute オーバーヘッドがゼロ
  - サーバー側からデータが push される
  - TCP ストリームでバッファリングが効く
  - ただし benchmark は Docker コンテナ内 (localhost) で実行しており、ネットワーク条件はやや有利

### Full pipeline (Pub/Sub emulator 込み)

- Pub/Sub emulator を含むフルパイプラインでは **~4,300 msgs/sec** (300B) — WAL read only の **約 1/24**
- **Pub/Sub publish が圧倒的なボトルネック**
- publish が全体の **96%** を占める

### Pub/Sub emulator vs 本番 Cloud Pub/Sub

Pub/Sub emulator は本番の Cloud Pub/Sub と特性が大きく異なる:

| | Emulator | Production Cloud Pub/Sub |
|--|---------|------------------------|
| 実装 | 単一プロセス Java app | 分散インフラ |
| スループット | ~4,700 msgs/sec | 数十万 msgs/sec (リージョン内) |
| レイテンシ | Docker 内 gRPC | ネットワーク RTT 依存 |
| バッチ最適化 | 限定的 | サーバーサイド batching |

**emulator のスループットは本番の性能指標としては使えない。** WAL read + decode のスループットが本番パイプラインの上限を決める可能性が高い。

### Throughput bottleneck breakdown (300B payload)

```
[WAL read + decode]      ~100K msgs/sec    ← CPU + network + data size
          ↓
[Pub/Sub publish]        ~4.3K msgs/sec    ← emulator 限界 (本番では改善)
          ↓
[Total pipeline]         ~4.3K msgs/sec    ← publish がボトルネック
```

本番では Pub/Sub publish が改善されるため、WAL read + decode (~100K msgs/sec) がスループットの上限になると予測される。

## Streaming implementation options

現在の Rust 実装は polling 方式。Streaming に切り替える場合の選択肢:

| Option | Plugin | Pros | Cons |
|--------|--------|------|------|
| `pgwire-replication` crate | `pgoutput` | Pure Rust, async/await | `test_decoding` 非対応。`pgoutput` (binary format) 用のデコーダーが必要 |
| Raw replication protocol | `test_decoding` | 既存デコーダー流用可 | PostgreSQL wire protocol の自前実装が必要 |
| `pg_recvlogical` subprocess | `test_decoding` | 実装ゼロ、組み込みツール | subprocess + pipe のオーバーヘッド、エラーハンドリングが複雑 |

### Recommendation

**短期**: 現在の polling 方式で十分。115K msgs/sec は 1 プロセスで 1B messages を約 2.4 時間で処理できる。Pub/Sub publish が本番で十分高速であれば、WAL read がボトルネックとなる。

**中長期**: スループットが不足する場合は `pgoutput` + streaming への移行を検討。`pgoutput` はバイナリフォーマットのため、test_decoding のテキスト変換オーバーヘッドもなくなり、1.3x 以上の改善が見込める。

## Reproduce

```bash
# Start services (PostgreSQL + Pub/Sub emulator)
docker compose up -d

# Run benchmark (default: 100K rows, 300B payload)
cargo run --example benchmark --release

# Run with custom row count
cargo run --example benchmark --release -- 1000000

# Run with custom payload size (bytes)
cargo run --example benchmark --release -- 100000 100
cargo run --example benchmark --release -- 100000 300
cargo run --example benchmark --release -- 100000 1000
```
