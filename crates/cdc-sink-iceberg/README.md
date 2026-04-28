# cdc-sink-iceberg

Apache Iceberg sink for the CDC pipeline. Writes `LsnEvent<E>` data to Iceberg tables using Parquet as the data file format.

## Design Decisions

### Data File Format: Parquet

Parquet is chosen over Avro/ORC for Iceberg data files because:

- Columnar format enables efficient analytical queries (e.g., `SELECT ... WHERE __lsn > X`)
- Better compression ratios for CDC workloads with many repeated column values
- Wide ecosystem support (Spark, DuckDB, Polars, etc.)

### Catalog: In-Memory (MemoryCatalog)

The initial implementation uses `iceberg::MemoryCatalog` backed by local filesystem storage.

- Suitable for development, testing, and single-process workloads
- Warehouse data is persisted to disk via `FileIO`; catalog metadata lives in memory
- **Future**: Migrate to REST Catalog for multi-process / production use

### Partitioning: None (Unpartitioned)

Tables are created without partition specs.

- Simplifies the initial implementation
- Sufficient for moderate data volumes
- **Future**: Add time-based or LSN-range partitioning as data volume grows

### Schema

Every table includes a `__lsn` column (`long` / `Int64`) as the first field, followed by the inner event fields. The `__lsn` column enables `ORDER BY __lsn` for ordering guarantees.
