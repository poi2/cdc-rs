# やりたいこと

outbox -> CDC -> Cloud Pub/Sub の CDC 部分を作りたい。

現在 outbox を select/update する形で publisher を作っているが perf の問題で限界である。
指定した tables の変更のみ取得する replication slot を作成し、そこから指定した Cloud Pub/Sub に Message を送るプロセスを作りたい。

/Users/id/src/github.com/poi2/iggy_poc
に以前 iggy poc をやったコードがある。
publisher の実装が参考になるはず。

この repo では PostgreSQL v15。
outbox の形は以下。

```sql
create table if not exists transactional_box.outbox
(
    event_id       uuid      not null
        primary key,
    entity_id      text      not null,
    occurred_at    timestamp not null,
    event_name     text      not null,
    payload        jsonb     not null,
    payload_binary bytea     not null
);
```

INSERT された順番で payload を Cloud Pub/Sub に送る。
UPDATE は無視する。
