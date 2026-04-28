pub mod protocol;

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use pgwire_replication::{Lsn, PgWireError, ReplicationClient, ReplicationConfig, ReplicationEvent};
use serde::de::DeserializeOwned;
use tracing::info;

use cdc_core::{LsnEvent, Source};
use cdc_source_pg_common as pg_common;
use protocol::{PgOutputMessage, ProtocolError, Relation, parse_pgoutput_message, tuple_to_json};

#[derive(Debug, thiserror::Error)]
pub enum PgStreamError {
    #[error(transparent)]
    Common(#[from] pg_common::PgCommonError),
    #[error("replication error")]
    Replication(#[source] PgWireError),
    #[error("replication stream ended")]
    StreamEnded,
    #[error("failed to deserialize streaming event")]
    Deserialization(#[source] serde_json::Error),
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
    #[error("invalid database URL: {0}")]
    InvalidUrl(String),
}

pub struct PgStreamSourceConfig {
    pub database_url: String,
    pub outbox_table: String,
    pub slot_name: String,
    pub publication_name: String,
}

pub struct PgStreamSource<E> {
    setup_client: pg_common::Client,
    repl_client: ReplicationClient,
    relations: HashMap<u32, Relation>,
    pending_events: Vec<LsnEvent<E>>,
    last_wal_end: Lsn,
    config: PgStreamSourceConfig,
}

impl<E: DeserializeOwned + Clone + Send + Sync + 'static> PgStreamSource<E> {
    pub async fn new(config: PgStreamSourceConfig) -> Result<Self, PgStreamError> {
        let setup_client = pg_common::connect(&config.database_url).await?;
        pg_common::setup_replication(
            &setup_client,
            &config.slot_name,
            &config.publication_name,
            &config.outbox_table,
            "pgoutput",
        )
        .await?;

        let repl_config = make_replication_config(&config)?;
        let repl_client = ReplicationClient::connect(repl_config)
            .await
            .map_err(PgStreamError::Replication)?;

        info!(
            slot = %config.slot_name,
            publication = %config.publication_name,
            "Streaming replication started"
        );

        Ok(Self {
            setup_client,
            repl_client,
            relations: HashMap::new(),
            pending_events: Vec::new(),
            last_wal_end: Lsn::ZERO,
            config,
        })
    }

    fn matches_table(&self, relation: &Relation) -> bool {
        if let Some((schema, table)) = self.config.outbox_table.split_once('.') {
            relation.namespace == schema && relation.name == table
        } else {
            relation.name == self.config.outbox_table
        }
    }

    async fn read_messages(&mut self) -> Result<(), PgStreamError> {
        loop {
            match tokio::time::timeout(Duration::from_millis(100), self.repl_client.recv()).await {
                Ok(Ok(Some(event))) => self.process_event(event)?,
                Ok(Ok(None)) => return Err(PgStreamError::StreamEnded),
                Ok(Err(e)) => return Err(PgStreamError::Replication(e)),
                Err(_) => break,
            }
        }
        Ok(())
    }

    fn process_event(&mut self, event: ReplicationEvent) -> Result<(), PgStreamError> {
        match event {
            ReplicationEvent::XLogData { data, wal_start, wal_end, .. } => {
                self.last_wal_end = self.last_wal_end.max(wal_end);
                self.process_pgoutput(&data, wal_start.as_u64())?;
            }
            ReplicationEvent::KeepAlive { wal_end, .. } => {
                self.last_wal_end = self.last_wal_end.max(wal_end);
            }
            ReplicationEvent::Begin { .. }
            | ReplicationEvent::Commit { .. }
            | ReplicationEvent::StoppedAt { .. }
            | ReplicationEvent::Message { .. } => {}
        }
        Ok(())
    }

    fn process_pgoutput(&mut self, data: &[u8], lsn: u64) -> Result<(), PgStreamError> {
        let Some(msg) = parse_pgoutput_message(data)? else {
            return Ok(());
        };

        match msg {
            PgOutputMessage::Relation(rel) => {
                self.relations.insert(rel.id, rel);
            }
            PgOutputMessage::Insert(ins) => {
                if let Some(rel) = self.relations.get(&ins.relation_id) {
                    if self.matches_table(rel) {
                        let json = tuple_to_json(rel, &ins.tuple);
                        match serde_json::from_value::<E>(json) {
                            Ok(event) => self.pending_events.push(LsnEvent { lsn, event }),
                            Err(e) => {
                                return Err(PgStreamError::Deserialization(e));
                            }
                        }
                    }
                }
            }
            PgOutputMessage::Begin(_) | PgOutputMessage::Commit(_) => {}
        }

        Ok(())
    }
}

#[async_trait]
impl<E: DeserializeOwned + Clone + Send + Sync + 'static> Source for PgStreamSource<E> {
    type Event = LsnEvent<E>;
    type Error = PgStreamError;

    async fn peek(&mut self) -> Result<Vec<LsnEvent<E>>, PgStreamError> {
        if !self.pending_events.is_empty() {
            return Ok(self.pending_events.clone());
        }

        self.read_messages().await?;
        Ok(self.pending_events.clone())
    }

    async fn advance(&mut self) -> Result<(), PgStreamError> {
        self.repl_client.update_applied_lsn(self.last_wal_end);
        self.pending_events.clear();
        Ok(())
    }

    async fn reconnect(&mut self) -> Result<(), PgStreamError> {
        self.setup_client = pg_common::connect(&self.config.database_url).await?;

        let mut repl_config = make_replication_config(&self.config)?;
        repl_config.start_lsn = self.last_wal_end;

        self.repl_client = ReplicationClient::connect(repl_config)
            .await
            .map_err(PgStreamError::Replication)?;

        self.relations.clear();
        self.pending_events.clear();

        info!("Streaming replication reconnected");
        Ok(())
    }

    fn is_retriable_error(&self, err: &PgStreamError) -> bool {
        match err {
            PgStreamError::Replication(e) => e.is_transient(),
            PgStreamError::Common(e) => e.is_connection_error(),
            _ => false,
        }
    }
}

fn make_replication_config(config: &PgStreamSourceConfig) -> Result<ReplicationConfig, PgStreamError> {
    let (host, port, user, password, database) = parse_database_url(&config.database_url)?;

    Ok(ReplicationConfig {
        host,
        port,
        user,
        password,
        database,
        slot: config.slot_name.clone(),
        publication: config.publication_name.clone(),
        start_lsn: Lsn::ZERO,
        ..Default::default()
    })
}

fn parse_database_url(url: &str) -> Result<(String, u16, String, String, String), PgStreamError> {
    let rest = url
        .strip_prefix("postgresql://")
        .or_else(|| url.strip_prefix("postgres://"))
        .ok_or_else(|| {
            PgStreamError::InvalidUrl(
                "must start with postgresql:// or postgres://".to_string(),
            )
        })?;

    let (userinfo, hostpath) = rest
        .split_once('@')
        .ok_or_else(|| PgStreamError::InvalidUrl("missing @".to_string()))?;

    let (user, password) = match userinfo.split_once(':') {
        Some((u, p)) => (u.to_string(), p.to_string()),
        None => (userinfo.to_string(), String::new()),
    };

    let hostpath = hostpath.split('?').next().unwrap_or(hostpath);

    let (hostport, database) = hostpath.split_once('/').unwrap_or((hostpath, "postgres"));

    let (host, port) = match hostport.split_once(':') {
        Some((h, p)) => (h.to_string(), p.parse::<u16>().unwrap_or(5432)),
        None => (hostport.to_string(), 5432),
    };

    Ok((host, port, user, password, database.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_database_url_full() {
        let (host, port, user, password, database) =
            parse_database_url("postgresql://myuser:mypass@db.example.com:5433/mydb").unwrap();
        assert_eq!(host, "db.example.com");
        assert_eq!(port, 5433);
        assert_eq!(user, "myuser");
        assert_eq!(password, "mypass");
        assert_eq!(database, "mydb");
    }

    #[test]
    fn test_parse_database_url_default_port() {
        let (host, port, _, _, _) =
            parse_database_url("postgresql://u:p@localhost/db").unwrap();
        assert_eq!(host, "localhost");
        assert_eq!(port, 5432);
    }

    #[test]
    fn test_parse_database_url_postgres_scheme() {
        let (_, _, user, _, database) =
            parse_database_url("postgres://admin:secret@host/testdb").unwrap();
        assert_eq!(user, "admin");
        assert_eq!(database, "testdb");
    }

    #[test]
    fn test_parse_database_url_with_params() {
        let (host, port, _, _, database) =
            parse_database_url("postgresql://u:p@myhost:5432/mydb?sslmode=disable&timeout=30")
                .unwrap();
        assert_eq!(host, "myhost");
        assert_eq!(port, 5432);
        assert_eq!(database, "mydb");
    }

    #[test]
    fn test_parse_database_url_no_password() {
        let (_, _, user, password, _) =
            parse_database_url("postgresql://onlyuser@localhost/db").unwrap();
        assert_eq!(user, "onlyuser");
        assert_eq!(password, "");
    }

    #[test]
    fn test_parse_database_url_invalid_scheme() {
        assert!(parse_database_url("mysql://u:p@host/db").is_err());
    }

    #[test]
    fn test_parse_database_url_missing_at() {
        assert!(parse_database_url("postgresql://localhost/db").is_err());
    }

    #[test]
    fn test_matches_table_with_schema() {
        let config = PgStreamSourceConfig {
            database_url: String::new(),
            outbox_table: "public.outbox_events".to_string(),
            slot_name: String::new(),
            publication_name: String::new(),
        };
        let rel = Relation {
            id: 1,
            namespace: "public".to_string(),
            name: "outbox_events".to_string(),
            replica_identity: b'd',
            columns: vec![],
        };
        assert!(matches_table_helper(&config, &rel));
    }

    #[test]
    fn test_matches_table_without_schema() {
        let config = PgStreamSourceConfig {
            database_url: String::new(),
            outbox_table: "outbox_events".to_string(),
            slot_name: String::new(),
            publication_name: String::new(),
        };
        let rel = Relation {
            id: 1,
            namespace: "public".to_string(),
            name: "outbox_events".to_string(),
            replica_identity: b'd',
            columns: vec![],
        };
        assert!(matches_table_helper(&config, &rel));
    }

    #[test]
    fn test_matches_table_different_table() {
        let config = PgStreamSourceConfig {
            database_url: String::new(),
            outbox_table: "outbox_events".to_string(),
            slot_name: String::new(),
            publication_name: String::new(),
        };
        let rel = Relation {
            id: 1,
            namespace: "public".to_string(),
            name: "other_table".to_string(),
            replica_identity: b'd',
            columns: vec![],
        };
        assert!(!matches_table_helper(&config, &rel));
    }

    fn matches_table_helper(config: &PgStreamSourceConfig, relation: &Relation) -> bool {
        if let Some((schema, table)) = config.outbox_table.split_once('.') {
            relation.namespace == schema && relation.name == table
        } else {
            relation.name == config.outbox_table
        }
    }
}
