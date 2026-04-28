use std::sync::Arc;

use rustls::ClientConfig;
use tokio_postgres::NoTls;
use tokio_postgres_rustls::MakeRustlsConnect;
use tracing::info;

pub use tokio_postgres::Client;

#[derive(Debug, thiserror::Error)]
pub enum PgCommonError {
    #[error(transparent)]
    Postgres(#[from] tokio_postgres::Error),
    #[error("invalid identifier `{name}`: {reason}")]
    InvalidIdentifier { name: String, reason: String },
}

impl PgCommonError {
    pub fn is_connection_error(&self) -> bool {
        matches!(self, PgCommonError::Postgres(e) if e.is_closed())
    }
}

pub async fn connect(database_url: &str) -> Result<Client, PgCommonError> {
    let sslmode = extract_sslmode(database_url);

    let client = match sslmode {
        SslMode::Disable => {
            let (client, connection) = tokio_postgres::connect(database_url, NoTls).await?;
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    tracing::error!("Database connection error: {}", e);
                }
            });
            client
        }
        SslMode::Require => {
            let tls_config = ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(NoVerifier))
                .with_no_client_auth();
            let tls = MakeRustlsConnect::new(tls_config);
            let (client, connection) = tokio_postgres::connect(database_url, tls).await?;
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    tracing::error!("Database connection error: {}", e);
                }
            });
            client
        }
        SslMode::VerifyCa | SslMode::VerifyFull => {
            let root_store =
                rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            let tls_config = ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_no_client_auth();
            let tls = MakeRustlsConnect::new(tls_config);
            let (client, connection) = tokio_postgres::connect(database_url, tls).await?;
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    tracing::error!("Database connection error: {}", e);
                }
            });
            client
        }
    };

    info!(sslmode = %sslmode, "Connected to PostgreSQL");
    Ok(client)
}

pub async fn get_slot_lag_bytes(client: &Client, slot_name: &str) -> Result<i64, PgCommonError> {
    validate_identifier(slot_name, "slot_name")?;
    let row = client
        .query_one(
            "SELECT coalesce(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0)::bigint \
             FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await?;
    Ok(row.get(0))
}

pub async fn setup_replication(
    client: &Client,
    slot_name: &str,
    publication_name: &str,
    outbox_table: &str,
    output_plugin: &str,
) -> Result<(), PgCommonError> {
    validate_identifier(slot_name, "slot_name")?;
    validate_identifier(publication_name, "publication_name")?;
    validate_identifier(outbox_table, "outbox_table")?;
    validate_identifier(output_plugin, "output_plugin")?;

    let existing_slots = client
        .query(
            "SELECT slot_name FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await?;

    if existing_slots.is_empty() {
        client
            .query(
                "SELECT pg_create_logical_replication_slot($1, $2)",
                &[&slot_name, &output_plugin],
            )
            .await?;
        info!(slot = %slot_name, "Replication slot created");
    } else {
        info!(slot = %slot_name, "Replication slot already exists");
    }

    let pub_exists = client
        .query(
            "SELECT pubname FROM pg_publication WHERE pubname = $1",
            &[&publication_name],
        )
        .await?;

    if pub_exists.is_empty() {
        let pub_query = format!(
            "CREATE PUBLICATION {publication_name} FOR TABLE {outbox_table}"
        );
        client.simple_query(&pub_query).await?;
        info!(
            publication = %publication_name,
            table = %outbox_table,
            "Publication created"
        );
    } else {
        info!(publication = %publication_name, "Publication already exists");
    }

    Ok(())
}

fn validate_identifier(value: &str, name: &str) -> Result<(), PgCommonError> {
    if value.is_empty() {
        return Err(PgCommonError::InvalidIdentifier {
            name: name.to_string(),
            reason: "must not be empty".to_string(),
        });
    }
    if !value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
    {
        return Err(PgCommonError::InvalidIdentifier {
            name: name.to_string(),
            reason: format!("contains invalid characters: {value:?} (only [a-zA-Z0-9_.] allowed)"),
        });
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum SslMode {
    Disable,
    Require,
    VerifyCa,
    VerifyFull,
}

impl std::fmt::Display for SslMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SslMode::Disable => write!(f, "disable"),
            SslMode::Require => write!(f, "require"),
            SslMode::VerifyCa => write!(f, "verify-ca"),
            SslMode::VerifyFull => write!(f, "verify-full"),
        }
    }
}

fn extract_sslmode(url: &str) -> SslMode {
    let url_lower = url.to_lowercase();
    if let Some(pos) = url_lower.find("sslmode=") {
        let value_start = pos + "sslmode=".len();
        let value = &url_lower[value_start..];
        let value = value.split('&').next().unwrap_or(value);
        match value {
            "require" => SslMode::Require,
            "verify-ca" => SslMode::VerifyCa,
            "verify-full" | "verify_full" => SslMode::VerifyFull,
            _ => SslMode::Disable,
        }
    } else {
        SslMode::Disable
    }
}

#[derive(Debug)]
struct NoVerifier;

impl rustls::client::danger::ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_sslmode_disable() {
        assert!(matches!(
            extract_sslmode("postgresql://u:p@host/db?sslmode=disable"),
            SslMode::Disable
        ));
    }

    #[test]
    fn test_extract_sslmode_require() {
        assert!(matches!(
            extract_sslmode("postgresql://u:p@host/db?sslmode=require"),
            SslMode::Require
        ));
    }

    #[test]
    fn test_extract_sslmode_verify_ca() {
        assert!(matches!(
            extract_sslmode("postgresql://u:p@host/db?sslmode=verify-ca"),
            SslMode::VerifyCa
        ));
    }

    #[test]
    fn test_extract_sslmode_verify_full() {
        assert!(matches!(
            extract_sslmode("postgresql://u:p@host/db?sslmode=verify-full"),
            SslMode::VerifyFull
        ));
    }

    #[test]
    fn test_extract_sslmode_missing() {
        assert!(matches!(
            extract_sslmode("postgresql://u:p@host/db"),
            SslMode::Disable
        ));
    }

    #[test]
    fn test_extract_sslmode_with_other_params() {
        assert!(matches!(
            extract_sslmode("postgresql://u:p@host/db?timeout=30&sslmode=require&app=cdc"),
            SslMode::Require
        ));
    }

    #[test]
    fn test_extract_sslmode_case_insensitive() {
        assert!(matches!(
            extract_sslmode("postgresql://u:p@host/db?sslmode=REQUIRE"),
            SslMode::Require
        ));
    }

    #[test]
    fn test_validate_identifier_valid() {
        assert!(validate_identifier("my_slot", "slot").is_ok());
        assert!(validate_identifier("public.outbox_events", "table").is_ok());
        assert!(validate_identifier("testDecoding123", "plugin").is_ok());
    }

    #[test]
    fn test_validate_identifier_empty() {
        assert!(validate_identifier("", "slot").is_err());
    }

    #[test]
    fn test_validate_identifier_injection() {
        assert!(validate_identifier("slot'; DROP TABLE users;--", "slot").is_err());
        assert!(validate_identifier("slot name", "slot").is_err());
        assert!(validate_identifier("slot-name", "slot").is_err());
    }
}
