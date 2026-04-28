use clap::Parser;

#[derive(Parser)]
#[command(author, version, about = "Stream PostgreSQL outbox changes to Cloud Pub/Sub via CDC")]
pub struct Config {
    #[arg(long, env = "DATABASE_URL")]
    pub database_url: String,

    #[arg(long, env = "OUTBOX_TABLE", default_value = "transactional_box.outbox")]
    pub outbox_table: String,

    #[arg(long, env = "SLOT_NAME", default_value = "cdc_outbox_slot")]
    pub slot_name: String,

    #[arg(long, env = "PUBLICATION_NAME", default_value = "cdc_outbox_pub")]
    pub publication_name: String,

    #[arg(long, env = "PUBSUB_TOPIC")]
    pub pubsub_topic: String,

    #[arg(long, env = "POLL_INTERVAL_MS", default_value = "100")]
    pub poll_interval_ms: u64,

    #[arg(long, env = "MAX_CHANGES_PER_POLL", default_value = "1000")]
    pub max_changes_per_poll: u32,

    #[arg(long, env = "HEALTH_CHECK_PORT", default_value = "8080")]
    pub health_check_port: u16,
}
