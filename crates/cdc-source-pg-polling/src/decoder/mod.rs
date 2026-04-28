mod test_decoding;
mod wal2json;

#[derive(Debug, Clone, Default)]
pub enum WalPlugin {
    #[default]
    TestDecoding,
    Wal2Json,
}

impl WalPlugin {
    pub fn pg_output_plugin(&self) -> &'static str {
        match self {
            WalPlugin::TestDecoding => "test_decoding",
            WalPlugin::Wal2Json => "wal2json",
        }
    }

    pub fn slot_options(&self) -> Vec<(&'static str, &'static str)> {
        match self {
            WalPlugin::TestDecoding => vec![],
            WalPlugin::Wal2Json => vec![("format-version", "2")],
        }
    }

    pub fn decode(&self, raw: &str, table: &str) -> Option<serde_json::Value> {
        match self {
            WalPlugin::TestDecoding => test_decoding::decode_to_json(raw, table),
            WalPlugin::Wal2Json => wal2json::decode_to_json(raw, table),
        }
    }
}
