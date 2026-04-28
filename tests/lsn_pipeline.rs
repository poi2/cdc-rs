use std::io::Write;

use serde::{Deserialize, Serialize};

use cdc_core::{CdcPipeline, LsnEvent, PipelineConfig, Source};
use cdc_sink_file::FileSink;
use cdc_source_file::{FileSource, FileSourceConfig};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TestEvent {
    id: u32,
    name: String,
}

fn write_temp_file(content: &str) -> tempfile::NamedTempFile {
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(content.as_bytes()).unwrap();
    f.flush().unwrap();
    f
}

#[tokio::test]
async fn test_lsn_event_flows_through_file_source_to_file_sink() {
    let input = [
        r#"{"__lsn":100,"id":1,"name":"first"}"#,
        r#"{"__lsn":200,"id":2,"name":"second"}"#,
        r#"{"__lsn":300,"id":3,"name":"third"}"#,
    ]
    .join("\n")
        + "\n";

    let input_file = write_temp_file(&input);
    let output_dir = tempfile::tempdir().unwrap();
    let output_path = output_dir.path().join("output.jsonl");

    let config = FileSourceConfig {
        path: input_file.path().to_path_buf(),
        batch_size: 100,
    };
    let mut source = FileSource::<LsnEvent<TestEvent>>::new(config).unwrap();
    let sink = FileSink::<LsnEvent<TestEvent>>::new(&output_path).unwrap();

    let events = source.peek().await.unwrap();
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].lsn, 100);
    assert_eq!(events[1].lsn, 200);
    assert_eq!(events[2].lsn, 300);
    assert_eq!(events[0].event.name, "first");

    use cdc_core::Sink;
    sink.publish(&events).await.unwrap();

    let output = std::fs::read_to_string(&output_path).unwrap();
    let lines: Vec<&str> = output.lines().collect();
    assert_eq!(lines.len(), 3);

    for (i, line) in lines.iter().enumerate() {
        let parsed: serde_json::Value = serde_json::from_str(line).unwrap();
        assert_eq!(parsed["__lsn"], (i as u64 + 1) * 100);
        assert_eq!(parsed["id"], i as u64 + 1);
        assert!(parsed.get("lsn").is_none(), "__lsn should be used, not lsn");
        assert!(parsed.get("event").is_none(), "event should be flattened");
    }
}

#[tokio::test]
async fn test_lsn_event_pipeline_with_identity_transform() {
    let input = [
        r#"{"__lsn":1000,"id":1,"name":"event1"}"#,
        r#"{"__lsn":2000,"id":2,"name":"event2"}"#,
    ]
    .join("\n")
        + "\n";

    let input_file = write_temp_file(&input);
    let output_dir = tempfile::tempdir().unwrap();
    let output_path = output_dir.path().join("pipeline_output.jsonl");

    let source_config = FileSourceConfig {
        path: input_file.path().to_path_buf(),
        batch_size: 100,
    };
    let source = FileSource::<LsnEvent<TestEvent>>::new(source_config).unwrap();
    let sink = FileSink::<LsnEvent<TestEvent>>::new(&output_path).unwrap();

    let cancel = tokio_util::sync::CancellationToken::new();
    let cancel_clone = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        cancel_clone.cancel();
    });

    let pipeline_config = PipelineConfig {
        poll_interval: std::time::Duration::from_millis(10),
        ..Default::default()
    };

    CdcPipeline::new(source, sink, pipeline_config)
        .run(cancel)
        .await
        .unwrap();

    let output = std::fs::read_to_string(&output_path).unwrap();
    let lines: Vec<&str> = output.lines().collect();
    assert_eq!(lines.len(), 2);

    let first: LsnEvent<TestEvent> = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(first.lsn, 1000);
    assert_eq!(first.event.name, "event1");

    let second: LsnEvent<TestEvent> = serde_json::from_str(lines[1]).unwrap();
    assert_eq!(second.lsn, 2000);
    assert_eq!(second.event.name, "event2");
}
