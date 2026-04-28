use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_json::ReaderBuilder;
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use async_trait::async_trait;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{DataFileFormat, NestedField, PrimitiveType, Schema as IcebergSchema, Type};
use iceberg::transaction::Transaction;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::file::properties::WriterProperties;
use serde::Serialize;
use tokio::sync::Mutex;
use tracing::info;
use uuid::Uuid;

use cdc_core::{LsnEvent, Sink};

#[derive(Debug, thiserror::Error)]
pub enum IcebergSinkError {
    #[error("iceberg error: {0}")]
    Iceberg(#[from] iceberg::Error),
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
    #[error("json serialization error: {0}")]
    Json(#[from] serde_json::Error),
}

pub struct IcebergSinkConfig {
    pub warehouse_path: String,
    pub namespace: String,
    pub table_name: String,
}

pub struct IcebergSink<E, C: Catalog> {
    catalog: Arc<C>,
    namespace: NamespaceIdent,
    table_name: String,
    arrow_schema: Arc<ArrowSchema>,
    iceberg_schema: Arc<IcebergSchema>,
    table: Mutex<iceberg::table::Table>,
    _marker: PhantomData<E>,
}

pub struct IcebergFieldDef {
    pub name: String,
    pub iceberg_type: PrimitiveType,
    pub arrow_type: DataType,
    pub required: bool,
}

fn build_arrow_schema(fields: &[IcebergFieldDef]) -> Arc<ArrowSchema> {
    let arrow_fields: Vec<Field> = fields
        .iter()
        .enumerate()
        .map(|(i, f)| {
            Field::new(&f.name, f.arrow_type.clone(), !f.required).with_metadata(HashMap::from(
                [(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    (i + 1).to_string(),
                )],
            ))
        })
        .collect();
    Arc::new(ArrowSchema::new(arrow_fields))
}

fn build_iceberg_schema(fields: &[IcebergFieldDef]) -> Result<IcebergSchema, IcebergSinkError> {
    let iceberg_fields: Vec<Arc<NestedField>> = fields
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let field_id = (i + 1) as i32;
            if f.required {
                Arc::new(NestedField::required(
                    field_id,
                    &f.name,
                    Type::Primitive(f.iceberg_type.clone()),
                ))
            } else {
                Arc::new(NestedField::optional(
                    field_id,
                    &f.name,
                    Type::Primitive(f.iceberg_type.clone()),
                ))
            }
        })
        .collect();

    Ok(IcebergSchema::builder()
        .with_fields(iceberg_fields)
        .build()?)
}

impl<E> IcebergSink<E, iceberg::MemoryCatalog> {
    pub async fn new(
        config: IcebergSinkConfig,
        event_fields: Vec<IcebergFieldDef>,
    ) -> Result<Self, IcebergSinkError> {
        let mut all_fields = vec![IcebergFieldDef {
            name: "__lsn".to_string(),
            iceberg_type: PrimitiveType::Long,
            arrow_type: DataType::Int64,
            required: true,
        }];
        all_fields.extend(event_fields);

        let arrow_schema = build_arrow_schema(&all_fields);
        let iceberg_schema = build_iceberg_schema(&all_fields)?;

        let warehouse_url = format!("file://{}", config.warehouse_path);

        let catalog = MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "cdc",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse_url)]),
            )
            .await?;
        let catalog = Arc::new(catalog);

        let namespace = NamespaceIdent::new(config.namespace.clone());

        if catalog.get_namespace(&namespace).await.is_err() {
            catalog
                .create_namespace(&namespace, HashMap::new())
                .await?;
        }

        let table_creation = TableCreation::builder()
            .name(config.table_name.clone())
            .schema(iceberg_schema.clone())
            .build();

        let table = catalog
            .create_table(&namespace, table_creation)
            .await?;

        info!(
            namespace = %config.namespace,
            table = %config.table_name,
            "Iceberg sink initialized"
        );

        Ok(Self {
            catalog,
            namespace,
            table_name: config.table_name,
            arrow_schema,
            iceberg_schema: Arc::new(iceberg_schema),
            table: Mutex::new(table),
            _marker: PhantomData,
        })
    }
}

impl<E, C: Catalog> IcebergSink<E, C> {
    pub async fn table(&self) -> tokio::sync::MutexGuard<'_, iceberg::table::Table> {
        self.table.lock().await
    }
}

impl<E: Serialize, C: Catalog> IcebergSink<E, C> {
    fn events_to_record_batch(
        &self,
        events: &[LsnEvent<E>],
    ) -> Result<RecordBatch, IcebergSinkError> {
        let mut buf = Vec::new();
        for event in events {
            serde_json::to_writer(&mut buf, event)?;
            buf.push(b'\n');
        }

        let mut decoder = ReaderBuilder::new(self.arrow_schema.clone()).build_decoder()?;
        decoder.decode(buf.as_slice())?;
        let batch = decoder
            .flush()?
            .ok_or_else(|| arrow_schema::ArrowError::InvalidArgumentError("empty batch".into()))?;

        Ok(batch)
    }
}

#[async_trait]
impl<E, C> Sink for IcebergSink<E, C>
where
    E: Serialize + Send + Sync + 'static,
    C: Catalog + Send + Sync + 'static,
{
    type Event = LsnEvent<E>;
    type Error = IcebergSinkError;

    async fn publish(&self, events: &[LsnEvent<E>]) -> Result<(), IcebergSinkError> {
        if events.is_empty() {
            return Ok(());
        }

        let batch = self.events_to_record_batch(events)?;

        let mut table = self.table.lock().await;

        let location_gen =
            DefaultLocationGenerator::new(table.metadata().clone())?;
        let prefix = format!("data-{}", Uuid::new_v4());
        let file_name_gen =
            DefaultFileNameGenerator::new(prefix, None, DataFileFormat::Parquet);

        let parquet_writer_builder = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            self.iceberg_schema.clone(),
        );

        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_writer_builder,
            table.file_io().clone(),
            location_gen,
            file_name_gen,
        );

        let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);
        let mut writer = data_file_writer_builder.build(None).await?;

        writer.write(batch).await?;
        let data_files = writer.close().await?;

        let tx = Transaction::new(&*table);
        let append = tx.fast_append().add_data_files(data_files);
        let tx = iceberg::transaction::ApplyTransactionAction::apply(append, tx)?;
        let updated_table = tx.commit(&*self.catalog).await?;
        *table = updated_table;

        info!(
            count = events.len(),
            namespace = %self.namespace,
            table = %self.table_name,
            "Events written to Iceberg table"
        );

        Ok(())
    }

    async fn shutdown(&mut self) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::TryStreamExt;
    use serde::Deserialize;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestEvent {
        id: i32,
        name: String,
    }

    fn test_event_fields() -> Vec<IcebergFieldDef> {
        vec![
            IcebergFieldDef {
                name: "id".to_string(),
                iceberg_type: PrimitiveType::Int,
                arrow_type: DataType::Int32,
                required: true,
            },
            IcebergFieldDef {
                name: "name".to_string(),
                iceberg_type: PrimitiveType::String,
                arrow_type: DataType::Utf8,
                required: true,
            },
        ]
    }

    #[test]
    fn test_build_schemas() {
        let fields = test_event_fields();
        let mut all_fields = vec![IcebergFieldDef {
            name: "__lsn".to_string(),
            iceberg_type: PrimitiveType::Long,
            arrow_type: DataType::Int64,
            required: true,
        }];
        all_fields.extend(fields);

        let arrow_schema = build_arrow_schema(&all_fields);
        assert_eq!(arrow_schema.fields().len(), 3);
        assert_eq!(arrow_schema.field(0).name(), "__lsn");
        assert_eq!(*arrow_schema.field(0).data_type(), DataType::Int64);
        assert_eq!(
            arrow_schema.field(0).metadata().get(PARQUET_FIELD_ID_META_KEY),
            Some(&"1".to_string())
        );

        let iceberg_schema = build_iceberg_schema(&all_fields).unwrap();
        assert_eq!(iceberg_schema.as_struct().fields().len(), 3);
    }

    #[tokio::test]
    async fn test_iceberg_sink_write() {
        let dir = tempfile::tempdir().unwrap();
        let warehouse_path = dir.path().to_str().unwrap().to_string();

        let mut sink = IcebergSink::new(
            IcebergSinkConfig {
                warehouse_path,
                namespace: "test_ns".to_string(),
                table_name: "test_table".to_string(),
            },
            test_event_fields(),
        )
        .await
        .unwrap();

        let events = vec![
            LsnEvent {
                lsn: 100,
                event: TestEvent {
                    id: 1,
                    name: "first".into(),
                },
            },
            LsnEvent {
                lsn: 200,
                event: TestEvent {
                    id: 2,
                    name: "second".into(),
                },
            },
        ];

        sink.publish(&events).await.unwrap();

        {
            let table = sink.table.lock().await;
            let scan = table.scan().build().unwrap();
            let batches: Vec<RecordBatch> = scan
                .to_arrow()
                .await
                .unwrap()
                .try_collect()
                .await
                .unwrap();

            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total_rows, 2);
        }

        sink.shutdown().await;
    }

    #[tokio::test]
    async fn test_iceberg_sink_empty_publish() {
        let dir = tempfile::tempdir().unwrap();
        let warehouse_path = dir.path().to_str().unwrap().to_string();

        let sink = IcebergSink::<TestEvent, _>::new(
            IcebergSinkConfig {
                warehouse_path,
                namespace: "test_ns".to_string(),
                table_name: "empty_table".to_string(),
            },
            test_event_fields(),
        )
        .await
        .unwrap();

        sink.publish(&[]).await.unwrap();
    }

    #[tokio::test]
    async fn test_iceberg_sink_multiple_batches() {
        let dir = tempfile::tempdir().unwrap();
        let warehouse_path = dir.path().to_str().unwrap().to_string();

        let mut sink = IcebergSink::new(
            IcebergSinkConfig {
                warehouse_path,
                namespace: "test_ns".to_string(),
                table_name: "batch_table".to_string(),
            },
            test_event_fields(),
        )
        .await
        .unwrap();

        sink.publish(&[LsnEvent {
            lsn: 100,
            event: TestEvent {
                id: 1,
                name: "a".into(),
            },
        }])
        .await
        .unwrap();

        sink.publish(&[
            LsnEvent {
                lsn: 200,
                event: TestEvent {
                    id: 2,
                    name: "b".into(),
                },
            },
            LsnEvent {
                lsn: 300,
                event: TestEvent {
                    id: 3,
                    name: "c".into(),
                },
            },
        ])
        .await
        .unwrap();

        let table = sink.table.lock().await;
        let scan = table.scan().build().unwrap();
        let batches: Vec<RecordBatch> = scan
            .to_arrow()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);

        drop(table);
        sink.shutdown().await;
    }
}
