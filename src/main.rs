use arrow::{
    array::RecordBatch,
    datatypes::{Field, FieldRef, Schema, SchemaRef},
};
use bytes::Bytes;
use parquet::{
    arrow::{
        ArrowWriter,
        arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder},
    },
    file::properties::WriterProperties,
};
use serde::{Deserialize, Serialize};
use serde_arrow::schema::{SchemaLike, TracingOptions};
use std::{
    io::{Cursor, Seek, Write},
    sync::Arc,
};

fn make_arrow_writer<T, W>(samples: &[T], writer: W) -> (ArrowWriter<W>, Vec<Arc<Field>>)
where
    T: Serialize + for<'a> Deserialize<'a>,
    W: Write + Seek + Send + 'static,
{
    let fields = Vec::<FieldRef>::from_samples(
        samples,
        TracingOptions::default().enums_without_data_as_strings(true),
    )
    .unwrap();
    let arrow_schema = SchemaRef::new(Schema::new(
        fields
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>(),
    ));
    let props = WriterProperties::builder().build();
    let writer = ArrowWriter::try_new(writer, arrow_schema, Some(props)).unwrap();
    (writer, fields)
}

fn parquet_round_trip<T>(items: &[T]) -> Vec<T>
where
    T: Serialize + for<'a> Deserialize<'a>,
{
    let mut data: Vec<u8> = Vec::new();
    {
        let cursor = Cursor::new(data);
        let make_arrow_writer = make_arrow_writer(items, cursor);
        let (mut writer, fields) = make_arrow_writer;
        let batch = serde_arrow::to_record_batch(&fields, &items).unwrap();
        writer.write(&batch).unwrap();
        let cursor = writer.into_inner().unwrap();
        data = cursor.into_inner();
    }
    let options = ArrowReaderOptions::new().with_skip_arrow_metadata(true);
    let bytes: Bytes = data.into();
    let mut batch_reader = ParquetRecordBatchReaderBuilder::try_new_with_options(bytes, options)
        .unwrap()
        .with_batch_size(1024)
        .build()
        .unwrap();
    let batch: RecordBatch = batch_reader.next().unwrap().unwrap();
    serde_arrow::from_record_batch(&batch).unwrap()
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
enum E {
    A,
    B,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct S {
    e: Option<E>,
}

fn main() {
    // let items = vec![S { e: None }]; // using this as the sample will fail
    let items = vec![S { e: Some(E::A) }]; // using this as the sample will succeed
    let items_read = parquet_round_trip(&items);
    assert_eq!(items, items_read);
}
