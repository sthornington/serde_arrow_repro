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

fn make_arrow_writer<T, W>(writer: W) -> (ArrowWriter<W>, Vec<Arc<Field>>)
where
    T: Serialize + for<'a> Deserialize<'a>,
    W: Write + Seek + Send + 'static,
{
    let fields = Vec::<FieldRef>::from_type::<T>(
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
        let make_arrow_writer = make_arrow_writer::<T, _>(cursor);
        let make_arrow_writer = make_arrow_writer;
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

#[derive(Debug, Clone, PartialEq)]
#[repr(transparent)]
/// negative number
struct N(i64);

impl Default for N {
    fn default() -> Self {
        Self(-1)
    }
}

impl Serialize for N {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_i64(self.0)
    }
}

impl<'de> Deserialize<'de> for N {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let v = i64::deserialize(deserializer)?;
        println!("{}", v);
        if v < 0 {
            Ok(N(v))
        } else {
            Err(serde::de::Error::custom("i64 non-negative"))
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct S {
    n: N,
}

fn main() {
    let items = vec![S { n: N(-100) }];
    let items_read = parquet_round_trip(&items);
    assert_eq!(items, items_read);
}
