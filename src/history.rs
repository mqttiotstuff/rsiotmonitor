use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;
use std::{fmt, fs, u128};

use leveldb::database::Database;
use leveldb::iterator::Iterable;
use leveldb::kv::KV;
use leveldb::options::{Options, ReadOptions, WriteOptions};

use db_key::Key;
#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};
use parquet::basic::Compression;
use parquet::data_type::{ByteArray, ByteArrayType, FixedLenByteArrayType, Int32Type, Int96, Int64Type};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::format::StringType;
use parquet::schema::parser::parse_message_type;
use rusty_leveldb::CompressionType;

use std::error::Error;

/// time stamp storage
struct I64 {
    value: i64,
}

impl Key for I64 {
    fn from_u8(key: &[u8]) -> I64 {
        assert!(key.len() == 8);
        let b: [u8; 8] = key[0..8].try_into().unwrap();
        I64 {
            value: i64::from_le_bytes(b),
        }
    }

    fn as_slice<T, F: Fn(&[u8]) -> T>(&self, f: F) -> T {
        let b = self.value.to_le_bytes();
        f(&b)
    }
}

struct TopicPayload<'a> {
    topic: String,
    payload: &'a [u8],
}

impl<'a> TopicPayload<'a> {
    fn from_u8(key: &'a [u8]) -> TopicPayload<'a> {
        let tb = key[0..2].try_into().unwrap();
        let topic_size: usize = u16::from_le_bytes(tb).try_into().unwrap();
        let topic: String = String::from_utf8(key[2..2 + topic_size].to_vec()).unwrap();
        let payload = &key[2 + topic_size..];
        TopicPayload { topic, payload }
    }

    fn as_slice<T, F: Fn(&[u8]) -> Result<T, Box<dyn Error>>>(
        &self,
        f: F,
    ) -> Result<T, Box<dyn Error>> {
        let mut b = Vec::with_capacity(self.topic.len() * 2 + 2 + self.payload.len());
        let l: u16 = self.topic.len().try_into().unwrap();
        b.extend_from_slice(&l.to_le_bytes());
        b.extend_from_slice(self.topic.as_bytes());
        b.extend_from_slice(self.payload);
        f(&b)
    }
}

#[derive(Clone, Debug)]
pub struct HistoryError;

impl fmt::Display for HistoryError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "history error")
    }
}
impl Error for HistoryError {}

pub struct History {
    database: Box<Database<I64>>,
}

impl History {
    /// init the history, and open the archive database
    pub fn init() -> Result<Box<History>, Box<dyn Error>> {
        let path = Path::new("history");

        let mut options = Options::new();
        options.create_if_missing = true;
        let database = match Database::open(path, options) {
            Ok(db) => db,
            Err(e) => {
                panic!("failed to open database: {:?}", e);
            }
        };

        Ok(Box::new(History {
            database: Box::new(database),
        }))
    }

    /// add a new event in the historical database
    pub fn store_event(&self, topic: String, payload: &[u8]) -> Result<(), Box<dyn Error>> {
        let write_opts = WriteOptions::new();
        let instant = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();
        let t = I64 {
            value: instant.as_micros().try_into().unwrap(),
        };
        let p = TopicPayload { topic, payload };

        return p.as_slice(|payload_bytes| {
            debug!("payload : {:?}", payload_bytes);
            match self.database.put(write_opts, &t, payload_bytes) {
                Ok(_) => Ok(()),
                Err(e) => {
                    error!("failed to write to database: {:?}", e);
                    Err(Box::new(HistoryError {}))
                }
            }
        });
    }

    pub fn export_to_parquet(&self, output_file: &str) -> Result<(), Box<dyn Error>> {
        let path = Path::new(output_file);

        let record_type = "
          message schema {
            REQUIRED Int64 timestamp;
            REQUIRED binary topic;
            REQUIRED binary payload;
          }
        ";
        let schema = Arc::new(parse_message_type(record_type).unwrap());
        let props = Arc::new(
            WriterProperties::builder()
                .set_compression(parquet::basic::Compression::SNAPPY)
                .build(),
        );
        let file = fs::File::create(&path).unwrap();
        let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();

        // writing rows ..
        let mut cpt: u128 = 0;

        let mut it = self.database.iter(ReadOptions::new());

        let mut row = it.next();

        while row.is_some() {
            let mut row_group_writer = writer.next_row_group().unwrap();

            cpt = 1;

            let mut all_topics: Vec<String> = Vec::new();
            let mut all_timestamps: Vec<i64> = Vec::new();
            let mut all_payloads: Vec<Vec<u8>> = Vec::new();

            while row.is_some() && (cpt % 10_000) != 0 {
                if let Some(a) = row.as_ref() {
                    let timestamp = &a.0;
                    let tp = TopicPayload::from_u8(&a.1);
                    let topic = tp.topic;
                    let payload = tp.payload;

                    all_topics.push(topic);
                    let b = payload.to_vec();
                    all_payloads.push(b);

                    let tbytes = timestamp.value;
                    all_timestamps.push(tbytes);

                    cpt += 1;
                    row = it.next();
                }
            }

            if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
                // write all

               

                col_writer
                    .typed::<Int64Type>()
                    .write_batch(
                        &all_timestamps, None,
                        None, // Some(&[3, 3, 3, 2, 2]),
                             //Some(&[0, 1, 0, 1, 1]),
                    )
                    .expect("error in writing columns");

                col_writer.close().unwrap();
            } else {
                return Err(Box::new(HistoryError {}));
            }

            if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
                // write all

                let v: Vec<ByteArray> = all_topics
                    .iter()
                    .map(|i| ByteArray::from(i.as_bytes()))
                    .collect();

                col_writer
                    .typed::<ByteArrayType>()
                    .write_batch(
                        &v, None,
                        None, // Some(&[3, 3, 3, 2, 2]),
                             //Some(&[0, 1, 0, 1, 1]),
                    )
                    .expect("error in writing columns");

                col_writer.close().unwrap();
            } else {
                return Err(Box::new(HistoryError {}));
            }
            if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
                // write all

                let v: Vec<ByteArray> = all_payloads
                    .iter()
                    .map(|i| ByteArray::from(i.clone()))
                    .collect();

                col_writer
                    .typed::<ByteArrayType>()
                    .write_batch(
                        &v, None,
                        None, // Some(&[3, 3, 3, 2, 2]),
                             //Some(&[0, 1, 0, 1, 1]),
                    )
                    .expect("error in writing columns");

                col_writer.close().unwrap();
            } else {
                return Err(Box::new(HistoryError {}));
            }

            row_group_writer.close().unwrap();
        }

        writer.close().unwrap();

        Ok(())
    }
}

#[test]
pub fn test_storage() {
    let h = History::init().unwrap();
    h.store_event("a".into(), "b".as_bytes()).unwrap();
    h.store_event("a1".into(), "b".as_bytes()).unwrap();
    h.store_event("a2".into(), "b".as_bytes()).unwrap();
    h.store_event("a3".into(), "b".as_bytes()).unwrap();
    h.store_event("a4".into(), "b".as_bytes()).unwrap();

    // browse and dump the content
    for i in h.database.iter(ReadOptions::new()) {
        println!("{} : {:?}", &i.0.value, &i.1);
    }
}

#[test]
pub fn test_export() {
    let h = History::init().unwrap();
    for i in 0..100_000 {
        h.store_event("a".into(), "b".as_bytes()).unwrap();
        h.store_event("a1".into(), "b".as_bytes()).unwrap();
        h.store_event("a2".into(), "b".as_bytes()).unwrap();
        h.store_event("a3".into(), "b".as_bytes()).unwrap();
        h.store_event("a4".into(), "b".as_bytes()).unwrap();
    }
    // export to parquet
    h.export_to_parquet("test.parquet").unwrap();
}
