use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;
use std::{fmt, fs, u128};

use leveldb::database::Database;
use leveldb::iterator::Iterable;
use leveldb::options::{Options, ReadOptions, WriteOptions};

#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};
use parquet::data_type::{ByteArray, ByteArrayType, Int64Type};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;

use leveldb::util::FromU8;

use std::error::Error;

/// Content of the value column
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
    database: Box<Database>,
}


impl History {
    /// init the history, and open the archive database
    pub fn init() -> Result<Box<History>, Box<dyn Error>> {
        let path = Path::new("history");

        let mut options = Options::new();
        options.create_if_missing = true;
        let database = match Database::open(path, &options) {
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

        let t: i64 = instant.as_micros().try_into().unwrap();

        let p = TopicPayload { topic, payload };

        return p.as_slice(|payload_bytes| {
            debug!("payload : {:?}", payload_bytes);
            match self.database.put(&write_opts, &t, payload_bytes) {
                Ok(_) => Ok(()),
                Err(e) => {
                    error!("failed to write to database: {:?}", e);
                    Err(Box::new(HistoryError {}))
                }
            }
        });
    }

    pub fn export_to_parquet(
        &self,
        output_file: &str,
        delete_values: bool,
    ) -> Result<(), Box<dyn Error>> {
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

        let mut it = self.database.iter(&ReadOptions::new());

        let mut row = it.next();

        let mut last: Option<i64> = None;

        while row.is_some() {
            let mut row_group_writer = writer.next_row_group().unwrap();

            cpt = 1;

            let mut all_topics: Vec<String> = Vec::new();
            let mut all_timestamps: Vec<i64> = Vec::new();
            let mut all_payloads: Vec<Vec<u8>> = Vec::new();

            while row.is_some() && (cpt % 10_000) != 0 {
                if let Some(a) = row.as_ref() {
                    let timestamp = i64::from_u8(&a.0);
                    last = Some(timestamp);
                    let tp = TopicPayload::from_u8(&a.1);
                    let topic = tp.topic;
                    let payload = tp.payload;

                    all_topics.push(topic);
                    let b = payload.to_vec();
                    all_payloads.push(b);

                    let tbytes = timestamp;
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
                        &all_timestamps,
                        None,
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

        // self.database.delete(&WriteOptions::new(), &key);

        if delete_values {
            if let Some(last_timestamp) = last {
                for (k, v) in self.database.iter(&ReadOptions::new()) {
                    let key_value = i64::from_u8(&k);

                    if let Err(e) = self.database.delete(&WriteOptions::new(), &key_value) {
                        warn!("error while deleting the value : {}", e);
                    }
                    if key_value == last_timestamp {
                        break;
                    }
                }
            }
        }
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
    for i in h.database.iter(&ReadOptions::new()) {
        println!("{} : {:?}", &i64::from_u8(&i.0), &i.1);
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
    h.export_to_parquet("test.parquet", true).unwrap();

    // no more elements
    assert!(h.database.iter(&ReadOptions::new()).next().is_none());
}

#[test]
pub fn export() {
    let h = History::init().unwrap();

    // export to parquet
    h.export_to_parquet("h.parquet", false).unwrap();
}
