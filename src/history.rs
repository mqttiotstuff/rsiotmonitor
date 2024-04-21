use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;
use std::{fmt, fs, u128};

use chrono::Datelike;
use leveldb::compaction::Compaction;
use leveldb::database::Database;
use leveldb::iterator::Iterable;
use leveldb::options::{Options, ReadOptions, WriteOptions};

#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;

use leveldb::util::FromU8;

use std::convert::TryInto;

use std::error::Error;

/// Content of the value column
pub struct TopicPayload<'a> {
    pub topic: String,
    pub payload: &'a [u8],
}

impl<'a> TopicPayload<'a> {
    pub fn from_u8(key: &'a [u8]) -> TopicPayload<'a> {
        let tb = key[0..2].try_into().unwrap();
        let topic_size: usize = u16::from_le_bytes(tb).try_into().unwrap();
        let topic: String = String::from_utf8(key[2..2 + topic_size].to_vec()).unwrap();
        let payload = &key[2 + topic_size..];
        TopicPayload { topic, payload }
    }

    pub fn as_slice<T, F: Fn(&[u8]) -> Result<T, Box<dyn Error>>>(
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
    pub database: Arc<Database>,
}

impl History {
    /// init the history, and open the archive database
    pub fn init() -> Result<Arc<History>, Box<dyn Error>> {
        let path = Path::new("history");

        let mut options = Options::new();
        options.create_if_missing = true;

        if let Err(e) = leveldb::management::repair(path, &options) {
            warn!("error encountered for repair : {:?}", e);
            warn!("the repair action is not successfull, or database does not exists, continue");
        };

        let database = match Database::open(path, &options) {
            Ok(db) => db,
            Err(e) => {
                panic!("failed to open database: {:?}", e);
            }
        };
        println!("database compaction");
        database.compact(&[], &[255, 255, 255, 255, 255, 255, 255, 255, 255]);

        Ok(Arc::new(History {
            database: Arc::new(database),
        }))
    }

    pub fn store_event(&self, topic: String, payload: &[u8]) -> Result<(), Box<dyn Error>> {
        let instant = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();

        let t: i64 = instant.as_micros().try_into().unwrap();
        self.store_event_with_timestamp(t, topic, payload)
    }

    /// add a new event in the historical database
    pub fn store_event_with_timestamp(
        &self,
        timestamp: i64,
        topic: String,
        payload: &[u8],
    ) -> Result<(), Box<dyn Error>> {
        let write_opts = WriteOptions::new();

        let p = TopicPayload { topic, payload };

        return p.as_slice(|payload_bytes| {
            debug!("payload : {:?}", payload_bytes);
            match self.database.put(&write_opts, &timestamp, payload_bytes) {
                Ok(_) => Ok(()),
                Err(e) => {
                    error!("failed to write to database: {:?}", e);
                    Err(Box::new(HistoryError {}))
                }
            }
        });
    }

    // export the current database events to a parquet file
    pub fn export_to_parquet(
        &self,
        output_file: &str,
        date_range: Option<(i64, i64)>,
        delete_values: bool,
    ) -> Result<(), Box<dyn Error>> {
        let path = Path::new(output_file);

        let record_type = "
          message schema {
            REQUIRED Int32 day;
            REQUIRED Int32 month;
            REQUIRED Int32 year;
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
        let file = fs::File::create(path).unwrap();
        let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();

        // writing rows count ..
        let mut cpt: u128;

        let mut it = self.database.iter(&ReadOptions::new());

        let mut row = it.next();

        let mut last: Option<i64> = None;

        while row.is_some() {
            let mut row_group_writer = writer.next_row_group().unwrap();

            cpt = 1;

            let mut all_topics: Vec<String> = Vec::new();
            let mut all_timestamps: Vec<i64> = Vec::new();
            let mut all_year: Vec<i32> = Vec::new();
            let mut all_month: Vec<i32> = Vec::new();
            let mut all_days: Vec<i32> = Vec::new();
            let mut all_payloads: Vec<Vec<u8>> = Vec::new();

            // read 10_000 rows in the memory vector to create the parquet group
            while row.is_some() && cpt % 10_000 != 0 {
                if let Some(a) = row.as_ref() {
                    let timestamp = i64::from_u8(&a.0);

                    if date_range.is_none()
                        || (timestamp >= date_range.unwrap().0 && timestamp < date_range.unwrap().1)
                    {
                        last = Some(timestamp);
                        let tp = TopicPayload::from_u8(&a.1);
                        let topic = tp.topic;
                        let payload = tp.payload;

                        all_topics.push(topic);
                        let b = payload.to_vec();
                        all_payloads.push(b);

                        let tbytes = timestamp;
                        all_timestamps.push(tbytes);

                        // year, month, day
                        let naive =
                            chrono::NaiveDateTime::from_timestamp_opt(timestamp / 1_000_000, 0)
                                .unwrap();
                        let date = naive.date();
                        let year: i32 = date.year();
                        all_year.push(year);
                        let month: i32 = date.month().try_into().unwrap();
                        all_month.push(month);

                        let day: i32 = date.day().try_into().unwrap();
                        all_days.push(day);

                        cpt += 1;
                        if cpt % 10_000 == 0 {
                            debug!("{} elements exported", cpt);
                        }
                    }

                    row = it.next();
                }
            }

            if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
                col_writer
                    .typed::<Int32Type>()
                    .write_batch(
                        &all_days, None,
                        None, // Some(&[3, 3, 3, 2, 2]),
                             //Some(&[0, 1, 0, 1, 1]),
                    )
                    .expect("error in writing columns");

                col_writer.close().unwrap();
            } else {
                return Err(Box::new(HistoryError {}));
            }

            if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
                col_writer
                    .typed::<Int32Type>()
                    .write_batch(
                        &all_month, None,
                        None, // Some(&[3, 3, 3, 2, 2]),
                             //Some(&[0, 1, 0, 1, 1]),
                    )
                    .expect("error in writing columns");

                col_writer.close().unwrap();
            } else {
                return Err(Box::new(HistoryError {}));
            }

            if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
                col_writer
                    .typed::<Int32Type>()
                    .write_batch(
                        &all_year, None,
                        None, // Some(&[3, 3, 3, 2, 2]),
                             //Some(&[0, 1, 0, 1, 1]),
                    )
                    .expect("error in writing columns");

                col_writer.close().unwrap();
            } else {
                return Err(Box::new(HistoryError {}));
            }

            // write the rows in the parquet file
            if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
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
                for (k, _v) in self.database.iter(&ReadOptions::new()) {
                    let key_value = i64::from_u8(&k);

                    if date_range.is_none()
                        || (key_value >= date_range.unwrap().0 && key_value < date_range.unwrap().1)
                    {
                        if let Err(e) = self.database.delete(&WriteOptions::new(), &key_value) {
                            warn!("error while deleting the value : {}", e);
                        }
                        if key_value == last_timestamp {
                            break;
                        }
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
pub fn test_storage_timestamp() -> Result<(), Box<dyn Error>> {
    let h = History::init().unwrap();
    for t in 0..63 {
        let e: i64 = 1 << t;
        h.store_event_with_timestamp(e, "a".into(), "b".as_bytes())?;
    }

    // dump

    let mut cpt: u128 = 0;
    let mut it = h.database.iter(&ReadOptions::new());

    // writing rows ..
    let mut row = it.next();
    while row.is_some() {
        if let Some(a) = row {
            let timestamp = i64::from_u8(&a.0);

            let tp = TopicPayload::from_u8(&a.1);
            let _topic = tp.topic;
            let _payload = tp.payload;

            println!("{}", timestamp);

            cpt += 1;
            row = it.next();
        }
    }

    info!("{} written", cpt);

    h.export_to_parquet("final.parquet", None, true)?;

    Ok(())
}

#[test]
pub fn test_export() -> Result<(), Box<dyn Error>> {
    let h = History::init()?;
    for _i in 0..100_000 {
        h.store_event("a".into(), "b".as_bytes())?;
        h.store_event("a1".into(), "b".as_bytes())?;
        h.store_event("a2".into(), "b".as_bytes())?;
        h.store_event("a3".into(), "b".as_bytes())?;
        h.store_event("a4".into(), "b".as_bytes())?;
    }
    // export to parquet
    h.export_to_parquet("test.parquet", None, true)?;

    // no more elements
    assert!(h.database.iter(&ReadOptions::new()).next().is_none());
    Ok(())
}

#[test]
pub fn test_export_with_range() -> Result<(), Box<dyn Error>> {
    let h = History::init()?;
    for _i in 0..100_000 {
        h.store_event_with_timestamp(0, "a".into(), "b".as_bytes())?;
        h.store_event_with_timestamp(1, "a1".into(), "b".as_bytes())?;
        h.store_event_with_timestamp(2, "a2".into(), "b".as_bytes())?;
        h.store_event_with_timestamp(3, "a3".into(), "b".as_bytes())?;
        h.store_event_with_timestamp(4, "a4".into(), "b".as_bytes())?;
    }
    // export to parquet
    h.export_to_parquet("test.parquet", Some((0, 2)), true)?;

    // no more elements
    // assert!(h.database.iter(&ReadOptions::new()).next().is_none());

    Ok(())
}

#[test]
pub fn export() -> Result<(), Box<dyn Error>> {
    let h = History::init()?;

    // export to parquet
    h.export_to_parquet("h.parquet", None, false)?;
    Ok(())
}
