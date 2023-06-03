use std::path::Path;
use std::time::SystemTime;
use std::{fmt, u128};

use leveldb::database::Database;
use leveldb::kv::KV;
use leveldb::options::{Options, WriteOptions};

use db_key::Key;
#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};

use std::error::Error;

/// time stamp storage
struct U128 {
    value: u128,
}

impl Key for U128 {
    fn from_u8(key: &[u8]) -> U128 {
        assert!(key.len() == 16);
        let b: [u8; 16] = key[0..15].try_into().unwrap();
        U128 {
            value: u128::from_le_bytes(b),
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
        let tb = key[0..1].try_into().unwrap();
        let topic_size: usize = u16::from_le_bytes(tb).try_into().unwrap();
        let topic: String = String::from_utf8(key[2..2 + topic_size].to_vec()).unwrap();
        let payload = &key[2 + topic_size + 1..];
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
    database: Box<Database<U128>>,
}

impl History {
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

    pub fn store_event(&self, topic: String, payload: &[u8]) -> Result<(), Box<dyn Error>> {
        let write_opts = WriteOptions::new();
        let instant = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();
        let t = U128 {
            value: instant.as_micros(),
        };
        let p = TopicPayload { topic, payload };

        return p.as_slice(
            |payload_bytes| match self.database.put(write_opts, &t, payload_bytes) {
                Ok(_) => Ok(()),
                Err(e) => {
                    error!("failed to write to database: {:?}", e);
                    Err(Box::new(HistoryError {}))
                }
            },
        );
    }
}
