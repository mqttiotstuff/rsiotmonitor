use std::error;

use log::debug;
use sqlite::{self, Connection, Result, State, ConnectionWithFullMutex};


pub fn init() -> Result<ConnectionWithFullMutex> {
    let connection = Connection::open_with_full_mutex("iotstates.db").unwrap();

    let query = "CREATE TABLE IF NOT EXISTS iotstates (topic TEXT PRIMARY KEY, state BLOB)";
    connection
        .execute(query)
        .expect("error in creating the table");
    Ok(connection)
}

pub fn save_state(connection: &Connection, topic: &String, payload: &[u8]) -> Result<State> {
    let query = "INSERT OR REPLACE INTO iotstates (topic,state) VALUES(?,?)";
    let mut statement = connection.prepare(query).unwrap();
    statement.bind((1, topic.as_str())).unwrap();
    statement.bind((2, payload)).unwrap();
    statement.next()
}

pub fn read_state(connection: &Connection, topic: &String) -> Result<Vec<u8>> {
    let query = "select state from iotstates where topic = ?";

    let mut statement = connection.prepare(query).unwrap();
    statement.bind((1, topic.as_str())).unwrap();

    if let Ok(State::Row) = statement.next() {
        let value = statement.read::<Vec<u8>, _>("state").unwrap();
        return Ok(value);
    }

    Err(sqlite::Error {
        code: Some(99 as isize),
        message: Some("no elements".to_string()),
    })
}


#[test]
fn test_init() -> sqlite::Result<()> {

    let r = init()?;

    let s : String = "h".into();
    let u : Vec<u8> = vec![1,2,3];
    save_state(&r, &s,&u)?;

    let verif = read_state(&r, &s)?;
    
    debug!("{:?}", verif);
    assert_eq!(vec![1,2,3],verif);
    
    Ok(())
}