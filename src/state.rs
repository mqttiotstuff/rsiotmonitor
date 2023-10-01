use sqlite::{self, Connection, ConnectionWithFullMutex, Result, State};

#[allow(unused_imports)]
use log::debug;

pub fn init() -> Result<ConnectionWithFullMutex> {
    let connection = Connection::open_with_full_mutex("iotstates.db").unwrap();
    let query =
        "CREATE TABLE IF NOT EXISTS iotstates (name TEXT, topic TEXT PRIMARY KEY, state BLOB)";
    connection
        .execute(query)
        .expect("error in creating the table");
    Ok(connection)
}

pub fn save_state(
    connection: &Connection,
    topic: &str,
    name: &str,
    payload: &[u8],
) -> Result<State> {
    let query = "INSERT OR REPLACE INTO iotstates (name, topic,state) VALUES(?,?,?)";
    let mut statement = connection.prepare(query).unwrap();
    statement.bind((1, name)).expect("fail to bind name");
    statement.bind((2, topic)).unwrap();
    statement.bind((3, payload)).unwrap();
    statement.next()
}

pub fn read_state(connection: &Connection, name: &str, topic: &str) -> Result<Vec<u8>> {
    let query = "select state from iotstates where topic = ? and name = ?";

    let mut statement = connection.prepare(query).unwrap();
    statement.bind((1, topic)).unwrap();
    statement.bind((2, name)).unwrap();

    if let Ok(State::Row) = statement.next() {
        let value = statement.read::<Vec<u8>, _>("state").unwrap();
        return Ok(value);
    }

    Err(sqlite::Error {
        code: Some(99_isize),
        message: Some("no elements".to_string()),
    })
}

pub fn get_all_states(connection: &Connection, name: &str) -> Result<Vec<(String, Vec<u8>)>> {
    let query = "select topic, state from iotstates where name = ?";

    let mut statement = connection.prepare(query).unwrap();
    statement.bind((1, name)).unwrap();

    let mut result: Vec<(String, Vec<u8>)> = Vec::new();
    while let Ok(State::Row) = statement.next() {
        let topic = statement.read::<String, _>("topic").unwrap();
        let value = statement.read::<Vec<u8>, _>("state").unwrap();
        result.push((topic, value))
    }

    Ok(result)
}

#[test]
fn test_init() -> sqlite::Result<()> {
    let r = init()?;

    let s: String = "h".into();
    let u: Vec<u8> = vec![1, 2, 3];
    let n: String = "iot2".into();
    save_state(&r, &s, &n, &u)?;

    let verif = read_state(&r, &n, &s)?;

    debug!("{:?}", verif);
    assert_eq!(vec![1, 2, 3], verif);

    Ok(())
}
