mod history_analysis;
pub mod history_storage;

pub use self::history_storage::History;
pub use self::history_storage::TopicPayload;

// creating a prepregistered session for history querying
pub use self::history_analysis::create_session;
