//! Some useful types.

mod async_stream;
#[cfg(feature = "websocket")]
pub(crate) use async_stream::tungstenite_error_to_std_io_error;
pub(crate) use async_stream::AsyncStream;

mod free_pid_list;
pub(crate) use free_pid_list::FreePidList;

mod tokio_runtime;
pub use tokio_runtime::TokioRuntime;
