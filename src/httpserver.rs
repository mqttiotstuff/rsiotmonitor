use actix_cors::Cors;
use actix_files::Files;
use actix_web::{
    web::Bytes,
    App, HttpResponseBuilder, HttpServer,
};
// use http::{Request, Response};

use arrow::error::ArrowError;
use async_stream::stream;
use futures_core::Stream;

use std::{
    io::{BufWriter, IntoInnerError},
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};

use tokio::{sync::Semaphore, time::timeout};

use crate::history::{create_history_sql_session, History, HistoryAnalyticProfile};

use datafusion::{
    arrow::array::RecordBatch,
    error::DataFusionError,
};

use actix_web::{
    error, get,
    http::StatusCode,
    middleware, web, HttpRequest, HttpResponse, Result,
};

use derive_more::{Display, Error};

#[derive(Debug, Display, Error)]
#[display(fmt = "processing error: {}", name)]
struct HttpProcessingError {
    name: String,
}

impl From<&str> for HttpProcessingError {
    fn from(value: &str) -> Self {
        Self {
            name: String::from(value),
        }
    }
}

impl From<DataFusionError> for HttpProcessingError {
    fn from(value: DataFusionError) -> Self {
        let message = format!("{}", &value);
        Self {
            name: message,
        }
    }
}

impl From<ArrowError> for HttpProcessingError {
    fn from(value: ArrowError) -> Self {
        let message = format!("{}", &value);
        Self {
            name: message,
        }
    }
}

impl<T> From<IntoInnerError<T>> for HttpProcessingError {
    fn from(value: IntoInnerError<T>) -> Self {
        Self {
            name: format!("{}", &value),
        }
    }
}

impl From<Box<dyn std::error::Error>> for HttpProcessingError {
    fn from(value: Box<dyn std::error::Error>) -> Self {
        Self {
            name: format!("{}", &value),
        }
    }
}

// Use default implementation for `error_response()` method
impl error::ResponseError for HttpProcessingError {}

///////////////////////////////////////////////////////////////////////////////////////////
// server implementation

/// Default wait between attempts to acquire a concurrent SQL query slot.
pub const DEFAULT_TIMEOUT_TO_ACQUIRE_SLOT: Duration = Duration::from_millis(100);

// Static semaphore for limiting concurrent requests
static CONCURRENT_REQUESTS_SEMAPHORE: OnceLock<Arc<Semaphore>> = OnceLock::new();

#[derive(Debug)]
pub enum AnalyticProfileType {
    Small,
    Large,
}

pub struct HttpSqlEndPointConfig {
    pub simultaneous_queries: usize,
    pub max_attempts_to_acquire_slot: usize,
    pub timeout_to_acquire_slot: Duration,
    pub timeout_to_execute_query: Duration,
    pub timeout_to_stream: Duration, // Maximum time allowed for streaming response
    pub analytic_profile_type: Option<AnalyticProfileType>,
}

pub struct HttpServerConfig {
    pub v4_binding: (IpAddr, u16),
    pub sql_endpoint_config: HttpSqlEndPointConfig,
}

#[derive(Clone)]
struct AppData {
    pub history_db: Option<Arc<History>>,
    pub config: Arc<HttpServerConfig>,
}

// Wrapper stream that holds a semaphore permit for its entire duration
// This ensures the permit is released when the stream completes
// 
// UNSAFE EXPLANATION:
// We need 2 unsafe blocks:
// 1. `map_unchecked_mut` - Standard pattern for Stream wrappers (safe, just accessing field through Pin)
// 2. `Box::from_raw` - Reclaims leaked box (safe because we're the only owner)
//
// The permit lifetime must be extended because:
// - Permit is acquired in the handler function (short lifetime)
// - Stream outlives the function (needs 'static lifetime)
// - Semaphore is Arc<Semaphore> which is effectively 'static, so this is safe
struct StreamWithPermit<S> {
    inner: S,
    _permit_ptr: *mut tokio::sync::SemaphorePermit<'static>, // Pointer to leaked box - will be reclaimed in Drop
}

impl<S: Stream> Stream for StreamWithPermit<S> {
    type Item = S::Item;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        // UNSAFE #1: Standard pattern for implementing Stream on wrapper types
        // Safe because: We're just accessing a field through Pin, not moving or invalidating anything
        unsafe {
            self.map_unchecked_mut(|s| &mut s.inner).poll_next(cx)
        }
    }
}

// Custom Drop to reclaim the leaked box
impl<S> Drop for StreamWithPermit<S> {
    fn drop(&mut self) {
        // UNSAFE #2: Reclaim the box we leaked earlier
        // Safe because:
        // 1. We created this pointer from Box::leak, so we own it
        // 2. We're the only owner (no other code has this pointer)
        // 3. The permit will be properly dropped, releasing the semaphore slot
        unsafe {
            let _ = Box::from_raw(self._permit_ptr);
        }
    }
}

fn stream_recordbatch<S: Stream<Item = Result<RecordBatch, DataFusionError>>>(
    input: S,
    timeout_duration: Duration,
) -> impl Stream<Item = Result<Bytes, actix_web::Error>> {
    stream! {
            let mut first = true;  // for headers
            let stream_start = Instant::now();

            for await value_result in input {
                // Check if we've exceeded the timeout
                if stream_start.elapsed() > timeout_duration {
                    log::warn!("Stream timeout exceeded after {:?}, stopping stream", timeout_duration);
                    let timeout_error = HttpProcessingError {
                        name: format!("Stream timeout exceeded after {} seconds", timeout_duration.as_secs()),
                    };
                    yield Err(timeout_error.into());
                    break;
                }

                let value = value_result;

                yield match value {
                    Ok(r) => {
                        let buf = BufWriter::new(Vec::new());
                        //WriterBuilder::
                        let mut writer = arrow::csv::WriterBuilder::new().with_header(first).build(buf);
                        first = false;

                        match writer.write(&r) {
                             Err(e) => {
                                 let msg = format!("erreur in fetching : {}", e);
                                 let new_error = HttpProcessingError { name: msg};
                                 log::error!("{}", new_error);
                                 Err(new_error.into())
                             }
                             Ok (_) => {
                                match writer.into_inner().into_inner() { // this flush
                                    Ok(b) => {
                                    Ok(Bytes::from(b))
                                    }
                                    Err(e) => {
                                        let msg = format!("erreur in fetching : {}", e);
                                        let new_error = HttpProcessingError {  name: msg};
                                        log::error!("{}", new_error);
                                        Err(new_error.into())
                                    }
                                }
                             }
                        }
                    }
                    Err(e) => {
                        let msg = format!("error in streaming record batch: {}", e);
                        let new_error = HttpProcessingError { name: msg };
                        log::error!("{}", new_error);
                        Err(new_error.into())
                    }
                }
            }

    }
}

// async fn create_response(elements: &Vec<RecordBatch>) -> Result<Bytes, HttpProcessingError> {
//     let buf = BufWriter::new(Vec::new());
//     let mut writer = arrow::csv::Writer::new(buf);

//     for value in elements {
//         match writer.write(value) {
//             Err(e) => {
//                 let msg = format!("erreur in fetching : {}", e);
//                 let new_error = HttpProcessingError { name: msg.into() };
//                 log::error!("{}", new_error);
//                 return Err(new_error.into());
//             }
//             Ok(_) => {}
//         }
//     }
//     match writer.into_inner().into_inner() {
//         // this flush
//         Ok(b) => Ok(Bytes::from(b)),
//         Err(e) => {
//             let msg = format!("erreur in fetching : {}", e);
//             let new_error = HttpProcessingError { name: msg.into() };
//             log::error!("{}", new_error);
//             Err(new_error.into())
//         }
//     }
// }

// usage example :
// http://localhost:3000/sql/select%20year,month,day,topic,timestamp%20from%20history%20where%20topic%20=%20'home%2fesp13%2factuators%2fledstrip';

#[get("sql/{sql}")]
async fn sql_query(
    req: HttpRequest,
    sql: web::Path<String>,
) -> Result<HttpResponse, HttpProcessingError> {
    use datafusion::prelude::*;

    let d = req.app_data::<AppData>();
    if d.is_none() {
        return Err("error, no app data found".into());
    }
    let app_data = d.unwrap();

    if app_data.history_db.is_none() {
        return Err("error, no historical data found".into());
    }

    // Get the static semaphore
    let semaphore = CONCURRENT_REQUESTS_SEMAPHORE
        .get()
        .ok_or_else(|| HttpProcessingError {
            name: "error, semaphore not initialized".into(),
        })?;

    // Try to acquire permit - if not available, return error immediately
    let semaphore_permit = match semaphore.try_acquire() {
        Ok(permit) => permit,
        Err(_) => {
            return Err(HttpProcessingError {
                name: "error, too many concurrent requests".into(),
            });
        }
    };

    assert!(app_data.history_db.is_some());
    let h: Arc<History> = app_data.history_db.clone().unwrap();

    // implementation
    log::debug!("creating session");

    let profile = match app_data.config.sql_endpoint_config.analytic_profile_type {
        Some(AnalyticProfileType::Small) => {
            log::debug!("PROFILING : small profile activated");
            HistoryAnalyticProfile::Small
        }
        Some(AnalyticProfileType::Large) => HistoryAnalyticProfile::Large,
        None => HistoryAnalyticProfile::Default,
    };

    let ctx: SessionContext = create_history_sql_session(&h, profile).await?;

    let execute_options = SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false);

    log::info!("executing sql query: {}", &sql);
    let start_time = Instant::now();
    let asyncdf = ctx.sql_with_options(&sql, execute_options);
    let df = asyncdf.await?;

    let result = timeout(
        app_data.config.sql_endpoint_config.timeout_to_execute_query,
        df.execute_stream(),
    )
    .await;

    let response = match result {
        Ok(Ok(batches)) => {
            // success

            log::debug!("dataframe created, collecting");

            log::debug!("streaming content");
            let stream = stream_recordbatch(batches, app_data.config.sql_endpoint_config.timeout_to_stream);

            // Wrap the stream with the permit to keep it alive for the entire stream duration
            // The permit will be dropped when the stream completes, releasing the semaphore slot
            // 
            // Why we need this: The semaphore permit has a lifetime tied to the function scope,
            // but we need it to live for the entire stream duration (which outlives the function).
            // 
            // How it works:
            // 1. Box::leak (safe Rust) extends the lifetime to 'static by leaking the box
            // 2. We store a pointer to the leaked box
            // 3. In Drop, we reclaim the box (requires unsafe, but is safe because we're the only owner)
            // 
            // This is safe because:
            // - The semaphore is Arc<Semaphore> which is effectively 'static
            // - We're the only owner of the leaked box
            // - The box is properly reclaimed in Drop
            let permit_boxed = Box::new(semaphore_permit);
            let permit_static: &'static mut tokio::sync::SemaphorePermit<'static> = Box::leak(permit_boxed);
            let guarded_stream = StreamWithPermit {
                inner: stream,
                _permit_ptr: permit_static as *mut tokio::sync::SemaphorePermit<'static>,
            };

            let response = HttpResponseBuilder::new(StatusCode::OK)
                // .append_header(("Content-Type", "plain/text"))
                .streaming(guarded_stream);

            log::info!(
                "query executed in {} seconds, starting streaming",
                start_time.elapsed().as_secs_f64()
            );
            response
        }
        Ok(Err(e)) => {
            // query failed - permit will be dropped when function returns
            log::error!("error, query failed: {}", e);
            return Err(HttpProcessingError {
                name: format!("error, query failed: {}", e),
            });
        }
        Err(e) => {
            // timeout hit - permit will be dropped when function returns
            // query future is dropped -> execution stops
            log::error!(
                "error, query timed out after {} seconds: {}",
                app_data.config.sql_endpoint_config.timeout_to_execute_query.as_secs(),
                e
            );
            return Err(HttpProcessingError {
                name: format!(
                    "error, query timed out after {} seconds: {}",
                    app_data.config.sql_endpoint_config.timeout_to_execute_query.as_secs(),
                    e
                ),
            });
        }
    };

    // Note: semaphore permit is held by the stream wrapper and will be released
    // when the stream completes (when the HTTP response finishes)
    Ok(response)
}

/// Directory for static HTML/assets, served at `/` (fallback after `/sql/…`).
/// Override with env `RSIOTMONITOR_PAGES_DIR`; default is `./pages` (cwd at startup).
fn pages_directory() -> PathBuf {
    std::env::var("RSIOTMONITOR_PAGES_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            std::env::current_dir()
                .unwrap_or_else(|_| PathBuf::from("."))
                .join("pages")
        })
}

fn prepare_pages_root() -> Option<PathBuf> {
    let pages_dir = pages_directory();
    if !pages_dir.exists() {
        if let Err(e) = std::fs::create_dir_all(&pages_dir) {
            log::warn!("Could not create pages directory {:?}: {}", pages_dir, e);
            return None;
        }
    }
    if !pages_dir.is_dir() {
        log::warn!(
            "Static pages path {:?} is not a directory; root static serving disabled",
            pages_dir
        );
        return None;
    }
    match pages_dir.canonicalize() {
        Ok(abs) => Some(abs),
        Err(e) => {
            log::warn!(
                "Could not resolve pages directory {:?}: {}; using as-is",
                pages_dir,
                e
            );
            Some(pages_dir)
        }
    }
}

// start the server
// binding is the address and port to bind to
/// `history_db` is optional: the server still listens so `/sql` can respond when history is configured later; without history, queries return an error.
pub async fn server_start(config: HttpServerConfig, history_db: Option<Arc<History>>) {
    // And run our service using `actix`
    let addr = SocketAddr::from(config.v4_binding);

    // Initialize the static semaphore
    CONCURRENT_REQUESTS_SEMAPHORE
        .set(Arc::new(Semaphore::new(config.sql_endpoint_config.simultaneous_queries)))
        .expect("semaphore already initialized");

    let query_endpoint = AppData {
        history_db,
        config: Arc::new(config),
    };

    let pages_root = prepare_pages_root();
    if let Some(ref root) = pages_root {
        log::info!(
            "Serving static pages at / from {} (/sql/ takes precedence)",
            root.display()
        );
    }

    HttpServer::new(move || {
        // let cors = Cors::default()
        //     .send_wildcard()
        //     // .allowed_origin_fn(|_origin, _req_head| true)
        //     // .allowed_methods(vec!["GET", "POST"])
        //     // .allowed_headers(vec![http::header::AUTHORIZATION, http::header::ACCEPT])
        //     // .allowed_header(http::header::CONTENT_TYPE)
        //     .max_age(3600);

        let cors = Cors::permissive();

        let local_query: AppData = query_endpoint.clone();
        let mut app = App::new()
            .app_data(local_query)
            .wrap(middleware::DefaultHeaders::new().add(("X-Version", "0.2")))
            .wrap(middleware::Compress::default())
            .wrap(cors)
            .wrap(middleware::Logger::default())
            // Registered before static fallback so `/sql/{sql}` is never served as a file.
            .service(sql_query);

        if let Some(ref root) = pages_root {
            app = app.default_service(
                Files::new("/", root.clone())
                    .index_file("index.html")
                    .prefer_utf8(true),
            );
        }

        app
    })
    .bind(addr)
    .unwrap_or_else(|e| {
        panic!(
            "failed to bind HTTP server on {addr}: {e}. \
             Port may already be in use (check with `ss -tlnp | grep {port}` or change `[http] port` in config.toml)",
            port = addr.port()
        );
    })
    .run()
    .await
    .unwrap();
}
