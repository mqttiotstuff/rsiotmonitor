use actix_cors::Cors;
use actix_web::{
    http::{self, header::Header},
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
    pin::Pin,
    process::Output,
    sync::{Arc, OnceLock},
    task::{Context, Poll},
    time::{Duration, Instant},
};

use tokio::{sync::Semaphore, time::timeout};

use crate::history::{create_session, History};

use datafusion::{
    arrow::array::RecordBatch,
    dataframe::DataFrame,
    error::DataFusionError,
    execution::{
        context::SessionContext,
        memory_pool::GreedyMemoryPool,
        runtime_env::{RuntimeEnv, RuntimeEnvBuilder},
        SendableRecordBatchStream,
    },
};

use actix_web::{
    error, get,
    http::{
        header::{self, ContentType},
        Method, StatusCode,
    },
    middleware, web, Either, HttpRequest, HttpResponse, Responder, Result,
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
            name: String::from(message),
        }
    }
}

impl From<ArrowError> for HttpProcessingError {
    fn from(value: ArrowError) -> Self {
        let message = format!("{}", &value);
        Self {
            name: String::from(message),
        }
    }
}

impl<T> From<IntoInnerError<T>> for HttpProcessingError {
    fn from(value: IntoInnerError<T>) -> Self {
        Self {
            name: String::from(format!("{}", &value)),
        }
    }
}

impl From<Box<dyn std::error::Error>> for HttpProcessingError {
    fn from(value: Box<dyn std::error::Error>) -> Self {
        Self {
            name: String::from(format!("{}", &value)),
        }
    }
}

// Use default implementation for `error_response()` method
impl error::ResponseError for HttpProcessingError {}

///////////////////////////////////////////////////////////////////////////////////////////
// server implementation

const TIMEOUT_TO_ACQUIRE_SLOT: Duration = Duration::from_millis(100);

// Static semaphore for limiting concurrent requests
static CONCURRENT_REQUESTS_SEMAPHORE: OnceLock<Arc<Semaphore>> = OnceLock::new();

#[derive(Debug)]
pub enum AnalyticProfileType {
    Small,
    Large,
}

pub struct HttpServerConfig {
    pub v4_binding: (IpAddr, u16),
    pub simultaneous_queries: usize,
    pub max_attempts_to_acquire_slot: usize,
    pub timeout_to_acquire_slot: Duration,
    pub timeout_to_execute_query: Duration,
    pub timeout_to_stream: Duration, // Maximum time allowed for streaming response
    pub analytic_profile_type: Option<AnalyticProfileType>,
}

#[derive(Clone)]
struct AppData {
    pub history_db: Option<Arc<History>>,
    pub config: Arc<HttpServerConfig>,
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
                        name: format!("Stream timeout exceeded after {} seconds", timeout_duration.as_secs()).into(),
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
                                 let new_error = HttpProcessingError { name: msg.into()};
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
                                        let new_error = HttpProcessingError {  name: msg.into()};
                                        log::error!("{}", new_error);
                                        Err(new_error.into())
                                    }
                                }
                             }
                        }
                    }
                    Err(_e) => {
                        let new_error = HttpProcessingError {  name: "error in fetching".into()};
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

    if let Ok(permit) = semaphore.try_acquire() {
        let semaphore_permit = permit;

        assert!(app_data.history_db.is_some());
        let h: Arc<History> = app_data.history_db.clone().unwrap();

        // implementation
        log::debug!("creating session");

        let runtime_env = match app_data.config.analytic_profile_type {
            Some(AnalyticProfileType::Small) => {
                log::debug!("PROFILING : small profile activated");
                // restrict to using at most 50MB of memory
                let pool_size = 50 * 1024 * 1024;
                let runtime_env = RuntimeEnvBuilder::new()
                    .with_memory_pool(Arc::new(GreedyMemoryPool::new(pool_size)))
                    .build()
                    .unwrap();

                runtime_env
            }
            Some(AnalyticProfileType::Large) => {
                // restrict to using at most 1GB of memory
                let pool_size = 1024 * 1024 * 1024;
                let runtime_env = RuntimeEnvBuilder::new()
                    .with_memory_pool(Arc::new(GreedyMemoryPool::new(pool_size)))
                    .build()
                    .unwrap();
                runtime_env
            }
            None => RuntimeEnv::default(),
        };

        let config_env = match app_data.config.analytic_profile_type {
            Some(AnalyticProfileType::Small) => SessionConfig::new()
                .with_batch_size(512)
                .with_target_partitions(2),
            Some(AnalyticProfileType::Large) => SessionConfig::new()
                .with_target_partitions(8)
                .with_batch_size(2048),
            None => SessionConfig::new(),
        };

        let ctx: SessionContext = create_session(&h, config_env, runtime_env).await?;

        let execute_options = SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false)
            .with_allow_statements(false);

        log::info!("executing sql query: {}", &sql);
        let start_time = Instant::now();
        let asyncdf = ctx.sql_with_options(&sql, execute_options);
        let df = asyncdf.await?;

        let result = timeout(
            app_data.config.timeout_to_execute_query,
            df.execute_stream(),
        )
        .await;

        let response = match result {
            Ok(Ok(batches)) => {
                // success

                log::debug!("dataframe created, collecting");

                log::debug!("streaming content");
                let stream = stream_recordbatch(batches, app_data.config.timeout_to_stream);

                use futures_util::stream::StreamExt;
                let guarded_stream = stream.map(move |item| {
                    let _keep_permit = &semaphore_permit;
                    item
                });

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
                // query failed
                log::error!("error, query failed: {}", e);
                return Err(HttpProcessingError {
                    name: format!("error, query failed: {}", e).into(),
                })
                .into();
            }
            Err(e) => {
                // timeout hit
                // query future is dropped -> execution stops
                log::error!(
                    "error, query timed out after {} seconds: {}",
                    app_data.config.timeout_to_execute_query.as_secs(),
                    e
                );
                return Err(HttpProcessingError {
                    name: format!(
                        "error, query timed out after {} seconds: {}",
                        app_data.config.timeout_to_execute_query.as_secs(),
                        e
                    )
                    .into(),
                })
                .into();
            }
        };

        // release the semaphore

        return Ok(response);
    } else {
        return Err("error, too many requests".into());
    }
}

// start the server
// binding is the address and port to bind to
// history_db is the history database
pub async fn server_start(config: HttpServerConfig, history_db: &Arc<History>) {
    // And run our service using `actix`
    let addr = SocketAddr::from(config.v4_binding.clone());
    let local_history_db = history_db.clone();

    // Initialize the static semaphore
    CONCURRENT_REQUESTS_SEMAPHORE
        .set(Arc::new(Semaphore::new(config.simultaneous_queries)))
        .expect("semaphore already initialized");

    let query_endpoint = AppData {
        history_db: Some(local_history_db),
        config: Arc::new(config),
    };

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
        App::new()
            .app_data(local_query)
            .wrap(middleware::DefaultHeaders::new().add(("X-Version", "0.2")))
            .wrap(middleware::Compress::default())
            .wrap(cors)
            .wrap(middleware::Logger::default())
            .service(sql_query)
    })
    .bind(addr)
    .expect("fail to bind")
    .run()
    .await
    .unwrap();
}
