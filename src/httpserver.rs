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
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use tokio::sync::Semaphore;

use crate::history::{create_session, History};

use datafusion::{
    arrow::array::RecordBatch,
    dataframe::DataFrame,
    error::DataFusionError,
    execution::{context::SessionContext, SendableRecordBatchStream},
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

const MAX_SIMULTANEOUS_QUERIES: usize = 2;
const MAX_ATTEMPTS_TO_ACQUIRE_SLOT: usize = 100;
const TIMEOUT_TO_ACQUIRE_SLOT: Duration = Duration::from_millis(100);

#[derive(Clone)]
struct Data {
    pub history_db: Arc<History>,
    pub simultaneous_queries: Arc<Semaphore>,
}

fn stream_recordbatch<S: Stream<Item = Result<RecordBatch, DataFusionError>>>(
    input: S,
) -> impl Stream<Item = Result<Bytes, actix_web::Error>> {
    stream! {
            let mut first = true;  // for headers
            for await value in input {

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

async fn create_response(elements: &Vec<RecordBatch>) -> Result<Bytes, HttpProcessingError> {
    let buf = BufWriter::new(Vec::new());
    let mut writer = arrow::csv::Writer::new(buf);

    for value in elements {
        match writer.write(value) {
            Err(e) => {
                let msg = format!("erreur in fetching : {}", e);
                let new_error = HttpProcessingError { name: msg.into() };
                log::error!("{}", new_error);
                return Err(new_error.into());
            }
            Ok(_) => {}
        }
    }
    match writer.into_inner().into_inner() {
        // this flush
        Ok(b) => Ok(Bytes::from(b)),
        Err(e) => {
            let msg = format!("erreur in fetching : {}", e);
            let new_error = HttpProcessingError { name: msg.into() };
            log::error!("{}", new_error);
            Err(new_error.into())
        }
    }
}

// usage example :
// http://localhost:3000/sql/select%20year,month,day,topic,timestamp%20from%20history%20where%20topic%20=%20'home%2fesp13%2factuators%2fledstrip';

#[get("sql/{sql}")]
async fn sql_query(
    req: HttpRequest,
    sql: web::Path<String>,
) -> Result<HttpResponse, HttpProcessingError> {
    use datafusion::prelude::*;
    let d: Option<&Data> = req.app_data();
    if d.is_none() {
        return Err("error, no historical data found".into());
    }

    let semaphore = d.unwrap().simultaneous_queries.clone();
    let mut max_attempts :i32 = MAX_ATTEMPTS_TO_ACQUIRE_SLOT as i32;
    let mut semaphore_permit;
    loop {
        let result = semaphore.acquire().await;
        match result {
            Ok(_permit) => {
                log::info!("acquired semaphore {}", _permit.num_permits());
                semaphore_permit = _permit;
                break;
            }
            Err(e) => {
                max_attempts -= 1;
                if max_attempts < 0 {
                    return Err(HttpProcessingError {
                        name: "error, no available slots for simultaneous queries".into(),
                    })
                    .into();
                }
                log::error!(
                    "error, on aquiring slot for simultaneous queries: {}, remaining attempts: {}",
                    e,
                    max_attempts
                );
                tokio::time::sleep(TIMEOUT_TO_ACQUIRE_SLOT).await;
            }
        }
    }

    assert!(d.is_some());
    let h: Arc<History> = d.unwrap().history_db.clone();

    // implementation
    log::debug!("creating session");
    let ctx: SessionContext = create_session(&h).await?;

    //  for low memory usage,
    // // Query still gets parallelized, but each partition will have more memory to use
    // SET datafusion.execution.target_partitions = 4;
    // // Smaller than the default '8192', while still keep the benefit of vectorized execution
    // SET datafusion.execution.batch_size = 1024;

    ctx.sql("SET datafusion.execution.target_partitions = 2").await?;
    ctx.sql("SET datafusion.execution.batch_size = 512").await?;

    let execute_options = SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false);

    log::info!("executing sql query: {}", &sql);
    let asyncdf = ctx.sql_with_options(&sql, execute_options);
    let df = asyncdf.await?;
    log::debug!("dataframe {:?} created, collecting", &df);
    let dfcontent = df.execute_stream().await?;

    log::debug!("streaming content");
    let stream = stream_recordbatch(dfcontent);

    let response = HttpResponseBuilder::new(StatusCode::OK)
        // .append_header(("Content-Type", "plain/text"))
        .streaming(stream);

    // release the semaphore
    drop(semaphore_permit);

    return Ok(response);
}

// start the server
// binding is the address and port to bind to
// history_db is the history database
pub async fn server_start<I>(binding: (I, u16), history_db: &Arc<History>)
where
    I: Into<IpAddr>,
{
    // And run our service using `actix`
    let addr = SocketAddr::from(binding);
    let local_history_db = history_db.clone();

    let query_endpoint = Data {
        history_db: local_history_db,
        simultaneous_queries: Arc::new(Semaphore::new(MAX_SIMULTANEOUS_QUERIES)),
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

        let local_query: Data = query_endpoint.clone();
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
