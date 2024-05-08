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
};

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

#[derive(Clone)]
struct Data {
    pub history_db: Arc<History>,
}

fn stream_recordbatch<S: Stream<Item = Result<RecordBatch, DataFusionError>>>(
    input: S,
) -> impl Stream<Item = Result<Bytes, actix_web::Error>> {
    stream! {
            for await value in input {

                yield match value {
                    Ok(r) => {
                        let buf = BufWriter::new(Vec::new());
                        let mut writer = arrow::csv::Writer::new(buf);

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

    assert!(d.is_some());
    let h: Arc<History> = d.unwrap().history_db.clone();

    // implementation
    log::debug!("creating session");
    let ctx: SessionContext = create_session(&h).await?;
    let execute_options = SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false);

    log::info!("execute sql {}", &sql);
    let asyncdf = ctx.sql_with_options(&sql, execute_options);
    let df = asyncdf.await?;
    log::info!("dataframe {:?} created, collecting", &df);
    let dfcontent = df.execute_stream().await?;

    log::info!("streaming content");
    let stream = stream_recordbatch(dfcontent);

    let response = HttpResponseBuilder::new(StatusCode::OK)
        // .append_header(("Content-Type", "plain/text"))
        .streaming(stream);

    return Ok(response);

    // let elements = df.collect().await?;
    // log::info!("end of execution of {}", &sql);

    // let response = HttpResponseBuilder::new(StatusCode::OK).body(create_response(&elements).await?);
    // return Ok(response);


}

pub async fn server_start<I>(binding: (I, u16), history_db: &Arc<History>)
where
    I: Into<IpAddr>,
{
    // And run our service using `hyper`
    let addr = SocketAddr::from(binding);
    let local_history_db = history_db.clone();

    let query_endpoint = Data {
        history_db: local_history_db,
    };

    HttpServer::new(move || {
        let cors = Cors::default()
            .send_wildcard()
            // .allowed_origin_fn(|_origin, _req_head| true)
            .allowed_methods(vec!["GET", "POST"])
            .allowed_headers(vec![http::header::AUTHORIZATION, http::header::ACCEPT])
            .allowed_header(http::header::CONTENT_TYPE)
            .max_age(3600);

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
