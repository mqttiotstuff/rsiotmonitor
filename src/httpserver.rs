use actix_cors::Cors;
use actix_web::{http, App, HttpResponseBuilder, HttpServer};
// use http::{Request, Response};

use arrow::error::ArrowError;

use std::{
    io::{BufWriter, IntoInnerError},
    net::{IpAddr, SocketAddr},
    sync::Arc,
};

use crate::history::{create_session, History};

use datafusion::{
    arrow::array::RecordBatch, error::DataFusionError, execution::context::SessionContext,
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

    log::debug!("execute sql {}", &sql);
    let df = ctx.sql(&sql).await?;

    log::debug!("collect elements");
    let record_batches = df.collect().await?;

    log::debug!("result received");

    // Write the record batch out as JSON
    let mut buf = BufWriter::new(Vec::new());
    {
        let mut writer = arrow::json::LineDelimitedWriter::new(&mut buf);
        let refs: Vec<&RecordBatch> = record_batches.iter().collect();
        writer.write_batches(&refs)?;
        writer.finish()?;
    }

    log::debug!("converted");

    let content = buf.into_inner()?;

    let response = HttpResponseBuilder::new(StatusCode::OK)
        .append_header(ContentType::json())
        .body(content);

    return Ok(response);
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
            .allowed_origin("*")
            .allowed_origin_fn(|_origin, _req_head| true)
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
