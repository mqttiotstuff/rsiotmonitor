use http::{Request, Response};
use hyper::{server::Server, Body, Error};
use std::net::{IpAddr, SocketAddr};
use tower::{make::Shared, ServiceBuilder};
use tower_http::{compression::CompressionLayer, trace::TraceLayer};

// Our request handler. This is where we would implement the application logic
// for responding to HTTP requests...
async fn handler(_request: Request<Body>) -> Result<Response<Body>, Error> {
    Ok(Response::new(Body::from("Hello World")))
}

pub async fn server_start<I>(binding: (I, u16))
where
    I: Into<IpAddr>,
{
    // Use tower's `ServiceBuilder` API to build a stack of tower middleware
    // wrapping our request handler.
    let service = ServiceBuilder::new()
        .layer(TraceLayer::new_for_http())
        .layer(CompressionLayer::new())
        .service_fn(handler);

    // And run our service using `hyper`
    let addr = SocketAddr::from(binding);
    Server::bind(&addr)
        .serve(Shared::new(service))
        .await
        .expect("server start error");
}
