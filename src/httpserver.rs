use http::{
    header::{HeaderName, AUTHORIZATION, CONTENT_TYPE},
    Request, Response,
};
use hyper::{server::Server, service::make_service_fn, Body, Error};
use std::{convert::Infallible, iter::once, net::SocketAddr, sync::Arc};
use tower::{make::Shared, service_fn, ServiceBuilder};
use tower_http::{
    add_extension::AddExtensionLayer, auth::RequireAuthorizationLayer,
    compression::CompressionLayer, propagate_header::PropagateHeaderLayer,
    sensitive_headers::SetSensitiveRequestHeadersLayer, set_header::SetResponseHeaderLayer,
    trace::TraceLayer, validate_request::ValidateRequestHeaderLayer,
};

// Our request handler. This is where we would implement the application logic
// for responding to HTTP requests...
async fn handler(request: Request<Body>) -> Result<Response<Body>, Error> {
    Ok(Response::new(Body::from("Hello World")))
}

pub async fn server_start() {
    // Use tower's `ServiceBuilder` API to build a stack of tower middleware
    // wrapping our request handler.
    let service = ServiceBuilder::new()
        .layer(TraceLayer::new_for_http())
        .layer(CompressionLayer::new())
        .service_fn(handler);

    // And run our service using `hyper`
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    Server::bind(&addr)
        .serve(Shared::new(service))
        .await
        .expect("server start error");
}
