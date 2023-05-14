use tower_http::{
    add_extension::AddExtensionLayer,
    compression::CompressionLayer,
    propagate_header::PropagateHeaderLayer,
    auth::RequireAuthorizationLayer,
    sensitive_headers::SetSensitiveRequestHeadersLayer,
    set_header::SetResponseHeaderLayer,
    trace::TraceLayer,
    validate_request::ValidateRequestHeaderLayer,
};
use tower::{ServiceBuilder, service_fn, make::Shared};
use http::{Request, Response, header::{HeaderName, CONTENT_TYPE, AUTHORIZATION}};
use hyper::{Body, Error, server::Server, service::make_service_fn};
use std::{sync::Arc, net::SocketAddr, convert::Infallible, iter::once};


// Our request handler. This is where we would implement the application logic
// for responding to HTTP requests...
async fn handler(request: Request<Body>) -> Result<Response<Body>, Error> {
    Ok(Response::new(Body::from("Hello World")))
}


pub async fn server_start() {
    // Use tower's `ServiceBuilder` API to build a stack of tower middleware
    // wrapping our request handler.
    let service = ServiceBuilder::new()
        // Mark the `Authorization` request header as sensitive so it doesn't show in logs
        // .layer(SetSensitiveRequestHeadersLayer::new(once(AUTHORIZATION)))
        // High level logging of requests and responses
        .layer(TraceLayer::new_for_http())
        // Share an `Arc<State>` with all requests
       //  .layer(AddExtensionLayer::new(Arc::new(state)))
        // Compress responses
        .layer(CompressionLayer::new())
        // Propagate `X-Request-Id`s from requests to responses
        // .layer(PropagateHeaderLayer::new(HeaderName::from_static(
        //     "x-request-id",
        // )))
        // If the response has a known size set the `Content-Length` header
        // .layer(SetResponseHeaderLayer::overriding(
        //     CONTENT_TYPE,
        //     content_length_from_response,
        // ))
        // Authorize requests using a token
        // .layer(RequireAuthorizationLayer::bearer("passwordlol"))
        // Accept only application/json, application/* and */* in a request's ACCEPT header
        //.layer(ValidateRequestHeaderLayer::accept("application/json"))
        // Wrap a `Service` in our middleware stack
        .service_fn(handler);

    // And run our service using `hyper`
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    Server::bind(&addr)
        .serve(Shared::new(service))
        .await
        .expect("server error");
}
