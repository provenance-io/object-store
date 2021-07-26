use std::{fmt, net::SocketAddr, task::{Context, Poll}};

use minitrace::{FutureExt, Span};
use tonic::{Response, body::BoxBody, transport::Body};
use tower::{Layer, Service};

// todo: move trace reporting to separate thread w/ channel for traces

#[derive(Debug, Clone, Default)]
pub struct MinitraceGrpcMiddlewareLayer;

impl<S> Layer<S> for MinitraceGrpcMiddlewareLayer {
    type Service = MinitraceGrpcMiddleware<S>;

    fn layer(&self, service: S) -> Self::Service {
        MinitraceGrpcMiddleware { inner: service }
    }
}

#[derive(Debug, Clone)]
pub struct MinitraceGrpcMiddleware<S> {
    inner: S,
}

impl<S> Service<tonic::codegen::http::Request<Body>> for MinitraceGrpcMiddleware<S>
where
    S: Service<tonic::codegen::http::Request<Body>, Response = tonic::codegen::http::Response<BoxBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = futures::future::BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: tonic::codegen::http::Request<Body>) -> Self::Future {
        // This is necessary because tonic internally uses `tower::buffer::Buffer`.
        // See https://github.com/tower-rs/tower/issues/547#issuecomment-767629149
        // for details on why this is necessary
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        Box::pin(async move {
            let (root_span, collector) = Span::root("root2");

            let response = inner.call(req).in_span(root_span).await?;

            let spans = collector.collect();

            // todo: set up reporting to DD w/ trace id pulled from headers

            Ok(response)
        })
    }
}