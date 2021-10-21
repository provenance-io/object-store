use crate::config::Config;

use std::sync::Arc;
use std::task::{Context, Poll};

use reqwest::header::HeaderValue;
use tonic::{body::BoxBody, transport::Body};
use tower::{Layer, Service};

#[derive(Debug, Clone)]
pub struct LoggingMiddlewareLayer {
    config: Arc<Config>,
}

impl LoggingMiddlewareLayer {
    pub fn new(config: Arc<Config>) -> Self {
        Self { config }
    }
}

impl<S> Layer<S> for LoggingMiddlewareLayer {
    type Service = LoggingGrpcMiddleware<S>;

    fn layer(&self, service: S) -> Self::Service {
        LoggingGrpcMiddleware {
            inner: service,
            trace_header: self.config.clone().trace_header.clone(),
            lower_logging_bounds: self.config.logging_threshold_seconds,
            upper_logging_bounds: self.config.logging_threshold_seconds * 10f64,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LoggingGrpcMiddleware<S> {
    inner: S,
    trace_header: String,
    lower_logging_bounds: f64,
    upper_logging_bounds: f64,
}

impl<S> Service<tonic::codegen::http::Request<Body>> for LoggingGrpcMiddleware<S>
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
        let headers = req.headers().clone();
        let trace_header = self.trace_header.clone();
        let lower_logging_bounds = self.lower_logging_bounds;
        let upper_logging_bounds = self.upper_logging_bounds;

        Box::pin(async move {
            let default_trace_id = HeaderValue::from_str(&uuid::Uuid::new_v4().to_hyphenated().to_string()).unwrap();
            let trace_id = headers.get(trace_header)
                .unwrap_or(&default_trace_id)
                .to_str()
                .unwrap();
            let start = minstant::now();
            let response = inner.call(req).await?;
            let end = minstant::now();
            let elapsed_seconds = (end - start) as f64 * minstant::nanos_per_cycle() / 1_000_000_000f64;

            if elapsed_seconds > upper_logging_bounds {
                log::warn!("Trace ID: {} took {} second(s)", trace_id, elapsed_seconds);
            } else if elapsed_seconds > lower_logging_bounds {
                log::info!("Trace ID: {} took {} second(s)", trace_id, elapsed_seconds);
            } else {
                log::trace!("Trace ID: {} took {} second(s)", trace_id, elapsed_seconds);
            }

            Ok(response)
        })
    }
}
