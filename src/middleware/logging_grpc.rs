use crate::config::Config;

use std::sync::Arc;
use std::task::{Context, Poll};

use http::{Request, Response};
use http_body::Body;
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

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for LoggingGrpcMiddleware<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>>,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    ReqBody: Body + Send + 'static,
    ReqBody::Data: Send,
    ReqBody::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    ResBody: Body + Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = futures::future::BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        let headers = req.headers().clone();
        let uri = req.uri().clone();
        let trace_header = self.trace_header.clone();
        let lower_logging_bounds = self.lower_logging_bounds;
        let upper_logging_bounds = self.upper_logging_bounds;

        let trace_id = headers
            .get(trace_header)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
            .unwrap_or_else(|| uuid::Uuid::new_v4().as_hyphenated().to_string());

        let start = minstant::Instant::now();
        let future = self.inner.call(req);

        Box::pin(async move {
            let response = future.await?;

            let elapsed_seconds = start.elapsed().as_secs_f64();

            if elapsed_seconds > upper_logging_bounds {
                log::warn!(
                    "Trace ID: {} uri={} took {:.3}s",
                    trace_id,
                    uri,
                    elapsed_seconds
                );
            } else if elapsed_seconds > lower_logging_bounds {
                log::info!(
                    "Trace ID: {} uri={} took {:.3}s",
                    trace_id,
                    uri,
                    elapsed_seconds
                );
            } else {
                log::trace!(
                    "Trace ID: {} uri={} took {:.3}s",
                    trace_id,
                    uri,
                    elapsed_seconds
                );
            }

            Ok(response)
        })
    }
}
