use fastrace::prelude::*;
use http::{Request, Response};
use http_body::Body;
use reqwest::header::HeaderMap;
use std::{
    fmt::Debug,
    task::{Context, Poll},
};
use tonic::{Code, codegen::http::HeaderValue};
use tower::{Layer, Service};

// TODO add logging in Trace middleware

#[derive(Debug, Clone)]
pub struct MinitraceGrpcMiddlewareLayer {
    span_tags: Vec<(&'static str, String)>,
}

impl MinitraceGrpcMiddlewareLayer {
    pub fn new(span_tags: Vec<(&'static str, String)>) -> Self {
        Self { span_tags }
    }
}

impl<S> Layer<S> for MinitraceGrpcMiddlewareLayer {
    type Service = MinitraceGrpcMiddleware<S>;

    fn layer(&self, service: S) -> Self::Service {
        MinitraceGrpcMiddleware {
            inner: service,
            span_tags: self.span_tags.clone(),
            default_status_code: HeaderValue::from_str("0").unwrap(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MinitraceGrpcMiddleware<S> {
    inner: S,
    span_tags: Vec<(&'static str, String)>,
    default_status_code: HeaderValue,
}

pub trait ResponseUtil {
    fn status_code(&self, default_status_code: HeaderValue) -> Code;
}

impl<T> ResponseUtil for tonic::codegen::http::Response<T> {
    fn status_code(&self, default_status_code: HeaderValue) -> Code {
        let status_code = self
            .headers()
            .get("grpc-status")
            .unwrap_or(&default_status_code)
            .to_str()
            .unwrap();

        tonic::Code::from_bytes(status_code.as_bytes())
    }
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for MinitraceGrpcMiddleware<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>>,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    ReqBody: Body + Send + 'static,
    ReqBody::Data: Send,
    ReqBody::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = futures::future::BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    /// https://docs.datadoghq.com/tracing/trace_collection/trace_context_propagation/#custom-header-formats
    fn call(&mut self, req: tonic::codegen::http::Request<ReqBody>) -> Self::Future {
        let default_status_code: HeaderValue = self.default_status_code.clone();
        let span_tags = self.span_tags.clone();

        let headers: HeaderMap = req.headers().clone();

        let root_span = {
            let parent_span_context = {
                let parent_span_id: SpanId =
                    if let Some(parent_span_id_header) = headers.get("x-datadog-parent-id") {
                        parent_span_id_header
                            .to_str()
                            .map(|h| h.parse())
                            .map(|n| n.map(SpanId).unwrap_or(SpanId(0)))
                            .unwrap_or(SpanId(0))
                    } else {
                        SpanId(0)
                    };

                let parent_trace_id: TraceId =
                    if let Some(parent_trace_id_header) = headers.get("x-datadog-trace-id") {
                        parent_trace_id_header
                            .to_str()
                            .map(|h| h.parse())
                            .map(|n| n.map(TraceId).unwrap_or(TraceId::random()))
                            .unwrap_or(TraceId::random())
                    } else {
                        TraceId::random()
                    };

                SpanContext::new(parent_trace_id, parent_span_id)
            };

            let span = Span::root("grpc.server", parent_span_context);

            if parent_span_context.span_id == SpanId(0) {
                span.add_properties(|| span_tags);
            }

            span
        };

        let future = self.inner.call(req).in_span(root_span);

        Box::pin(async move {
            let response = future.await?;

            match response.status_code(default_status_code) {
                tonic::Code::Ok => {}
                _ => {
                    log::warn!("rpc call failed");
                }
            };

            Ok(response)
        })
    }
}
