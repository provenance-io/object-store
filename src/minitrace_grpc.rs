use std::{net::{IpAddr, SocketAddr}, sync::mpsc::{Receiver, Sender}, task::{Context, Poll}};

use minitrace::{FutureExt, Span};
use minitrace_datadog::Reporter;
use tonic::{body::BoxBody, codegen::http::HeaderValue, transport::Body};
use tower::{Layer, Service};

#[derive(Debug, Clone)]
pub struct MinitraceGrpcMiddlewareLayer {
    sender: Sender<MinitraceSpans>,
}

impl MinitraceGrpcMiddlewareLayer {
    pub fn new(sender: Sender<MinitraceSpans>) -> Self {
        Self { sender }
    }
}

impl<S> Layer<S> for MinitraceGrpcMiddlewareLayer {
    type Service = MinitraceGrpcMiddleware<S>;

    fn layer(&self, service: S) -> Self::Service {
        MinitraceGrpcMiddleware {
            inner: service,
            sender: self.sender.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MinitraceGrpcMiddleware<S> {
    inner: S,
    sender: Sender<MinitraceSpans>,
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

        let sender = self.sender.clone();

        let headers = req.headers().clone();

        Box::pin(async move {
            let (root_span, collector) = Span::root("service_root");

            let response = inner.call(req).in_span(root_span).await?;

            let spans = collector.collect();

            let rand: u32 = rand::random(); // todo: what is an appropriate default span id if not present in headers, uuid? something other than random number?
            let default_trace_id_header_value = HeaderValue::from_str(&rand.to_string()).unwrap();
            let trace_id_header = headers.get("x-datadog-trace-id")
                .unwrap_or(&default_trace_id_header_value).to_str();
            let trace_id: u64 = trace_id_header.unwrap().parse::<u64>().unwrap();
            let span_id_prefix: u32 = 0;
            let default_parent_span_id_header_value = HeaderValue::from_str(&rand.to_string()).unwrap();
            let parent_span_id_header = headers.get("x-datadog-parent-id")
                .unwrap_or(&default_parent_span_id_header_value).to_str();
            let parent_span_id: u64 = parent_span_id_header.unwrap().parse::<u64>().unwrap();

            sender.send(MinitraceSpans {
                trace_id,
                parent_span_id,
                span_id_prefix,
                spans: Box::new(spans)
            }).expect("Failed to send spans to channel");

            Ok(response)
        })
    }
}

pub struct MinitraceSpans {
    trace_id: u64,
    parent_span_id: u64,
    span_id_prefix: u32,
    spans: Box<Vec<minitrace::span::Span>>
}

pub async fn report_datadog_traces(receiver: Receiver<MinitraceSpans>, host: IpAddr, port: u16, service_name: String) {
    let socket = SocketAddr::new(host, port);
    loop {
        let spans = receiver.recv().unwrap();

        let bytes = Reporter::encode(
            &service_name,
            spans.trace_id,
            spans.parent_span_id,
            spans.span_id_prefix,
            &*spans.spans,
        )
        .expect("encode error");
        Reporter::report_blocking(socket, bytes).expect("report error");
    }
}