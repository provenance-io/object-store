use std::{fmt::Debug, net::{IpAddr, SocketAddr}, sync::Arc, task::{Context, Poll}};

use crate::config::Config;

use minitrace::{FutureExt, Span};
use minitrace_datadog::Reporter;
use reqwest::header::HeaderMap;
use tokio::sync::mpsc::{Receiver, Sender};
use tonic::{body::BoxBody, codegen::http::HeaderValue, transport::Body};
use tower::{Layer, Service};

// TODO move trace_async macro to take a CoW or Into<String> trait instead

#[derive(Debug, Clone)]
pub struct MinitraceGrpcMiddlewareLayer {
    config: Arc<Config>,
    span_tags: Vec<(String, String)>,
    sender: Sender<MinitraceSpans>,
}

impl MinitraceGrpcMiddlewareLayer {
    pub fn new(config: Arc<Config>, span_tags: Vec<(String, String)>, sender: Sender<MinitraceSpans>) -> Self {
        Self { config, span_tags, sender }
    }
}

impl<S> Layer<S> for MinitraceGrpcMiddlewareLayer {
    type Service = MinitraceGrpcMiddleware<S>;

    fn layer(&self, service: S) -> Self::Service {
        MinitraceGrpcMiddleware {
            inner: service,
            config: self.config.clone(),
            span_tags: self.span_tags.clone(),
            sender: self.sender.clone(),
            default_status_code: HeaderValue::from_str("0").unwrap(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MinitraceGrpcMiddleware<S> {
    inner: S,
    config: Arc<Config>,
    span_tags: Vec<(String, String)>,
    sender: Sender<MinitraceSpans>,
    default_status_code: HeaderValue,
}

impl<S> Service<tonic::codegen::http::Request<Body>> for MinitraceGrpcMiddleware<S>
where
    S: Service<tonic::codegen::http::Request<Body>, Response = tonic::codegen::http::Response<BoxBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + Debug,
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

        let config = self.config.clone();
        let mut span_tags = self.span_tags.clone();
        let default_status_code = self.default_status_code.clone();
        let sender = self.sender.clone();

        let headers = req.headers().clone();
        let mut resource = req.uri().path().chars();
        resource.next();
        let resource = resource.as_str().to_owned();

        Box::pin(async move {
            let (root_span, collector) = Span::root("grpc.server".to_owned());
            let response = inner.call(req).in_span(root_span).await?;

            let status_code = response.headers().get("grpc-status").unwrap_or(&default_status_code).to_str().unwrap();
            let status_code = tonic::Code::from_bytes(status_code.as_bytes());
            let error_code = match status_code {
                tonic::Code::Ok => {
                    span_tags.push(("status.code".to_owned(), format!("{:?}", &status_code)));
                    0i32
                },
                _ => {
                    span_tags.push(("status.code".to_owned(), format!("{:?}", &status_code)));
                    span_tags.push(("status.description".to_owned(), status_code.description().to_owned()));
                    1i32
                },
            };
            let spans: Vec<minitrace::span::Span> = collector.collect()
                .into_iter()
                .map(|mut span| {
                    if span.parent_id == 0 {
                        span.properties.extend(span_tags.clone());
                    }

                    span
                }).collect();

            let rand: u32 = rand::random(); // todo: what is an appropriate default span id if not present in headers, uuid? something other than random number?
            let default_trace_id_header_value = HeaderValue::from_str(&rand.to_string()).unwrap();
            let trace_id_header = headers.get(&config.trace_header)
                .unwrap_or(&default_trace_id_header_value).to_str();
            let trace_id: u64 = trace_id_header.unwrap().parse::<u64>().unwrap();
            let span_id_prefix: u32 = 0;
            let default_parent_span_id_header_value = HeaderValue::from_str(&rand.to_string()).unwrap();
            let parent_span_id_header = headers.get("x-datadog-parent-id")
                .unwrap_or(&default_parent_span_id_header_value).to_str();
            let parent_span_id: u64 = parent_span_id_header.unwrap().parse::<u64>().unwrap();

            sender.send(MinitraceSpans {
                r#type: String::from("rpc"),
                resource,
                error_code,
                trace_id,
                parent_span_id,
                span_id_prefix,
                spans: Box::new(spans)
            }).await.unwrap_or(());

            Ok(response)
        })
    }
}

pub struct MinitraceSpans {
    r#type: String,
    resource: String,
    error_code: i32,
    trace_id: u64,
    parent_span_id: u64,
    span_id_prefix: u32,
    spans: Box<Vec<minitrace::span::Span>>
}

pub async fn report_datadog_traces(
    mut receiver: Receiver<MinitraceSpans>,
    host: IpAddr,
    port: u16,
    service_name: String,
) {
    log::info!("Starting Datadog reporting");

    let socket = SocketAddr::new(host, port);
    let url = format!("http://{}/v0.4/traces", socket);
    let mut headers = HeaderMap::new();
    headers.append("Datadog-Meta-Tracer-Version", "v1.27.0".parse().unwrap());
    headers.append("Content-Type", "application/msgpack".parse().unwrap());
    let client = reqwest::Client::builder()
        .default_headers(headers)
        .build();

    match client {
        Ok(client) => {
            while let Some(spans) = receiver.recv().await {
                let bytes = Reporter::encode(
                    &service_name,
                    &spans.r#type,
                    &spans.resource,
                    spans.error_code,
                    spans.trace_id,
                    spans.parent_span_id,
                    spans.span_id_prefix,
                    &*spans.spans,
                );

                match bytes {
                    Ok(bytes) => {
                        let response = client.post(&url)
                            .body(bytes)
                            .send().await;

                        if let Err(error) = response {
                            log::warn!("error sending dd trace {:#?}", error);
                        }
                    },
                    Err(error) => {
                        log::warn!("Error encoding spans {:#?}", error);
                    }
                }
            }
        },
        Err(error) => {
            log::warn!("Error creating client for sending datadog traces {:#?}", error);
        }
    }

    log::info!("Datadog reporting loop is shutting down");
}
