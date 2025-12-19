pub mod health;
mod trace;

use tonic::transport::{Error, Server};
use tower::ServiceBuilder;

use crate::{
    AppContext,
    middleware::{LoggingMiddlewareLayer, MinitraceGrpcMiddlewareLayer},
    pb::{
        mailbox_service_server::MailboxServiceServer, object_service_server::ObjectServiceServer,
        public_key_service_server::PublicKeyServiceServer,
    },
    server::trace::start_trace_reporter,
};

/// 1. Run [AppContext::init]
/// 2. Build and start server
pub async fn configure_and_start_server(mut context: AppContext) -> Result<(), Error> {
    log::info!("Starting server on {:?}", context.config.url);

    let health_service = context.init().await;

    let tracing_layer = if let Some(ref dd_config) = context.config.dd_config {
        start_trace_reporter(dd_config);

        Some(MinitraceGrpcMiddlewareLayer::new(
            dd_config.span_tags.clone(),
        ))
    } else {
        None
    };

    Server::builder()
        .layer(
            ServiceBuilder::new()
                .layer(LoggingMiddlewareLayer::new(context.config.clone()))
                .option_layer(tracing_layer)
                .into_inner(),
        )
        .add_optional_service(health_service)
        .add_service(PublicKeyServiceServer::new(context.public_key_service))
        .add_service(MailboxServiceServer::new(context.mailbox_service))
        .add_service(ObjectServiceServer::new(context.object_service))
        .serve(context.config.url)
        .await?;

    Ok(())
}
