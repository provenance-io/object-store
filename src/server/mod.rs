pub mod health;
mod trace;

use std::sync::Arc;

use tonic::transport::{Error, Server};

use crate::{
    AppContext,
    config::Config,
    middleware::{LoggingMiddlewareLayer, MinitraceGrpcMiddlewareLayer},
    pb::{
        mailbox_service_server::MailboxServiceServer, object_service_server::ObjectServiceServer,
        public_key_service_server::PublicKeyServiceServer,
    },
    server::trace::start_trace_reporter,
};

fn base_server(config: Arc<Config>) -> Server<LoggingMiddlewareLayer> {
    Server::builder().layer(LoggingMiddlewareLayer::new(config))
}

/// 1. Run [AppContext::init]
/// 2. Build and start server
pub async fn configure_and_start_server(context: AppContext) -> Result<(), Error> {
    log::info!("Starting server on {:?}", context.config.url);

    let health_service = context.init().await;

    // TODO add server fields that make sense
    if let Some(ref dd_config) = context.config.dd_config {
        start_trace_reporter(dd_config);

        base_server(context.config.clone())
            .layer(MinitraceGrpcMiddlewareLayer::new(
                dd_config.span_tags.clone(),
            ))
            .add_optional_service(health_service)
            .add_service(PublicKeyServiceServer::new(context.public_key_service))
            .add_service(MailboxServiceServer::new(context.mailbox_service))
            .add_service(ObjectServiceServer::new(context.object_service))
            .serve(context.config.url)
            .await?
    } else {
        base_server(context.config.clone())
            .add_optional_service(health_service)
            .add_service(PublicKeyServiceServer::new(context.public_key_service))
            .add_service(MailboxServiceServer::new(context.mailbox_service))
            .add_service(ObjectServiceServer::new(context.object_service))
            .serve(context.config.url)
            .await?
    };

    Ok(())
}
