mod health;
mod trace;

use std::sync::Arc;

use tonic::transport::{Error, Server};

use crate::{
    config::Config,
    middleware::{LoggingMiddlewareLayer, MinitraceGrpcMiddlewareLayer},
    pb::{
        mailbox_service_server::MailboxServiceServer, object_service_server::ObjectServiceServer,
        public_key_service_server::PublicKeyServiceServer,
    },
    replication::init_replication,
    server::{health::init_health_service, trace::start_trace_reporter},
    AppContext,
};

fn base_server(config: Arc<Config>) -> Server<LoggingMiddlewareLayer> {
    Server::builder().layer(LoggingMiddlewareLayer::new(config))
}

/// 1. Init health service, if enabled (default: true)
/// 2. Init replication, if enabled (default: false)
pub async fn configure_and_start_server(context: AppContext) -> Result<(), Error> {
    let health_service = init_health_service(&context).await;

    init_replication(&context);

    // TODO add server fields that make sense
    if let Some(ref dd_config) = context.config.dd_config {
        let datadog_sender = start_trace_reporter(dd_config);

        base_server(context.config.clone())
            .layer(MinitraceGrpcMiddlewareLayer::new(
                context.config.clone(),
                dd_config.span_tags.clone(),
                datadog_sender,
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
