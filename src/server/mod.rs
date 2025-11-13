mod health;
mod trace;

use std::sync::Arc;

pub use health::*;
use tonic::transport::{Error, Server};
use tonic_health::proto::health_server::{Health, HealthServer};
pub use trace::*;

use crate::{
    config::Config,
    mailbox::MailboxGrpc,
    middleware::{LoggingMiddlewareLayer, MinitraceGrpcMiddlewareLayer},
    object::ObjectGrpc,
    pb::{
        mailbox_service_server::MailboxServiceServer, object_service_server::ObjectServiceServer,
        public_key_service_server::PublicKeyServiceServer,
    },
    public_key::PublicKeyGrpc,
};

fn base_server(config: Arc<Config>) -> Server<LoggingMiddlewareLayer> {
    Server::builder().layer(LoggingMiddlewareLayer::new(config))
}

pub async fn configure_and_start_server(
    config: Arc<Config>,
    health_service: HealthServer<impl Health>,
    public_key_service: PublicKeyGrpc,
    mailbox_service: MailboxGrpc,
    object_service: ObjectGrpc,
) -> Result<(), Error> {
    // TODO add server fields that make sense
    if let Some(ref dd_config) = config.dd_config {
        let datadog_sender = start_trace_reporter(dd_config);

        base_server(config.clone())
            .layer(MinitraceGrpcMiddlewareLayer::new(
                config.clone(),
                dd_config.span_tags.clone(),
                datadog_sender,
            ))
            .add_service(health_service)
            .add_service(PublicKeyServiceServer::new(public_key_service))
            .add_service(MailboxServiceServer::new(mailbox_service))
            .add_service(ObjectServiceServer::new(object_service))
            .serve(config.url)
            .await?;
    } else {
        base_server(config.clone())
            .add_service(health_service)
            .add_service(PublicKeyServiceServer::new(public_key_service))
            .add_service(MailboxServiceServer::new(mailbox_service))
            .add_service(ObjectServiceServer::new(object_service))
            .serve(config.url)
            .await?;
    };

    Ok(())
}
