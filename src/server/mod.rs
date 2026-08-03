pub mod health;
mod middleware;
mod trace;

use tonic::transport::{Error, Server};
use tower::ServiceBuilder;

use crate::{
    AppContext,
    domain::OsError,
    pb::{
        admin_service_server::AdminServiceServer, mailbox_service_server::MailboxServiceServer,
        object_service_server::ObjectServiceServer,
        public_key_service_server::PublicKeyServiceServer,
    },
    server::{
        middleware::{
            logging_grpc::LoggingMiddlewareLayer, minitrace_grpc::MinitraceGrpcMiddlewareLayer,
        },
        trace::start_trace_reporter,
    },
};

pub struct ObjectStoreServer {
    context: AppContext,
}

impl ObjectStoreServer {
    pub async fn from_env() -> Result<Self, OsError> {
        let app_context = AppContext::from_env().await?;

        Ok(ObjectStoreServer::new(app_context))
    }

    fn new(context: AppContext) -> Self {
        Self { context }
    }

    /// 1. Run [AppContext::init]
    /// 2. Build and start server
    pub async fn start(self) -> Result<(), Error> {
        let mut context = self.context;

        log::info!("Starting server on {:?}", context.config.url);

        let health_service = context.init().await;

        let tracing_layer = if let Some(ref dd_config) = context.config.datadog {
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
                    .layer(LoggingMiddlewareLayer::new(
                        context.config.middleware.clone(),
                    ))
                    .option_layer(tracing_layer)
                    .into_inner(),
            )
            .add_optional_service(health_service)
            .add_service(AdminServiceServer::new(context.admin_service))
            .add_service(PublicKeyServiceServer::new(context.public_key_service))
            .add_service(MailboxServiceServer::new(context.mailbox_service))
            .add_service(ObjectServiceServer::new(context.object_service))
            .serve(context.config.url)
            .await?;

        Ok(())
    }
}
