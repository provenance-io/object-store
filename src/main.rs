use object_store::config::Config;
use object_store::replication::init_replication;
use object_store::server::{configure_and_start_server, init_health_service};
use object_store::types::Result;
use object_store::AppContext;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let app_context = AppContext::new(Config::from_env()).await?;

    init_replication(&app_context);

    let health_service = init_health_service(&app_context).await;

    log::info!("Starting server on {:?}", app_context.config.url);

    configure_and_start_server(health_service, app_context).await?;

    Ok(())
}
