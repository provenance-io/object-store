use object_store::config::Config;
use object_store::server::configure_and_start_server;
use object_store::types::Result;
use object_store::AppContext;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let app_context = AppContext::new(Config::from_env()).await?;

    log::info!("Starting server on {:?}", app_context.config.url);

    configure_and_start_server(app_context).await?;

    Ok(())
}
