use object_store::AppContext;
use object_store::config::Config;
use object_store::server::configure_and_start_server;
use object_store::types::Result;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let app_context = AppContext::new(Config::from_env()).await?;

    configure_and_start_server(app_context).await?;

    Ok(())
}
