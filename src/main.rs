use object_store::AppContext;
use object_store::config::Config;
use object_store::domain::Result;
use object_store::server::configure_and_start_server;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let app_context = AppContext::new(Config::from_env()).await?;

    configure_and_start_server(app_context).await?;

    Ok(())
}
