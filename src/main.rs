use object_store::domain::Result;
use object_store::server::ObjectStoreServer;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    ObjectStoreServer::from_env().await?.start().await?;

    Ok(())
}
