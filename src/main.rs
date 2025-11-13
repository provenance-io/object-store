use object_store::cache::Cache;
use object_store::config::Config;
use object_store::mailbox::MailboxGrpc;
use object_store::object::ObjectGrpc;
use object_store::public_key::PublicKeyGrpc;
use object_store::replication::{reap_unknown_keys, replicate, ReplicationState};
use object_store::server::{configure_and_start_server, init_health_service};
use object_store::storage::new_storage;
use object_store::types::Result;

use object_store::db::connect_and_migrate;

// TODO add logging in Trace middleware
// TODO implement checksum in filestore

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let config = Config::new();

    let storage = new_storage(config.clone())?;

    let pool = connect_and_migrate(config.clone()).await?;

    let cache = Cache::new(pool.clone()).await?;

    if config.replication_enabled {
        let replication_state =
            ReplicationState::new(cache.clone(), config.clone(), pool.clone(), storage.clone());

        // start replication
        tokio::spawn(replicate(replication_state));

        // start unknown reaper - removes replication objects for public_keys that moved from Unknown -> Local
        tokio::spawn(reap_unknown_keys(pool.clone(), cache.clone()));
    }

    let public_key_service = PublicKeyGrpc::new(cache.clone(), pool.clone());
    let mailbox_service = MailboxGrpc::new(cache.clone(), config.clone(), pool.clone());
    let object_service =
        ObjectGrpc::new(cache.clone(), config.clone(), pool.clone(), storage.clone());

    let health_service = init_health_service(pool.clone()).await;

    log::info!("Starting server on {:?}", &config.url);

    configure_and_start_server(
        config,
        health_service,
        public_key_service,
        mailbox_service,
        object_service,
    )
    .await?;

    Ok(())
}
