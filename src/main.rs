mod cache;
mod consts;
mod config;
mod types;
mod dime;
mod datastore;
mod domain;
mod storage;
mod object;
mod public_key;
mod mailbox;

use crate::cache::Cache;
use crate::config::Config;
use crate::object::ObjectGrpc;
use crate::public_key::PublicKeyGrpc;
use crate::mailbox::MailboxGrpc;
use crate::types::{OsError, Result};
use crate::storage::FileSystem;

use std::sync::{Arc, Mutex};
use sqlx::{migrate::Migrator, postgres::{PgPool, PgPoolOptions}, Executor};
use tonic::transport::Server;

mod pb {
    tonic::include_proto!("objectstore");
}

use pb::public_key_service_server::PublicKeyServiceServer;
use pb::object_service_server::ObjectServiceServer;
use pb::mailbox_service_server::MailboxServiceServer;

static MIGRATOR: Migrator = sqlx::migrate!();

// TODO add logging in Trace middleware
// TODO datadog apm integration
// TODO implement checksum in filestore

async fn health_status(mut reporter: tonic_health::server::HealthReporter, db: Arc<PgPool>) {
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        if let Err(err) = datastore::health_check(&db).await {
            log::warn!("Failed to health check the database connection {:?}", err);

            reporter
                .set_service_status("", tonic_health::ServingStatus::NotServing)
                .await;
        } else {
            log::trace!("Database health check success!");

            reporter
                .set_service_status("", tonic_health::ServingStatus::Serving)
                .await;
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let cache = Cache::default();
    let config = Config::new();
    let storage = match config.storage_type.as_str() {
        "file_system" => Ok(FileSystem::new(config.storage_base_path.as_str())),
        _ => Err(OsError::InvalidApplicationState("".to_owned()))
    }?;
    let schema = Arc::new(config.db_schema.clone());
    let pool = PgPoolOptions::new()
        .after_connect(move |conn| {
            let schema = Arc::clone(&schema);
            Box::pin(async move {
                conn.execute(format!("SET search_path = '{}';", &schema).as_ref()).await?;

                Ok(())
            })
        })
        .max_connections(config.db_connection_pool_size.into())
        .connect(config.db_connection_string().as_ref())
        .await?;
    let pool = Arc::new(pool);
    let cache = Arc::new(Mutex::new(cache));
    let config = Arc::new(config);

    MIGRATOR.run(&*pool).await?;

    // TODO initial cache populate

    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    let public_key_service = PublicKeyGrpc::new(Arc::clone(&cache), Arc::clone(&pool));
    let mailbox_service = MailboxGrpc::new(Arc::clone(&pool));
    let object_service = ObjectGrpc::new(Arc::clone(&cache), Arc::clone(&config), Arc::clone(&pool), storage);

    // set initial health status and start a background
    health_reporter
        .set_service_status("", tonic_health::ServingStatus::NotServing)
        .await;
    tokio::spawn(health_status(health_reporter, Arc::clone(&pool)));

    log::info!("Starting server on {:?}", &config.url);

    // TODO add server fields that make sense
    Server::builder()
        .add_service(health_service)
        .add_service(PublicKeyServiceServer::new(public_key_service))
        .add_service(MailboxServiceServer::new(mailbox_service))
        .add_service(ObjectServiceServer::new(object_service))
        .serve(config.url)
        .await?;

    Ok(())
}
