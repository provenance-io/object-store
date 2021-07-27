mod cache;
mod consts;
mod config;
mod datastore;
mod dime;
mod domain;
mod mailbox;
mod object;
mod public_key;
mod proto_helpers;
mod replication;
mod storage;
mod types;
mod minitrace_grpc;

use crate::cache::Cache;
use crate::config::Config;
use crate::minitrace_grpc::{MinitraceGrpcMiddlewareLayer, MinitraceSpans, report_datadog_traces};
use crate::object::ObjectGrpc;
use crate::public_key::PublicKeyGrpc;
use crate::mailbox::MailboxGrpc;
use crate::types::{OsError, Result};
use crate::storage::FileSystem;
use crate::replication::{reap_unknown_keys, replicate, ReplicationState};

use std::sync::mpsc::channel;
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

    let mut cache = Cache::default();
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

    // populate initial cache
    for (public_key, url) in datastore::get_all_public_keys(&pool).await? {
        if let Some(url) = url {
            log::trace!("Adding remote public key - {} {}", &public_key, &url);

            cache.add_remote_public_key(public_key, url);
        } else {
            log::trace!("Adding local public key - {}", &public_key);

            cache.add_local_public_key(public_key);
        }
    }

    log::debug!("Seeded public keys - remote: {} local: {}", cache.remote_public_keys.len(), cache.local_public_keys.len());

    let pool = Arc::new(pool);
    let cache = Arc::new(Mutex::new(cache));
    let config = Arc::new(config);
    let storage = Arc::new(storage);

    MIGRATOR.run(&*pool).await?;

    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    let public_key_service = PublicKeyGrpc::new(Arc::clone(&cache), Arc::clone(&pool));
    let mailbox_service = MailboxGrpc::new(Arc::clone(&pool));
    let object_service = ObjectGrpc::new(Arc::clone(&cache), Arc::clone(&config), Arc::clone(&pool), Arc::clone(&storage));
    let replication_state = ReplicationState::new(Arc::clone(&cache), Arc::clone(&config), Arc::clone(&pool), Arc::clone(&storage));

    // set initial health status and start a background
    health_reporter
        .set_service_status("", tonic_health::ServingStatus::NotServing)
        .await;
    tokio::spawn(health_status(health_reporter, Arc::clone(&pool)));

    // start replication
    tokio::spawn(replicate(replication_state));
    // start unknown reaper - removes replication objects for public_keys that moved from Unkown -> Local
    tokio::spawn(reap_unknown_keys(Arc::clone(&pool), Arc::clone(&cache)));
    // start datadog reporter
    let (datadog_sender, datadog_receiver) = channel::<MinitraceSpans>();
    tokio::spawn(report_datadog_traces(datadog_receiver, config.dd_agent_host.clone(), config.dd_agent_port, config.dd_service_name.clone()));

    log::info!("Starting server on {:?}", &config.url);

    // TODO add server fields that make sense
    Server::builder()
        .layer(MinitraceGrpcMiddlewareLayer { sender: datadog_sender })
        .add_service(health_service)
        .add_service(PublicKeyServiceServer::new(public_key_service))
        .add_service(MailboxServiceServer::new(mailbox_service))
        .add_service(ObjectServiceServer::new(object_service))
        .serve(config.url)
        .await?;

    Ok(())
}
