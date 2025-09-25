mod authorization;
mod cache;
mod config;
mod consts;
mod datastore;
mod dime;
mod domain;
mod mailbox;
mod middleware;
mod object;
mod proto_helpers;
mod public_key;
mod replication;
mod storage;
mod types;

use crate::cache::Cache;
use crate::config::Config;
use crate::mailbox::MailboxGrpc;
use crate::middleware::{
    report_datadog_traces, LoggingMiddlewareLayer, MinitraceGrpcMiddlewareLayer, MinitraceSpans,
};
use crate::object::ObjectGrpc;
use crate::public_key::PublicKeyGrpc;
use crate::replication::{reap_unknown_keys, replicate, ReplicationState};
use crate::storage::{FileSystem, GoogleCloud, Storage};
use crate::types::{OsError, Result};

use sqlx::{
    migrate::Migrator,
    postgres::{PgPool, PgPoolOptions},
    Executor,
};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::channel;
use tonic::transport::Server;

mod pb {
    tonic::include_proto!("objectstore");
}

use pb::mailbox_service_server::MailboxServiceServer;
use pb::object_service_server::ObjectServiceServer;
use pb::public_key_service_server::PublicKeyServiceServer;

static MIGRATOR: Migrator = sqlx::migrate!();

// TODO add logging in Trace middleware
// TODO implement checksum in filestore

async fn health_status(mut reporter: tonic_health::server::HealthReporter, db: Arc<PgPool>) {
    log::info!("Starting health status check");

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
        "file_system" => {
            Ok(Box::new(FileSystem::new(config.storage_base_path.as_str())) as Box<dyn Storage>)
        }
        "google_cloud" => Ok(Box::new(GoogleCloud::new(
            config.storage_base_url.clone(),
            config.storage_base_path.clone(),
        )) as Box<dyn Storage>),
        _ => Err(OsError::InvalidApplicationState("".to_owned())),
    }?;
    let schema = Arc::new(config.db_schema.clone());
    let pool = PgPoolOptions::new()
        .after_connect(move |conn, _meta| {
            let schema = Arc::clone(&schema);
            Box::pin(async move {
                conn.execute(format!("SET search_path = '{}';", &schema).as_ref())
                    .await?;

                Ok(())
            })
        })
        .max_connections(config.db_connection_pool_size.into())
        .connect(config.db_connection_string().as_ref())
        .await?;

    log::debug!("Running migrations");

    MIGRATOR.run(&pool).await?;

    // populate initial cache
    for key in datastore::get_all_public_keys(&pool).await? {
        log::debug!(
            "Adding public key {} with url {}",
            &key.public_key,
            &key.url
        );

        cache.add_public_key(key);
    }

    let pool = Arc::new(pool);
    let cache = Arc::new(Mutex::new(cache));
    let config = Arc::new(config);
    let storage = Arc::new(storage);

    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    let public_key_service = PublicKeyGrpc::new(Arc::clone(&cache), Arc::clone(&pool));
    let mailbox_service =
        MailboxGrpc::new(Arc::clone(&cache), Arc::clone(&config), Arc::clone(&pool));
    let object_service = ObjectGrpc::new(
        Arc::clone(&cache),
        Arc::clone(&config),
        Arc::clone(&pool),
        Arc::clone(&storage),
    );
    let replication_state = ReplicationState::new(
        Arc::clone(&cache),
        Arc::clone(&config),
        Arc::clone(&pool),
        Arc::clone(&storage),
    );

    // set initial health status and start a background
    health_reporter
        .set_service_status("", tonic_health::ServingStatus::NotServing)
        .await;
    tokio::spawn(health_status(health_reporter, Arc::clone(&pool)));

    // start replication
    if config.replication_enabled {
        tokio::spawn(replicate(replication_state));
        // start unknown reaper - removes replication objects for public_keys that moved from Unknown -> Local
        tokio::spawn(reap_unknown_keys(Arc::clone(&pool), Arc::clone(&cache)));
    }

    // start datadog reporter
    let (datadog_sender, datadog_receiver) = channel::<MinitraceSpans>(10);
    if let Some(ref dd_config) = config.dd_config {
        tokio::spawn(report_datadog_traces(
            datadog_receiver,
            dd_config.agent_host,
            dd_config.agent_port,
            dd_config.service.clone(),
        ));
    }

    log::info!("Starting server on {:?}", &config.url);

    // TODO add server fields that make sense
    if let Some(ref dd_config) = config.dd_config {
        Server::builder()
            .layer(LoggingMiddlewareLayer::new(Arc::clone(&config)))
            .layer(MinitraceGrpcMiddlewareLayer::new(
                Arc::clone(&config),
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
        Server::builder()
            .layer(LoggingMiddlewareLayer::new(Arc::clone(&config)))
            .add_service(health_service)
            .add_service(PublicKeyServiceServer::new(public_key_service))
            .add_service(MailboxServiceServer::new(mailbox_service))
            .add_service(ObjectServiceServer::new(object_service))
            .serve(config.url)
            .await?;
    }

    Ok(())
}
