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
mod server;
mod storage;
mod types;

use crate::cache::Cache;
use crate::config::Config;
use crate::mailbox::MailboxGrpc;
use crate::middleware::MinitraceGrpcMiddlewareLayer;
use crate::object::ObjectGrpc;
use crate::public_key::PublicKeyGrpc;
use crate::replication::{reap_unknown_keys, replicate, ReplicationState};
use crate::server::{base_server, init_health_service, start_trace_reporter};
use crate::storage::{FileSystem, GoogleCloud, Storage};
use crate::types::{OsError, Result};

use sqlx::{migrate::Migrator, postgres::PgPoolOptions, Executor};
use std::sync::{Arc, Mutex};

mod pb {
    tonic::include_proto!("objectstore");
}

use pb::mailbox_service_server::MailboxServiceServer;
use pb::object_service_server::ObjectServiceServer;
use pb::public_key_service_server::PublicKeyServiceServer;

static MIGRATOR: Migrator = sqlx::migrate!();

// TODO add logging in Trace middleware
// TODO implement checksum in filestore

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let config = Arc::new(Config::new());

    let storage = {
        let storage =
            match config.storage_type.as_str() {
                "file_system" => Ok(Box::new(FileSystem::new(config.storage_base_path.as_str()))
                    as Box<dyn Storage>),
                "google_cloud" => Ok(Box::new(GoogleCloud::new(
                    config.storage_base_url.clone(),
                    config.storage_base_path.clone(),
                )) as Box<dyn Storage>),
                _ => Err(OsError::InvalidApplicationState("".to_owned())),
            }?;

        Arc::new(storage)
    };

    let pool = {
        let schema = config.db_schema.clone();

        let pool = PgPoolOptions::new()
            .after_connect(move |conn, _meta| {
                let schema = schema.clone();
                Box::pin(async move {
                    conn.execute(format!("SET search_path = '{}';", schema).as_ref())
                        .await?;

                    Ok(())
                })
            })
            .max_connections(config.db_connection_pool_size.into())
            .connect(config.db_connection_string().as_ref())
            .await?;

        log::debug!("Running migrations");

        MIGRATOR.run(&pool).await?;

        Arc::new(pool)
    };

    // populate initial cache
    let cache = {
        let mut cache = Cache::default();
        for key in datastore::get_all_public_keys(&pool).await? {
            log::debug!(
                "Adding public key {} with url {}",
                &key.public_key,
                &key.url
            );

            cache.add_public_key(key);
        }

        Arc::new(Mutex::new(cache))
    };

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

    // TODO add server fields that make sense
    if let Some(ref dd_config) = config.dd_config {
        let datadog_sender = start_trace_reporter(dd_config);

        base_server(config.clone())
            .layer(MinitraceGrpcMiddlewareLayer::new(
                config.clone(),
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
        base_server(config.clone())
            .add_service(health_service)
            .add_service(PublicKeyServiceServer::new(public_key_service))
            .add_service(MailboxServiceServer::new(mailbox_service))
            .add_service(ObjectServiceServer::new(object_service))
            .serve(config.url)
            .await?;
    };

    Ok(())
}
