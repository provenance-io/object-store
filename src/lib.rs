use std::sync::{Arc, Mutex};

use sqlx::PgPool;
use tonic_health::proto::health_server::{Health, HealthServer};

use crate::{
    cache::Cache,
    config::Config,
    db::connect_and_migrate,
    mailbox::MailboxGrpc,
    object::ObjectGrpc,
    public_key::PublicKeyGrpc,
    replication::init_replication,
    server::health::init_health_service,
    storage::{new_storage, Storage},
    types::OsError,
};

pub mod authorization;
pub mod cache;
pub mod config;
pub mod consts;
pub mod datastore;
pub mod db;
pub mod dime;
pub mod domain;
pub mod mailbox;
pub mod middleware;
pub mod object;
pub mod proto_helpers;
pub mod public_key;
pub mod replication;
pub mod server;
pub mod storage;
pub mod types;

pub mod pb {
    tonic::include_proto!("objectstore");
}

#[derive(Debug)]
pub struct AppContext {
    pub config: Arc<Config>,
    pub cache: Arc<Mutex<Cache>>,
    pub db_pool: Arc<PgPool>,
    pub storage: Arc<Box<dyn Storage>>,
    pub public_key_service: PublicKeyGrpc,
    pub mailbox_service: MailboxGrpc,
    pub object_service: ObjectGrpc,
}

impl AppContext {
    /// 1. Connect to database and migrate
    /// 2. Initialize cache
    /// 3. Build gRPC services
    pub async fn new(config: Arc<Config>) -> Result<Self, OsError> {
        let db_pool = connect_and_migrate(&config).await?;
        let cache = Cache::new(db_pool.clone()).await?;
        let storage = new_storage(&config)?;

        let public_key_service = PublicKeyGrpc::new(cache.clone(), db_pool.clone());
        let mailbox_service = MailboxGrpc::new(cache.clone(), config.clone(), db_pool.clone());
        let object_service = ObjectGrpc::new(
            cache.clone(),
            config.clone(),
            db_pool.clone(),
            storage.clone(),
        );

        Ok(Self {
            config,
            cache,
            db_pool,
            storage,
            public_key_service,
            mailbox_service,
            object_service,
        })
    }

    /// 1. Init health service, if enabled (default: true)
    /// 2. Init replication, if enabled (default: false)
    pub async fn init(&self) -> Option<HealthServer<impl Health>> {
        let health_service = init_health_service(self).await;

        init_replication(self);

        health_service
    }
}
