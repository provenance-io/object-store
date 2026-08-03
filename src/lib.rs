use std::sync::{Arc, Mutex};

use tonic_health::pb::health_server::{Health, HealthServer};

use crate::{
    admin::AdminGrpc,
    config::Config,
    datastore::{Datastore, new_datastore},
    domain::OsError,
    mailbox::MailboxGrpc,
    object::ObjectGrpc,
    public_key::{PublicKeyCache, PublicKeyGrpc},
    replication::ReplicationState,
    server::health::init_health_service,
    storage::{Storage, new_storage},
};

pub mod admin;
pub mod config;
pub mod consts;
pub mod datastore;
pub mod db;
pub mod dime;
pub mod domain;
pub mod mailbox;
pub mod object;
pub mod proto;
pub mod public_key;
pub mod replication;
pub mod server;
pub mod storage;

pub mod pb {
    tonic::include_proto!("objectstore");
}

#[derive(Debug)]
pub struct AppContext {
    pub config: Arc<Config>,
    pub public_key_cache: Arc<Mutex<PublicKeyCache>>,
    pub datastore: Arc<dyn Datastore>,
    pub storage: Arc<dyn Storage>,
    pub admin_service: AdminGrpc,
    pub public_key_service: PublicKeyGrpc,
    pub mailbox_service: MailboxGrpc,
    pub object_service: ObjectGrpc,
    pub replication_state: ReplicationState,
}

impl AppContext {
    pub async fn from_env() -> Result<Self, OsError> {
        AppContext::new(Config::from_env()).await
    }

    /// 1. Connect to database and migrate
    /// 2. Initialize cache
    /// 3. Build gRPC services
    pub async fn new(config: Arc<Config>) -> Result<Self, OsError> {
        let datastore = new_datastore(&config.datastore).await?;
        let storage = new_storage(&config.storage).await?;

        let public_key_cache = {
            let initial_keys = datastore.get_all_public_keys().await?;
            PublicKeyCache::new(initial_keys).await?
        };

        let admin_service = AdminGrpc::new(config.clone());
        let public_key_service =
            PublicKeyGrpc::new(public_key_cache.clone(), config.clone(), datastore.clone());
        let mailbox_service =
            MailboxGrpc::new(public_key_cache.clone(), config.clone(), datastore.clone());
        let object_service = ObjectGrpc::new(
            public_key_cache.clone(),
            config.clone(),
            datastore.clone(),
            storage.clone(),
        );

        let replication_state = ReplicationState::new(
            public_key_cache.clone(),
            config.replication.clone(),
            datastore.clone(),
            storage.clone(),
        );

        Ok(Self {
            config,
            public_key_cache,
            datastore,
            storage,
            admin_service,
            public_key_service,
            mailbox_service,
            object_service,
            replication_state,
        })
    }

    /// 1. Init health service, if enabled (default: true)
    /// 2. Init replication, if enabled (default: false)
    pub async fn init(&mut self) -> Option<HealthServer<impl Health>> {
        if self.config.replication.enabled {
            self.replication_state.init();
        }

        init_health_service(self).await
    }
}
