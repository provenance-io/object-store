pub mod client_cache;
mod error;
mod public_key;

use error::ReplicationError;
use fastrace::future::FutureExt;
use fastrace::prelude::SpanContext;
use fastrace::{Span, func_path, trace};

use crate::config::ReplicationConfig;
use crate::datastore;
use crate::pb::object_service_client::ObjectServiceClient;
use crate::proto_helpers::{
    create_data_chunk, create_multi_stream_header, create_stream_end, create_stream_header_field,
};
use crate::replication::client_cache::{ClientCache, ID};
use crate::replication::public_key::PublicKey;
use crate::storage::{Storage, StoragePath};
use crate::{cache::Cache, consts, types::OsError};

use bytes::Bytes;
use std::sync::{Arc, Mutex};

use chrono::prelude::*;
use futures::stream;

use sqlx::postgres::PgPool;

struct ReplicationOk {
    public_key: PublicKey,
    url: String,
    count: usize,
    client: ID<ObjectServiceClient<tonic::transport::Channel>>,
}

impl ReplicationOk {
    fn new(
        public_key: PublicKey,
        url: String,
        count: usize,
        client: ID<ObjectServiceClient<tonic::transport::Channel>>,
    ) -> Self {
        Self {
            public_key,
            url,
            count,
            client,
        }
    }
}

struct ReplicationErr {
    public_key: PublicKey,
    url: String,
    error: ReplicationError,
    client: ID<ObjectServiceClient<tonic::transport::Channel>>,
}

impl ReplicationErr {
    fn new<E: Into<ReplicationError>>(
        error: E,
        public_key: PublicKey,
        url: String,
        client: ID<ObjectServiceClient<tonic::transport::Channel>>,
    ) -> Self {
        Self {
            error: error.into(),
            public_key,
            url,
            client,
        }
    }
}

// ----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ReplicationState {
    cache: Arc<Mutex<Cache>>,
    config: ReplicationConfig,
    pub snapshot_cache: (DateTime<Utc>, Cache), // TODO remove
    db_pool: Arc<PgPool>,
    storage: Arc<Box<dyn Storage>>,
    pub client_cache: Arc<tokio::sync::Mutex<ClientCache>>, // TODO remove
}

impl ReplicationState {
    pub fn new(
        cache: Arc<Mutex<Cache>>,
        config: ReplicationConfig,
        db_pool: Arc<PgPool>,
        storage: Arc<Box<dyn Storage>>,
    ) -> Self {
        let snapshot_cache = cache.lock().unwrap().clone();
        let snapshot_cache = (DateTime::UNIX_EPOCH, snapshot_cache);

        let client_cache = Arc::new(tokio::sync::Mutex::new(ClientCache::new(
            config.backoff_min_wait,
            config.backoff_max_wait,
        )));

        Self {
            cache,
            config,
            snapshot_cache,
            db_pool,
            storage,
            client_cache,
        }
    }

    /// 1. Start [ReplicationState::replicate] job
    /// 2. Start [ReplicationState::reap_unknown_keys] job
    pub fn init(&mut self) {
        self.update_snapshot_cache();

        // start replication
        tokio::spawn(self.clone().replicate());
        // start unknown reaper - removes replication objects for public_keys that moved from Unknown -> Local
        tokio::spawn(self.clone().reap_unknown_keys());
    }

    #[trace(name = "replication::replicate_public_key")]
    async fn replicate_public_key(
        &self,
        mut client: ID<ObjectServiceClient<tonic::transport::Channel>>,
        public_key: PublicKey,
        url: &str,
    ) -> std::result::Result<ReplicationOk, ReplicationErr> {
        // unfortunately, `?` cannot be used because `client` is considered moved into
        // `ReplicationResult:err`, whereas, the use of return makes the fact explicit
        // that client cannot be used beyond the error case.
        let batch = match datastore::replication_object_uuids(
            &self.db_pool,
            public_key.as_ref(),
            self.config.replication_batch_size,
        )
        .await
        {
            Ok(data) => data,
            Err(e) => return Err(ReplicationErr::new(e, public_key, url.to_owned(), client)),
        };

        let batch_size = batch.len();

        if batch_size == 0 {
            return Ok(ReplicationOk::new(public_key, url.to_owned(), 0, client));
        }

        for (uuid, object_uuid) in batch.iter() {
            let object = match datastore::get_object_by_uuid(&self.db_pool, object_uuid).await {
                Ok(data) => data,
                Err(e) => return Err(ReplicationErr::new(e, public_key, url.to_owned(), client)),
            };

            let payload = if let Some(payload) = &object.payload {
                Bytes::copy_from_slice(payload.as_slice())
            } else {
                let storage_path = StoragePath {
                    dir: object.directory.clone(),
                    file: object.name.clone(),
                };

                let payload = match self.storage.fetch(&storage_path, object.dime_length).await {
                    Ok(data) => data,
                    Err(e) => {
                        return Err(ReplicationErr::new(
                            Into::<OsError>::into(e),
                            public_key,
                            url.to_owned(),
                            client,
                        ));
                    }
                };

                Bytes::copy_from_slice(payload.as_slice())
            };

            let mut packets = Vec::new();

            packets.push(create_multi_stream_header(uuid::Uuid::nil(), 1, true));

            for (idx, chunk) in payload.chunks(consts::CHUNK_SIZE).enumerate() {
                let content_length = if idx == 0 {
                    Some(object.content_length)
                } else {
                    None
                };

                packets.push(create_data_chunk(content_length, chunk.to_vec()));
            }

            for (key, value) in object.properties {
                packets.push(create_stream_header_field(key, value));
            }
            packets.push(create_stream_header_field(
                consts::SOURCE_KEY.to_owned(),
                consts::SOURCE_REPLICATION.as_bytes().to_owned(),
            ));

            packets.push(create_stream_end());

            let stream = stream::iter(packets);

            match client.put(tonic::Request::new(stream)).await {
                Ok(_) => {
                    match datastore::ack_object_replication(&self.db_pool, uuid).await {
                        Ok(data) => data,
                        Err(e) => {
                            return Err(ReplicationErr::new(e, public_key, url.to_owned(), client));
                        }
                    };
                }
                Err(status) => {
                    log::error!(
                        "Failed to put object {} for key {} - {:?}",
                        object_uuid,
                        public_key,
                        status
                    );
                }
            }
        }

        Ok(ReplicationOk::new(
            public_key,
            url.to_owned(),
            batch_size,
            client,
        ))
    }

    fn update_snapshot_cache(&mut self) {
        // Update snapshot cache every 5 minutes
        let now = Utc::now();
        let delta = now.signed_duration_since(self.snapshot_cache.0);

        if delta > self.config.snapshot_cache_refresh_frequency {
            log::trace!("Updating snapshot cache");

            let snapshot_cache = self.cache.lock().unwrap().clone();
            self.snapshot_cache = (now, snapshot_cache);
        }
    }

    pub async fn replicate_iteration(&mut self) {
        self.update_snapshot_cache();

        let mut futures = Vec::new();

        for (public_key, value) in self.snapshot_cache.1.get_remote_public_keys() {
            let public_key = PublicKey::new(public_key);

            match self.client_cache.lock().await.request(&value.url).await {
                Ok(Some(client)) => {
                    futures.push(self.replicate_public_key(client, public_key.clone(), &value.url));
                }
                Ok(None) => {
                    log::trace!("Waiting to retry for client for {}", &value.url);
                }
                Err(e) => {
                    log::error!(
                        "Failed to cache service connection {} - {:?}",
                        &value.url,
                        e
                    )
                }
            }
        }

        for result in futures::future::join_all(futures).await {
            let (client, url) = match result {
                Ok(ReplicationOk {
                    public_key,
                    url,
                    count,
                    client,
                    ..
                }) => {
                    log::trace!("Replicated {} items for {}", count, public_key);
                    (client, url)
                }
                Err(ReplicationErr {
                    error,
                    client,
                    public_key,
                    url,
                }) => {
                    match error {
                        ReplicationError::ClientCacheError(_) => {
                            log::error!("Failed replication for {} to {} - {:?}", public_key, url, error)
                        }
                        ReplicationError::CrateError(_) => {
                            log::error!("Failed replication for {} to {} - {:?}", public_key, url, error)
                        }
                        ReplicationError::TonicStatusError(_) => {
                            log::error!("Failed replication for {} to {} - {:?}", public_key, url, error)
                        }
                        ReplicationError::TonicTransportError(e) => {
                            log::trace!("Failed replication for {} to {} - {:?}", public_key, url, e)
                        }
                    }
                    (client, url)
                }
            };

            if let Err(e) = self.client_cache.lock().await.restore(&url, client).await {
                log::error!("Failed to return client for {} - {:?}", &url, e);
            }
        }
    }

    /// Run [ReplicationState::reap_unknown_keys_iteration] with a one hours delay between runs
    async fn reap_unknown_keys(self) {
        log::info!("Starting reap unknown keys");

        loop {
            log::trace!("Reaping previously unknown keys");

            self.reap_unknown_keys_iteration()
                .in_span(Span::root(func_path!(), SpanContext::random()))
                .await;

            tokio::time::sleep(self.config.reap_unknown_keys_fixed_delay).await;
        }
    }

    /// Run [ReplicationState::replicate_iteration] with a one second delay between runs
    async fn replicate(mut self) {
        log::info!("Starting replication");

        loop {
            self.replicate_iteration()
                .in_span(Span::root(func_path!(), SpanContext::random()))
                .await;

            tokio::time::sleep(self.config.replicate_fixed_delay).await;
        }
    }

    pub async fn reap_unknown_keys_iteration(&self) {
        let public_keys = self.cache.lock().unwrap().public_keys.clone();

        let unknown_keys = public_keys.iter().filter(|(_, v)| v.url.is_empty());

        for (public_key, _) in unknown_keys {
            let result = datastore::replication_object_uuids(&self.db_pool, public_key, 1).await;

            match result {
                Ok(result) => {
                    if !result.is_empty() {
                        match datastore::reap_object_replication(&self.db_pool, public_key).await {
                            Ok(rows_affected) => log::info!(
                                "Reaping public_key {} - rows_affected {}",
                                &public_key,
                                rows_affected
                            ),
                            Err(e) => log::error!(
                                "Reaper - reap_object_replication for {} - {:?}",
                                &public_key,
                                &e
                            ),
                        }
                    }
                }
                Err(e) => log::error!(
                    "Reaper - replication_object_uuids for {} - {:?}",
                    &public_key,
                    &e
                ),
            }
        }
    }
}
