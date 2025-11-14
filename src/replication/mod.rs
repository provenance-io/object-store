mod error;

use error::{ReplicationError, Result};

use crate::datastore::{self, reap_object_replication, replication_object_uuids};
use crate::pb::object_service_client::ObjectServiceClient;
use crate::proto_helpers::{
    create_data_chunk, create_multi_stream_header, create_stream_end, create_stream_header_field,
};
use crate::storage::{Storage, StoragePath};
use crate::AppContext;
use crate::{cache::Cache, config::Config, consts, types::OsError};

use bytes::Bytes;
use std::cmp::min;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};

use chrono::prelude::*;
use futures::stream;

use sqlx::postgres::PgPool;
use uuid::Uuid;

/// Newtype wrapper around string public keys
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct PublicKey(String);

impl PublicKey {
    fn new<S: Into<String>>(key: S) -> Self {
        Self(key.into())
    }
}

impl AsRef<str> for PublicKey {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

impl From<PublicKey> for String {
    fn from(key: PublicKey) -> String {
        key.0
    }
}

impl std::fmt::Display for PublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// ----------------------------------------------------------------------------

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

#[derive(Debug)]
pub struct ClientCache {
    clients: HashMap<String, CachedClient<ID<ObjectServiceClient<tonic::transport::Channel>>>>,
    backoff_min_wait: i64,
    backoff_max_wait: i64,
}

impl ClientCache {
    pub fn new(backoff_min_wait: i64, backoff_max_wait: i64) -> Self {
        Self {
            clients: HashMap::new(),
            backoff_min_wait,
            backoff_max_wait,
        }
    }

    /// Request a service client
    ///
    /// This assumes a one-to-one exclusive mapping between a url and an instance
    /// of `ObjectServiceClient`. Calls to `request` should be paired
    /// with an accompanying call to `restore`, which returns the client
    /// instance to the `clients` map.
    ///
    /// Potential TODO: expire clients beyond a certain age to force reconnection.
    pub async fn request(
        &mut self,
        url: &str,
    ) -> Result<Option<ID<ObjectServiceClient<tonic::transport::Channel>>>> {
        let error_url = url.to_owned();
        let min_wait_time = self.backoff_min_wait;
        let max_wait_time = self.backoff_max_wait;

        match self.clients.entry(url.to_owned()) {
            // A client was already marked as being created for the given url.
            // If it was returned via `restore`, take the client instance from
            // the cache entry and return it immediately. Otherwise, try to create a
            // new instance. If that fails, timestamp the attempt and increase the wait period.
            Entry::Occupied(mut entry) => {
                let cache_entry = entry.get_mut();
                match cache_entry.take() {
                    Some(client) => Ok(Some(client)),
                    None => {
                        // if enough time has elapsed, we can try to create another client:
                        if cache_entry.ready_to_try() {
                            match ObjectServiceClient::connect(url.to_owned()).await {
                                Ok(client) => {
                                    // mark the client in the entry as used and return it:
                                    cache_entry.used();
                                    Ok(Some(ID::new(client)))
                                }
                                Err(e) => {
                                    log::error!(
                                        "Failed to create client for {} - {:?}",
                                        error_url,
                                        e
                                    );
                                    cache_entry.wait_longer();
                                    // not ready, wait more:
                                    Ok(None)
                                }
                            }
                        } else {
                            Ok(None) // still waiting or the client is in use
                        }
                    }
                }
            }
            // If no client is present, attempt to create a new instance and
            // a new cache entry. If the client was successfully created,
            // return it immediately, otherwise record the attempt and
            // signal the initial waiting period should begin before attempting
            // to create a client again.
            Entry::Vacant(entry) => match ObjectServiceClient::connect(url.to_owned()).await {
                Ok(client) => {
                    entry.insert(CachedClient::using());
                    Ok(Some(ID::new(client)))
                }
                Err(e) => {
                    log::error!("Failed to create client for {} - {:?}", error_url, e);
                    entry.insert(CachedClient::waiting(min_wait_time, max_wait_time));
                    Ok(None)
                }
            },
        }
    }

    /// Return/restore a service client
    ///
    /// The corresponding function to `request` that returns a client
    /// to the `clients` map so it can be reused later.
    pub async fn restore(
        &mut self,
        url: &str,
        client: ID<ObjectServiceClient<tonic::transport::Channel>>,
    ) -> Result<()> {
        match self.clients.entry(url.to_owned()) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().replace(client);
                Ok(())
            }
            // Attempting to place a client into the map without having gone through
            // `request` is an error:
            Entry::Vacant(_) => Err(ReplicationError::ClientCacheError(url.to_owned())),
        }
    }
}

/// `ClientState` is used to describe the state  a `CacheEntry` can be in at
/// a given time:
///
/// - it is waiting to attempt to create a new service client instance
/// - a client has been created, but taken
/// - the client is present in the map
#[derive(Debug)]
enum ClientState<T> {
    Using,
    Waiting(DateTime<Utc>, i64, i64),
    Present(T),
}

#[derive(Debug)]
struct CachedClient<T> {
    state: ClientState<T>,
}

impl<T> CachedClient<T> {
    pub fn using() -> Self {
        Self {
            state: ClientState::Using,
        }
    }

    pub fn waiting(min_wait_time: i64, max_wait_time: i64) -> Self {
        Self {
            state: ClientState::Waiting(Utc::now(), min_wait_time, max_wait_time),
        }
    }

    pub fn take(&mut self) -> Option<T> {
        match self.state {
            ClientState::Present(_) => {
                if let ClientState::Present(client) =
                    mem::replace(&mut self.state, ClientState::Using)
                {
                    Some(client)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub fn replace(&mut self, instance: T) {
        self.state = ClientState::Present(instance);
    }

    pub fn ready_to_try(&self) -> bool {
        match self.state {
            ClientState::Using | ClientState::Present(_) => false,
            ClientState::Waiting(last_attempt, wait_time, _) => {
                (Utc::now() - last_attempt) >= chrono::Duration::seconds(wait_time)
            }
        }
    }

    pub fn used(&mut self) {
        self.state = ClientState::Using;
    }

    pub fn wait_longer(&mut self) {
        if let ClientState::Waiting(_, wait_time, max_wait_time) = self.state {
            self.state =
                ClientState::Waiting(Utc::now(), min(wait_time * 2, max_wait_time), max_wait_time)
        }
    }
}

// Grafts on an additional ID associated with the contents, but allows for
// dereferencing whatever's contained inside and also prevents cloning.
#[derive(Debug)]
pub struct ID<T>(T, Uuid);

impl<T> ID<T> {
    fn new(contents: T) -> Self {
        Self(contents, Uuid::new_v4())
    }

    #[allow(dead_code)]
    pub fn id(&self) -> &Uuid {
        &self.1
    }
}

impl<T> Deref for ID<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for ID<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

// ----------------------------------------------------------------------------

#[derive(Debug)]
pub struct ReplicationState {
    cache: Arc<Mutex<Cache>>,
    config: Arc<Config>,
    snapshot_cache: (DateTime<Utc>, Cache),
    db_pool: Arc<PgPool>,
    storage: Arc<Box<dyn Storage>>,
}

impl ReplicationState {
    pub fn new(
        cache: Arc<Mutex<Cache>>,
        config: Arc<Config>,
        db_pool: Arc<PgPool>,
        storage: Arc<Box<dyn Storage>>,
    ) -> Self {
        let snapshot_cache = cache.lock().unwrap().clone();
        let snapshot_cache = (Utc::now(), snapshot_cache);

        Self {
            cache,
            config,
            snapshot_cache,
            db_pool,
            storage,
        }
    }
}

// ----------------------------------------------------------------------------

async fn replicate_public_key(
    inner: &ReplicationState,
    mut client: ID<ObjectServiceClient<tonic::transport::Channel>>,
    public_key: PublicKey,
    url: &str,
) -> std::result::Result<ReplicationOk, ReplicationErr> {
    // unfortunately, `?` cannot be used because `client` is considered moved into
    // `ReplicationResult:err`, whereas, the use of return makes the fact explicit
    // that client cannot be used beyond the error case.
    let batch = match datastore::replication_object_uuids(
        &inner.db_pool,
        public_key.as_ref(),
        inner.config.replication_batch_size,
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
        let object = match datastore::get_object_by_uuid(&inner.db_pool, object_uuid).await {
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

            let payload = match inner
                .storage
                .fetch(&storage_path, object.dime_length as u64)
                .await
            {
                Ok(data) => data,
                Err(e) => {
                    return Err(ReplicationErr::new(
                        Into::<OsError>::into(e),
                        public_key,
                        url.to_owned(),
                        client,
                    ))
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
                match datastore::ack_object_replication(&inner.db_pool, uuid).await {
                    Ok(data) => data,
                    Err(e) => {
                        return Err(ReplicationErr::new(e, public_key, url.to_owned(), client))
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

pub async fn replicate_iteration(inner: &mut ReplicationState, client_cache: &mut ClientCache) {
    // Update snapshot cache every 5 minutes
    let now = Utc::now();
    let last_cache_update = &inner.snapshot_cache.0;
    if last_cache_update.time() + chrono::Duration::minutes(5) < now.time() {
        log::trace!("Updating snapshot cache");

        let snapshot_cache = inner.cache.lock().unwrap().clone();
        inner.snapshot_cache = (now, snapshot_cache);
    }

    let mut futures = Vec::new();

    for (public_key, value) in inner.snapshot_cache.1.get_remote_public_keys() {
        let public_key = PublicKey::new(public_key);

        match client_cache.request(&value.url).await {
            Ok(Some(client)) => {
                futures.push(replicate_public_key(
                    inner,
                    client,
                    public_key.clone(),
                    &value.url,
                ));
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

    // TODO: consider using FuturesUnordered instead of join_all

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
                        log::error!("Failed replication for {} - {:?}", public_key, error)
                    }
                    ReplicationError::CrateError(_) => {
                        log::error!("Failed replication for {} - {:?}", public_key, error)
                    }
                    ReplicationError::TonicStatusError(_) => {
                        log::error!("Failed replication for {} - {:?}", public_key, error)
                    }
                    ReplicationError::TonicTransportError(e) => {
                        log::trace!("Failed replication for {} - {:?}", public_key, e)
                    }
                }
                (client, url)
            }
        };

        if let Err(e) = client_cache.restore(&url, client).await {
            log::error!("Failed to return client for {} - {:?}", &url, e);
        }
    }
}

/// Every second, run [replicate_iteration]
pub async fn replicate(mut inner: ReplicationState) {
    log::info!("Starting replication");

    let mut client_cache =
        ClientCache::new(inner.config.backoff_min_wait, inner.config.backoff_max_wait);
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        replicate_iteration(&mut inner, &mut client_cache).await;
    }
}

pub async fn reap_unknown_keys_iteration(db_pool: &Arc<PgPool>, cache: &Arc<Mutex<Cache>>) {
    let public_keys = cache.lock().unwrap().public_keys.clone();

    let unknown_keys = public_keys.iter().filter(|(_, v)| v.url.is_empty());

    for (public_key, _) in unknown_keys {
        let result = replication_object_uuids(db_pool, public_key, 1).await;

        match result {
            Ok(result) => {
                if !result.is_empty() {
                    match reap_object_replication(db_pool, public_key).await {
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

/// Every hour, run [reap_unknown_keys_iteration]
pub async fn reap_unknown_keys(db_pool: Arc<PgPool>, cache: Arc<Mutex<Cache>>) {
    log::info!("Starting reap unknown keys");

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(60 * 60)).await;

        log::trace!("Reaping previously unknown keys");

        reap_unknown_keys_iteration(&db_pool, &cache).await;
    }
}

/// If replication is enabled:
/// 1. Start [replicate] job
/// 2. Start [reap_unknown_keys] job
pub fn init_replication(app_context: &AppContext) {
    if app_context.config.replication_enabled {
        let replication_state = ReplicationState::new(
            app_context.cache.clone(),
            app_context.config.clone(),
            app_context.db_pool.clone(),
            app_context.storage.clone(),
        );

        // start replication
        tokio::spawn(replicate(replication_state));

        // start unknown reaper - removes replication objects for public_keys that moved from Unknown -> Local
        tokio::spawn(reap_unknown_keys(
            app_context.db_pool.clone(),
            app_context.cache.clone(),
        ));
    }
}
