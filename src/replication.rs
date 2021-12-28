use crate::{cache::Cache, config::Config, consts, types::OsError};
use crate::datastore::{self, reap_object_replication, replication_object_uuids};
use crate::storage::{Storage, StoragePath};
use crate::pb::object_service_client::ObjectServiceClient;
use crate::proto_helpers::{create_data_chunk, create_multi_stream_header, create_stream_header_field, create_stream_end};

use bytes::Bytes;
use std::cmp::min;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::collections::hash_map::Entry;

use chrono::prelude::*;
use futures::stream;
use sqlx::postgres::PgPool;
use quick_error::quick_error;
use uuid::Uuid;

// ----------------------------------------------------------------------------

// TODO implement remaining tests

// ----------------------------------------------------------------------------

quick_error! {
    #[derive(Debug)]
    enum ReplicationError {
        CrateError(err: OsError) {
            from()
        }
        TonicTransportError(err: tonic::transport::Error) {
            from()
        }
        TonicStatusError(err: tonic::Status) {
            from()
        }
        ClientCacheError(url: String) {
            display("no cached client found for {}", url)
        }
    }
}

type Result<T> = std::result::Result<T, ReplicationError>;

// ----------------------------------------------------------------------------

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
    client: ID<ObjectServiceClient<tonic::transport::Channel>>
}

impl ReplicationOk {
    fn new(public_key: PublicKey, url: String, count: usize, client: ID<ObjectServiceClient<tonic::transport::Channel>>) -> Self {
        Self {
            public_key,
            url,
            count,
            client
        }
    }
}

struct ReplicationErr {
    public_key: PublicKey,
    url: String,
    error: ReplicationError,
    client: ID<ObjectServiceClient<tonic::transport::Channel>>
}

impl ReplicationErr {
    fn new<E: Into<ReplicationError>>(error: E, public_key: PublicKey, url: String, client: ID<ObjectServiceClient<tonic::transport::Channel>>) -> Self {
        Self {
            error: error.into(),
            public_key,
            url,
            client
        }
    }
}

// ----------------------------------------------------------------------------

#[derive(Debug)]
struct ClientCache {
    clients: HashMap<String, CachedClient<ID<ObjectServiceClient<tonic::transport::Channel>>>>,
    backoff_min_wait: i64,
    backoff_max_wait: i64
}

impl ClientCache {
    fn new(backoff_min_wait: i64, backoff_max_wait: i64) -> Self {
        Self {
            clients: HashMap::new(),
            backoff_min_wait,
            backoff_max_wait
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
    async fn request(&mut self, url: &String) -> Result<Option<ID<ObjectServiceClient<tonic::transport::Channel>>>> {
        let error_url = url.clone();
        let min_wait_time = self.backoff_min_wait;
        let max_wait_time = self.backoff_max_wait;

        match self.clients.entry(url.clone()) {
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
                            match ObjectServiceClient::connect(url.clone()).await {
                                Ok(client) => {
                                    // mark the client in the entry as used and return it:
                                    cache_entry.used();
                                    Ok(Some(ID::new(client)))
                                },
                                Err(e) => {
                                    log::error!("Failed to create client for {} - {:?}", error_url, e);
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
            },
            // If no client is present, attempt to create a new instance and
            // a new cache entry. If the client was successfully created,
            // return it immediately, otherwise record the attempt and
            // signal the initial waiting period should begin before attempting
            // to create a client again.
            Entry::Vacant(entry) => {
                match ObjectServiceClient::connect(url.clone()).await {
                    Ok(client) => {
                        entry.insert(CachedClient::using());
                        Ok(Some(ID::new(client)))
                    },
                    Err(e) => {
                        log::error!("Failed to create client for {} - {:?}", error_url, e);
                        entry.insert(CachedClient::waiting(min_wait_time, max_wait_time));
                        Ok(None)
                    }
                }
            }
        }
    }

    /// Return/restore a service client
    ///
    /// The corresponding function to `request` that returns a client
    /// to the `clients` map so it can be reused later.
    async fn restore(&mut self, url: &String, client: ID<ObjectServiceClient<tonic::transport::Channel>>) -> Result<()> {
        match self.clients.entry(url.clone()) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().replace(client);
                Ok(())
            },
            // Attempting to place a client into the map without having gone through
            // `request` is an error:
            Entry::Vacant(_) => Err(ReplicationError::ClientCacheError(url.clone()))
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
    Present(T)
}

#[derive(Debug)]
struct CachedClient<T> {
    state: ClientState<T>,
}

impl <T> CachedClient<T> {

    pub fn using() -> Self {
        Self { state: ClientState::Using }
    }

    pub fn waiting(min_wait_time: i64, max_wait_time: i64) -> Self {
        Self { state: ClientState::Waiting(Utc::now(), min_wait_time, max_wait_time) }
    }

    pub fn take(&mut self) -> Option<T> {
        match self.state {
            ClientState::Present(_) =>
                if let ClientState::Present(client) = mem::replace(&mut self.state, ClientState::Using) {
                    Some(client)
                } else {
                    None
                },
            _ => None
        }
    }

    pub fn replace(&mut self, instance: T) {
        self.state = ClientState::Present(instance);
    }

    pub fn ready_to_try(&self) -> bool {
        match self.state {
            ClientState::Using | ClientState::Present(_) => false,
            ClientState::Waiting(last_attempt, wait_time, _) => (Utc::now() - last_attempt) >= chrono::Duration::seconds(wait_time)
        }
    }

    pub fn used(&mut self) {
        self.state = ClientState::Using;
    }

    pub fn wait_longer(&mut self) {
        match self.state {
            ClientState::Waiting(_, wait_time, max_wait_time) => {
                self.state = ClientState::Waiting(Utc::now(), min(wait_time * 2, max_wait_time), max_wait_time)
            },
            _ => {}
        }
    }
}

// Grafts on an additional ID associated with the contents, but allows for
// dereferencing whatever's contained inside and also prevents cloning.
#[derive(Debug)]
struct ID<T>(T, Uuid);

impl <T> ID<T> {
    fn new(contents: T) -> Self {
        Self(contents, Uuid::new_v4())
    }

    #[allow(dead_code)]
    fn id(&self) -> &Uuid {
        &self.1
    }
}

impl <T> Deref for ID<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl <T> DerefMut for ID<T> {
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
    storage: Arc<Box<dyn Storage>>
}

impl ReplicationState {
    pub fn new(cache: Arc<Mutex<Cache>>, config: Arc<Config>, db_pool: Arc<PgPool>, storage: Arc<Box<dyn Storage>>) -> Self {
        let snapshot_cache = cache.lock().unwrap().clone();
        let snapshot_cache = (Utc::now(), snapshot_cache);

        Self { cache, config, snapshot_cache, db_pool, storage }
    }
}

// ----------------------------------------------------------------------------

async fn replicate_public_key(inner: &ReplicationState,
                              mut client: ID<ObjectServiceClient<tonic::transport::Channel>>,
                              public_key: PublicKey,
                              url: &String) -> std::result::Result<ReplicationOk, ReplicationErr> {

    // unfortunately, `?` cannot be used because `client` is considered moved into
    // `ReplicationResult:err`, whereas, the use of return makes the fact explicit
    // that client cannot be used beyond the error case.
    let batch = match datastore::replication_object_uuids(&inner.db_pool, public_key.as_ref(), inner.config.replication_batch_size).await {
        Ok(data) => data,
        Err(e) => return Err(ReplicationErr::new(e, public_key, url.clone(), client))
    };

    let batch_size = batch.len();

    if batch_size == 0 {
        return Ok(ReplicationOk::new(public_key, url.clone(), 0, client));
    }

    for (uuid, object_uuid) in batch.iter() {

        let object = match datastore::get_object_by_uuid(&inner.db_pool, object_uuid).await {
            Ok(data) => data,
            Err(e) => return Err(ReplicationErr::new(e, public_key, url.clone(), client))
        };

        let payload = if let Some(payload) = &object.payload {
            Bytes::copy_from_slice(payload.as_slice())
        } else {
            let storage_path = StoragePath {
                dir: object.directory.clone(),
                file: object.name.clone(),
            };

            let payload = match inner.storage.fetch(&storage_path, object.dime_length as u64).await {
                Ok(data) => data,
                Err(e) => return Err(ReplicationErr::new(Into::<OsError>::into(e), public_key, url.clone(), client))
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
        packets.push(create_stream_header_field(consts::SOURCE_KEY.to_owned(), consts::SOURCE_REPLICATION.as_bytes().to_owned()));

        packets.push(create_stream_end());

        let stream = stream::iter(packets);

        match client.put(tonic::Request::new(stream)).await {
            Ok(_) => {
                match datastore::ack_object_replication(&inner.db_pool, uuid).await {
                    Ok(data) => data,
                    Err(e) => return Err(ReplicationErr::new(e, public_key, url.clone(), client))
                };
            },
            Err(status) => {
                log::error!("Failed to put object {} for key {} - {:?}", object_uuid, public_key, status);
            }
        }
    }

    Ok(ReplicationOk::new(public_key, url.clone(), batch_size, client))
}

async fn replicate_iteration(inner: &mut ReplicationState, client_cache: &mut ClientCache) {

    // Update snapshot cache every 5 minutes
    let last_cache_update = &inner.snapshot_cache.0;
    if last_cache_update.time() + chrono::Duration::minutes(5) < Utc::now().time() {
        log::trace!("Updating snapshot cache");

        let snapshot_cache = inner.cache.lock().unwrap().clone();
        inner.snapshot_cache = (Utc::now(), snapshot_cache);
    }

    let mut futures = Vec::new();

    for (public_key, value) in inner.snapshot_cache.1.get_remote_public_keys() {

        let public_key = PublicKey::new(public_key);

        match client_cache.request(&value.url).await {
            Ok(Some(client)) => {
                futures.push(replicate_public_key(&inner, client, public_key.clone(), &value.url));
            },
            Ok(None) => {
                log::trace!("Waiting to retry for client for {}", &value.url);
            },
            Err(e) => {
                log::error!("Failed to cache service connection {} - {:?}", &value.url, e)
            }
        }
    }

    // TODO: consider using FuturesUnordered instead of join_all

    for result in futures::future::join_all(futures).await {

        let (client, url) = match result {
            Ok(ReplicationOk { public_key, url, count, client, .. }) => {
                log::trace!("Replicated {} items for {}", count, public_key);
                (client, url)
            },
            Err(ReplicationErr { error, client, public_key, url }) => {
                match error {
                    ReplicationError::ClientCacheError(_) => {
                        log::error!("Failed replication for {} - {:?}", public_key, error)
                    },
                    ReplicationError::CrateError(_) => {
                        log::error!("Failed replication for {} - {:?}", public_key, error)
                    },
                    ReplicationError::TonicStatusError(_) => {
                        log::error!("Failed replication for {} - {:?}", public_key, error)
                    },
                    ReplicationError::TonicTransportError(e) => {
                        log::trace!("Failed replication for {} - {:?}", public_key, e)
                    },
                }
                (client, url)
            }
        };

        if let Err(e) = client_cache.restore(&url, client).await {
            log::error!("Failed to return client for {} - {:?}", &url, e);
        }
    }
}

pub async fn replicate(mut inner: ReplicationState) {
    log::info!("Starting replication");

    let mut client_cache = ClientCache::new(inner.config.backoff_min_wait, inner.config.backoff_max_wait);
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        replicate_iteration(&mut inner, &mut client_cache).await;
    }
}

async fn reap_unknown_keys_iteration(db_pool: &Arc<PgPool>, cache: &Arc<Mutex<Cache>>) {
    let public_keys = cache.lock().unwrap().public_keys.clone();

    for (public_key, _) in public_keys.iter().filter(|(_, v)| v.url.is_empty()) {
        let result = replication_object_uuids(&db_pool, &public_key, 1).await;

        match result {
            Ok(result) => {
                if !result.is_empty() {
                    match reap_object_replication(&db_pool, &public_key).await {
                        Ok(rows_affected) => log::info!("Reaping public_key {} - rows_affected {}", &public_key, rows_affected),
                        Err(e) => log::error!("Reaper - reap_object_replication for {} - {:?}", &public_key, &e),
                    }
                }
            },
            Err(e) => log::error!("Reaper - replication_object_uuids for {} - {:?}", &public_key, &e),
        }
    }
}

pub async fn reap_unknown_keys(db_pool: Arc<PgPool>, cache: Arc<Mutex<Cache>>) {
    log::info!("Starting reap unknown keys");

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(60 * 60)).await;

        log::trace!("Reaping previously unknown keys");

        reap_unknown_keys_iteration(&db_pool, &cache).await;
    }
}

#[cfg(test)]
pub mod tests {
    use crate::MIGRATOR;
    use crate::config::Config;
    use crate::datastore::{AuthType, KeyType, PublicKey, replication_object_uuids};
    use crate::object::*;
    use crate::object::tests::*;
    use crate::pb::{self};
    use crate::replication::*;
    use crate::storage::FileSystem;

    use std::collections::HashMap;

    use futures::StreamExt;
    use sqlx::postgres::{PgPool, PgPoolOptions};
    use tonic::transport::Channel;

    use serial_test::serial;
    use testcontainers::*;

    pub fn test_config_one() -> Config {
        Config {
            url: "0.0.0.0:6789".parse().unwrap(),
            uri_host: String::default(),
            db_connection_pool_size: 0,
            db_host: String::default(),
            db_port: 0,
            db_user: String::default(),
            db_password: String::default(),
            db_database: String::default(),
            db_schema: String::default(),
            storage_type: "file_system_one".to_owned(),
            storage_base_url: None,
            storage_base_path: "/tmp".to_owned(),
            storage_threshold: 5000,
            replication_enabled: true,
            replication_batch_size: 2,
            dd_config: None,
            backoff_min_wait: 5,
            backoff_max_wait: 5,
            logging_threshold_seconds: 1f64,
            trace_header: String::default(),
            user_auth_enabled: false,
        }
    }

    pub fn test_config_one_no_replication() -> Config {
        Config {
            url: "0.0.0.0:6789".parse().unwrap(),
            uri_host: String::default(),
            db_connection_pool_size: 0,
            db_host: String::default(),
            db_port: 0,
            db_user: String::default(),
            db_password: String::default(),
            db_database: String::default(),
            db_schema: String::default(),
            storage_type: "file_system_one".to_owned(),
            storage_base_url: None,
            storage_base_path: "/tmp".to_owned(),
            storage_threshold: 5000,
            replication_enabled: false,
            replication_batch_size: 2,
            dd_config: None,
            backoff_min_wait: 5,
            backoff_max_wait: 5,
            logging_threshold_seconds: 1f64,
            trace_header: String::default(),
            user_auth_enabled: false,
        }
    }

    pub fn test_config_two() -> Config {
        Config {
            url: "0.0.0.0:6790".parse().unwrap(),
            uri_host: String::default(),
            db_connection_pool_size: 0,
            db_host: String::default(),
            db_port: 0,
            db_user: String::default(),
            db_password: String::default(),
            db_database: String::default(),
            db_schema: String::default(),
            storage_type: "file_system_two".to_owned(),
            storage_base_url: None,
            storage_base_path: "/tmp".to_owned(),
            storage_threshold: 5000,
            replication_enabled: true,
            replication_batch_size: 2,
            dd_config: None,
            backoff_min_wait: 5,
            backoff_max_wait: 5,
            logging_threshold_seconds: 1f64,
            trace_header: String::default(),
            user_auth_enabled: false,
        }
    }

    pub async fn setup_postgres(container: &Container<'_, clients::Cli, images::postgres::Postgres>) -> PgPool {
        let connection_string = &format!(
            "postgres://postgres:postgres@localhost:{}/postgres",
            container.get_host_port(5432).unwrap(),
        );

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&connection_string)
            .await
            .unwrap();

        MIGRATOR.run(&pool).await.unwrap();

        pool
    }

    fn set_cache(cache: &mut Cache) {
        cache.add_public_key(PublicKey {
            uuid: uuid::Uuid::default(),
            public_key: std::str::from_utf8(&party_1().0.public_key).unwrap().to_owned(),
            public_key_type: KeyType::Secp256k1,
            url: String::from(""),
            metadata: Vec::default(),
            auth_type: Some(AuthType::Header),
            auth_data: Some(String::from("X-Test-Header:test_value")),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        });
        cache.add_public_key(PublicKey {
            uuid: uuid::Uuid::default(),
            public_key: std::str::from_utf8(&party_2().0.public_key).unwrap().to_owned(),
            public_key_type: KeyType::Secp256k1,
            url: String::from("tcp://0.0.0.0:6790"),
            metadata: Vec::default(),
            auth_type: Some(AuthType::Header),
            auth_data: Some(String::from("X-Test-Header:test_value")),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        });
    }

    async fn start_server_one(config_override: Option<Config>) -> ReplicationState {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);

        tokio::spawn(async move {
            let mut cache = Cache::default();
            set_cache(&mut cache);
            let cache = Mutex::new(cache);
            let config = config_override.unwrap_or(test_config_one());
            let url = config.url.clone();
            let docker = clients::Cli::default();
            let image = images::postgres::Postgres::default().with_version(9);
            let container = docker.run(image);
            let pool = setup_postgres(&container).await;
            let storage = FileSystem::new(config.storage_base_path.as_str());
            let cache = Arc::new(cache);
            let config = Arc::new(config);
            let db_pool = Arc::new(pool);
            let storage: Arc<Box<dyn Storage>> = Arc::new(Box::new(storage));
            let replication_state = ReplicationState::new(Arc::clone(&cache), Arc::clone(&config), Arc::clone(&db_pool), Arc::clone(&storage));
            let object_service = ObjectGrpc {
                cache: Arc::clone(&cache),
                config: Arc::clone(&config),
                db_pool: Arc::clone(&db_pool),
                storage: Arc::clone(&storage),
            };

            tx.send(replication_state).await.unwrap();

            tonic::transport::Server::builder()
                .add_service(pb::object_service_server::ObjectServiceServer::new(object_service))
                .serve(url)
                .await
                .unwrap()
        });

        rx.recv().await.unwrap()
    }

    async fn start_server_two() -> ReplicationState {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);

        tokio::spawn(async move {
            let mut cache = Cache::default();
            set_cache(&mut cache);
            let cache = Mutex::new(cache);
            let config = test_config_two();
            let url = config.url.clone();
            let docker = clients::Cli::default();
            let image = images::postgres::Postgres::default().with_version(9);
            let container = docker.run(image);
            let pool = setup_postgres(&container).await;
            let storage = FileSystem::new(config.storage_base_path.as_str());
            let cache = Arc::new(cache);
            let config = Arc::new(config);
            let db_pool = Arc::new(pool);
            let storage: Arc<Box<dyn Storage>> = Arc::new(Box::new(storage));
            let replication_state = ReplicationState::new(Arc::clone(&cache), Arc::clone(&config), Arc::clone(&db_pool), Arc::clone(&storage));
            let object_service = ObjectGrpc {
                cache: Arc::clone(&cache),
                config: Arc::clone(&config),
                db_pool: Arc::clone(&db_pool),
                storage: Arc::clone(&storage),
            };

            tx.send(replication_state).await.unwrap();

            tonic::transport::Server::builder()
                .add_service(pb::object_service_server::ObjectServiceServer::new(object_service))
                .serve(url)
                .await
                .unwrap()
        });

        rx.recv().await.unwrap()
    }

    pub async fn get_object_count(db: &PgPool) -> i64 {
        let row: (i64,) = sqlx::query_as("SELECT count(*) as count FROM object")
            .fetch_one(db)
            .await
            .unwrap();

        row.0
    }

    async fn get_client_one() -> pb::object_service_client::ObjectServiceClient<Channel> {
        // allow server to start
        tokio::time::sleep(tokio::time::Duration::from_millis(1_000)).await;

        pb::object_service_client::ObjectServiceClient::connect("tcp://0.0.0.0:6789").await.unwrap()
    }

    async fn get_client_two() -> pb::object_service_client::ObjectServiceClient<Channel> {
        // allow server to start
        tokio::time::sleep(tokio::time::Duration::from_millis(1_000)).await;

        pb::object_service_client::ObjectServiceClient::connect("tcp://0.0.0.0:6790").await.unwrap()
    }

    #[tokio::test]
    #[serial(grpc_server)]
    async fn client_caching() {
        let _state_one = start_server_one(None).await;

        let config_one = test_config_one();
        let config_two = test_config_two();

        let url_one = format!("tcp://{}", config_one.url);
        let url_two = format!("tcp://{}", config_two.url);

        let mut client_cache = ClientCache::new(config_one.backoff_min_wait, config_one.backoff_max_wait);

        // Client 1:

        // Check that ReplicationState connection caching works when given the same URL:
        let client_one = client_cache.request(&url_one).await.unwrap().unwrap();
        // the result from calling again with the same URL should be empty since the client is in use:
        assert_eq!(client_cache.request(&url_one).await.unwrap().is_none(), true);

        let client_one_id_first = client_one.id().clone();
        client_cache.restore(&url_one, client_one).await.unwrap();
        // we should get an instance again:
        let client_one = client_cache.request(&url_one).await.unwrap().unwrap();
        let client_one_id_second = client_one.id().clone();
        // The IDs of the the two clients should be the same, since they originated from the
        // same URL:
        assert_eq!(client_one_id_first, client_one_id_second);

        // Client 2:

        let client_two = client_cache.request(&url_two).await.unwrap();
        // client 2 should be empty because server two is not running:
        assert_eq!(client_two.is_none(), true);

        let _state_two = start_server_two().await;
        let client_two = client_cache.request(&url_two).await.unwrap();
        // client 2 will still fail even if state_two is ready because there's still time to wait
        // out until the client can attempt to be created again.
        assert_eq!(client_two.is_none(), true);

        // wait a little more and it should work:
        tokio::time::sleep(tokio::time::Duration::from_millis(2_000)).await;

        let client_two = client_cache.request(&url_two).await.unwrap().unwrap();

        // also client_two should be distinct from client one:
        assert_ne!(client_one_id_first, client_two.id().clone());
    }

    #[tokio::test]
    #[serial(grpc_server)]
    async fn replication_can_be_disabled() {
        let state_one = start_server_one(Some(test_config_one_no_replication())).await;

        let (audience1, signature1) = party_1();
        let (audience2, signature2) = party_2();
        let (audience3, signature3) = party_3();

        // put 3 objects for party_1, party_2, party_3
        let dime = generate_dime(vec![audience1.clone(), audience2.clone(), audience3.clone()], vec![signature1.clone(), signature2.clone(), signature3.clone()]);
        let payload: bytes::Bytes = "testing small payload 2".as_bytes().into();
        let chunk_size = 500; // full payload in one packet
        let response = put_helper(dime, payload, chunk_size, HashMap::default(), Vec::default()).await;

        match response {
            Ok(_) => (),
            _ => assert_eq!(format!("{:?}", response), ""),
        }

        let dime = generate_dime(vec![audience1.clone(), audience2.clone(), audience3.clone()], vec![signature1.clone(), signature2.clone(), signature3.clone()]);
        let payload: bytes::Bytes = "testing small payload 3".as_bytes().into();
        let response = put_helper(dime, payload, chunk_size, HashMap::default(), Vec::default()).await;

        match response {
            Ok(_) => (),
            _ => assert_eq!(format!("{:?}", response), ""),
        }

        let dime = generate_dime(vec![audience1.clone(), audience2.clone(), audience3.clone()], vec![signature1.clone(), signature2.clone(), signature3.clone()]);
        let payload: bytes::Bytes = "testing small payload 4".as_bytes().into();
        let response = put_helper(dime, payload.clone(), chunk_size, HashMap::default(), Vec::default()).await;

        match response {
            Ok(_) => {
                assert_eq!(replication_object_uuids(&state_one.db_pool, String::from_utf8(audience1.public_key.clone()).unwrap().as_str(), 50).await.unwrap().len(), 0);
                assert_eq!(replication_object_uuids(&state_one.db_pool, String::from_utf8(audience2.public_key.clone()).unwrap().as_str(), 50).await.unwrap().len(), 0);
                assert_eq!(replication_object_uuids(&state_one.db_pool, String::from_utf8(audience3.public_key.clone()).unwrap().as_str(), 50).await.unwrap().len(), 0);
            }
            _ => assert_eq!(format!("{:?}", response), ""),
        }
    }

    #[tokio::test]
    #[serial(grpc_server)]
    async fn end_to_end_replication() {
        let config_one = test_config_one();
        let mut state_one = start_server_one(None).await;
        let state_two = start_server_two().await;
        let mut client_cache_one = ClientCache::new(config_one.backoff_min_wait, config_one.backoff_max_wait);

        let (audience1, signature1) = party_1();
        let (audience2, signature2) = party_2();
        let (audience3, signature3) = party_3();

        // put object for party_1 - requires no replication
        let dime = generate_dime(vec![audience1.clone()], vec![signature1.clone()]);
        let payload: bytes::Bytes = "testing small payload 1".as_bytes().into();
        let chunk_size = 500; // full payload in one packet
        let response = put_helper(dime, payload, chunk_size, HashMap::default(), Vec::default()).await;

        match response {
            Ok(_) => {
                assert_eq!(replication_object_uuids(&state_one.db_pool, String::from_utf8(audience1.public_key.clone()).unwrap().as_str(), 50).await.unwrap().len(), 0);
            },
            _ => assert_eq!(format!("{:?}", response), ""),
        }

        // put 3 objects for party_1, party_2, party_3
        let dime = generate_dime(vec![audience1.clone(), audience2.clone(), audience3.clone()], vec![signature1.clone(), signature2.clone(), signature3.clone()]);
        let payload: bytes::Bytes = "testing small payload 2".as_bytes().into();
        let response = put_helper(dime, payload, chunk_size, HashMap::default(), Vec::default()).await;

        match response {
            Ok(_) => (),
            _ => assert_eq!(format!("{:?}", response), ""),
        }

        let dime = generate_dime(vec![audience1.clone(), audience2.clone(), audience3.clone()], vec![signature1.clone(), signature2.clone(), signature3.clone()]);
        let payload: bytes::Bytes = "testing small payload 3".as_bytes().into();
        let response = put_helper(dime, payload, chunk_size, HashMap::default(), Vec::default()).await;

        match response {
            Ok(_) => (),
            _ => assert_eq!(format!("{:?}", response), ""),
        }

        let dime = generate_dime(vec![audience1.clone(), audience2.clone(), audience3.clone()], vec![signature1.clone(), signature2.clone(), signature3.clone()]);
        let payload: bytes::Bytes = "testing small payload 4".as_bytes().into();
        let response = put_helper(dime, payload.clone(), chunk_size, HashMap::default(), Vec::default()).await;

        match response {
            Ok(_) => {
                assert_eq!(replication_object_uuids(&state_one.db_pool, String::from_utf8(audience1.public_key.clone()).unwrap().as_str(), 50).await.unwrap().len(), 0);
                assert_eq!(replication_object_uuids(&state_one.db_pool, String::from_utf8(audience2.public_key.clone()).unwrap().as_str(), 50).await.unwrap().len(), 3);
                assert_eq!(replication_object_uuids(&state_one.db_pool, String::from_utf8(audience3.public_key.clone()).unwrap().as_str(), 50).await.unwrap().len(), 3);

                assert_eq!(replication_object_uuids(&state_two.db_pool, String::from_utf8(audience1.public_key.clone()).unwrap().as_str(), 50).await.unwrap().len(), 0);
                assert_eq!(replication_object_uuids(&state_two.db_pool, String::from_utf8(audience2.public_key.clone()).unwrap().as_str(), 50).await.unwrap().len(), 0);
                assert_eq!(replication_object_uuids(&state_two.db_pool, String::from_utf8(audience3.public_key.clone()).unwrap().as_str(), 50).await.unwrap().len(), 0);
            },
            _ => assert_eq!(format!("{:?}", response), ""),
        }

        // run replication iteration
        replicate_iteration(&mut state_one, &mut client_cache_one).await;

        assert_eq!(replication_object_uuids(&state_one.db_pool, String::from_utf8(audience1.public_key.clone()).unwrap().as_str(), 50).await.unwrap().len(), 0);
        assert_eq!(replication_object_uuids(&state_one.db_pool, String::from_utf8(audience2.public_key.clone()).unwrap().as_str(), 50).await.unwrap().len(), 1);
        assert_eq!(replication_object_uuids(&state_one.db_pool, String::from_utf8(audience3.public_key.clone()).unwrap().as_str(), 50).await.unwrap().len(), 3);

        assert_eq!(replication_object_uuids(&state_two.db_pool, String::from_utf8(audience1.public_key.clone()).unwrap().as_str(), 50).await.unwrap().len(), 0);
        assert_eq!(replication_object_uuids(&state_two.db_pool, String::from_utf8(audience2.public_key.clone()).unwrap().as_str(), 50).await.unwrap().len(), 0);
        assert_eq!(replication_object_uuids(&state_two.db_pool, String::from_utf8(audience3.public_key.clone()).unwrap().as_str(), 50).await.unwrap().len(), 0);

        replicate_iteration(&mut state_one, &mut client_cache_one).await;

        assert_eq!(replication_object_uuids(&state_one.db_pool, String::from_utf8(audience1.public_key.clone()).unwrap().as_str(), 50).await.unwrap().len(), 0);
        assert_eq!(replication_object_uuids(&state_one.db_pool, String::from_utf8(audience2.public_key.clone()).unwrap().as_str(), 50).await.unwrap().len(), 0);
        assert_eq!(replication_object_uuids(&state_one.db_pool, String::from_utf8(audience3.public_key.clone()).unwrap().as_str(), 50).await.unwrap().len(), 3);

        assert_eq!(replication_object_uuids(&state_two.db_pool, String::from_utf8(audience1.public_key.clone()).unwrap().as_str(), 50).await.unwrap().len(), 0);
        assert_eq!(replication_object_uuids(&state_two.db_pool, String::from_utf8(audience2.public_key.clone()).unwrap().as_str(), 50).await.unwrap().len(), 0);
        assert_eq!(replication_object_uuids(&state_two.db_pool, String::from_utf8(audience3.public_key.clone()).unwrap().as_str(), 50).await.unwrap().len(), 0);

        // verify db on remote instance to check for 3 objects for party_2
        assert_eq!(get_object_count(&state_two.db_pool).await, 3);

        // pull one object from local instance and verify all rows against the same one that was replicated to the remote
        let mut client_one = get_client_one().await;
        let mut client_two = get_client_two().await;

        let public_key = base64::decode(audience3.public_key).unwrap();
        let request = pb::HashRequest { hash: hash(payload), public_key };
        let response_one = client_one.get(tonic::Request::new(request.clone())).await;
        let response_two = client_two.get(tonic::Request::new(request)).await;

        match response_one {
            Ok(response_one) => {
                match response_two {
                    Ok(response_two) => {
                        let response_one = response_one.into_inner();
                        let response_two = response_two.into_inner();

                        for (one, two) in response_one.zip(response_two).next().await {
                            assert_eq!(one.unwrap(), two.unwrap());
                        }
                    },
                    _ => assert_eq!(format!("{:?}", response_two), ""),
                }
            },
            _ => assert_eq!(format!("{:?}", response_one), ""),
        }
    }

    #[tokio::test]
    #[serial(grpc_server)]
    async fn late_remote_url_can_replicate() {
        // TODO implement
    }

    #[tokio::test]
    #[serial(grpc_server)]
    async fn late_local_url_can_cleanup() {
        let state_one = start_server_one(None).await;

        let (audience1, signature1) = party_1();
        let (audience3, signature3) = party_3();

        // put 3 objects for party_1, party_3
        let dime = generate_dime(vec![audience1.clone(), audience3.clone()], vec![signature1.clone(), signature3.clone()]);
        let payload: bytes::Bytes = "testing small payload 2".as_bytes().into();
        let chunk_size = 500; // full payload in one packet
        let response = put_helper(dime, payload, chunk_size, HashMap::default(), Vec::default()).await;

        match response {
            Ok(_) => (),
            _ => assert_eq!(format!("{:?}", response), ""),
        }

        let dime = generate_dime(vec![audience1.clone(), audience3.clone()], vec![signature1.clone(), signature3.clone()]);
        let payload: bytes::Bytes = "testing small payload 3".as_bytes().into();
        let response = put_helper(dime, payload, chunk_size, HashMap::default(), Vec::default()).await;

        match response {
            Ok(_) => (),
            _ => assert_eq!(format!("{:?}", response), ""),
        }

        let dime = generate_dime(vec![audience1.clone(), audience3.clone()], vec![signature1.clone(), signature3.clone()]);
        let payload: bytes::Bytes = "testing small payload 4".as_bytes().into();
        let response = put_helper(dime, payload.clone(), chunk_size, HashMap::default(), Vec::default()).await;

        match response {
            Ok(_) => {
                assert_eq!(replication_object_uuids(&state_one.db_pool, String::from_utf8(audience1.public_key.clone()).unwrap().as_str(), 50).await.unwrap().len(), 0);
                assert_eq!(replication_object_uuids(&state_one.db_pool, String::from_utf8(audience3.public_key.clone()).unwrap().as_str(), 50).await.unwrap().len(), 3);
            },
            _ => assert_eq!(format!("{:?}", response), ""),
        }

        // set unknown key to local and reap
        let cache = state_one.cache.clone();

        {
            let mut cache = cache.lock().unwrap();

            cache.add_public_key(PublicKey {
                uuid: uuid::Uuid::default(),
                public_key: std::str::from_utf8(audience3.public_key.as_slice()).unwrap().to_owned(),
                public_key_type: KeyType::Secp256k1,
                url: String::from(""),
                metadata: Vec::default(),
                auth_type: Some(AuthType::Header),
                auth_data: Some(String::from("X-Test-Header:test_value")),
                created_at: Utc::now(),
                updated_at: Utc::now(),
            });
        }

        reap_unknown_keys_iteration(&state_one.db_pool, &cache).await;

        assert_eq!(replication_object_uuids(&state_one.db_pool, String::from_utf8(audience1.public_key.clone()).unwrap().as_str(), 50).await.unwrap().len(), 0);
        assert_eq!(replication_object_uuids(&state_one.db_pool, String::from_utf8(audience3.public_key.clone()).unwrap().as_str(), 50).await.unwrap().len(), 0);
    }

    #[tokio::test]
    #[serial(grpc_server)]
    async fn handles_offline_remote() {
    }
}
