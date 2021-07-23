use crate::{cache::Cache, config::Config, consts, types::OsError};
use crate::datastore::{self, reap_object_replication, replication_object_uuids};
use crate::storage::{FileSystem, StoragePath};
use crate::pb::object_service_client::ObjectServiceClient;
use crate::proto_helpers::{create_data_chunk, create_multi_stream_header, create_stream_header_field, create_stream_end};

use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::future::Future;

use backoff::{self, ExponentialBackoff};
use backoff::future::retry;
use bytes::Bytes;
use chrono::prelude::*;
use futures::stream;
use sqlx::postgres::PgPool;
use quick_error::quick_error;
use futures_locks::{MutexGuard, Mutex as MutexF};
use uuid::Uuid;

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
    }
}

type Result<T> = std::result::Result<T, ReplicationError>;

// TODO implement remaining tests

#[derive(Debug)]
pub struct ReplicationState {
    cache: Arc<Mutex<Cache>>,
    config: Arc<Config>,
    snapshot_cache: (DateTime<Utc>, Cache),
    db_pool: Arc<PgPool>,
    storage: Arc<FileSystem>,
    clients: MutexF<HashMap<String, CachedClient<ObjectServiceClient<tonic::transport::Channel>>>>
}

#[derive(Clone, Debug)]
struct CachedClient<T> {
    id: String,
    client: MutexF<T>
}

impl <T> CachedClient<T> {
    pub fn new(client: T) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            client: MutexF::new(client)
        }
    }

    pub fn id(&self) -> &String {
         &self.id
    }

    pub fn lock(&self) -> impl Future<Output = MutexGuard<T>> {
        self.client.lock()
    }
}


impl ReplicationState {
    pub fn new(cache: Arc<Mutex<Cache>>, config: Arc<Config>, db_pool: Arc<PgPool>, storage: Arc<FileSystem>) -> Self {
        let snapshot_cache = cache.lock().unwrap().clone();
        let snapshot_cache = (Utc::now(), snapshot_cache);

        Self { cache, config, snapshot_cache, db_pool, storage, clients: MutexF::new(HashMap::new()) }
    }

    async fn cached_client(&self, url: &String) -> Result<CachedClient<ObjectServiceClient<tonic::transport::Channel>>> {
        let cc = {
            let mut guard = self.clients.lock().await;
            let connect_url = url.clone();
            match guard.entry(url.clone()) {
                Entry::Occupied(entry) => (*entry.get()).clone(),
                Entry::Vacant(entry) => {
                    let client = ObjectServiceClient::connect(connect_url)
                        .await
                        .map_err(ReplicationError::TonicTransportError)?;
                    entry.insert(CachedClient::new(client)).clone()
                }
            }
        };
        Ok(cc)
    }
}

async fn replicate_public_key(inner: &ReplicationState, public_key: &String, url: String) -> Result<usize> {
    let batch = datastore::replication_object_uuids(&inner.db_pool, public_key, inner.config.replication_batch_size).await?;
    let batch_size = batch.len();
    let backoff_strategy = ExponentialBackoff::default();

    // Retry the attempt to connect up to 10 times (the default with the `backoff` crate) with exponential backoff.
    let cached_client = retry(backoff_strategy, || async {
        inner.cached_client(&url).await.map_err(|e| {
            match e {
                ReplicationError::TonicTransportError(_) => backoff::Error::Transient(e),
                err => backoff::Error::Permanent(err)
            }
        })
    }).await?;

    for (uuid, object_uuid) in batch.iter() {
        let object = datastore::get_object_by_uuid(&inner.db_pool, &object_uuid).await?;
        let payload = if let Some(payload) = &object.payload {
            Bytes::copy_from_slice(payload.as_slice())
        } else {
            let storage_path = StoragePath {
                dir: object.directory.clone(),
                file: object.name.clone(),
            };
            let payload = inner.storage.fetch(&storage_path, object.dime_length as u64).await
                .map_err(Into::<OsError>::into)?;

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

        cached_client.lock().await.put(tonic::Request::new(stream)).await?;

        datastore::ack_object_replication(&inner.db_pool, &uuid).await?;
    }

    Ok(batch_size)
}

async fn replicate_iteration(inner: &mut ReplicationState) {
        // update snapshot cache every 5 minutes
        let last_cache_update = &inner.snapshot_cache.0;
        if last_cache_update.time() + chrono::Duration::minutes(5) < Utc::now().time() {
            log::trace!("Updating snapshot cache");

            let snapshot_cache = inner.cache.lock().unwrap().clone();
            inner.snapshot_cache = (Utc::now(), snapshot_cache);
        }

        let mut keys = Vec::new();
        let mut futures = Vec::new();

        for (public_key, url) in &inner.snapshot_cache.1.remote_public_keys {
            keys.push(public_key);
            futures.push(replicate_public_key(&inner, public_key, url.clone()));
        }

        let results = futures::future::join_all(futures).await;

        for (public_key, result) in keys.iter().zip(results) {
            match result {
                Ok(count) => log::trace!("Replicated {} items for {}", count, &public_key),
                Err(e) => {
                    match e {
                        ReplicationError::CrateError(_) => {
                            log::error!("Failed replication for {} - {:?}", public_key, e)
                        },
                        ReplicationError::TonicStatusError(_) => {
                            log::error!("Failed replication for {} - {:?}", public_key, e)
                        },
                        ReplicationError::TonicTransportError(e) => {
                            log::trace!("Failed replication for {} - {:?}", public_key, e)
                        },
                    }
                },
            }
        }
}

pub async fn replicate(mut inner: ReplicationState) {
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        replicate_iteration(&mut inner).await;
    }
}

async fn reap_unknown_keys_iteration(db_pool: &Arc<PgPool>, cache: &Arc<Mutex<Cache>>) {
    let local_public_keys = cache.lock().unwrap().local_public_keys.clone();

    for public_key in local_public_keys {
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
    use crate::datastore::replication_object_uuids;
    use crate::object::*;
    use crate::object::tests::*;
    use crate::pb::{self};
    use crate::replication::*;

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
            storage_base_path: "/tmp".to_owned(),
            storage_threshold: 5000,
            replication_batch_size: 2,
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
            storage_base_path: "/tmp".to_owned(),
            storage_threshold: 5000,
            replication_batch_size: 2,
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

    async fn start_server_one() -> ReplicationState {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);

        tokio::spawn(async move {
            let mut cache = Cache::default();
            cache.add_local_public_key(std::str::from_utf8(&party_1().0.public_key).unwrap().to_owned());
            cache.add_remote_public_key(std::str::from_utf8(&party_2().0.public_key).unwrap().to_owned(), String::from("tcp://0.0.0.0:6790"));
            let cache = Mutex::new(cache);
            let config = test_config_one();
            let url = config.url.clone();
            let docker = clients::Cli::default();
            let image = images::postgres::Postgres::default().with_version(9);
            let container = docker.run(image);
            let pool = setup_postgres(&container).await;
            let storage = FileSystem::new(config.storage_base_path.as_str());
            let cache = Arc::new(cache);
            let config = Arc::new(config);
            let db_pool = Arc::new(pool);
            let storage = Arc::new(storage);
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
            cache.add_local_public_key(std::str::from_utf8(&party_1().0.public_key).unwrap().to_owned());
            cache.add_remote_public_key(std::str::from_utf8(&party_2().0.public_key).unwrap().to_owned(), String::from("tcp://0.0.0.0:6790"));
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
            let storage = Arc::new(storage);
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
    async fn end_to_end_replication() {
        let mut state_one = start_server_one().await;
        let state_two = start_server_two().await;

        let client_url_1 = test_config_one().url;
        let client_url_2 = test_config_two().url;

        // Check that ReplicationState connection caching works when given the same URL:
        let client_copy_1 = state_one.cached_client(&format!("tcp://{}", client_url_1)).await.unwrap();
        let client_copy_2 = state_one.cached_client(&format!("tcp://{}", client_url_1)).await.unwrap();
        assert_eq!(client_copy_1.id(), client_copy_2.id());

        let client_copy_1 = state_one.cached_client(&format!("tcp://{}", client_url_1)).await.unwrap();
        let client_copy_2 = state_one.cached_client(&format!("tcp://{}", client_url_2)).await.unwrap();
        assert_ne!(client_copy_1.id(), client_copy_2.id());

        ////

        let (audience1, signature1) = party_1();
        let (audience2, signature2) = party_2();
        let (audience3, signature3) = party_3();

        // put object for party_1 - requires no replication
        let dime = generate_dime(vec![audience1.clone()], vec![signature1.clone()]);
        let payload: bytes::Bytes = "testing small payload 1".as_bytes().into();
        let chunk_size = 500; // full payload in one packet
        let response = put_helper(dime, payload, chunk_size, HashMap::default()).await;

        match response {
            Ok(_) => {
                assert_eq!(replication_object_uuids(&state_one.db_pool, String::from_utf8(audience1.public_key.clone()).unwrap().as_str(), 50).await.unwrap().len(), 0);
            },
            _ => assert_eq!(format!("{:?}", response), ""),
        }

        // put 3 objects for party_1, party_2, party_3
        let dime = generate_dime(vec![audience1.clone(), audience2.clone(), audience3.clone()], vec![signature1.clone(), signature2.clone(), signature3.clone()]);
        let payload: bytes::Bytes = "testing small payload 2".as_bytes().into();
        let response = put_helper(dime, payload, chunk_size, HashMap::default()).await;

        match response {
            Ok(_) => (),
            _ => assert_eq!(format!("{:?}", response), ""),
        }

        let dime = generate_dime(vec![audience1.clone(), audience2.clone(), audience3.clone()], vec![signature1.clone(), signature2.clone(), signature3.clone()]);
        let payload: bytes::Bytes = "testing small payload 3".as_bytes().into();
        let response = put_helper(dime, payload, chunk_size, HashMap::default()).await;

        match response {
            Ok(_) => (),
            _ => assert_eq!(format!("{:?}", response), ""),
        }

        let dime = generate_dime(vec![audience1.clone(), audience2.clone(), audience3.clone()], vec![signature1.clone(), signature2.clone(), signature3.clone()]);
        let payload: bytes::Bytes = "testing small payload 4".as_bytes().into();
        let response = put_helper(dime, payload.clone(), chunk_size, HashMap::default()).await;

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
        replicate_iteration(&mut state_one).await;

        assert_eq!(replication_object_uuids(&state_one.db_pool, String::from_utf8(audience1.public_key.clone()).unwrap().as_str(), 50).await.unwrap().len(), 0);
        assert_eq!(replication_object_uuids(&state_one.db_pool, String::from_utf8(audience2.public_key.clone()).unwrap().as_str(), 50).await.unwrap().len(), 1);
        assert_eq!(replication_object_uuids(&state_one.db_pool, String::from_utf8(audience3.public_key.clone()).unwrap().as_str(), 50).await.unwrap().len(), 3);

        assert_eq!(replication_object_uuids(&state_two.db_pool, String::from_utf8(audience1.public_key.clone()).unwrap().as_str(), 50).await.unwrap().len(), 0);
        assert_eq!(replication_object_uuids(&state_two.db_pool, String::from_utf8(audience2.public_key.clone()).unwrap().as_str(), 50).await.unwrap().len(), 0);
        assert_eq!(replication_object_uuids(&state_two.db_pool, String::from_utf8(audience3.public_key.clone()).unwrap().as_str(), 50).await.unwrap().len(), 0);

        replicate_iteration(&mut state_one).await;

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
    }

    #[tokio::test]
    #[serial(grpc_server)]
    async fn late_local_url_can_cleanup() {
        let state_one = start_server_one().await;

        let (audience1, signature1) = party_1();
        let (audience3, signature3) = party_3();

        // put 3 objects for party_1, party_3
        let dime = generate_dime(vec![audience1.clone(), audience3.clone()], vec![signature1.clone(), signature3.clone()]);
        let payload: bytes::Bytes = "testing small payload 2".as_bytes().into();
        let chunk_size = 500; // full payload in one packet
        let response = put_helper(dime, payload, chunk_size, HashMap::default()).await;

        match response {
            Ok(_) => (),
            _ => assert_eq!(format!("{:?}", response), ""),
        }

        let dime = generate_dime(vec![audience1.clone(), audience3.clone()], vec![signature1.clone(), signature3.clone()]);
        let payload: bytes::Bytes = "testing small payload 3".as_bytes().into();
        let response = put_helper(dime, payload, chunk_size, HashMap::default()).await;

        match response {
            Ok(_) => (),
            _ => assert_eq!(format!("{:?}", response), ""),
        }

        let dime = generate_dime(vec![audience1.clone(), audience3.clone()], vec![signature1.clone(), signature3.clone()]);
        let payload: bytes::Bytes = "testing small payload 4".as_bytes().into();
        let response = put_helper(dime, payload.clone(), chunk_size, HashMap::default()).await;

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

            cache.add_local_public_key(String::from_utf8(audience3.public_key.clone()).unwrap());
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
