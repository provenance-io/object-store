mod common;

use object_store::cache::Cache;
use object_store::config::Config;
use object_store::datastore::{replication_object_uuids, PublicKey};
use object_store::object::*;
use object_store::pb::{self};
use object_store::replication::*;
use object_store::storage::{FileSystem, Storage};

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use futures::StreamExt;
use sqlx::postgres::{PgPool, PgPoolOptions};
use tonic::transport::Channel;

use serial_test::serial;
use testcontainers::*;

use crate::common::db::start_postgres;
use crate::common::{
    generate_dime, hash, party_1, party_2, party_3, put_helper, test_config, test_public_key,
};

pub fn test_config_one() -> Config {
    Config {
        url: "0.0.0.0:6789".parse().unwrap(),
        storage_type: "file_system_one".to_owned(),
        storage_base_url: None,
        replication_enabled: true,
        replication_batch_size: 2,
        dd_config: None,
        backoff_min_wait: 5,
        backoff_max_wait: 5,
        ..test_config(0)
    }
}

pub fn test_config_one_no_replication() -> Config {
    Config {
        url: "0.0.0.0:6789".parse().unwrap(),
        storage_type: "file_system_one".to_owned(),
        replication_enabled: false,
        dd_config: None,
        backoff_min_wait: 5,
        backoff_max_wait: 5,
        ..test_config(0)
    }
}

pub fn test_config_two() -> Config {
    Config {
        url: "0.0.0.0:6790".parse().unwrap(),
        storage_type: "file_system_two".to_owned(),
        replication_enabled: true,
        dd_config: None,
        backoff_min_wait: 5,
        backoff_max_wait: 5,
        ..test_config(0)
    }
}

pub async fn setup_postgres(port: u16) -> PgPool {
    let connection_string = &format!("postgres://postgres:postgres@localhost:{}/postgres", port,);

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(connection_string)
        .await
        .unwrap();

    sqlx::migrate!().run(&pool).await.unwrap();

    pool
}

fn set_cache(cache: &mut Cache) {
    cache.add_public_key(PublicKey {
        auth_data: Some(String::from("X-Test-Header:test_value")),
        ..test_public_key(party_1().0.public_key)
    });
    cache.add_public_key(PublicKey {
        url: String::from("tcp://0.0.0.0:6790"),
        auth_data: Some(String::from("X-Test-Header:test_value")),
        ..test_public_key(party_2().0.public_key)
    });
}

async fn start_server_one(
    config_override: Option<Config>,
    pool: Arc<PgPool>,
) -> (Arc<Mutex<Cache>>, ReplicationState) {
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);

    let cache = {
        let mut cache = Cache::default();
        set_cache(&mut cache);

        Arc::new(Mutex::new(cache))
    };

    let config = config_override.unwrap_or(test_config_one());
    let storage = FileSystem::new(PathBuf::from(config.storage_base_path.as_str()));
    let config = Arc::new(config);
    let storage: Arc<Box<dyn Storage>> = Arc::new(Box::new(storage));
    let replication_state =
        ReplicationState::new(cache.clone(), config.clone(), pool.clone(), storage.clone());
    let object_service =
        ObjectGrpc::new(cache.clone(), config.clone(), pool.clone(), storage.clone());

    tokio::spawn(async move {
        tx.send(replication_state).await.unwrap();

        tonic::transport::Server::builder()
            .add_service(pb::object_service_server::ObjectServiceServer::new(
                object_service,
            ))
            .serve(config.url)
            .await
            .unwrap()
    });

    (cache, rx.recv().await.unwrap())
}

async fn start_server_two(postgres_port: u16) -> ReplicationState {
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);

    tokio::spawn(async move {
        let cache = {
            let mut cache = Cache::default();
            set_cache(&mut cache);

            Arc::new(Mutex::new(cache))
        };

        let config = test_config_two();
        let url = config.url;
        let pool = setup_postgres(postgres_port).await;
        let storage = FileSystem::new(PathBuf::from(config.storage_base_path.as_str()));
        let config = Arc::new(config);
        let db_pool = Arc::new(pool);
        let storage: Arc<Box<dyn Storage>> = Arc::new(Box::new(storage));
        let replication_state = ReplicationState::new(
            cache.clone(),
            config.clone(),
            db_pool.clone(),
            storage.clone(),
        );
        let object_service = ObjectGrpc::new(
            cache.clone(),
            config.clone(),
            db_pool.clone(),
            storage.clone(),
        );

        tx.send(replication_state).await.unwrap();

        tonic::transport::Server::builder()
            .add_service(pb::object_service_server::ObjectServiceServer::new(
                object_service,
            ))
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

    pb::object_service_client::ObjectServiceClient::connect("tcp://0.0.0.0:6789")
        .await
        .unwrap()
}

async fn get_client_two() -> pb::object_service_client::ObjectServiceClient<Channel> {
    // allow server to start
    tokio::time::sleep(tokio::time::Duration::from_millis(1_000)).await;

    pb::object_service_client::ObjectServiceClient::connect("tcp://0.0.0.0:6790")
        .await
        .unwrap()
}

#[tokio::test]
#[serial(grpc_server)]
async fn client_caching() {
    let docker = clients::Cli::default();
    let container = start_postgres(&docker).await;
    let postgres_port = container.get_host_port_ipv4(5432);
    let pool1 = Arc::new(setup_postgres(postgres_port).await);

    let container_two = start_postgres(&docker).await;
    let postgres_port_two = container_two.get_host_port_ipv4(5432);
    let _pool2 = Arc::new(setup_postgres(postgres_port).await);

    let _state_one = start_server_one(None, pool1.clone()).await;

    let config_one = test_config_one();
    let config_two = test_config_two();

    let url_one = format!("tcp://{}", config_one.url);
    let url_two = format!("tcp://{}", config_two.url);

    let mut client_cache =
        ClientCache::new(config_one.backoff_min_wait, config_one.backoff_max_wait);

    // Client 1:

    // Check that ReplicationState connection caching works when given the same URL:
    let client_one = client_cache.request(&url_one).await.unwrap().unwrap();
    // the result from calling again with the same URL should be empty since the client is in use:
    assert!(client_cache.request(&url_one).await.unwrap().is_none());

    let client_one_id_first = *client_one.id();
    client_cache.restore(&url_one, client_one).await.unwrap();
    // we should get an instance again:
    let client_one = client_cache.request(&url_one).await.unwrap().unwrap();
    let client_one_id_second = *client_one.id();
    // The IDs of the the two clients should be the same, since they originated from the
    // same URL:
    assert_eq!(client_one_id_first, client_one_id_second);

    // Client 2:

    let client_two = client_cache.request(&url_two).await.unwrap();
    // client 2 should be empty because server two is not running:
    assert!(client_two.is_none());

    let _state_two = start_server_two(postgres_port_two).await;
    let client_two = client_cache.request(&url_two).await.unwrap();
    // client 2 will still fail even if state_two is ready because there's still time to wait
    // out until the client can attempt to be created again.
    assert!(client_two.is_none());

    // wait a little more and it should work:
    tokio::time::sleep(tokio::time::Duration::from_millis(5_000)).await;

    let client_two = client_cache.request(&url_two).await.unwrap().unwrap();

    // also client_two should be distinct from client one:
    assert_ne!(client_one_id_first, client_two.id().clone());
}

#[tokio::test]
#[serial(grpc_server)]
async fn replication_can_be_disabled() {
    let docker = clients::Cli::default();
    let container = start_postgres(&docker).await;
    let postgres_port = container.get_host_port_ipv4(5432);
    let pool = Arc::new(setup_postgres(postgres_port).await);

    let _state_one = start_server_one(Some(test_config_one_no_replication()), pool.clone()).await;

    let (audience1, signature1) = party_1();
    let (audience2, signature2) = party_2();
    let (audience3, signature3) = party_3();

    // put 3 objects for party_1, party_2, party_3
    let dime = generate_dime(
        vec![audience1.clone(), audience2.clone(), audience3.clone()],
        vec![signature1.clone(), signature2.clone(), signature3.clone()],
    );
    let payload: bytes::Bytes = "testing small payload 2".as_bytes().into();
    let chunk_size = 500; // full payload in one packet
    let response = put_helper(
        dime,
        payload,
        chunk_size,
        HashMap::default(),
        Vec::default(),
    )
    .await;

    match response {
        Ok(_) => (),
        _ => assert_eq!(format!("{:?}", response), ""),
    }

    let dime = generate_dime(
        vec![audience1.clone(), audience2.clone(), audience3.clone()],
        vec![signature1.clone(), signature2.clone(), signature3.clone()],
    );
    let payload: bytes::Bytes = "testing small payload 3".as_bytes().into();
    let response = put_helper(
        dime,
        payload,
        chunk_size,
        HashMap::default(),
        Vec::default(),
    )
    .await;

    match response {
        Ok(_) => (),
        _ => assert_eq!(format!("{:?}", response), ""),
    }

    let dime = generate_dime(
        vec![audience1.clone(), audience2.clone(), audience3.clone()],
        vec![signature1.clone(), signature2.clone(), signature3.clone()],
    );
    let payload: bytes::Bytes = "testing small payload 4".as_bytes().into();
    let response = put_helper(
        dime,
        payload.clone(),
        chunk_size,
        HashMap::default(),
        Vec::default(),
    )
    .await;

    match response {
        Ok(_) => {
            assert_eq!(
                replication_object_uuids(
                    pool.clone().as_ref(),
                    String::from_utf8(audience1.public_key.clone())
                        .unwrap()
                        .as_str(),
                    50
                )
                .await
                .unwrap()
                .len(),
                0
            );
            assert_eq!(
                replication_object_uuids(
                    pool.clone().as_ref(),
                    String::from_utf8(audience2.public_key.clone())
                        .unwrap()
                        .as_str(),
                    50
                )
                .await
                .unwrap()
                .len(),
                0
            );
            assert_eq!(
                replication_object_uuids(
                    pool.clone().as_ref(),
                    String::from_utf8(audience3.public_key.clone())
                        .unwrap()
                        .as_str(),
                    50
                )
                .await
                .unwrap()
                .len(),
                0
            );
        }
        _ => assert_eq!(format!("{:?}", response), ""),
    }
}

#[tokio::test]
#[serial(grpc_server)]
async fn end_to_end_replication() {
    let docker = clients::Cli::default();
    let container = start_postgres(&docker).await;
    let postgres_port = container.get_host_port_ipv4(5432);
    let pool1 = Arc::new(setup_postgres(postgres_port).await);

    let container_two = start_postgres(&docker).await;
    let postgres_port_two = container_two.get_host_port_ipv4(5432);
    let pool2 = Arc::new(setup_postgres(postgres_port_two).await);

    let config_one = test_config_one();
    let (_, mut state_one) = start_server_one(None, pool1.clone()).await;
    let _state_two = start_server_two(postgres_port_two).await;

    let mut client_cache_one =
        ClientCache::new(config_one.backoff_min_wait, config_one.backoff_max_wait);

    let (audience1, signature1) = party_1();
    let (audience2, signature2) = party_2();
    let (audience3, signature3) = party_3();

    // put object for party_1 - requires no replication
    let dime = generate_dime(vec![audience1.clone()], vec![signature1.clone()]);
    let payload: bytes::Bytes = "testing small payload 1".as_bytes().into();
    let chunk_size = 500; // full payload in one packet
    let response = put_helper(
        dime,
        payload,
        chunk_size,
        HashMap::default(),
        Vec::default(),
    )
    .await;

    match response {
        Ok(_) => {
            assert_eq!(
                replication_object_uuids(
                    pool1.clone().as_ref(),
                    String::from_utf8(audience1.public_key.clone())
                        .unwrap()
                        .as_str(),
                    50
                )
                .await
                .unwrap()
                .len(),
                0
            );
        }
        _ => assert_eq!(format!("{:?}", response), ""),
    }

    // put 3 objects for party_1, party_2, party_3
    let dime = generate_dime(
        vec![audience1.clone(), audience2.clone(), audience3.clone()],
        vec![signature1.clone(), signature2.clone(), signature3.clone()],
    );
    let payload: bytes::Bytes = "testing small payload 2".as_bytes().into();
    let response = put_helper(
        dime,
        payload,
        chunk_size,
        HashMap::default(),
        Vec::default(),
    )
    .await;

    match response {
        Ok(_) => (),
        _ => assert_eq!(format!("{:?}", response), ""),
    }

    let dime = generate_dime(
        vec![audience1.clone(), audience2.clone(), audience3.clone()],
        vec![signature1.clone(), signature2.clone(), signature3.clone()],
    );
    let payload: bytes::Bytes = "testing small payload 3".as_bytes().into();
    let response = put_helper(
        dime,
        payload,
        chunk_size,
        HashMap::default(),
        Vec::default(),
    )
    .await;

    match response {
        Ok(_) => (),
        _ => assert_eq!(format!("{:?}", response), ""),
    }

    let dime = generate_dime(
        vec![audience1.clone(), audience2.clone(), audience3.clone()],
        vec![signature1.clone(), signature2.clone(), signature3.clone()],
    );
    let payload: bytes::Bytes = "testing small payload 4".as_bytes().into();
    let response = put_helper(
        dime,
        payload.clone(),
        chunk_size,
        HashMap::default(),
        Vec::default(),
    )
    .await;

    match response {
        Ok(_) => {
            assert_eq!(
                replication_object_uuids(
                    pool1.clone().as_ref(),
                    String::from_utf8(audience1.public_key.clone())
                        .unwrap()
                        .as_str(),
                    50
                )
                .await
                .unwrap()
                .len(),
                0
            );
            assert_eq!(
                replication_object_uuids(
                    pool1.clone().as_ref(),
                    String::from_utf8(audience2.public_key.clone())
                        .unwrap()
                        .as_str(),
                    50
                )
                .await
                .unwrap()
                .len(),
                3
            );
            assert_eq!(
                replication_object_uuids(
                    pool1.clone().as_ref(),
                    String::from_utf8(audience3.public_key.clone())
                        .unwrap()
                        .as_str(),
                    50
                )
                .await
                .unwrap()
                .len(),
                3
            );

            assert_eq!(
                replication_object_uuids(
                    pool2.clone().as_ref(),
                    String::from_utf8(audience1.public_key.clone())
                        .unwrap()
                        .as_str(),
                    50
                )
                .await
                .unwrap()
                .len(),
                0
            );
            assert_eq!(
                replication_object_uuids(
                    pool2.clone().as_ref(),
                    String::from_utf8(audience2.public_key.clone())
                        .unwrap()
                        .as_str(),
                    50
                )
                .await
                .unwrap()
                .len(),
                0
            );
            assert_eq!(
                replication_object_uuids(
                    pool2.clone().as_ref(),
                    String::from_utf8(audience3.public_key.clone())
                        .unwrap()
                        .as_str(),
                    50
                )
                .await
                .unwrap()
                .len(),
                0
            );
        }
        _ => assert_eq!(format!("{:?}", response), ""),
    }

    // run replication iteration
    replicate_iteration(&mut state_one, &mut client_cache_one).await;

    assert_eq!(
        replication_object_uuids(
            pool1.clone().as_ref(),
            String::from_utf8(audience1.public_key.clone())
                .unwrap()
                .as_str(),
            50
        )
        .await
        .unwrap()
        .len(),
        0
    );
    assert_eq!(
        replication_object_uuids(
            pool1.clone().as_ref(),
            String::from_utf8(audience2.public_key.clone())
                .unwrap()
                .as_str(),
            50
        )
        .await
        .unwrap()
        .len(),
        1
    );
    assert_eq!(
        replication_object_uuids(
            pool1.clone().as_ref(),
            String::from_utf8(audience3.public_key.clone())
                .unwrap()
                .as_str(),
            50
        )
        .await
        .unwrap()
        .len(),
        3
    );

    assert_eq!(
        replication_object_uuids(
            pool2.clone().as_ref(),
            String::from_utf8(audience1.public_key.clone())
                .unwrap()
                .as_str(),
            50
        )
        .await
        .unwrap()
        .len(),
        0
    );
    assert_eq!(
        replication_object_uuids(
            pool2.clone().as_ref(),
            String::from_utf8(audience2.public_key.clone())
                .unwrap()
                .as_str(),
            50
        )
        .await
        .unwrap()
        .len(),
        0
    );
    assert_eq!(
        replication_object_uuids(
            pool2.clone().as_ref(),
            String::from_utf8(audience3.public_key.clone())
                .unwrap()
                .as_str(),
            50
        )
        .await
        .unwrap()
        .len(),
        0
    );

    replicate_iteration(&mut state_one, &mut client_cache_one).await;

    assert_eq!(
        replication_object_uuids(
            pool1.clone().as_ref(),
            String::from_utf8(audience1.public_key.clone())
                .unwrap()
                .as_str(),
            50
        )
        .await
        .unwrap()
        .len(),
        0
    );
    assert_eq!(
        replication_object_uuids(
            pool1.clone().as_ref(),
            String::from_utf8(audience2.public_key.clone())
                .unwrap()
                .as_str(),
            50
        )
        .await
        .unwrap()
        .len(),
        0
    );
    assert_eq!(
        replication_object_uuids(
            pool1.clone().as_ref(),
            String::from_utf8(audience3.public_key.clone())
                .unwrap()
                .as_str(),
            50
        )
        .await
        .unwrap()
        .len(),
        3
    );

    assert_eq!(
        replication_object_uuids(
            pool2.clone().as_ref(),
            String::from_utf8(audience1.public_key.clone())
                .unwrap()
                .as_str(),
            50
        )
        .await
        .unwrap()
        .len(),
        0
    );
    assert_eq!(
        replication_object_uuids(
            pool2.clone().as_ref(),
            String::from_utf8(audience2.public_key.clone())
                .unwrap()
                .as_str(),
            50
        )
        .await
        .unwrap()
        .len(),
        0
    );
    assert_eq!(
        replication_object_uuids(
            pool2.clone().as_ref(),
            String::from_utf8(audience3.public_key.clone())
                .unwrap()
                .as_str(),
            50
        )
        .await
        .unwrap()
        .len(),
        0
    );

    // verify db on remote instance to check for 3 objects for party_2
    assert_eq!(get_object_count(pool2.clone().as_ref()).await, 3);

    // pull one object from local instance and verify all rows against the same one that was replicated to the remote
    let mut client_one = get_client_one().await;
    let mut client_two = get_client_two().await;

    let public_key = audience3.public_key_decoded();
    let request = pb::HashRequest {
        hash: hash(payload),
        public_key,
    };
    let response_one = client_one.get(tonic::Request::new(request.clone())).await;
    let response_two = client_two.get(tonic::Request::new(request)).await;

    match response_one {
        Ok(response_one) => match response_two {
            Ok(response_two) => {
                let response_one = response_one.into_inner();
                let response_two = response_two.into_inner();

                if let Some((one, two)) = response_one.zip(response_two).next().await {
                    assert_eq!(one.unwrap(), two.unwrap());
                }
            }
            _ => assert_eq!(format!("{:?}", response_two), ""),
        },
        _ => assert_eq!(format!("{:?}", response_one), ""),
    }
}

#[ignore = "unimplemented"]
#[tokio::test]
#[serial(grpc_server)]
async fn late_remote_url_can_replicate() {}

#[tokio::test]
#[serial(grpc_server)]
async fn late_local_url_can_cleanup() {
    let docker = clients::Cli::default();
    let container = start_postgres(&docker).await;
    let postgres_port = container.get_host_port_ipv4(5432);
    let pool = Arc::new(setup_postgres(postgres_port).await);
    let (cache, _state_one) = start_server_one(None, pool.clone()).await;

    let (audience1, signature1) = party_1();
    let (audience3, signature3) = party_3();

    // put 3 objects for party_1, party_3
    let dime = generate_dime(
        vec![audience1.clone(), audience3.clone()],
        vec![signature1.clone(), signature3.clone()],
    );
    let payload: bytes::Bytes = "testing small payload 2".as_bytes().into();
    let chunk_size = 500; // full payload in one packet
    let response = put_helper(
        dime,
        payload,
        chunk_size,
        HashMap::default(),
        Vec::default(),
    )
    .await;

    match response {
        Ok(_) => (),
        _ => assert_eq!(format!("{:?}", response), ""),
    }

    let dime = generate_dime(
        vec![audience1.clone(), audience3.clone()],
        vec![signature1.clone(), signature3.clone()],
    );
    let payload: bytes::Bytes = "testing small payload 3".as_bytes().into();
    let response = put_helper(
        dime,
        payload,
        chunk_size,
        HashMap::default(),
        Vec::default(),
    )
    .await;

    match response {
        Ok(_) => (),
        _ => assert_eq!(format!("{:?}", response), ""),
    }

    let dime = generate_dime(
        vec![audience1.clone(), audience3.clone()],
        vec![signature1.clone(), signature3.clone()],
    );
    let payload: bytes::Bytes = "testing small payload 4".as_bytes().into();
    let response = put_helper(
        dime,
        payload.clone(),
        chunk_size,
        HashMap::default(),
        Vec::default(),
    )
    .await;

    match response {
        Ok(_) => {
            assert_eq!(
                replication_object_uuids(
                    pool.clone().as_ref(),
                    String::from_utf8(audience1.public_key.clone())
                        .unwrap()
                        .as_str(),
                    50
                )
                .await
                .unwrap()
                .len(),
                0
            );
            assert_eq!(
                replication_object_uuids(
                    pool.clone().as_ref(),
                    String::from_utf8(audience3.public_key.clone())
                        .unwrap()
                        .as_str(),
                    50
                )
                .await
                .unwrap()
                .len(),
                3
            );
        }
        _ => assert_eq!(format!("{:?}", response), ""),
    }

    // set unknown key to local and reap
    let cache = cache.clone();

    {
        let mut cache = cache.lock().unwrap();

        cache.add_public_key(PublicKey {
            auth_data: Some(String::from("X-Test-Header:test_value")),
            ..test_public_key(audience3.public_key.clone())
        });
    }

    reap_unknown_keys_iteration(&pool, &cache).await;

    assert_eq!(
        replication_object_uuids(
            pool.clone().as_ref(),
            String::from_utf8(audience1.public_key.clone())
                .unwrap()
                .as_str(),
            50
        )
        .await
        .unwrap()
        .len(),
        0
    );
    assert_eq!(
        replication_object_uuids(
            pool.clone().as_ref(),
            String::from_utf8(audience3.public_key.clone())
                .unwrap()
                .as_str(),
            50
        )
        .await
        .unwrap()
        .len(),
        0
    );
}

#[ignore = "unimplemented"]
#[tokio::test]
#[serial(grpc_server)]
async fn handles_offline_remote() {}
