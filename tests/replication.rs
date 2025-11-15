mod common;

use object_store::cache::Cache;
use object_store::config::Config;
use object_store::datastore::{replication_object_uuids, PublicKey};
use object_store::object::ObjectGrpc;
use object_store::pb;
use object_store::pb::object_service_server::ObjectServiceServer;
use object_store::proto_helpers::AudienceUtil;
use object_store::replication::{
    reap_unknown_keys_iteration, replicate_iteration, ClientCache, ReplicationState,
};
use object_store::storage::new_storage;
use testcontainers::clients;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use futures::StreamExt;
use sqlx::postgres::PgPool;

use crate::common::client::get_object_client;
use crate::common::db::{setup_postgres, start_containers};
use crate::common::{
    generate_dime, hash, party_1, party_2, party_3, put_helper, test_config, test_public_key,
};

pub fn test_config_one(db_port: u16) -> Config {
    Config {
        storage_base_url: None,
        replication_enabled: true,
        replication_batch_size: 2,
        dd_config: None,
        backoff_min_wait: 5,
        backoff_max_wait: 5,
        ..test_config(db_port)
    }
}

pub fn test_config_one_no_replication(db_port: u16) -> Config {
    Config {
        replication_enabled: false,
        dd_config: None,
        backoff_min_wait: 5,
        backoff_max_wait: 5,
        ..test_config(db_port)
    }
}

pub fn test_config_two(db_port: u16) -> Config {
    Config {
        replication_enabled: true,
        dd_config: None,
        backoff_min_wait: 5,
        backoff_max_wait: 5,
        ..test_config(db_port)
    }
}

fn set_cache(cache: &mut Cache, remote_config: &Config) {
    cache.add_public_key(PublicKey {
        auth_data: Some(String::from("X-Test-Header:test_value")),
        ..test_public_key(party_1().0.public_key)
    });
    cache.add_public_key(PublicKey {
        url: String::from(format!("tcp://{}", remote_config.url)),
        auth_data: Some(String::from("X-Test-Header:test_value")),
        ..test_public_key(party_2().0.public_key)
    });
}

async fn start_server_one(
    config: Config,
    pool: Arc<PgPool>,
    remote_config: &Config,
) -> (Arc<Mutex<Cache>>, ReplicationState, Arc<Config>, SocketAddr) {
    let cache = {
        let mut cache = Cache::default();
        set_cache(&mut cache, remote_config);

        Arc::new(Mutex::new(cache))
    };

    // TODO new config with addr
    let db_port = config.db_port;
    let arc_config = Arc::new(config);
    let storage = new_storage(&arc_config).unwrap();
    let object_service = ObjectGrpc::new(
        cache.clone(),
        arc_config.clone(),
        pool.clone(),
        storage.clone(),
    );

    let listener = tokio::net::TcpListener::bind(arc_config.url).await.unwrap();
    let local_addr = listener.local_addr().unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    println!("test server running on {:?}", local_addr);

    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(ObjectServiceServer::new(object_service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap()
    });
    let updated_config = Arc::new(Config {
        url: local_addr,
        ..test_config_one(db_port)
    });
    let replication_state = ReplicationState::new(
        cache.clone(),
        updated_config.clone(),
        pool.clone(),
        storage.clone(),
    );

    (cache, replication_state, updated_config, local_addr)
}

async fn start_server_two(
    config: Config,
    pool: Arc<PgPool>,
) -> (ReplicationState, Arc<Config>, SocketAddr) {
    // TODO new config with addr
    let db_port = config.db_port;

    let cache = {
        let mut cache = Cache::default();
        set_cache(&mut cache, &test_config_one(db_port));

        Arc::new(Mutex::new(cache))
    };

    let config = Arc::new(config);
    let storage = new_storage(&config).unwrap();
    let object_service =
        ObjectGrpc::new(cache.clone(), config.clone(), pool.clone(), storage.clone());

    let listener = tokio::net::TcpListener::bind(config.url).await.unwrap();
    let local_addr = listener.local_addr().unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    println!("test server running on {:?}", local_addr);

    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(ObjectServiceServer::new(object_service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap()
    });
    let updated_config = Arc::new(Config {
        url: local_addr,
        ..test_config_two(db_port)
    });
    let replication_state = ReplicationState::new(
        cache.clone(),
        updated_config.clone(),
        pool.clone(),
        storage.clone(),
    );

    (replication_state, updated_config, local_addr)
}

pub async fn get_object_count(db: &PgPool) -> i64 {
    let row: (i64,) = sqlx::query_as("SELECT count(*) as count FROM object")
        .fetch_one(db)
        .await
        .unwrap();

    row.0
}

#[tokio::test]
async fn client_caching() {
    let docker = clients::Cli::default();

    let postgres = start_containers(&docker).await;
    let postgres_port = postgres.get_host_port_ipv4(5432);
    let config_one = test_config_one(postgres_port);
    let pool1 = setup_postgres(&config_one).await;

    let postgres_two = start_containers(&docker).await;
    let postgres_port_two = postgres_two.get_host_port_ipv4(5432);
    let config_two = test_config_two(postgres_port_two);
    let pool2 = setup_postgres(&config_two).await;

    let (_, _state_one, config_one, addr1) =
        start_server_one(config_one, pool1.clone(), &config_two).await;

    let url_one = format!("tcp://{}", addr1);
    println!("url_one: {}, config.url: {}", url_one, config_one.url);

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
    let (_state_two, config_two, addr2) = start_server_two(config_two, pool2.clone()).await;
    let url_two = format!("tcp://{}", addr2);
    println!("url_two: {}, config.url: {}", url_two, config_two.url);

    let client_two = client_cache.request(&url_two).await.unwrap().unwrap();

    // also client_two should be distinct from client one:
    assert_ne!(client_one_id_first, client_two.id().clone());
}

#[tokio::test]
async fn replication_can_be_disabled() {
    let docker = clients::Cli::default();

    let postgres = start_containers(&docker).await;
    let postgres_port = postgres.get_host_port_ipv4(5432);
    let config = test_config_one_no_replication(postgres_port);
    let pool = setup_postgres(&config).await;

    let (_, _state_one, _config_one, addr) =
        start_server_one(config, pool.clone(), &test_config_two(postgres_port)).await;

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
    let request = put_helper(
        dime,
        payload,
        chunk_size,
        HashMap::default(),
        Vec::default(),
    );

    let mut os_client = get_object_client(addr).await;
    let response = os_client.put(request).await;

    match response {
        Ok(_) => (),
        _ => assert_eq!(format!("{:?}", response), ""),
    }

    let dime = generate_dime(
        vec![audience1.clone(), audience2.clone(), audience3.clone()],
        vec![signature1.clone(), signature2.clone(), signature3.clone()],
    );
    let payload: bytes::Bytes = "testing small payload 3".as_bytes().into();
    let request = put_helper(
        dime,
        payload,
        chunk_size,
        HashMap::default(),
        Vec::default(),
    );

    let response = os_client.put(request).await;

    match response {
        Ok(_) => (),
        _ => assert_eq!(format!("{:?}", response), ""),
    }

    let dime = generate_dime(
        vec![audience1.clone(), audience2.clone(), audience3.clone()],
        vec![signature1.clone(), signature2.clone(), signature3.clone()],
    );
    let payload: bytes::Bytes = "testing small payload 4".as_bytes().into();
    let request = put_helper(
        dime,
        payload.clone(),
        chunk_size,
        HashMap::default(),
        Vec::default(),
    );

    let response = os_client.put(request).await;

    match response {
        Ok(_) => {
            assert_eq!(
                replication_object_uuids(&pool, audience1.public_key().as_str(), 50)
                    .await
                    .unwrap()
                    .len(),
                0
            );
            assert_eq!(
                replication_object_uuids(&pool, audience2.public_key().as_str(), 50)
                    .await
                    .unwrap()
                    .len(),
                0
            );
            assert_eq!(
                replication_object_uuids(&pool, audience3.public_key().as_str(), 50)
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
async fn end_to_end_replication() {
    env_logger::init();

    let docker = clients::Cli::default();

    let postgres = start_containers(&docker).await;
    let postgres_port = postgres.get_host_port_ipv4(5432);
    let config_one = test_config_one(postgres_port);
    let pool1 = setup_postgres(&config_one).await;

    let postgres_two = start_containers(&docker).await;
    let postgres_port_two = postgres_two.get_host_port_ipv4(5432);
    let config_two = Config {
        url: "0.0.0.0:6790".parse().unwrap(), //hardcoded - make sure it's different in other tests with replication
        ..test_config_two(postgres_port_two)
    };
    let pool2 = setup_postgres(&config_two).await;

    let (_, mut state_one, config_one, addr1) =
        start_server_one(config_one, pool1.clone(), &config_two).await;
    let (_state_two, _config_two, addr2) = start_server_two(config_two, pool2.clone()).await;

    let mut client_one = get_object_client(addr1).await;

    let (audience1, signature1) = party_1();
    let (audience2, signature2) = party_2();
    let (audience3, signature3) = party_3();

    // put object for party_1 - requires no replication
    {
        let dime = generate_dime(vec![audience1.clone()], vec![signature1.clone()]);
        let payload: bytes::Bytes = "testing small payload 1".as_bytes().into();
        let chunk_size = 500; // full payload in one packet
        let request = put_helper(
            dime,
            payload,
            chunk_size,
            HashMap::default(),
            Vec::default(),
        );

        let response = client_one.put(request).await;

        match response {
            Ok(_) => {
                assert_eq!(
                    replication_object_uuids(&pool1, audience1.public_key().as_str(), 50)
                        .await
                        .unwrap()
                        .len(),
                    0
                );
            }
            _ => assert_eq!(format!("{:?}", response), ""),
        }
    }

    // put 3 objects for party_1, party_2, party_3
    {
        let dime = generate_dime(
            vec![audience1.clone(), audience2.clone(), audience3.clone()],
            vec![signature1.clone(), signature2.clone(), signature3.clone()],
        );
        let payload: bytes::Bytes = "testing small payload 2".as_bytes().into();
        let chunk_size = 500; // full payload in one packet
        let request = put_helper(
            dime,
            payload,
            chunk_size,
            HashMap::default(),
            Vec::default(),
        );
        let response = client_one.put(request).await;

        // TODO remove theses matches - hard to read and coudl let undefined states fall through
        match response {
            Ok(_) => (),
            _ => assert_eq!(format!("{:?}", response), ""),
        }

        let dime = generate_dime(
            vec![audience1.clone(), audience2.clone(), audience3.clone()],
            vec![signature1.clone(), signature2.clone(), signature3.clone()],
        );
        let payload: bytes::Bytes = "testing small payload 3".as_bytes().into();
        let request = put_helper(
            dime,
            payload,
            chunk_size,
            HashMap::default(),
            Vec::default(),
        );
        let response = client_one.put(request).await;

        match response {
            Ok(_) => (),
            _ => assert_eq!(format!("{:?}", response), ""),
        }

        let dime = generate_dime(
            vec![audience1.clone(), audience2.clone(), audience3.clone()],
            vec![signature1.clone(), signature2.clone(), signature3.clone()],
        );
        let payload4: bytes::Bytes = "testing small payload 4".as_bytes().into();
        let request = put_helper(
            dime,
            payload4,
            chunk_size,
            HashMap::default(),
            Vec::default(),
        );

        let response = client_one.put(request).await;

        match response {
            Ok(_) => {
                assert_eq!(
                    replication_object_uuids(&pool1, audience1.public_key().as_str(), 50)
                        .await
                        .unwrap()
                        .len(),
                    0
                );
                assert_eq!(
                    replication_object_uuids(&pool1, audience2.public_key().as_str(), 50)
                        .await
                        .unwrap()
                        .len(),
                    3
                );
                assert_eq!(
                    replication_object_uuids(&pool1, audience3.public_key().as_str(), 50)
                        .await
                        .unwrap()
                        .len(),
                    3
                );

                assert_eq!(
                    replication_object_uuids(&pool2, audience1.public_key().as_str(), 50)
                        .await
                        .unwrap()
                        .len(),
                    0
                );
                assert_eq!(
                    replication_object_uuids(&pool2, audience2.public_key().as_str(), 50)
                        .await
                        .unwrap()
                        .len(),
                    0
                );
                assert_eq!(
                    replication_object_uuids(&pool2, audience3.public_key().as_str(), 50)
                        .await
                        .unwrap()
                        .len(),
                    0
                );
            }
            _ => assert_eq!(format!("{:?}", response), ""),
        }
    }

    let mut client_cache_one =
        ClientCache::new(config_one.backoff_min_wait, config_one.backoff_max_wait);

    // run replication iteration
    // Check source server for correct counts
    // Verify remote server is all 0
    {
        replicate_iteration(&mut state_one, &mut client_cache_one).await;

        assert_eq!(
            replication_object_uuids(&pool1, audience1.public_key().as_str(), 50)
                .await
                .unwrap()
                .len(),
            0
        );
        assert_eq!(
            replication_object_uuids(&pool1, audience2.public_key().as_str(), 50)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            replication_object_uuids(&pool1, audience3.public_key().as_str(), 50)
                .await
                .unwrap()
                .len(),
            3
        );

        // Count should be 0 for all audience on remote
        {
            assert_eq!(
                replication_object_uuids(&pool2, audience1.public_key().as_str(), 50)
                    .await
                    .unwrap()
                    .len(),
                0
            );
            assert_eq!(
                replication_object_uuids(&pool2, audience2.public_key().as_str(), 50)
                    .await
                    .unwrap()
                    .len(),
                0
            );
            assert_eq!(
                replication_object_uuids(&pool2, audience3.public_key().as_str(), 50)
                    .await
                    .unwrap()
                    .len(),
                0
            );
        }
    }

    // Run again - TODO: why 0-0-3?
    {
        replicate_iteration(&mut state_one, &mut client_cache_one).await;

        assert_eq!(
            replication_object_uuids(&pool1, audience1.public_key().as_str(), 50)
                .await
                .unwrap()
                .len(),
            0
        );
        assert_eq!(
            replication_object_uuids(&pool1, audience2.public_key().as_str(), 50)
                .await
                .unwrap()
                .len(),
            0
        );
        assert_eq!(
            replication_object_uuids(&pool1, audience3.public_key().as_str(), 50)
                .await
                .unwrap()
                .len(),
            3
        );

        assert_eq!(
            replication_object_uuids(&pool2, audience1.public_key().as_str(), 50)
                .await
                .unwrap()
                .len(),
            0
        );
        assert_eq!(
            replication_object_uuids(&pool2, audience2.public_key().as_str(), 50)
                .await
                .unwrap()
                .len(),
            0
        );
        assert_eq!(
            replication_object_uuids(&pool2, audience3.public_key().as_str(), 50)
                .await
                .unwrap()
                .len(),
            0
        );
    }

    // verify db on remote instance to check for 3 objects for party_2
    assert_eq!(get_object_count(&pool2).await, 3);

    // pull one object from local instance and verify all rows against the same one that was replicated to the remote
    {
        let mut client_two = get_object_client(addr2).await;

        let public_key = audience3.public_key_decoded();
        let payload4: bytes::Bytes = "testing small payload 4".as_bytes().into();
        let request = pb::HashRequest {
            hash: hash(payload4),
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
}

#[ignore = "unimplemented"]
#[tokio::test]
async fn late_remote_url_can_replicate() {}

#[tokio::test]
async fn late_local_url_can_cleanup() {
    let docker = clients::Cli::default();

    let postgres = start_containers(&docker).await;
    let postgres_port = postgres.get_host_port_ipv4(5432);
    let config = test_config_one(postgres_port);
    let pool = setup_postgres(&config).await;
    let (cache, _state_one, _config, addr) =
        start_server_one(config, pool.clone(), &test_config_two(postgres_port)).await;

    let (audience1, signature1) = party_1();
    let (audience3, signature3) = party_3();

    // put 3 objects for party_1, party_3
    let dime = generate_dime(
        vec![audience1.clone(), audience3.clone()],
        vec![signature1.clone(), signature3.clone()],
    );
    let payload: bytes::Bytes = "testing small payload 2".as_bytes().into();
    let chunk_size = 500; // full payload in one packet
    let request = put_helper(
        dime,
        payload,
        chunk_size,
        HashMap::default(),
        Vec::default(),
    );

    let mut os_client = get_object_client(addr).await;
    let response = os_client.put(request).await;

    match response {
        Ok(_) => (),
        _ => assert_eq!(format!("{:?}", response), ""),
    }

    let dime = generate_dime(
        vec![audience1.clone(), audience3.clone()],
        vec![signature1.clone(), signature3.clone()],
    );
    let payload: bytes::Bytes = "testing small payload 3".as_bytes().into();
    let request = put_helper(
        dime,
        payload,
        chunk_size,
        HashMap::default(),
        Vec::default(),
    );

    let response = os_client.put(request).await;

    match response {
        Ok(_) => (),
        _ => assert_eq!(format!("{:?}", response), ""),
    }

    let dime = generate_dime(
        vec![audience1.clone(), audience3.clone()],
        vec![signature1.clone(), signature3.clone()],
    );
    let payload: bytes::Bytes = "testing small payload 4".as_bytes().into();
    let request = put_helper(
        dime,
        payload.clone(),
        chunk_size,
        HashMap::default(),
        Vec::default(),
    );

    let response = os_client.put(request).await;

    match response {
        Ok(_) => {
            assert_eq!(
                replication_object_uuids(&pool, audience1.public_key().as_str(), 50)
                    .await
                    .unwrap()
                    .len(),
                0
            );
            assert_eq!(
                replication_object_uuids(&pool, audience3.public_key().as_str(), 50)
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
        replication_object_uuids(&pool, audience1.public_key().as_str(), 50)
            .await
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        replication_object_uuids(&pool, audience3.public_key().as_str(), 50)
            .await
            .unwrap()
            .len(),
        0
    );
}

#[ignore = "unimplemented"]
#[tokio::test]
async fn handles_offline_remote() {}
