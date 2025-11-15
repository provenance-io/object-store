mod common;

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use linked_hash_map::LinkedHashMap;
use object_store::cache::Cache;
use object_store::config::Config;
use object_store::datastore::{self, replication_object_uuids, PublicKey};
use object_store::object::ObjectGrpc;
use object_store::pb::chunk::Impl::{Data, End};
use object_store::pb::chunk_bidi::Impl::{
    Chunk as ChunkEnum, MultiStreamHeader as MultiStreamHeaderEnum,
};

use object_store::pb::object_service_server::ObjectServiceServer;
use object_store::proto_helpers::{AudienceUtil, StringUtil, VecUtil};
use object_store::storage::new_storage;
use object_store::{consts::*, pb::HashRequest};

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use testcontainers::*;
use tonic::Request;

use crate::common::client::get_object_client;
use crate::common::db::start_postgres;
use crate::common::{
    generate_dime, get_mailbox_keys_by_object, get_public_keys_by_object, hash, party_1, party_2,
    party_3, put_helper, test_config, test_public_key,
};

pub async fn setup_postgres(config: &Config) -> PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(config.db_connection_string().as_str())
        .await
        .unwrap();

    sqlx::migrate!().run(&pool).await.unwrap();

    pool
}

async fn start_server(config: Config) -> (Arc<PgPool>, Arc<Config>) {
    let cache = {
        let mut cache = Cache::default();
        cache.add_public_key(PublicKey {
            auth_data: Some(String::from("x-test-header:test_value_1")),
            ..test_public_key(party_1().0.public_key)
        });
        cache.add_public_key(PublicKey {
            url: String::from("tcp://party2:8080"),
            auth_data: Some(String::from("x-test-header:test_value_2")),
            ..test_public_key(party_2().0.public_key)
        });
        Arc::new(Mutex::new(cache))
    };

    let db_port = config.db_port;
    let pool = setup_postgres(&config).await;
    let pool = Arc::new(pool);
    let storage = new_storage(&config).unwrap();
    let config = Arc::new(config);
    let object_service = ObjectGrpc::new(cache, config.clone(), pool.clone(), storage.clone());

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
        ..test_config(db_port)
    });

    (pool, updated_config)
}

pub async fn delete_properties(db: &PgPool, object_uuid: &uuid::Uuid) -> u64 {
    let query_str = "UPDATE object SET properties = null WHERE uuid = $1";

    sqlx::query(query_str)
        .bind(object_uuid)
        .execute(db)
        .await
        .unwrap()
        .rows_affected()
}

// TODO test validation of sent data

#[tokio::test]
async fn simple_put() {
    let docker = clients::Cli::default();
    let container = start_postgres(&docker).await;
    let postgres_port = container.get_host_port_ipv4(5432);

    let (db, config) = start_server(test_config(postgres_port)).await;

    let (audience, signature) = party_1();
    let dime = generate_dime(vec![audience], vec![signature]);
    let dime_uuid = dime.uuid.as_hyphenated().to_string();
    let payload: bytes::Bytes = "testing small payload".as_bytes().into();
    let payload_len = payload.len() as i64;
    let chunk_size = 500; // full payload in one packet
    let request = put_helper(
        dime,
        payload,
        chunk_size,
        HashMap::default(),
        Vec::default(),
    );

    let mut os_client = get_object_client(config.url).await;
    let response = os_client.put(request).await;

    match response {
        Ok(response) => {
            let response = response.into_inner();
            let uuid = response.uuid.unwrap().value;
            let uuid_typed = uuid::Uuid::from_str(uuid.as_str()).unwrap();
            let object = datastore::get_object_by_uuid(&db, &uuid_typed)
                .await
                .unwrap();
            let mut properties = LinkedHashMap::new();
            properties.insert(HASH_FIELD_NAME.to_owned(), object.hash.decoded().unwrap());
            properties.insert(
                SIGNATURE_FIELD_NAME.to_owned(),
                "signature".as_bytes().to_owned(),
            );
            properties.insert(
                SIGNATURE_PUBLIC_KEY_FIELD_NAME.to_owned(),
                "signature public key".as_bytes().to_owned(),
            );

            assert_ne!(uuid, dime_uuid);
            assert_eq!(response.name, NOT_STORAGE_BACKED);
            assert_eq!(response.metadata.unwrap().content_length, payload_len);
            assert_eq!(object.properties, properties)
        }
        _ => assert_eq!(format!("{:?}", response), ""),
    }
}

#[tokio::test]
async fn simple_put_with_auth_failure_no_header() {
    let docker = clients::Cli::default();
    let container = start_postgres(&docker).await;
    let postgres_port = container.get_host_port_ipv4(5432);

    let config = Config {
        user_auth_enabled: true,
        ..test_config(postgres_port)
    };
    let (_, config) = start_server(config).await;

    let (audience, signature) = party_1();
    let dime = generate_dime(vec![audience], vec![signature]);
    let payload: bytes::Bytes = "testing small payload".as_bytes().into();
    let chunk_size = 500; // full payload in one packet
    let request = put_helper(
        dime,
        payload,
        chunk_size,
        HashMap::default(),
        Vec::default(),
    );

    let mut os_client = get_object_client(config.url).await;
    let response = os_client.put(request).await;

    match response {
        Err(err) => assert_eq!(err.code(), tonic::Code::PermissionDenied),
        _ => assert_eq!(format!("{:?}", response), ""),
    }
}

#[tokio::test]
async fn simple_put_with_auth_failure_incorrect_value() {
    let docker = clients::Cli::default();
    let container = start_postgres(&docker).await;
    let postgres_port = container.get_host_port_ipv4(5432);

    let config = Config {
        user_auth_enabled: true,
        ..test_config(postgres_port)
    };
    let (_, config) = start_server(config).await;

    let (audience, signature) = party_1();
    let dime = generate_dime(vec![audience], vec![signature]);
    let payload: bytes::Bytes = "testing small payload".as_bytes().into();
    let chunk_size = 500; // full payload in one packet
    let request = put_helper(
        dime,
        payload,
        chunk_size,
        HashMap::default(),
        vec![("x-test-header", "test_value_2")],
    );

    let mut os_client = get_object_client(config.url).await;
    let response = os_client.put(request).await;

    match response {
        Err(err) => assert_eq!(err.code(), tonic::Code::PermissionDenied),
        _ => assert_eq!(format!("{:?}", response), ""),
    }
}

#[tokio::test]
async fn simple_put_with_auth_success() {
    let docker = clients::Cli::default();
    let container = start_postgres(&docker).await;
    let postgres_port = container.get_host_port_ipv4(5432);

    let config = Config {
        user_auth_enabled: true,
        ..test_config(postgres_port)
    };
    let (db, config) = start_server(config).await;

    let (audience, signature) = party_1();
    let dime = generate_dime(vec![audience], vec![signature]);
    let dime_uuid = dime.uuid.as_hyphenated().to_string();
    let payload: bytes::Bytes = "testing small payload".as_bytes().into();
    let payload_len = payload.len() as i64;
    let chunk_size = 500; // full payload in one packet
    let request = put_helper(
        dime,
        payload,
        chunk_size,
        HashMap::default(),
        vec![("x-test-header", "test_value_1")],
    );

    let mut os_client = get_object_client(config.url).await;
    let response = os_client.put(request).await;

    match response {
        Ok(response) => {
            let response = response.into_inner();
            let uuid = response.uuid.unwrap().value;
            let uuid_typed = uuid::Uuid::from_str(uuid.as_str()).unwrap();
            let object = datastore::get_object_by_uuid(&db, &uuid_typed)
                .await
                .unwrap();
            let mut properties = LinkedHashMap::new();
            properties.insert(HASH_FIELD_NAME.to_owned(), object.hash.decoded().unwrap());
            properties.insert(
                SIGNATURE_FIELD_NAME.to_owned(),
                "signature".as_bytes().to_owned(),
            );
            properties.insert(
                SIGNATURE_PUBLIC_KEY_FIELD_NAME.to_owned(),
                "signature public key".as_bytes().to_owned(),
            );

            assert_ne!(uuid, dime_uuid);
            assert_eq!(response.name, NOT_STORAGE_BACKED);
            assert_eq!(response.metadata.unwrap().content_length, payload_len);
            assert_eq!(object.properties, properties)
        }
        _ => assert_eq!(format!("{:?}", response), ""),
    }
}

#[tokio::test]
async fn multi_packet_file_store_put() {
    let docker = clients::Cli::default();
    let container = start_postgres(&docker).await;
    let postgres_port = container.get_host_port_ipv4(5432);

    let (_, config) = start_server(test_config(postgres_port)).await;

    let (audience, signature) = party_1();
    let dime = generate_dime(vec![audience], vec![signature]);
    let dime_uuid = dime.uuid.as_hyphenated().to_string();
    let payload: bytes::Bytes = "testing larger payload ".repeat(250).into_bytes().into();
    let payload_len = payload.len() as i64;
    let chunk_size = 100; // split payload into packets
    let request = put_helper(
        dime,
        payload,
        chunk_size,
        HashMap::default(),
        Vec::default(),
    );

    let mut os_client = get_object_client(config.url).await;
    let response = os_client.put(request).await;

    match response {
        Ok(response) => {
            let response = response.into_inner();

            assert_ne!(response.uuid.unwrap().value, dime_uuid);
            assert_ne!(response.name, NOT_STORAGE_BACKED);
            assert_eq!(response.metadata.unwrap().content_length, payload_len);
        }
        _ => assert_eq!(format!("{:?}", response), ""),
    }
}

#[tokio::test]
async fn multi_party_put() {
    let docker = clients::Cli::default();
    let container = start_postgres(&docker).await;
    let postgres_port = container.get_host_port_ipv4(5432);

    let (db, config) = start_server(test_config(postgres_port)).await;

    let (audience1, signature1) = party_1();
    let (audience2, signature2) = party_2();
    let (audience3, signature3) = party_3();
    let dime = generate_dime(
        vec![audience1, audience2, audience3],
        vec![signature1, signature2, signature3],
    );
    let payload: bytes::Bytes = "testing small payload".as_bytes().into();
    let chunk_size = 500; // full payload in one packet
    let request = put_helper(
        dime,
        payload,
        chunk_size,
        HashMap::default(),
        Vec::default(),
    );

    let mut os_client = get_object_client(config.url).await;
    let response = os_client.put(request).await;

    match response {
        Ok(response) => {
            let response = response.into_inner();
            let uuid = response.uuid.unwrap().value;
            let uuid = uuid::Uuid::from_str(uuid.as_str()).unwrap();

            assert_eq!(get_public_keys_by_object(&db, &uuid).await.len(), 3);
            assert_eq!(get_mailbox_keys_by_object(&db, &uuid).await.len(), 0);
        }
        _ => assert_eq!(format!("{:?}", response), ""),
    }
}

#[tokio::test]
async fn small_mailbox_put() {
    let docker = clients::Cli::default();
    let container = start_postgres(&docker).await;
    let postgres_port = container.get_host_port_ipv4(5432);

    let (db, config) = start_server(test_config(postgres_port)).await;

    let (audience1, signature1) = party_1();
    let (audience2, signature2) = party_2();
    let (audience3, signature3) = party_3();
    let mut dime = generate_dime(
        vec![audience1, audience2, audience3],
        vec![signature1, signature2, signature3],
    );
    dime.metadata
        .insert(MAILBOX_KEY.to_owned(), MAILBOX_FRAGMENT_REQUEST.to_owned());
    let payload: bytes::Bytes = "testing small payload".as_bytes().into();
    let chunk_size = 500; // full payload in one packet
    let request = put_helper(
        dime,
        payload,
        chunk_size,
        HashMap::default(),
        Vec::default(),
    );

    let mut os_client = get_object_client(config.url).await;
    let response = os_client.put(request).await;

    match response {
        Ok(response) => {
            let response = response.into_inner();
            let uuid = response.uuid.unwrap().value;
            let uuid = uuid::Uuid::from_str(uuid.as_str()).unwrap();

            assert_eq!(response.name, NOT_STORAGE_BACKED);
            assert_eq!(get_public_keys_by_object(&db, &uuid).await.len(), 3);
            assert_eq!(get_mailbox_keys_by_object(&db, &uuid).await.len(), 2);
        }
        _ => assert_eq!(format!("{:?}", response), ""),
    }
}

#[tokio::test]
async fn large_mailbox_put() {
    let docker = clients::Cli::default();
    let container = start_postgres(&docker).await;
    let postgres_port = container.get_host_port_ipv4(5432);

    let (db, config) = start_server(test_config(postgres_port)).await;

    let (audience1, signature1) = party_1();
    let (audience2, signature2) = party_2();
    let (audience3, signature3) = party_3();
    let mut dime = generate_dime(
        vec![audience1, audience2, audience3],
        vec![signature1, signature2, signature3],
    );
    dime.metadata
        .insert(MAILBOX_KEY.to_owned(), MAILBOX_FRAGMENT_REQUEST.to_owned());
    let payload: bytes::Bytes = "testing larger payload ".repeat(250).into_bytes().into();
    let chunk_size = 100; // split payload into packets
    let request = put_helper(
        dime,
        payload,
        chunk_size,
        HashMap::default(),
        Vec::default(),
    );

    let mut os_client = get_object_client(config.url).await;
    let response = os_client.put(request).await;

    match response {
        Ok(response) => {
            let response = response.into_inner();
            let uuid = response.uuid.unwrap().value;
            let uuid = uuid::Uuid::from_str(uuid.as_str()).unwrap();

            assert_eq!(response.name, NOT_STORAGE_BACKED);
            assert_eq!(get_public_keys_by_object(&db, &uuid).await.len(), 3);
            assert_eq!(get_mailbox_keys_by_object(&db, &uuid).await.len(), 2);
        }
        _ => assert_eq!(format!("{:?}", response), ""),
    }
}

#[tokio::test]
async fn simple_get() {
    let docker = clients::Cli::default();
    let container = start_postgres(&docker).await;
    let postgres_port = container.get_host_port_ipv4(5432);

    let (_, config) = start_server(test_config(postgres_port)).await;

    let (audience, signature) = party_1();
    let dime = generate_dime(vec![audience.clone()], vec![signature]);
    let payload: bytes::Bytes = "testing small payload".as_bytes().into();
    let chunk_size = 500; // full payload in one packet
    let request = put_helper(
        dime,
        payload.clone(),
        chunk_size,
        HashMap::default(),
        Vec::default(),
    );

    let mut os_client = get_object_client(config.url).await;
    let response = os_client.put(request).await;

    match response {
        Ok(response) => {
            let response = response.into_inner();

            assert_eq!(response.name, NOT_STORAGE_BACKED);
        }
        _ => assert_eq!(format!("{:?}", response), ""),
    }

    let mut client = get_object_client(config.url).await;
    let public_key = audience.public_key_decoded();
    let request = HashRequest {
        hash: hash(payload),
        public_key,
    };
    let response = client.get(Request::new(request)).await;

    match response {
        Ok(response) => {
            let mut response = response.into_inner();

            // multi stream header
            let msg = response.message().await.unwrap();
            if let Some(msg) = msg {
                match msg.r#impl {
                    Some(MultiStreamHeaderEnum(stream_header)) => {
                        assert_eq!(stream_header.stream_count, 1);
                    }
                    _ => assert_eq!(format!("{:?}", msg), ""),
                }
            } else {
                assert_eq!(format!("{:?}", msg), "");
            }

            // data chunk
            let msg = response.message().await.unwrap();
            if let Some(msg) = msg {
                match msg.clone().r#impl {
                    Some(ChunkEnum(chunk)) => match chunk.r#impl {
                        Some(Data(_)) => (),
                        _ => assert_eq!(format!("{:?}", msg), ""),
                    },
                    _ => assert_eq!(format!("{:?}", msg), ""),
                }
            } else {
                assert_eq!(format!("{:?}", msg), "");
            }

            // end chunk
            let msg = response.message().await.unwrap();
            if let Some(msg) = msg {
                match msg.clone().r#impl {
                    Some(ChunkEnum(chunk)) => match chunk.r#impl {
                        Some(End(_)) => (),
                        _ => assert_eq!(format!("{:?}", msg), ""),
                    },
                    _ => assert_eq!(format!("{:?}", msg), ""),
                }
            } else {
                assert_eq!(format!("{:?}", msg), "");
            }
        }
        _ => assert_eq!(format!("{:?}", response), ""),
    }
}

#[tokio::test]
async fn auth_get_failure_no_key() {
    let docker = clients::Cli::default();
    let container = start_postgres(&docker).await;
    let postgres_port = container.get_host_port_ipv4(5432);

    let mut config = test_config(postgres_port);
    config.user_auth_enabled = true;
    let (_, config) = start_server(config).await;

    let (audience, signature) = party_1();
    let dime = generate_dime(vec![audience.clone()], vec![signature]);
    let payload: bytes::Bytes = "testing larger payload ".repeat(250).into_bytes().into();
    let chunk_size = 100; // split payload into packets
    let request = put_helper(
        dime,
        payload.clone(),
        chunk_size,
        HashMap::default(),
        vec![("x-test-header", "test_value_1")],
    );

    let mut os_client = get_object_client(config.url).await;
    let response = os_client.put(request).await;

    match response {
        Ok(response) => {
            let response = response.into_inner();

            assert_ne!(response.name, NOT_STORAGE_BACKED);
        }
        _ => assert_eq!(format!("{:?}", response), ""),
    }

    let mut client = get_object_client(config.url).await;
    let public_key = audience.public_key_decoded();
    let request = HashRequest {
        hash: hash(payload),
        public_key,
    };
    let response = client.get(Request::new(request)).await;

    match response {
        Err(err) => assert_eq!(err.code(), tonic::Code::PermissionDenied),
        _ => assert_eq!(format!("{:?}", response), ""),
    }
}

#[tokio::test]
async fn auth_get_failure_invalid_key() {
    let docker = clients::Cli::default();
    let container = start_postgres(&docker).await;
    let postgres_port = container.get_host_port_ipv4(5432);

    let mut config = test_config(postgres_port);
    config.user_auth_enabled = true;
    let (_, config) = start_server(config).await;

    let (audience, signature) = party_1();
    let dime = generate_dime(vec![audience.clone()], vec![signature]);
    let payload: bytes::Bytes = "testing larger payload ".repeat(250).into_bytes().into();
    let chunk_size = 100; // split payload into packets
    let request = put_helper(
        dime,
        payload.clone(),
        chunk_size,
        HashMap::default(),
        vec![("x-test-header", "test_value_1")],
    );

    let mut os_client = get_object_client(config.url).await;
    let response = os_client.put(request).await;

    match response {
        Ok(response) => {
            let response = response.into_inner();

            assert_ne!(response.name, NOT_STORAGE_BACKED);
        }
        _ => assert_eq!(format!("{:?}", response), ""),
    }

    let mut client = get_object_client(config.url).await;
    let public_key = audience.public_key_decoded();
    let mut request = Request::new(HashRequest {
        hash: hash(payload),
        public_key,
    });
    let metadata = request.metadata_mut();
    metadata.insert("x-test-header", "test_value_2".parse().unwrap());
    let response = client.get(request).await;

    match response {
        Err(err) => assert_eq!(err.code(), tonic::Code::PermissionDenied),
        _ => assert_eq!(format!("{:?}", response), ""),
    }
}

#[tokio::test]
async fn auth_get_success() {
    let docker = clients::Cli::default();
    let container = start_postgres(&docker).await;
    let postgres_port = container.get_host_port_ipv4(5432);

    let config = Config {
        user_auth_enabled: true,
        ..test_config(postgres_port)
    };
    let (_, config) = start_server(config).await;

    let (audience, signature) = party_1();
    let dime = generate_dime(vec![audience.clone()], vec![signature]);
    let payload: bytes::Bytes = "testing larger payload ".repeat(250).into_bytes().into();
    let chunk_size = 100; // split payload into packets
    let request = put_helper(
        dime,
        payload.clone(),
        chunk_size,
        HashMap::default(),
        vec![("x-test-header", "test_value_1")],
    );
    let mut os_client = get_object_client(config.url).await;
    let response = os_client.put(request).await;

    match response {
        Ok(response) => {
            let response = response.into_inner();

            assert_ne!(response.name, NOT_STORAGE_BACKED);
        }
        _ => assert_eq!(format!("{:?}", response), ""),
    }

    let mut client = get_object_client(config.url).await;
    let public_key = audience.public_key_decoded();
    let mut request = Request::new(HashRequest {
        hash: hash(payload),
        public_key,
    });
    let metadata = request.metadata_mut();
    metadata.insert("x-test-header", "test_value_1".parse().unwrap());
    let response = client.get(request).await;

    match response {
        Ok(_) => (),
        _ => assert_eq!(format!("{:?}", response), ""),
    }
}

#[tokio::test]
async fn multi_packet_file_store_get() {
    let docker = clients::Cli::default();
    let container = start_postgres(&docker).await;
    let postgres_port = container.get_host_port_ipv4(5432);

    let (_, config) = start_server(test_config(postgres_port)).await;

    let (audience, signature) = party_1();
    let dime = generate_dime(vec![audience.clone()], vec![signature]);
    let payload: bytes::Bytes = "testing larger payload ".repeat(250).into_bytes().into();
    let chunk_size = 100; // split payload into packets
    let request = put_helper(
        dime,
        payload.clone(),
        chunk_size,
        HashMap::default(),
        Vec::default(),
    );
    let mut os_client = get_object_client(config.url).await;
    let response = os_client.put(request).await;

    match response {
        Ok(response) => {
            let response = response.into_inner();

            assert_ne!(response.name, NOT_STORAGE_BACKED);
        }
        _ => assert_eq!(format!("{:?}", response), ""),
    }

    let mut client = get_object_client(config.url).await;
    let public_key = audience.public_key_decoded();
    let request = HashRequest {
        hash: hash(payload),
        public_key,
    };
    let response = client.get(Request::new(request)).await;

    match response {
        Ok(_) => (),
        _ => assert_eq!(format!("{:?}", response), ""),
    }
}

#[tokio::test]
async fn multi_party_non_owner_get() {
    let docker = clients::Cli::default();
    let container = start_postgres(&docker).await;
    let postgres_port = container.get_host_port_ipv4(5432);

    let (_, config) = start_server(test_config(postgres_port)).await;

    let (audience1, signature1) = party_1();
    let (audience2, signature2) = party_2();
    let (audience3, signature3) = party_3();
    let dime = generate_dime(
        vec![audience1, audience2, audience3.clone()],
        vec![signature1, signature2, signature3],
    );
    let payload: bytes::Bytes = "testing small payload".as_bytes().into();
    let chunk_size = 500; // full payload in one packet
    let request = put_helper(
        dime,
        payload.clone(),
        chunk_size,
        HashMap::default(),
        Vec::default(),
    );

    let mut os_client = get_object_client(config.url).await;
    let response = os_client.put(request).await;

    match response {
        Ok(_) => (),
        _ => assert_eq!(format!("{:?}", response), ""),
    }
    let mut client = get_object_client(config.url).await;
    let public_key = audience3.public_key_decoded();
    let request = HashRequest {
        hash: hash(payload),
        public_key,
    };
    let response = client.get(Request::new(request)).await;

    match response {
        Ok(_) => (),
        _ => assert_eq!(format!("{:?}", response), ""),
    }
}

#[tokio::test]
async fn dupe_objects_noop() {
    let docker = clients::Cli::default();
    let container = start_postgres(&docker).await;
    let postgres_port = container.get_host_port_ipv4(5432);

    let (_, config) = start_server(test_config(postgres_port)).await;

    let (audience, signature) = party_1();
    let dime = generate_dime(vec![audience], vec![signature]);
    let payload: bytes::Bytes = "testing small payload".as_bytes().into();
    let chunk_size = 500; // full payload in one packet
    let request = put_helper(
        dime.clone(),
        payload.clone(),
        chunk_size,
        HashMap::default(),
        Vec::default(),
    );
    let request_dupe = put_helper(
        dime,
        payload,
        chunk_size,
        HashMap::default(),
        Vec::default(),
    );
    let mut os_client = get_object_client(config.url).await;
    let response = os_client.put(request).await;
    let response_dupe = os_client.put(request_dupe).await;

    assert!(response.is_ok() && response_dupe.is_ok());

    let response_inner = response.unwrap().into_inner();
    let response_dupe_inner = response_dupe.unwrap().into_inner();

    assert_eq!(response_inner.uuid, response_dupe_inner.uuid);
}

#[tokio::test]
async fn dupe_objects_added_audience() {
    let docker = clients::Cli::default();
    let container = start_postgres(&docker).await;
    let postgres_port = container.get_host_port_ipv4(5432);

    let (_, config) = start_server(test_config(postgres_port)).await;

    let (audience, signature) = party_1();
    let (audience2, signature2) = party_2();
    let dime = generate_dime(vec![audience.clone()], vec![signature.clone()]);
    let dime2 = generate_dime(vec![audience, audience2], vec![signature, signature2]);
    let payload: bytes::Bytes = "testing small payload".as_bytes().into();
    let chunk_size = 500; // full payload in one packet
    let request = put_helper(
        dime.clone(),
        payload.clone(),
        chunk_size,
        HashMap::default(),
        Vec::default(),
    );
    let request_dupe = put_helper(
        dime2,
        payload,
        chunk_size,
        HashMap::default(),
        Vec::default(),
    );

    let mut os_client = get_object_client(config.url).await;
    let response = os_client.put(request).await;
    let response_dupe = os_client.put(request_dupe).await;

    assert!(response.is_ok() && response_dupe.is_ok());

    let response_inner = response.unwrap().into_inner();
    let response_dupe_inner = response_dupe.unwrap().into_inner();

    assert_ne!(response_inner.uuid, response_dupe_inner.uuid);
}

#[tokio::test]
async fn get_with_wrong_key() {
    let docker = clients::Cli::default();
    let container = start_postgres(&docker).await;
    let postgres_port = container.get_host_port_ipv4(5432);

    let (_, config) = start_server(test_config(postgres_port)).await;

    let (audience1, signature1) = party_1();
    let (audience2, signature2) = party_2();
    let (audience3, _) = party_3();
    let dime = generate_dime(vec![audience1, audience2], vec![signature1, signature2]);
    let payload: bytes::Bytes = "testing small payload".as_bytes().into();
    let chunk_size = 500; // full payload in one packet
    let request = put_helper(
        dime,
        payload.clone(),
        chunk_size,
        HashMap::default(),
        Vec::default(),
    );

    let mut os_client = get_object_client(config.url).await;
    let response = os_client.put(request).await;

    match response {
        Ok(_) => (),
        _ => assert_eq!(format!("{:?}", response), ""),
    }
    let mut client = get_object_client(config.url).await;
    let public_key = audience3.public_key_decoded();
    let request = HashRequest {
        hash: hash(payload),
        public_key,
    };
    let response = client.get(Request::new(request)).await;

    match response {
        Err(err) => assert_eq!(err.code(), tonic::Code::NotFound),
        _ => assert_eq!(format!("{:?}", response), ""),
    }
}

#[tokio::test]
async fn get_nonexistent_hash() {
    let docker = clients::Cli::default();
    let container = start_postgres(&docker).await;
    let postgres_port = container.get_host_port_ipv4(5432);

    let (_, config) = start_server(test_config(postgres_port)).await;

    let (audience1, _) = party_1();
    let payload: bytes::Bytes = "testing small payload".as_bytes().into();
    let mut client = get_object_client(config.url).await;
    let public_key = audience1.public_key_decoded();
    let request = HashRequest {
        hash: hash(payload),
        public_key,
    };
    let response = client.get(Request::new(request)).await;

    match response {
        Err(err) => assert_eq!(err.code(), tonic::Code::NotFound),
        _ => assert_eq!(format!("{:?}", response), ""),
    }
}

// TODO add test for local cache with non owner - make owner the unknown one

#[tokio::test]
async fn put_with_replication() {
    let docker = clients::Cli::default();
    let container = start_postgres(&docker).await;
    let postgres_port = container.get_host_port_ipv4(5432);

    let (db, config) = start_server(test_config(postgres_port)).await;

    let (audience1, signature1) = party_1();
    let (audience2, signature2) = party_2();
    let (audience3, signature3) = party_3();
    let dime = generate_dime(
        vec![audience1.clone(), audience2.clone(), audience3.clone()],
        vec![signature1, signature2, signature3],
    );
    let mut extra_properties = HashMap::new();
    extra_properties.insert(SOURCE_KEY.to_owned(), String::from("standard key"));
    let payload: bytes::Bytes = "testing small payload".as_bytes().into();
    let chunk_size = 500; // full payload in one packet
    let request = put_helper(dime, payload, chunk_size, extra_properties, Vec::default());

    let mut os_client = get_object_client(config.url).await;
    let response = os_client.put(request).await;

    match response {
        Ok(response) => {
            let response = response.into_inner();
            let uuid = response.uuid.unwrap().value;
            let uuid = uuid::Uuid::from_str(uuid.as_str()).unwrap();

            assert_eq!(response.name, NOT_STORAGE_BACKED);
            assert_eq!(get_public_keys_by_object(&db, &uuid).await.len(), 3);
            assert_eq!(
                replication_object_uuids(
                    &db,
                    String::from_utf8(audience1.public_key).unwrap().as_str(),
                    50
                )
                .await
                .unwrap()
                .len(),
                0
            );
            assert_eq!(
                replication_object_uuids(
                    &db,
                    String::from_utf8(audience2.public_key).unwrap().as_str(),
                    50
                )
                .await
                .unwrap()
                .len(),
                1
            );
            assert_eq!(
                replication_object_uuids(
                    &db,
                    String::from_utf8(audience3.public_key).unwrap().as_str(),
                    50
                )
                .await
                .unwrap()
                .len(),
                1
            );
        }
        _ => assert_eq!(format!("{:?}", response), ""),
    }
}

#[tokio::test]
async fn put_with_replication_different_owner() {
    let docker = clients::Cli::default();
    let container = start_postgres(&docker).await;
    let postgres_port = container.get_host_port_ipv4(5432);

    let (db, config) = start_server(test_config(postgres_port)).await;

    let (audience1, signature1) = party_1();
    let (audience2, signature2) = party_2();
    let (audience3, signature3) = party_3();
    let dime = generate_dime(
        vec![audience2.clone(), audience1.clone(), audience3.clone()],
        vec![signature2, signature1, signature3],
    );
    let mut extra_properties = HashMap::new();
    extra_properties.insert(SOURCE_KEY.to_owned(), String::from("standard key"));
    let payload: bytes::Bytes = "testing small payload".as_bytes().into();
    let chunk_size = 500; // full payload in one packet
    let request = put_helper(dime, payload, chunk_size, extra_properties, Vec::default());
    let mut os_client = get_object_client(config.url).await;
    let response = os_client.put(request).await;
    match response {
        Ok(response) => {
            let response = response.into_inner();
            let uuid = response.uuid.unwrap().value;
            let uuid = uuid::Uuid::from_str(uuid.as_str()).unwrap();

            assert_eq!(response.name, NOT_STORAGE_BACKED);
            assert_eq!(get_public_keys_by_object(&db, &uuid).await.len(), 3);
            assert_eq!(
                replication_object_uuids(
                    &db,
                    String::from_utf8(audience1.public_key).unwrap().as_str(),
                    50
                )
                .await
                .unwrap()
                .len(),
                0
            );
            assert_eq!(
                replication_object_uuids(
                    &db,
                    String::from_utf8(audience2.public_key).unwrap().as_str(),
                    50
                )
                .await
                .unwrap()
                .len(),
                0
            );
            assert_eq!(
                replication_object_uuids(
                    &db,
                    String::from_utf8(audience3.public_key).unwrap().as_str(),
                    50
                )
                .await
                .unwrap()
                .len(),
                1
            );
        }
        _ => assert_eq!(format!("{:?}", response), ""),
    }
}

#[tokio::test]
async fn put_with_double_replication() {
    let docker = clients::Cli::default();
    let container = start_postgres(&docker).await;
    let postgres_port = container.get_host_port_ipv4(5432);

    let (db, config) = start_server(test_config(postgres_port)).await;

    let (audience1, signature1) = party_1();
    let (audience2, signature2) = party_2();
    let (audience3, signature3) = party_3();
    let dime = generate_dime(
        vec![audience1.clone(), audience2.clone(), audience3.clone()],
        vec![signature1, signature2, signature3],
    );
    let mut extra_properties = HashMap::new();
    extra_properties.insert(SOURCE_KEY.to_owned(), SOURCE_REPLICATION.to_owned());
    let payload: bytes::Bytes = "testing small payload".as_bytes().into();
    let chunk_size = 500; // full payload in one packet
    let request = put_helper(dime, payload, chunk_size, extra_properties, Vec::default());

    let mut os_client = get_object_client(config.url).await;
    let response = os_client.put(request).await;

    match response {
        Ok(response) => {
            let response = response.into_inner();
            let uuid = response.uuid.unwrap().value;
            let uuid = uuid::Uuid::from_str(uuid.as_str()).unwrap();

            assert_eq!(response.name, NOT_STORAGE_BACKED);
            assert_eq!(get_public_keys_by_object(&db, &uuid).await.len(), 3);
            assert_eq!(
                replication_object_uuids(&db, audience1.public_key.encoded().as_str(), 50)
                    .await
                    .unwrap()
                    .len(),
                0
            );
            assert_eq!(
                replication_object_uuids(&db, audience2.public_key.encoded().as_str(), 50)
                    .await
                    .unwrap()
                    .len(),
                0
            );
            assert_eq!(
                replication_object_uuids(&db, audience3.public_key.encoded().as_str(), 50)
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
async fn get_object_no_properties() {
    let docker = clients::Cli::default();
    let container = start_postgres(&docker).await;
    let postgres_port = container.get_host_port_ipv4(5432);

    let (db, config) = start_server(test_config(postgres_port)).await;

    let (audience, signature) = party_1();
    let dime = generate_dime(vec![audience.clone()], vec![signature]);
    let payload: bytes::Bytes = "testing small payload".as_bytes().into();
    let chunk_size = 500; // full payload in one packet
    let request = put_helper(
        dime,
        payload.clone(),
        chunk_size,
        HashMap::default(),
        Vec::default(),
    );

    let mut os_client = get_object_client(config.url).await;
    let response = os_client.put(request).await;

    match response {
        Ok(response) => {
            let response = response.into_inner();
            let uuid = response.uuid.unwrap().value;
            let uuid = uuid::Uuid::from_str(uuid.as_str()).unwrap();

            assert_eq!(response.name, NOT_STORAGE_BACKED);
            assert_eq!(delete_properties(&db, &uuid).await, 1);
        }
        _ => assert_eq!(format!("{:?}", response), ""),
    }

    let mut client = get_object_client(config.url).await;
    let public_key = audience.public_key_decoded();
    let request = HashRequest {
        hash: hash(payload),
        public_key,
    };
    let response = client.get(Request::new(request)).await;

    match response {
        Ok(response) => {
            let mut response = response.into_inner();

            // multi stream header
            let msg = response.message().await.unwrap();
            if let Some(msg) = msg {
                match msg.r#impl {
                    Some(MultiStreamHeaderEnum(stream_header)) => {
                        assert_eq!(stream_header.stream_count, 1);
                    }
                    _ => assert_eq!(format!("{:?}", msg), ""),
                }
            } else {
                assert_eq!(format!("{:?}", msg), "");
            }

            // data chunk
            let msg = response.message().await.unwrap();
            if let Some(msg) = msg {
                match msg.clone().r#impl {
                    Some(ChunkEnum(chunk)) => match chunk.r#impl {
                        Some(Data(_)) => (),
                        _ => assert_eq!(format!("{:?}", msg), ""),
                    },
                    _ => assert_eq!(format!("{:?}", msg), ""),
                }
            } else {
                assert_eq!(format!("{:?}", msg), "");
            }

            // end chunk
            let msg = response.message().await.unwrap();
            if let Some(msg) = msg {
                match msg.clone().r#impl {
                    Some(ChunkEnum(chunk)) => match chunk.r#impl {
                        Some(End(_)) => (),
                        _ => assert_eq!(format!("{:?}", msg), ""),
                    },
                    _ => assert_eq!(format!("{:?}", msg), ""),
                }
            } else {
                assert_eq!(format!("{:?}", msg), "");
            }
        }
        _ => assert_eq!(format!("{:?}", response), ""),
    }
}

// TODO add test that has storage backed payload but the file wasn't written due to failure
// verify that fetch returns an accurate error and also a subsequent PUT can write the file
// TODO add test to verify owner signature is added to dime
