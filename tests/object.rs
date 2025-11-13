mod common;

use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use chrono::Utc;
use linked_hash_map::LinkedHashMap;
use object_store::cache::Cache;
use object_store::config::Config;
use object_store::datastore::{self, replication_object_uuids, AuthType, KeyType, PublicKey};
use object_store::object::ObjectGrpc;
use object_store::pb;
use object_store::pb::chunk::Impl::{Data, End};
use object_store::pb::chunk_bidi::Impl::{
    Chunk as ChunkEnum, MultiStreamHeader as MultiStreamHeaderEnum,
};

use object_store::proto_helpers::{StringUtil, VecUtil};
use object_store::storage::FileSystem;
use object_store::{consts::*, pb::HashRequest};

use serial_test::serial;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use testcontainers::*;
use tonic::Request;

use crate::common::{
    generate_dime, get_mailbox_keys_by_object, get_public_keys_by_object, hash, party_1, party_2,
    party_3, put_helper, test_config,
};

pub async fn setup_postgres(port: u16) -> PgPool {
    let connection_string = &format!("postgres://postgres:postgres@localhost:{}/postgres", port);

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(connection_string)
        .await
        .unwrap();

    sqlx::migrate!().run(&pool).await.unwrap();

    pool
}

async fn start_server(default_config: Option<Config>, postgres_port: u16) -> Arc<PgPool> {
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);

    tokio::spawn(async move {
        let mut cache = Cache::default();
        cache.add_public_key(PublicKey {
            uuid: uuid::Uuid::default(),
            public_key: std::str::from_utf8(&party_1().0.public_key)
                .unwrap()
                .to_owned(),
            public_key_type: KeyType::Secp256k1,
            url: String::from(""),
            metadata: Vec::default(),
            auth_type: Some(AuthType::Header),
            auth_data: Some(String::from("x-test-header:test_value_1")),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        });
        cache.add_public_key(PublicKey {
            uuid: uuid::Uuid::default(),
            public_key: std::str::from_utf8(&party_2().0.public_key)
                .unwrap()
                .to_owned(),
            public_key_type: KeyType::Secp256k1,
            url: String::from("tcp://party2:8080"),
            metadata: Vec::default(),
            auth_type: Some(AuthType::Header),
            auth_data: Some(String::from("x-test-header:test_value_2")),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        });
        let cache = Arc::new(Mutex::new(cache));
        let config = default_config.unwrap_or(test_config(postgres_port));
        let url = config.url;
        let pool = setup_postgres(postgres_port).await;
        let pool = Arc::new(pool);
        let storage = FileSystem::new(PathBuf::from(config.storage_base_path.as_str()));
        let object_service = ObjectGrpc::new(
            cache,
            Arc::new(config),
            pool.clone(),
            Arc::new(Box::new(storage)),
        );

        tx.send(pool.clone()).await.unwrap();

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
#[serial(grpc_server)]
async fn simple_put() {
    let docker = clients::Cli::default();
    let image = RunnableImage::from(images::postgres::Postgres::default()).with_tag("14-alpine");
    let container = docker.run(image);
    let postgres_port = container.get_host_port_ipv4(5432);

    let db = start_server(None, postgres_port).await;

    let (audience, signature) = party_1();
    let dime = generate_dime(vec![audience], vec![signature]);
    let dime_uuid = dime.uuid.as_hyphenated().to_string();
    let payload: bytes::Bytes = "testing small payload".as_bytes().into();
    let payload_len = payload.len() as i64;
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
#[serial(grpc_server)]
async fn simple_put_with_auth_failure_no_header() {
    let docker = clients::Cli::default();
    let image = RunnableImage::from(images::postgres::Postgres::default()).with_tag("14-alpine");
    let container = docker.run(image);
    let postgres_port = container.get_host_port_ipv4(5432);

    let mut config = test_config(postgres_port);
    config.user_auth_enabled = true;
    start_server(Some(config), postgres_port).await;

    let (audience, signature) = party_1();
    let dime = generate_dime(vec![audience], vec![signature]);
    let payload: bytes::Bytes = "testing small payload".as_bytes().into();
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
        Err(err) => assert_eq!(err.code(), tonic::Code::PermissionDenied),
        _ => assert_eq!(format!("{:?}", response), ""),
    }
}

#[tokio::test]
#[serial(grpc_server)]
async fn simple_put_with_auth_failure_incorrect_value() {
    let docker = clients::Cli::default();
    let image = RunnableImage::from(images::postgres::Postgres::default()).with_tag("14-alpine");
    let container = docker.run(image);
    let postgres_port = container.get_host_port_ipv4(5432);

    let mut config = test_config(postgres_port);
    config.user_auth_enabled = true;
    start_server(Some(config), postgres_port).await;

    let (audience, signature) = party_1();
    let dime = generate_dime(vec![audience], vec![signature]);
    let payload: bytes::Bytes = "testing small payload".as_bytes().into();
    let chunk_size = 500; // full payload in one packet
    let response = put_helper(
        dime,
        payload,
        chunk_size,
        HashMap::default(),
        vec![("x-test-header", "test_value_2")],
    )
    .await;

    match response {
        Err(err) => assert_eq!(err.code(), tonic::Code::PermissionDenied),
        _ => assert_eq!(format!("{:?}", response), ""),
    }
}

#[tokio::test]
#[serial(grpc_server)]
async fn simple_put_with_auth_success() {
    let docker = clients::Cli::default();
    let image = RunnableImage::from(images::postgres::Postgres::default()).with_tag("14-alpine");
    let container = docker.run(image);
    let postgres_port = container.get_host_port_ipv4(5432);

    let mut config = test_config(postgres_port);
    config.user_auth_enabled = true;
    let db = start_server(Some(config), postgres_port).await;

    let (audience, signature) = party_1();
    let dime = generate_dime(vec![audience], vec![signature]);
    let dime_uuid = dime.uuid.as_hyphenated().to_string();
    let payload: bytes::Bytes = "testing small payload".as_bytes().into();
    let payload_len = payload.len() as i64;
    let chunk_size = 500; // full payload in one packet
    let response = put_helper(
        dime,
        payload,
        chunk_size,
        HashMap::default(),
        vec![("x-test-header", "test_value_1")],
    )
    .await;

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
#[serial(grpc_server)]
async fn multi_packet_file_store_put() {
    let docker = clients::Cli::default();
    let image = RunnableImage::from(images::postgres::Postgres::default()).with_tag("14-alpine");
    let container = docker.run(image);
    let postgres_port = container.get_host_port_ipv4(5432);

    start_server(None, postgres_port).await;

    let (audience, signature) = party_1();
    let dime = generate_dime(vec![audience], vec![signature]);
    let dime_uuid = dime.uuid.as_hyphenated().to_string();
    let payload: bytes::Bytes = "testing larger payload ".repeat(250).into_bytes().into();
    let payload_len = payload.len() as i64;
    let chunk_size = 100; // split payload into packets
    let response = put_helper(
        dime,
        payload,
        chunk_size,
        HashMap::default(),
        Vec::default(),
    )
    .await;

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
#[serial(grpc_server)]
async fn multi_party_put() {
    let docker = clients::Cli::default();
    let image = RunnableImage::from(images::postgres::Postgres::default()).with_tag("14-alpine");
    let container = docker.run(image);
    let postgres_port = container.get_host_port_ipv4(5432);

    let db = start_server(None, postgres_port).await;

    let (audience1, signature1) = party_1();
    let (audience2, signature2) = party_2();
    let (audience3, signature3) = party_3();
    let dime = generate_dime(
        vec![audience1, audience2, audience3],
        vec![signature1, signature2, signature3],
    );
    let payload: bytes::Bytes = "testing small payload".as_bytes().into();
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
#[serial(grpc_server)]
async fn small_mailbox_put() {
    let docker = clients::Cli::default();
    let image = RunnableImage::from(images::postgres::Postgres::default()).with_tag("14-alpine");
    let container = docker.run(image);
    let postgres_port = container.get_host_port_ipv4(5432);

    let db = start_server(None, postgres_port).await;

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
    let response = put_helper(
        dime,
        payload,
        chunk_size,
        HashMap::default(),
        Vec::default(),
    )
    .await;

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
#[serial(grpc_server)]
async fn large_mailbox_put() {
    let docker = clients::Cli::default();
    let image = RunnableImage::from(images::postgres::Postgres::default()).with_tag("14-alpine");
    let container = docker.run(image);
    let postgres_port = container.get_host_port_ipv4(5432);

    let db = start_server(None, postgres_port).await;

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
    let response = put_helper(
        dime,
        payload,
        chunk_size,
        HashMap::default(),
        Vec::default(),
    )
    .await;

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
#[serial(grpc_server)]
async fn simple_get() {
    let docker = clients::Cli::default();
    let image = RunnableImage::from(images::postgres::Postgres::default()).with_tag("14-alpine");
    let container = docker.run(image);
    let postgres_port = container.get_host_port_ipv4(5432);

    start_server(None, postgres_port).await;

    let (audience, signature) = party_1();
    let dime = generate_dime(vec![audience.clone()], vec![signature]);
    let payload: bytes::Bytes = "testing small payload".as_bytes().into();
    let chunk_size = 500; // full payload in one packet
    let response = put_helper(
        dime,
        payload.clone(),
        chunk_size,
        HashMap::default(),
        Vec::default(),
    )
    .await;

    match response {
        Ok(response) => {
            let response = response.into_inner();

            assert_eq!(response.name, NOT_STORAGE_BACKED);
        }
        _ => assert_eq!(format!("{:?}", response), ""),
    }

    let mut client = pb::object_service_client::ObjectServiceClient::connect("tcp://0.0.0.0:6789")
        .await
        .unwrap();
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
#[serial(grpc_server)]
async fn auth_get_failure_no_key() {
    let docker = clients::Cli::default();
    let image = RunnableImage::from(images::postgres::Postgres::default()).with_tag("14-alpine");
    let container = docker.run(image);
    let postgres_port = container.get_host_port_ipv4(5432);

    let mut config = test_config(postgres_port);
    config.user_auth_enabled = true;
    start_server(Some(config), postgres_port).await;

    let (audience, signature) = party_1();
    let dime = generate_dime(vec![audience.clone()], vec![signature]);
    let payload: bytes::Bytes = "testing larger payload ".repeat(250).into_bytes().into();
    let chunk_size = 100; // split payload into packets
    let response = put_helper(
        dime,
        payload.clone(),
        chunk_size,
        HashMap::default(),
        vec![("x-test-header", "test_value_1")],
    )
    .await;

    match response {
        Ok(response) => {
            let response = response.into_inner();

            assert_ne!(response.name, NOT_STORAGE_BACKED);
        }
        _ => assert_eq!(format!("{:?}", response), ""),
    }

    let mut client = pb::object_service_client::ObjectServiceClient::connect("tcp://0.0.0.0:6789")
        .await
        .unwrap();
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
#[serial(grpc_server)]
async fn auth_get_failure_invalid_key() {
    let docker = clients::Cli::default();
    let image = RunnableImage::from(images::postgres::Postgres::default()).with_tag("14-alpine");
    let container = docker.run(image);
    let postgres_port = container.get_host_port_ipv4(5432);

    let mut config = test_config(postgres_port);
    config.user_auth_enabled = true;
    start_server(Some(config), postgres_port).await;

    let (audience, signature) = party_1();
    let dime = generate_dime(vec![audience.clone()], vec![signature]);
    let payload: bytes::Bytes = "testing larger payload ".repeat(250).into_bytes().into();
    let chunk_size = 100; // split payload into packets
    let response = put_helper(
        dime,
        payload.clone(),
        chunk_size,
        HashMap::default(),
        vec![("x-test-header", "test_value_1")],
    )
    .await;

    match response {
        Ok(response) => {
            let response = response.into_inner();

            assert_ne!(response.name, NOT_STORAGE_BACKED);
        }
        _ => assert_eq!(format!("{:?}", response), ""),
    }

    let mut client = pb::object_service_client::ObjectServiceClient::connect("tcp://0.0.0.0:6789")
        .await
        .unwrap();
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
#[serial(grpc_server)]
async fn auth_get_success() {
    let docker = clients::Cli::default();
    let image = RunnableImage::from(images::postgres::Postgres::default()).with_tag("14-alpine");
    let container = docker.run(image);
    let postgres_port = container.get_host_port_ipv4(5432);

    let mut config = test_config(postgres_port);
    config.user_auth_enabled = true;
    start_server(Some(config), postgres_port).await;

    let (audience, signature) = party_1();
    let dime = generate_dime(vec![audience.clone()], vec![signature]);
    let payload: bytes::Bytes = "testing larger payload ".repeat(250).into_bytes().into();
    let chunk_size = 100; // split payload into packets
    let response = put_helper(
        dime,
        payload.clone(),
        chunk_size,
        HashMap::default(),
        vec![("x-test-header", "test_value_1")],
    )
    .await;

    match response {
        Ok(response) => {
            let response = response.into_inner();

            assert_ne!(response.name, NOT_STORAGE_BACKED);
        }
        _ => assert_eq!(format!("{:?}", response), ""),
    }

    let mut client = pb::object_service_client::ObjectServiceClient::connect("tcp://0.0.0.0:6789")
        .await
        .unwrap();
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
#[serial(grpc_server)]
async fn multi_packet_file_store_get() {
    let docker = clients::Cli::default();
    let image = RunnableImage::from(images::postgres::Postgres::default()).with_tag("14-alpine");
    let container = docker.run(image);
    let postgres_port = container.get_host_port_ipv4(5432);

    start_server(None, postgres_port).await;

    let (audience, signature) = party_1();
    let dime = generate_dime(vec![audience.clone()], vec![signature]);
    let payload: bytes::Bytes = "testing larger payload ".repeat(250).into_bytes().into();
    let chunk_size = 100; // split payload into packets
    let response = put_helper(
        dime,
        payload.clone(),
        chunk_size,
        HashMap::default(),
        Vec::default(),
    )
    .await;

    match response {
        Ok(response) => {
            let response = response.into_inner();

            assert_ne!(response.name, NOT_STORAGE_BACKED);
        }
        _ => assert_eq!(format!("{:?}", response), ""),
    }

    let mut client = pb::object_service_client::ObjectServiceClient::connect("tcp://0.0.0.0:6789")
        .await
        .unwrap();
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
#[serial(grpc_server)]
async fn multi_party_non_owner_get() {
    let docker = clients::Cli::default();
    let image = RunnableImage::from(images::postgres::Postgres::default()).with_tag("14-alpine");
    let container = docker.run(image);
    let postgres_port = container.get_host_port_ipv4(5432);

    start_server(None, postgres_port).await;

    let (audience1, signature1) = party_1();
    let (audience2, signature2) = party_2();
    let (audience3, signature3) = party_3();
    let dime = generate_dime(
        vec![audience1, audience2, audience3.clone()],
        vec![signature1, signature2, signature3],
    );
    let payload: bytes::Bytes = "testing small payload".as_bytes().into();
    let chunk_size = 500; // full payload in one packet
    let response = put_helper(
        dime,
        payload.clone(),
        chunk_size,
        HashMap::default(),
        Vec::default(),
    )
    .await;

    match response {
        Ok(_) => (),
        _ => assert_eq!(format!("{:?}", response), ""),
    }
    let mut client = pb::object_service_client::ObjectServiceClient::connect("tcp://0.0.0.0:6789")
        .await
        .unwrap();
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
#[serial(grpc_server)]
async fn dupe_objects_noop() {
    let docker = clients::Cli::default();
    let image = RunnableImage::from(images::postgres::Postgres::default()).with_tag("14-alpine");
    let container = docker.run(image);
    let postgres_port = container.get_host_port_ipv4(5432);

    start_server(None, postgres_port).await;

    let (audience, signature) = party_1();
    let dime = generate_dime(vec![audience], vec![signature]);
    let payload: bytes::Bytes = "testing small payload".as_bytes().into();
    let chunk_size = 500; // full payload in one packet
    let response = put_helper(
        dime.clone(),
        payload.clone(),
        chunk_size,
        HashMap::default(),
        Vec::default(),
    )
    .await;
    let response_dupe = put_helper(
        dime,
        payload,
        chunk_size,
        HashMap::default(),
        Vec::default(),
    )
    .await;

    assert!(response.is_ok() && response_dupe.is_ok());

    let response_inner = response.unwrap().into_inner();
    let response_dupe_inner = response_dupe.unwrap().into_inner();

    assert_eq!(response_inner.uuid, response_dupe_inner.uuid);
}

#[tokio::test]
#[serial(grpc_server)]
async fn dupe_objects_added_audience() {
    let docker = clients::Cli::default();
    let image = RunnableImage::from(images::postgres::Postgres::default()).with_tag("14-alpine");
    let container = docker.run(image);
    let postgres_port = container.get_host_port_ipv4(5432);

    start_server(None, postgres_port).await;

    let (audience, signature) = party_1();
    let (audience2, signature2) = party_2();
    let dime = generate_dime(vec![audience.clone()], vec![signature.clone()]);
    let dime2 = generate_dime(vec![audience, audience2], vec![signature, signature2]);
    let payload: bytes::Bytes = "testing small payload".as_bytes().into();
    let chunk_size = 500; // full payload in one packet
    let response = put_helper(
        dime.clone(),
        payload.clone(),
        chunk_size,
        HashMap::default(),
        Vec::default(),
    )
    .await;
    let response_dupe = put_helper(
        dime2,
        payload,
        chunk_size,
        HashMap::default(),
        Vec::default(),
    )
    .await;

    assert!(response.is_ok() && response_dupe.is_ok());

    let response_inner = response.unwrap().into_inner();
    let response_dupe_inner = response_dupe.unwrap().into_inner();

    assert_ne!(response_inner.uuid, response_dupe_inner.uuid);
}

#[tokio::test]
#[serial(grpc_server)]
async fn get_with_wrong_key() {
    let docker = clients::Cli::default();
    let image = RunnableImage::from(images::postgres::Postgres::default()).with_tag("14-alpine");
    let container = docker.run(image);
    let postgres_port = container.get_host_port_ipv4(5432);

    start_server(None, postgres_port).await;

    let (audience1, signature1) = party_1();
    let (audience2, signature2) = party_2();
    let (audience3, _) = party_3();
    let dime = generate_dime(vec![audience1, audience2], vec![signature1, signature2]);
    let payload: bytes::Bytes = "testing small payload".as_bytes().into();
    let chunk_size = 500; // full payload in one packet
    let response = put_helper(
        dime,
        payload.clone(),
        chunk_size,
        HashMap::default(),
        Vec::default(),
    )
    .await;

    match response {
        Ok(_) => (),
        _ => assert_eq!(format!("{:?}", response), ""),
    }
    let mut client = pb::object_service_client::ObjectServiceClient::connect("tcp://0.0.0.0:6789")
        .await
        .unwrap();
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
#[serial(grpc_server)]
async fn get_nonexistent_hash() {
    let docker = clients::Cli::default();
    let image = RunnableImage::from(images::postgres::Postgres::default()).with_tag("14-alpine");
    let container = docker.run(image);
    let postgres_port = container.get_host_port_ipv4(5432);

    start_server(None, postgres_port).await;

    let (audience1, _) = party_1();
    let payload: bytes::Bytes = "testing small payload".as_bytes().into();
    let mut client = pb::object_service_client::ObjectServiceClient::connect("tcp://0.0.0.0:6789")
        .await
        .unwrap();
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
#[serial(grpc_server)]
async fn put_with_replication() {
    let docker = clients::Cli::default();
    let image = RunnableImage::from(images::postgres::Postgres::default()).with_tag("14-alpine");
    let container = docker.run(image);
    let postgres_port = container.get_host_port_ipv4(5432);

    let db = start_server(None, postgres_port).await;

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
    let response = put_helper(dime, payload, chunk_size, extra_properties, Vec::default()).await;

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
#[serial(grpc_server)]
async fn put_with_replication_different_owner() {
    let docker = clients::Cli::default();
    let image = RunnableImage::from(images::postgres::Postgres::default()).with_tag("14-alpine");
    let container = docker.run(image);
    let postgres_port = container.get_host_port_ipv4(5432);

    let db = start_server(None, postgres_port).await;

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
    let response = put_helper(dime, payload, chunk_size, extra_properties, Vec::default()).await;

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
#[serial(grpc_server)]
async fn put_with_double_replication() {
    let docker = clients::Cli::default();
    let image = RunnableImage::from(images::postgres::Postgres::default()).with_tag("14-alpine");
    let container = docker.run(image);
    let postgres_port = container.get_host_port_ipv4(5432);

    let db = start_server(None, postgres_port).await;

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
    let response = put_helper(dime, payload, chunk_size, extra_properties, Vec::default()).await;

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
#[serial(grpc_server)]
async fn get_object_no_properties() {
    let docker = clients::Cli::default();
    let image = RunnableImage::from(images::postgres::Postgres::default()).with_tag("14-alpine");
    let container = docker.run(image);
    let postgres_port = container.get_host_port_ipv4(5432);

    let db = start_server(None, postgres_port).await;

    let (audience, signature) = party_1();
    let dime = generate_dime(vec![audience.clone()], vec![signature]);
    let payload: bytes::Bytes = "testing small payload".as_bytes().into();
    let chunk_size = 500; // full payload in one packet
    let response = put_helper(
        dime,
        payload.clone(),
        chunk_size,
        HashMap::default(),
        Vec::default(),
    )
    .await;

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

    let mut client = pb::object_service_client::ObjectServiceClient::connect("tcp://0.0.0.0:6789")
        .await
        .unwrap();
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
