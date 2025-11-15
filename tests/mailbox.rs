use std::collections::HashMap;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use object_store::cache::Cache;
use object_store::config::Config;
use object_store::consts::*;
use object_store::datastore::PublicKey;
use object_store::db::connect_and_migrate;
use object_store::mailbox::MailboxGrpc;
use object_store::object::ObjectGrpc;
use object_store::pb::mailbox_service_server::MailboxServiceServer;
use object_store::pb::object_service_client::ObjectServiceClient;
use object_store::pb::object_service_server::ObjectServiceServer;
use object_store::pb::{
    mailbox_service_client::MailboxServiceClient, AckRequest, Audience, GetRequest,
};
use object_store::proto_helpers::AudienceUtil;
use object_store::storage::new_storage;

use sqlx::postgres::PgPool;
use testcontainers::clients;
use tonic::transport::Channel;
use tonic::Request;

mod common;

use crate::common::config::test_config;
use crate::common::containers::start_containers;
use crate::common::data::{generate_dime, party_1, party_2, party_3};
use crate::common::{
    get_mailbox_keys_by_object, get_public_keys_by_object, put_helper, test_public_key,
};

async fn start_server(
    default_config: Option<Config>,
    postgres_port: u16,
) -> (Arc<PgPool>, SocketAddr) {
    let config = default_config.unwrap_or(test_config(postgres_port));

    let cache = {
        let mut cache = Cache::default();
        cache.add_public_key(PublicKey {
            auth_data: Some(String::from("x-test-header:test_value_1")),
            ..test_public_key(party_1().0.public_key)
        });
        cache.add_public_key(PublicKey {
            auth_data: Some(String::from("x-test-header:test_value_2")),
            ..test_public_key(party_2().0.public_key)
        });
        Arc::new(Mutex::new(cache))
    };

    let pool = connect_and_migrate(&config).await.unwrap();

    let storage = new_storage(&config).unwrap();

    let config = Arc::new(config);

    let mailbox_service = MailboxGrpc::new(cache.clone(), config.clone(), pool.clone());
    let object_service =
        ObjectGrpc::new(cache.clone(), config.clone(), pool.clone(), storage.clone());

    let listener = tokio::net::TcpListener::bind(config.url).await.unwrap();
    let local_addr = listener.local_addr().unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    println!("test server running on {:?}", local_addr);

    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(MailboxServiceServer::new(mailbox_service))
            .add_service(ObjectServiceServer::new(object_service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap()
    });

    (pool, local_addr)
}

async fn get_mailbox_client(addr: SocketAddr) -> MailboxServiceClient<Channel> {
    MailboxServiceClient::connect(format!("tcp://{}", addr))
        .await
        .unwrap()
}

async fn get_object_client(addr: SocketAddr) -> ObjectServiceClient<Channel> {
    ObjectServiceClient::connect(format!("tcp://{:?}", addr))
        .await
        .unwrap()
}

async fn get_and_ack_helper(
    client: &mut MailboxServiceClient<Channel>,
    audience: Audience,
    expected_size: usize,
) {
    let public_key = audience.public_key_decoded();
    let request = GetRequest {
        public_key,
        max_results: 50,
    };
    let response = client.get(request).await;

    match response {
        Ok(response) => {
            let mut response = response.into_inner();
            let mut mail = Vec::new();

            while let Some(msg) = response.message().await.unwrap() {
                mail.push(msg);
            }

            assert_eq!(mail.len(), expected_size);

            for msg in mail {
                let request = AckRequest {
                    uuid: msg.uuid,
                    public_key: Vec::default(),
                };
                let response = client.ack(request).await;

                match response {
                    Ok(_) => (),
                    _ => assert_eq!(format!("{:?}", response), ""),
                }
            }
        }
        _ => assert_eq!(format!("{:?}", response), ""),
    }
}

async fn authed_get_and_ack_helper(
    client: &mut MailboxServiceClient<Channel>,
    audience: Audience,
    expected_size: usize,
    grpc_metadata: Vec<(&'static str, &'static str)>,
) {
    let public_key = audience.public_key_decoded();
    let mut request = Request::new(GetRequest {
        public_key: public_key.clone(),
        max_results: 50,
    });
    let metadata = request.metadata_mut();
    for (k, v) in &grpc_metadata {
        metadata.insert(*k, v.parse().unwrap());
    }
    let response = client.get(request).await;

    match response {
        Ok(response) => {
            let mut response = response.into_inner();
            let mut mail = Vec::new();

            while let Some(msg) = response.message().await.unwrap() {
                mail.push(msg);
            }

            assert_eq!(mail.len(), expected_size);

            for msg in mail {
                let mut request = Request::new(AckRequest {
                    uuid: msg.uuid,
                    public_key: public_key.clone(),
                });
                let metadata = request.metadata_mut();
                for (k, v) in &grpc_metadata {
                    metadata.insert(*k, v.parse().unwrap());
                }
                let response = client.ack(request).await;

                match response {
                    Ok(_) => (),
                    _ => assert_eq!(format!("{:?}", response), ""),
                }
            }
        }
        _ => assert_eq!(format!("{:?}", response), ""),
    }
}

#[tokio::test]
async fn get_and_ack_flow() {
    let docker = clients::Cli::default();
    let (db_port, _postgres) = start_containers(&docker).await;

    let (db, addr) = start_server(None, db_port).await;
    let mut client = get_mailbox_client(addr).await;

    // post fragment request
    let (audience1, signature1) = party_1();
    let (audience2, signature2) = party_2();
    let (audience3, signature3) = party_3();
    let mut dime = generate_dime(
        vec![audience1.clone(), audience2.clone(), audience3.clone()],
        vec![signature1.clone(), signature2.clone(), signature3.clone()],
    );
    dime.metadata
        .insert(MAILBOX_KEY.to_owned(), MAILBOX_FRAGMENT_REQUEST.to_owned());
    let payload: bytes::Bytes = "fragment request envelope".as_bytes().into();
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

    // ack fragment request
    get_and_ack_helper(&mut client, audience1.clone(), 0).await;
    get_and_ack_helper(&mut client, audience2.clone(), 1).await;
    get_and_ack_helper(&mut client, audience3.clone(), 1).await;

    // post fragment response
    let mut dime = generate_dime(
        vec![audience3.clone(), audience2.clone(), audience1.clone()],
        vec![signature3.clone(), signature2.clone(), signature1.clone()],
    );
    dime.metadata
        .insert(MAILBOX_KEY.to_owned(), MAILBOX_FRAGMENT_RESPONSE.to_owned());
    let payload: bytes::Bytes = "fragment response envelope".as_bytes().into();
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

    // ack fragment response
    get_and_ack_helper(&mut client, audience1.clone(), 1).await;
    get_and_ack_helper(&mut client, audience2.clone(), 1).await;
    get_and_ack_helper(&mut client, audience3.clone(), 0).await;

    // post envelope error
    let mut dime = generate_dime(
        vec![audience1.clone(), audience2.clone(), audience3.clone()],
        vec![signature1.clone(), signature2.clone(), signature3.clone()],
    );
    dime.metadata
        .insert(MAILBOX_KEY.to_owned(), MAILBOX_ERROR_RESPONSE.to_owned());
    let payload: bytes::Bytes = "error envelope".as_bytes().into();
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

    // ack error mail
    get_and_ack_helper(&mut client, audience1.clone(), 0).await;
    get_and_ack_helper(&mut client, audience2.clone(), 1).await;
    get_and_ack_helper(&mut client, audience3.clone(), 1).await;

    // verify no mail left to read
    get_and_ack_helper(&mut client, audience1, 0).await;
    get_and_ack_helper(&mut client, audience2, 0).await;
    get_and_ack_helper(&mut client, audience3, 0).await;
}

#[tokio::test]
async fn duplicate_objects_does_not_dup_mail() {
    let docker = clients::Cli::default();
    let (db_port, _postgres) = start_containers(&docker).await;

    let (_, addr) = start_server(None, db_port).await;
    let mut client = get_mailbox_client(addr).await;

    let (audience1, signature1) = party_1();
    let (audience2, signature2) = party_2();
    let mut dime = generate_dime(
        vec![audience1, audience2.clone()],
        vec![signature1, signature2],
    );
    dime.metadata
        .insert(MAILBOX_KEY.to_owned(), MAILBOX_FRAGMENT_REQUEST.to_owned());
    let payload: bytes::Bytes = "testing small payload".as_bytes().into();
    let chunk_size = 500; // full payload in one packet

    for _ in 0..10 {
        let request = put_helper(
            dime.clone(),
            payload.clone(),
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
    }

    dime.metadata
        .insert(MAILBOX_KEY.to_owned(), MAILBOX_ERROR_RESPONSE.to_owned());

    for _ in 0..10 {
        let request = put_helper(
            dime.clone(),
            payload.clone(),
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
    }

    get_and_ack_helper(&mut client, audience2, 1).await;
}

#[tokio::test]
async fn get_and_ack_many() {
    let docker = clients::Cli::default();
    let (db_port, _postgres) = start_containers(&docker).await;

    let (_, addr) = start_server(None, db_port).await;
    let mut client = get_mailbox_client(addr).await;

    let (audience1, signature1) = party_1();
    let (audience2, signature2) = party_2();
    let mut dime = generate_dime(
        vec![audience1, audience2.clone()],
        vec![signature1, signature2],
    );
    dime.metadata
        .insert(MAILBOX_KEY.to_owned(), MAILBOX_FRAGMENT_REQUEST.to_owned());
    let chunk_size = 500; // full payload in one packet

    for _ in 0..10 {
        let payload: bytes::Bytes = uuid::Uuid::new_v4()
            .as_hyphenated()
            .to_string()
            .into_bytes()
            .into();
        let request = put_helper(
            dime.clone(),
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
    }

    dime.metadata
        .insert(MAILBOX_KEY.to_owned(), MAILBOX_ERROR_RESPONSE.to_owned());

    for _ in 0..10 {
        let payload: bytes::Bytes = uuid::Uuid::new_v4()
            .as_hyphenated()
            .to_string()
            .into_bytes()
            .into();
        let request = put_helper(
            dime.clone(),
            payload.clone(),
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
    }

    get_and_ack_helper(&mut client, audience2.clone(), 20).await;
    get_and_ack_helper(&mut client, audience2, 0).await;
}

#[tokio::test]
async fn auth_get_and_ack_success() {
    let docker = clients::Cli::default();
    let (db_port, _postgres) = start_containers(&docker).await;

    let config = Config {
        user_auth_enabled: true,
        ..test_config(db_port)
    };

    let (_, addr) = start_server(Some(config), db_port).await;
    let mut client = get_mailbox_client(addr).await;

    let (audience1, signature1) = party_1();
    let (audience2, signature2) = party_2();
    let mut dime = generate_dime(
        vec![audience1, audience2.clone()],
        vec![signature1, signature2],
    );
    dime.metadata
        .insert(MAILBOX_KEY.to_owned(), MAILBOX_FRAGMENT_REQUEST.to_owned());
    let chunk_size = 500; // full payload in one packet

    let payload: bytes::Bytes = uuid::Uuid::new_v4()
        .as_hyphenated()
        .to_string()
        .into_bytes()
        .into();
    let request = put_helper(
        dime.clone(),
        payload,
        chunk_size,
        HashMap::default(),
        vec![("x-test-header", "test_value_1")],
    );

    let mut os_client = get_object_client(addr).await;
    let response = os_client.put(request).await;

    match response {
        Ok(_) => (),
        _ => assert_eq!(format!("{:?}", response), ""),
    }

    authed_get_and_ack_helper(
        &mut client,
        audience2.clone(),
        1,
        vec![("x-test-header", "test_value_2")],
    )
    .await;
    authed_get_and_ack_helper(
        &mut client,
        audience2,
        0,
        vec![("x-test-header", "test_value_2")],
    )
    .await;
}

#[tokio::test]
async fn auth_get_invalid_key() {
    let docker = clients::Cli::default();
    let (db_port, _postgres) = start_containers(&docker).await;

    let config = Config {
        user_auth_enabled: true,
        ..test_config(db_port)
    };

    let (_, addr) = start_server(Some(config), db_port).await;
    let mut client = get_mailbox_client(addr).await;

    let (audience1, signature1) = party_1();
    let (audience2, signature2) = party_2();
    let mut dime = generate_dime(
        vec![audience1, audience2.clone()],
        vec![signature1, signature2],
    );
    dime.metadata
        .insert(MAILBOX_KEY.to_owned(), MAILBOX_FRAGMENT_REQUEST.to_owned());
    let chunk_size = 500; // full payload in one packet

    let payload: bytes::Bytes = uuid::Uuid::new_v4()
        .as_hyphenated()
        .to_string()
        .into_bytes()
        .into();
    let request = put_helper(
        dime.clone(),
        payload,
        chunk_size,
        HashMap::default(),
        vec![("x-test-header", "test_value_1")],
    );

    let mut os_client = get_object_client(addr).await;
    let response = os_client.put(request).await;

    match response {
        Ok(_) => (),
        _ => assert_eq!(format!("{:?}", response), ""),
    }

    let public_key = audience2.public_key_decoded();
    let mut request = Request::new(GetRequest {
        public_key,
        max_results: 50,
    });
    let metadata = request.metadata_mut();
    metadata.insert("x-test-header", "test_value_1".parse().unwrap());
    let response = client.get(request).await;

    match response {
        Err(err) => assert_eq!(err.code(), tonic::Code::PermissionDenied),
        _ => assert_eq!(format!("{:?}", response), ""),
    }
}

#[tokio::test]
async fn auth_ack_invalid_key() {
    let docker = clients::Cli::default();
    let (db_port, _postgres) = start_containers(&docker).await;

    let config = Config {
        user_auth_enabled: true,
        ..test_config(db_port)
    };

    let (_, addr) = start_server(Some(config), db_port).await;
    let mut client = get_mailbox_client(addr).await;

    let (audience1, signature1) = party_1();
    let (audience2, signature2) = party_2();
    let mut dime = generate_dime(
        vec![audience1, audience2.clone()],
        vec![signature1, signature2],
    );
    dime.metadata
        .insert(MAILBOX_KEY.to_owned(), MAILBOX_FRAGMENT_REQUEST.to_owned());
    let chunk_size = 500; // full payload in one packet

    let payload: bytes::Bytes = uuid::Uuid::new_v4()
        .as_hyphenated()
        .to_string()
        .into_bytes()
        .into();
    let request = put_helper(
        dime.clone(),
        payload,
        chunk_size,
        HashMap::default(),
        vec![("x-test-header", "test_value_1")],
    );

    let mut os_client = get_object_client(addr).await;
    let response = os_client.put(request).await;

    match response {
        Ok(_) => (),
        _ => assert_eq!(format!("{:?}", response), ""),
    }

    let public_key = audience2.public_key_decoded();
    let mut request = Request::new(AckRequest {
        uuid: response.unwrap().into_inner().uuid,
        public_key,
    });
    let metadata = request.metadata_mut();
    metadata.insert("x-test-header", "test_value_1".parse().unwrap());
    let response = client.ack(request).await;

    match response {
        Err(err) => assert_eq!(err.code(), tonic::Code::PermissionDenied),
        _ => assert_eq!(format!("{:?}", response), ""),
    }
}

#[tokio::test]
async fn auth_get_no_key() {
    let docker = clients::Cli::default();
    let (db_port, _postgres) = start_containers(&docker).await;

    let config = Config {
        user_auth_enabled: true,
        ..test_config(db_port)
    };

    let (_, addr) = start_server(Some(config), db_port).await;
    let mut client = get_mailbox_client(addr).await;

    let (audience1, signature1) = party_1();
    let (audience2, signature2) = party_2();
    let mut dime = generate_dime(
        vec![audience1, audience2.clone()],
        vec![signature1, signature2],
    );
    dime.metadata
        .insert(MAILBOX_KEY.to_owned(), MAILBOX_FRAGMENT_REQUEST.to_owned());
    let chunk_size = 500; // full payload in one packet

    let payload: bytes::Bytes = uuid::Uuid::new_v4()
        .as_hyphenated()
        .to_string()
        .into_bytes()
        .into();
    let request = put_helper(
        dime.clone(),
        payload,
        chunk_size,
        HashMap::default(),
        vec![("x-test-header", "test_value_1")],
    );

    let mut os_client = get_object_client(addr).await;
    let response = os_client.put(request).await;

    match response {
        Ok(_) => (),
        _ => assert_eq!(format!("{:?}", response), ""),
    }

    let public_key = audience2.public_key_decoded();
    let request = Request::new(GetRequest {
        public_key,
        max_results: 50,
    });
    let response = client.get(request).await;

    match response {
        Err(err) => assert_eq!(err.code(), tonic::Code::PermissionDenied),
        _ => assert_eq!(format!("{:?}", response), ""),
    }
}

#[tokio::test]
async fn auth_ack_no_key() {
    let docker = clients::Cli::default();
    let (db_port, _postgres) = start_containers(&docker).await;

    let config = Config {
        user_auth_enabled: true,
        ..test_config(db_port)
    };

    let (_, addr) = start_server(Some(config), db_port).await;
    let mut client = get_mailbox_client(addr).await;

    let (audience1, signature1) = party_1();
    let (audience2, signature2) = party_2();
    let mut dime = generate_dime(
        vec![audience1, audience2.clone()],
        vec![signature1, signature2],
    );
    dime.metadata
        .insert(MAILBOX_KEY.to_owned(), MAILBOX_FRAGMENT_REQUEST.to_owned());
    let chunk_size = 500; // full payload in one packet

    let payload: bytes::Bytes = uuid::Uuid::new_v4()
        .as_hyphenated()
        .to_string()
        .into_bytes()
        .into();
    let request = put_helper(
        dime.clone(),
        payload,
        chunk_size,
        HashMap::default(),
        vec![("x-test-header", "test_value_1")],
    );

    let mut os_client = get_object_client(addr).await;
    let response = os_client.put(request).await;

    match response {
        Ok(_) => (),
        _ => assert_eq!(format!("{:?}", response), ""),
    }

    let public_key = audience2.public_key_decoded();
    let request = Request::new(AckRequest {
        uuid: response.unwrap().into_inner().uuid,
        public_key,
    });
    let response = client.ack(request).await;

    match response {
        Err(err) => assert_eq!(err.code(), tonic::Code::PermissionDenied),
        _ => assert_eq!(format!("{:?}", response), ""),
    }
}
