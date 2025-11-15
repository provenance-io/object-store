mod common;

use object_store::cache::Cache;
use object_store::pb::public_key_request::Impl::HeaderAuth as HeaderAuthEnumRequest;
use object_store::pb::public_key_response::Impl::HeaderAuth as HeaderAuthEnumResponse;
use object_store::pb::public_key_service_server::PublicKeyService;
use object_store::pb::PublicKeyRequest;
use object_store::pb::{public_key::Key, HeaderAuth, PublicKey};
use object_store::proto_helpers::VecUtil;
use object_store::public_key::PublicKeyGrpc;
use testcontainers::clients;
use tonic::Request;

use crate::common::config::test_config;
use crate::common::containers::start_containers;
use crate::common::db::setup_postgres;

#[tokio::test]
async fn invalid_url() {
    let docker = clients::Cli::default();

    let (db_port, _postgres) = start_containers(&docker).await;
    let config = test_config(db_port);
    let pool = setup_postgres(&config).await;
    let cache = Cache::new(pool.clone()).await.unwrap();
    let public_key_service = PublicKeyGrpc::new(cache.clone(), pool.clone());

    let request = PublicKeyRequest {
        public_key: Some(PublicKey {
            key: Some(vec![].into()),
        }),
        r#impl: None,
        url: "invalidurl.com".to_owned(),
        metadata: None,
    };

    let response = public_key_service.add(Request::new(request)).await;

    match response {
        Err(err) => {
            assert_eq!(err.code(), tonic::Code::InvalidArgument);
            assert_eq!(
                err.message(),
                "Unable to parse url invalidurl.com - RelativeUrlWithoutBase".to_owned()
            );
        }
        _ => assert_eq!(format!("{:?}", response), ""),
    }
}

#[tokio::test]
async fn empty_url() {
    let docker = clients::Cli::default();

    let (db_port, _postgres) = start_containers(&docker).await;
    let config = test_config(db_port);
    let pool = setup_postgres(&config).await;
    let cache = Cache::new(pool.clone()).await.unwrap();
    let public_key_service = PublicKeyGrpc::new(cache.clone(), pool.clone());

    let request = PublicKeyRequest {
        public_key: Some(vec![1u8, 2u8, 3u8].into()),
        r#impl: None,
        url: String::default(),
        metadata: None,
    };

    let response = public_key_service.add(Request::new(request)).await;

    match response {
        Ok(result) => {
            let result = result.into_inner();
            assert!(result.uuid.is_some());
            assert_eq!(
                result.public_key.unwrap().key.unwrap(),
                Key::Secp256k1(vec![1u8, 2u8, 3u8])
            );
            assert!(result.url.is_empty());
            assert!(result.metadata.is_none());
            assert!(result.created_at.is_some());
            assert!(result.updated_at.is_some());
        }
        _ => {
            assert_eq!(format!("{:?}", response), "");
        }
    }
}

#[tokio::test]
async fn empty_auth_header_header() {
    let docker = clients::Cli::default();

    let (db_port, _postgres) = start_containers(&docker).await;
    let config = test_config(db_port);
    let pool = setup_postgres(&config).await;
    let cache = Cache::new(pool.clone()).await.unwrap();
    let public_key_service = PublicKeyGrpc::new(cache.clone(), pool.clone());

    let request = PublicKeyRequest {
        public_key: Some(vec![1u8, 2u8, 3u8].into()),
        r#impl: Some(HeaderAuthEnumRequest(HeaderAuth {
            header: String::from(""),
            value: String::from("custom_value"),
        })),
        url: String::default(),
        metadata: None,
    };

    let response = public_key_service.add(Request::new(request)).await;

    match response {
        Err(err) => {
            assert_eq!(err.code(), tonic::Code::InvalidArgument);
            assert_eq!(
                err.message(),
                "must specify non empty auth header".to_owned()
            );
        }
        _ => assert_eq!(format!("{:?}", response), ""),
    }
}

#[tokio::test]
async fn empty_auth_header_value() {
    let docker = clients::Cli::default();

    let (db_port, _postgres) = start_containers(&docker).await;
    let config = test_config(db_port);
    let pool = setup_postgres(&config).await;
    let cache = Cache::new(pool.clone()).await.unwrap();
    let public_key_service = PublicKeyGrpc::new(cache.clone(), pool.clone());

    let request = PublicKeyRequest {
        public_key: Some(vec![1u8, 2u8, 3u8].into()),
        r#impl: Some(HeaderAuthEnumRequest(HeaderAuth {
            header: String::from("X-Custom-Header"),
            value: String::from(""),
        })),
        url: String::default(),
        metadata: None,
    };
    let response = public_key_service.add(Request::new(request)).await;

    match response {
        Err(err) => {
            assert_eq!(err.code(), tonic::Code::InvalidArgument);
            assert_eq!(
                err.message(),
                "must specify non empty auth value".to_owned()
            );
        }
        _ => assert_eq!(format!("{:?}", response), ""),
    }
}

#[tokio::test]
async fn missing_public_key() {
    let docker = clients::Cli::default();

    let (db_port, _postgres) = start_containers(&docker).await;
    let config = test_config(db_port);
    let pool = setup_postgres(&config).await;
    let cache = Cache::new(pool.clone()).await.unwrap();
    let public_key_service = PublicKeyGrpc::new(cache.clone(), pool.clone());

    let request = PublicKeyRequest {
        public_key: None,
        r#impl: None,
        url: "http://test.com".to_owned(),
        metadata: None,
    };
    let response = public_key_service.add(Request::new(request)).await;

    match response {
        Err(err) => {
            assert_eq!(err.code(), tonic::Code::InvalidArgument);
            assert_eq!(err.message(), "must specify public key".to_owned());
        }
        _ => assert_eq!(format!("{:?}", response), ""),
    }
}

#[tokio::test]
async fn missing_key() {
    let docker = clients::Cli::default();

    let (db_port, _postgres) = start_containers(&docker).await;
    let config = test_config(db_port);
    let pool = setup_postgres(&config).await;
    let cache = Cache::new(pool.clone()).await.unwrap();
    let public_key_service = PublicKeyGrpc::new(cache.clone(), pool.clone());

    let request = PublicKeyRequest {
        public_key: Some(PublicKey { key: None }),
        r#impl: None,
        url: "http://test.com".to_owned(),
        metadata: None,
    };
    let response = public_key_service.add(Request::new(request)).await;

    match response {
        Err(err) => {
            assert_eq!(err.code(), tonic::Code::InvalidArgument);
            assert_eq!(err.message(), "must specify key type".to_owned());
        }
        _ => assert_eq!(format!("{:?}", response), ""),
    }
}

#[tokio::test]
async fn duplicate_key() {
    let docker = clients::Cli::default();

    let (db_port, _postgres) = start_containers(&docker).await;
    let config = test_config(db_port);
    let pool = setup_postgres(&config).await;
    let cache = Cache::new(pool.clone()).await.unwrap();
    let public_key_service = PublicKeyGrpc::new(cache.clone(), pool.clone());

    let request1 = PublicKeyRequest {
        public_key: Some(vec![1u8, 2u8, 3u8].into()),
        r#impl: None,
        url: "http://test.com".to_owned(),
        metadata: None,
    };
    let request2 = PublicKeyRequest {
        public_key: Some(vec![1u8, 2u8, 3u8].into()),
        r#impl: None,
        url: "http://test2.com".to_owned(),
        metadata: None,
    };

    let response = public_key_service.add(Request::new(request1)).await;

    let (uuid, url) = match response {
        Ok(result) => {
            let result = result.into_inner();
            assert!(result.uuid.is_some());
            (result.uuid, result.url)
        }
        _ => {
            assert_eq!(format!("{:?}", response), "");
            unreachable!();
        }
    };

    let response = public_key_service.add(Request::new(request2)).await;

    match response {
        Ok(result) => {
            let result = result.into_inner();
            assert!(result.uuid.is_some());
            assert_eq!(uuid, result.uuid);
            assert_ne!(url, result.url);
            assert_eq!("http://test2.com", result.url);
        }
        _ => assert_eq!(format!("{:?}", response), ""),
    };
}

#[tokio::test]
async fn returns_full_proto() {
    let docker = clients::Cli::default();

    let (db_port, _postgres) = start_containers(&docker).await;
    let config = test_config(db_port);
    let pool = setup_postgres(&config).await;
    let cache = Cache::new(pool.clone()).await.unwrap();
    let public_key_service = PublicKeyGrpc::new(cache.clone(), pool.clone());

    // TODO add metadata to this request
    let request = PublicKeyRequest {
        public_key: Some(vec![1u8, 2u8, 3u8].into()),
        r#impl: Some(HeaderAuthEnumRequest(HeaderAuth {
            header: String::from("X-Custom-Header"),
            value: String::from("custom_value"),
        })),
        url: "http://test.com".to_owned(),
        metadata: None,
    };
    let response = public_key_service.add(Request::new(request)).await;

    match response {
        Ok(result) => {
            let result = result.into_inner();
            assert!(result.uuid.is_some());
            assert_eq!(
                result.public_key.unwrap().key.unwrap(),
                Key::Secp256k1(vec![1u8, 2u8, 3u8])
            );
            assert_eq!(
                result.r#impl.unwrap(),
                HeaderAuthEnumResponse(HeaderAuth {
                    header: String::from("x-custom-header"),
                    value: String::from("custom_value"),
                }),
            );
            assert_eq!(result.url, String::from("http://test.com"));
            assert!(result.metadata.is_none());
            assert!(result.created_at.is_some());
            assert!(result.updated_at.is_some());
        }
        _ => assert_eq!(format!("{:?}", response), ""),
    }
}

#[tokio::test]
async fn adds_empty_urls_to_local_cache() {
    let docker = clients::Cli::default();

    let (db_port, _postgres) = start_containers(&docker).await;
    let config = test_config(db_port);
    let pool = setup_postgres(&config).await;
    let cache = Cache::new(pool.clone()).await.unwrap();
    let public_key_service = PublicKeyGrpc::new(cache.clone(), pool.clone());

    let request = PublicKeyRequest {
        public_key: Some(vec![1u8, 2u8, 3u8].into()),
        r#impl: None,
        url: String::default(),
        metadata: None,
    };
    let response = public_key_service.add(Request::new(request)).await;

    match response {
        Ok(_) => (),
        _ => assert_eq!(format!("{:?}", response), ""),
    }

    let cache = cache.lock().unwrap();
    assert_eq!(cache.public_keys.len(), 1);
    assert!(cache
        .public_keys
        .contains_key(&vec![1u8, 2u8, 3u8].encoded()));
    assert_eq!(
        cache
            .public_keys
            .get(&vec![1u8, 2u8, 3u8].encoded())
            .unwrap()
            .url,
        String::from("")
    );
}

#[tokio::test]
async fn adds_nonempty_urls_to_remote_cache() {
    let docker = clients::Cli::default();

    let (db_port, _postgres) = start_containers(&docker).await;
    let config = test_config(db_port);
    let pool = setup_postgres(&config).await;
    let cache = Cache::new(pool.clone()).await.unwrap();
    let public_key_service = PublicKeyGrpc::new(cache.clone(), pool.clone());

    let request = PublicKeyRequest {
        public_key: Some(vec![1u8, 2u8, 3u8].into()),
        r#impl: None,
        url: "http://test.com".to_owned(),
        metadata: None,
    };
    let response = public_key_service.add(Request::new(request)).await;

    match response {
        Ok(_) => (),
        _ => assert_eq!(format!("{:?}", response), ""),
    }

    let cache = cache.lock().unwrap();
    assert_eq!(cache.public_keys.len(), 1);
    assert!(cache
        .public_keys
        .contains_key(&vec![1u8, 2u8, 3u8].encoded()));
    assert_ne!(
        cache
            .public_keys
            .get(&vec![1u8, 2u8, 3u8].encoded())
            .unwrap()
            .url,
        String::from("")
    );
}
