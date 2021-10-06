use crate::cache::Cache;
use crate::datastore;
use crate::types::{GrpcResult, OsError};
use crate::pb::{public_key::Key, PublicKeyRequest, PublicKeyResponse};
use crate::pb::public_key_service_server::PublicKeyService;

use std::sync::{Arc, Mutex};
use sqlx::{postgres::PgPool};
use tonic::{Request, Response, Status};
use url::Url;

// TODO background process that updates cache on occasion

#[derive(Debug)]
pub struct PublicKeyGrpc {
    // This cache is using a std::sync::Mutex because the tokio docs mention that this is often
    // preferrable to the tokio Mutex when you are strictly locking data. In cases where you
    // are locking over a database connection, or io resource, the tokio Mutex is required.
    cache: Arc<Mutex<Cache>>,
    db_pool: Arc<PgPool>,
}

impl PublicKeyGrpc {
    pub fn new(cache: Arc<Mutex<Cache>>, db_pool: Arc<PgPool>) -> Self {
        Self { cache, db_pool }
    }
}

#[tonic::async_trait]
impl PublicKeyService for PublicKeyGrpc {
    async fn add(
        &self,
        request: Request<PublicKeyRequest>,
    ) -> GrpcResult<Response<PublicKeyResponse>> {
        let request = request.into_inner();

        // validate public_key
        if let Some(ref public_key) = request.public_key {
            if public_key.key.is_none() {
                return Err(Status::invalid_argument("must specify key type"))
            }
        } else {
            return Err(Status::invalid_argument("must specify public key"))
        }

        // validate signing public_key
        if let Some(ref signing_public_key) = request.signing_public_key {
            if signing_public_key.key.is_none() {
                return Err(Status::invalid_argument("must specify signing key type"))
            }
        } else {
            return Err(Status::invalid_argument("must specify signing public key"))
        }

        // validate url if it is not empty
        if !request.url.is_empty() {
            Url::parse(&request.url)
                .map_err(|e| Status::invalid_argument(format!("Unable to parse url {} - {:?}", &request.url, e)))?;
        }

        let response = datastore::add_public_key(&self.db_pool, request).await?;
        let key = match response.public_key.clone().unwrap().key.unwrap() {
            Key::Secp256k1(data) => base64::encode(data),
        };

        if response.url.is_empty() {
            let mut cache = self.cache.lock().unwrap();
            
            cache.add_local_public_key(key);
        } else {
            let mut cache = self.cache.lock().unwrap();
            
            cache.add_remote_public_key(key, response.url.clone());
        }

        Ok(Response::new(response))
    }
}

#[cfg(test)]
mod tests {
    use crate::*;
    use crate::public_key::*;

    use crate::pb::PublicKey;

    use testcontainers::*;
    use testcontainers::images::postgres::Postgres;
    use testcontainers::clients::Cli;

    async fn setup_postgres(container: &Container<'_, Cli, Postgres>) -> PgPool {
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

    #[tokio::test]
    async fn invalid_url() {
        let docker = clients::Cli::default();
        let image = images::postgres::Postgres::default().with_version(9);
        let container = docker.run(image);
        let cache = Mutex::new(Cache::default());
        let pool = setup_postgres(&container).await;
        let public_key_service = PublicKeyGrpc { cache: Arc::new(cache), db_pool: Arc::new(pool) };
        let request = PublicKeyRequest {
            public_key: Some(PublicKey {
                key: Some(Key::Secp256k1(vec![])),
            }),
            signing_public_key: Some(PublicKey {
                key: Some(Key::Secp256k1(vec![])),
            }),
            url: "invalidurl.com".to_owned(),
            metadata: None,
        };
        let response = public_key_service.add(Request::new(request)).await;

        match response {
            Err(err) => {
                assert_eq!(err.code(), tonic::Code::InvalidArgument);
                assert_eq!(err.message(), "Unable to parse url invalidurl.com - RelativeUrlWithoutBase".to_owned());
            },
            _ => assert_eq!(format!("{:?}", response), ""),
        }
    }

    #[tokio::test]
    async fn empty_url() {
        let docker = clients::Cli::default();
        let image = images::postgres::Postgres::default().with_version(9);
        let container = docker.run(image);
        let cache = Mutex::new(Cache::default());
        let pool = setup_postgres(&container).await;
        let public_key_service = PublicKeyGrpc { cache: Arc::new(cache), db_pool: Arc::new(pool) };
        let request = PublicKeyRequest {
            public_key: Some(PublicKey {
                key: Some(Key::Secp256k1(vec![1u8, 2u8, 3u8])),
            }),
            signing_public_key: Some(PublicKey {
                key: Some(Key::Secp256k1(vec![])),
            }),
            url: String::default(),
            metadata: None,
        };
        let response = public_key_service.add(Request::new(request)).await;

        match response {
            Ok(result) => {
                let result = result.into_inner();
                assert!(!result.uuid.is_none());
                assert_eq!(result.public_key.unwrap().key.unwrap(), Key::Secp256k1(vec![1u8, 2u8, 3u8]));
                assert!(result.url.is_empty());
                assert!(result.metadata.is_none());
                assert!(!result.created_at.is_none());
                assert!(!result.updated_at.is_none());
            },
            _ => {
                assert_eq!(format!("{:?}", response), "");
            }
        }
    }

    #[tokio::test]
    async fn missing_public_key() {
        let docker = clients::Cli::default();
        let image = images::postgres::Postgres::default().with_version(9);
        let container = docker.run(image);
        let cache = Mutex::new(Cache::default());
        let pool = setup_postgres(&container).await;
        let public_key_service = PublicKeyGrpc { cache: Arc::new(cache), db_pool: Arc::new(pool) };
        let request = PublicKeyRequest {
            public_key: None,
            signing_public_key: Some(PublicKey {
                key: Some(Key::Secp256k1(vec![1u8, 2u8, 3u8])),
            }),
            url: "http://test.com".to_owned(),
            metadata: None,
        };
        let response = public_key_service.add(Request::new(request)).await;

        match response {
            Err(err) => {
                assert_eq!(err.code(), tonic::Code::InvalidArgument);
                assert_eq!(err.message(), "must specify public key".to_owned());
            },
            _ => assert_eq!(format!("{:?}", response), ""),
        }
    }

    #[tokio::test]
    async fn missing_signing_public_key() {
        let docker = clients::Cli::default();
        let image = images::postgres::Postgres::default().with_version(9);
        let container = docker.run(image);
        let cache = Mutex::new(Cache::default());
        let pool = setup_postgres(&container).await;
        let public_key_service = PublicKeyGrpc { cache: Arc::new(cache), db_pool: Arc::new(pool) };
        let request = PublicKeyRequest {
            public_key: Some(PublicKey {
                key: Some(Key::Secp256k1(vec![1u8, 2u8, 3u8])),
            }),
            signing_public_key: None,
            url: "http://test.com".to_owned(),
            metadata: None,
        };
        let response = public_key_service.add(Request::new(request)).await;

        match response {
            Err(err) => {
                assert_eq!(err.code(), tonic::Code::InvalidArgument);
                assert_eq!(err.message(), "must specify signing public key".to_owned());
            },
            _ => assert_eq!(format!("{:?}", response), ""),
        }
    }

    #[tokio::test]
    async fn missing_key() {
        let docker = clients::Cli::default();
        let image = images::postgres::Postgres::default().with_version(9);
        let container = docker.run(image);
        let cache = Mutex::new(Cache::default());
        let pool = setup_postgres(&container).await;
        let public_key_service = PublicKeyGrpc { cache: Arc::new(cache), db_pool: Arc::new(pool) };
        let request = PublicKeyRequest {
            public_key: Some(PublicKey { key: None }),
            signing_public_key: Some(PublicKey {
                key: Some(Key::Secp256k1(vec![1u8, 2u8, 3u8])),
            }),
            url: "http://test.com".to_owned(),
            metadata: None,
        };
        let response = public_key_service.add(Request::new(request)).await;

        match response {
            Err(err) => {
                assert_eq!(err.code(), tonic::Code::InvalidArgument);
                assert_eq!(err.message(), "must specify key type".to_owned());
            },
            _ => assert_eq!(format!("{:?}", response), ""),
        }
    }

    #[tokio::test]
    async fn missing_p8e_key() {
        let docker = clients::Cli::default();
        let image = images::postgres::Postgres::default().with_version(9);
        let container = docker.run(image);
        let cache = Mutex::new(Cache::default());
        let pool = setup_postgres(&container).await;
        let public_key_service = PublicKeyGrpc { cache: Arc::new(cache), db_pool: Arc::new(pool) };
        let request = PublicKeyRequest {
            public_key: Some(PublicKey {
                key: Some(Key::Secp256k1(vec![1u8, 2u8, 3u8])),
            }),
            signing_public_key: Some(PublicKey { key: None }),
            url: "http://test.com".to_owned(),
            metadata: None,
        };
        let response = public_key_service.add(Request::new(request)).await;

        match response {
            Err(err) => {
                assert_eq!(err.code(), tonic::Code::InvalidArgument);
                assert_eq!(err.message(), "must specify signing key type".to_owned());
            },
            _ => assert_eq!(format!("{:?}", response), ""),
        }
    }

    #[tokio::test]
    async fn duplicate_key() {
        let docker = clients::Cli::default();
        let image = images::postgres::Postgres::default().with_version(9);
        let container = docker.run(image);
        let cache = Mutex::new(Cache::default());
        let pool = setup_postgres(&container).await;
        let public_key_service = PublicKeyGrpc { cache: Arc::new(cache), db_pool: Arc::new(pool) };
        let request1 = PublicKeyRequest {
            public_key: Some(PublicKey {
                key: Some(Key::Secp256k1(vec![1u8, 2u8, 3u8])),
            }),
            signing_public_key: Some(PublicKey {
                key: Some(Key::Secp256k1(vec![1u8, 2u8, 3u8])),
            }),
            url: "http://test.com".to_owned(),
            metadata: None,
        };
        let request2 = PublicKeyRequest {
            public_key: Some(PublicKey {
                key: Some(Key::Secp256k1(vec![1u8, 2u8, 3u8])),
            }),
            signing_public_key: Some(PublicKey {
                key: Some(Key::Secp256k1(vec![1u8, 2u8, 3u8])),
            }),
            url: "http://test2.com".to_owned(),
            metadata: None,
        };
        let response = public_key_service.add(Request::new(request1)).await;

        let (uuid, url) = match response {
            Ok(result) => {
                let result = result.into_inner();
                assert!(!result.uuid.is_none());
                (result.uuid, result.url)
            },
            _ => {
                assert_eq!(format!("{:?}", response), "");
                unreachable!();
            },
        };

        let response = public_key_service.add(Request::new(request2)).await;

        match response {
            Ok(result) => {
                let result = result.into_inner();
                assert!(!result.uuid.is_none());
                assert_eq!(uuid, result.uuid);
                assert_ne!(url, result.url);
                assert_eq!("http://test2.com", result.url);
            }
            _ => assert_eq!(format!("{:?}", response), ""),
        };
    }

    #[tokio::test]
    async fn cannot_update_when_both_keys_do_not_match() {
        let docker = clients::Cli::default();
        let image = images::postgres::Postgres::default().with_version(9);
        let container = docker.run(image);
        let cache = Mutex::new(Cache::default());
        let pool = setup_postgres(&container).await;
        let public_key_service = PublicKeyGrpc { cache: Arc::new(cache), db_pool: Arc::new(pool) };
        let request1 = PublicKeyRequest {
            public_key: Some(PublicKey {
                key: Some(Key::Secp256k1(vec![1u8, 2u8, 3u8])),
            }),
            signing_public_key: Some(PublicKey {
                key: Some(Key::Secp256k1(vec![1u8, 2u8, 3u8])),
            }),
            url: "http://test.com".to_owned(),
            metadata: None,
        };
        let request2 = PublicKeyRequest {
            public_key: Some(PublicKey {
                key: Some(Key::Secp256k1(vec![1u8, 2u8, 3u8, 4u8])),
            }),
            signing_public_key: Some(PublicKey {
                key: Some(Key::Secp256k1(vec![1u8, 2u8, 3u8])),
            }),
            url: "http://test2.com".to_owned(),
            metadata: None,
        };
        let response = public_key_service.add(Request::new(request1)).await;

        match response {
            Ok(result) => {
                let result = result.into_inner();
                assert!(!result.uuid.is_none());
            },
            _ => {
                assert_eq!(format!("{:?}", response), "");
                unreachable!();
            },
        };

        let response = public_key_service.add(Request::new(request2)).await;

        match &response {
            Ok(_) => assert_eq!(format!("{:?}", &response), ""),
            Err(e) => assert_eq!(e.message(), "SqlError(RowNotFound)"),
        };
    }

    #[tokio::test]
    async fn returns_full_proto() {
        let docker = clients::Cli::default();
        let image = images::postgres::Postgres::default().with_version(9);
        let container = docker.run(image);
        let cache = Mutex::new(Cache::default());
        let pool = setup_postgres(&container).await;
        let public_key_service = PublicKeyGrpc { cache: Arc::new(cache), db_pool: Arc::new(pool) };
        // TODO add metadata to this request
        let request = PublicKeyRequest {
            public_key: Some(PublicKey {
                key: Some(Key::Secp256k1(vec![1u8, 2u8, 3u8])),
            }),
            signing_public_key: Some(PublicKey {
                key: Some(Key::Secp256k1(vec![4u8, 5u8, 6u8])),
            }),
            url: "http://test.com".to_owned(),
            metadata: None,
        };
        let response = public_key_service.add(Request::new(request)).await;

        match response {
            Ok(result) => {
                let result = result.into_inner();
                assert!(!result.uuid.is_none());
                assert_eq!(result.public_key.unwrap().key.unwrap(), Key::Secp256k1(vec![1u8, 2u8, 3u8]));
                assert_eq!(result.signing_public_key.unwrap().key.unwrap(), Key::Secp256k1(vec![4u8, 5u8, 6u8]));
                assert_eq!(result.url, String::from("http://test.com"));
                assert!(result.metadata.is_none());
                assert!(!result.created_at.is_none());
                assert!(!result.updated_at.is_none());
            },
            _ => assert_eq!(format!("{:?}", response), ""),
        }
    }

    #[tokio::test]
    async fn adds_empty_urls_to_local_cache() {
        let docker = clients::Cli::default();
        let image = images::postgres::Postgres::default().with_version(9);
        let container = docker.run(image);
        let cache = Arc::new(Mutex::new(Cache::default()));
        let pool = setup_postgres(&container).await;
        let public_key_service = PublicKeyGrpc { cache: Arc::clone(&cache), db_pool: Arc::new(pool) };
        let request = PublicKeyRequest {
            public_key: Some(PublicKey {
                key: Some(Key::Secp256k1(vec![1u8, 2u8, 3u8])),
            }),
            signing_public_key: Some(PublicKey {
                key: Some(Key::Secp256k1(vec![4u8, 5u8, 6u8])),
            }),
            url: String::default(),
            metadata: None,
        };
        let response = public_key_service.add(Request::new(request)).await;

        match response {
            Ok(_) => (),
            _ => assert_eq!(format!("{:?}", response), ""),
        }

        let cache = Arc::clone(&cache);
        let cache = cache.lock().unwrap();
        assert_eq!(cache.local_public_keys.len(), 1);
        assert_eq!(cache.remote_public_keys.len(), 0);
        assert!(cache.local_public_keys.contains(&base64::encode(&vec![1u8, 2u8, 3u8])));
    }

    #[tokio::test]
    async fn adds_nonempty_urls_to_remote_cache() {
        let docker = clients::Cli::default();
        let image = images::postgres::Postgres::default().with_version(9);
        let container = docker.run(image);
        let cache = Arc::new(Mutex::new(Cache::default()));
        let pool = setup_postgres(&container).await;
        let public_key_service = PublicKeyGrpc { cache: Arc::clone(&cache), db_pool: Arc::new(pool) };
        let request = PublicKeyRequest {
            public_key: Some(PublicKey {
                key: Some(Key::Secp256k1(vec![1u8, 2u8, 3u8])),
            }),
            signing_public_key: Some(PublicKey {
                key: Some(Key::Secp256k1(vec![4u8, 5u8, 6u8])),
            }),
            url: "http://test.com".to_owned(),
            metadata: None,
        };
        let response = public_key_service.add(Request::new(request)).await;

        match response {
            Ok(_) => (),
            _ => assert_eq!(format!("{:?}", response), ""),
        }

        let cache = Arc::clone(&cache);
        let cache = cache.lock().unwrap();
        assert_eq!(cache.local_public_keys.len(), 0);
        assert_eq!(cache.remote_public_keys.len(), 1);
        assert!(cache.remote_public_keys.contains_key(&base64::encode(&vec![1u8, 2u8, 3u8])));
    }
}
