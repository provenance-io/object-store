use crate::cache::Cache;
use crate::datastore;
use crate::domain::PublicKeyApiResponse;
use crate::pb::public_key_request::Impl::HeaderAuth as HeaderAuthEnumRequest;
use crate::pb::public_key_service_server::PublicKeyService;
use crate::pb::{PublicKeyRequest, PublicKeyResponse};
use crate::types::GrpcResult;

use sqlx::postgres::PgPool;
use std::convert::TryInto;
use std::sync::{Arc, Mutex};
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
                return Err(Status::invalid_argument("must specify key type"));
            }
        } else {
            return Err(Status::invalid_argument("must specify public key"));
        }

        // validate auth methods
        if let Some(HeaderAuthEnumRequest(ref auth)) = request.r#impl {
            if auth.header.trim().is_empty() {
                return Err(Status::invalid_argument(
                    "must specify non empty auth header",
                ));
            }
            if auth.value.trim().is_empty() {
                return Err(Status::invalid_argument(
                    "must specify non empty auth value",
                ));
            }
        };

        // validate url if it is not empty
        if !request.url.is_empty() {
            Url::parse(&request.url).map_err(|e| {
                Status::invalid_argument(format!("Unable to parse url {} - {:?}", &request.url, e))
            })?;
        }

        let key = datastore::add_public_key(&self.db_pool, request.try_into()?).await?;
        let response = key.clone().to_response()?;

        {
            let mut cache = self.cache.lock().unwrap();

            cache.add_public_key(key);
        }

        Ok(Response::new(response))
    }
}

#[cfg(test)]
mod tests {
    use crate::public_key::*;

    use crate::pb::public_key_response::Impl::HeaderAuth as HeaderAuthEnumResponse;
    use crate::pb::{public_key::Key, HeaderAuth, PublicKey};

    use base64::prelude::BASE64_STANDARD;
    use base64::Engine;
    use sqlx::postgres::PgPoolOptions;
    use testcontainers::images::postgres::Postgres;
    use testcontainers::*;

    async fn setup_postgres(container: &Container<'_, Postgres>) -> PgPool {
        let connection_string = &format!(
            "postgres://postgres:postgres@localhost:{}/postgres",
            container.get_host_port_ipv4(5432),
        );

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(connection_string)
            .await
            .unwrap();

        sqlx::migrate!().run(&pool).await.unwrap();

        pool
    }

    #[tokio::test]
    async fn invalid_url() {
        let docker = clients::Cli::default();
        let image =
            RunnableImage::from(images::postgres::Postgres::default()).with_tag("14-alpine");
        let container = docker.run(image);
        let cache = Mutex::new(Cache::default());
        let pool = setup_postgres(&container).await;
        let public_key_service = PublicKeyGrpc {
            cache: Arc::new(cache),
            db_pool: Arc::new(pool),
        };
        let request = PublicKeyRequest {
            public_key: Some(PublicKey {
                key: Some(Key::Secp256k1(vec![])),
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
        let image =
            RunnableImage::from(images::postgres::Postgres::default()).with_tag("14-alpine");
        let container = docker.run(image);
        let cache = Mutex::new(Cache::default());
        let pool = setup_postgres(&container).await;
        let public_key_service = PublicKeyGrpc {
            cache: Arc::new(cache),
            db_pool: Arc::new(pool),
        };
        let request = PublicKeyRequest {
            public_key: Some(PublicKey {
                key: Some(Key::Secp256k1(vec![1u8, 2u8, 3u8])),
            }),
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
        let image =
            RunnableImage::from(images::postgres::Postgres::default()).with_tag("14-alpine");
        let container = docker.run(image);
        let cache = Mutex::new(Cache::default());
        let pool = setup_postgres(&container).await;
        let public_key_service = PublicKeyGrpc {
            cache: Arc::new(cache),
            db_pool: Arc::new(pool),
        };
        let request = PublicKeyRequest {
            public_key: Some(PublicKey {
                key: Some(Key::Secp256k1(vec![1u8, 2u8, 3u8])),
            }),
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
        let image =
            RunnableImage::from(images::postgres::Postgres::default()).with_tag("14-alpine");
        let container = docker.run(image);
        let cache = Mutex::new(Cache::default());
        let pool = setup_postgres(&container).await;
        let public_key_service = PublicKeyGrpc {
            cache: Arc::new(cache),
            db_pool: Arc::new(pool),
        };
        let request = PublicKeyRequest {
            public_key: Some(PublicKey {
                key: Some(Key::Secp256k1(vec![1u8, 2u8, 3u8])),
            }),
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
        let image =
            RunnableImage::from(images::postgres::Postgres::default()).with_tag("14-alpine");
        let container = docker.run(image);
        let cache = Mutex::new(Cache::default());
        let pool = setup_postgres(&container).await;
        let public_key_service = PublicKeyGrpc {
            cache: Arc::new(cache),
            db_pool: Arc::new(pool),
        };
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
        let image =
            RunnableImage::from(images::postgres::Postgres::default()).with_tag("14-alpine");
        let container = docker.run(image);
        let cache = Mutex::new(Cache::default());
        let pool = setup_postgres(&container).await;
        let public_key_service = PublicKeyGrpc {
            cache: Arc::new(cache),
            db_pool: Arc::new(pool),
        };
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
        let image =
            RunnableImage::from(images::postgres::Postgres::default()).with_tag("14-alpine");
        let container = docker.run(image);
        let cache = Mutex::new(Cache::default());
        let pool = setup_postgres(&container).await;
        let public_key_service = PublicKeyGrpc {
            cache: Arc::new(cache),
            db_pool: Arc::new(pool),
        };
        let request1 = PublicKeyRequest {
            public_key: Some(PublicKey {
                key: Some(Key::Secp256k1(vec![1u8, 2u8, 3u8])),
            }),
            r#impl: None,
            url: "http://test.com".to_owned(),
            metadata: None,
        };
        let request2 = PublicKeyRequest {
            public_key: Some(PublicKey {
                key: Some(Key::Secp256k1(vec![1u8, 2u8, 3u8])),
            }),
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
        let image =
            RunnableImage::from(images::postgres::Postgres::default()).with_tag("14-alpine");
        let container = docker.run(image);
        let cache = Mutex::new(Cache::default());
        let pool = setup_postgres(&container).await;
        let public_key_service = PublicKeyGrpc {
            cache: Arc::new(cache),
            db_pool: Arc::new(pool),
        };
        // TODO add metadata to this request
        let request = PublicKeyRequest {
            public_key: Some(PublicKey {
                key: Some(Key::Secp256k1(vec![1u8, 2u8, 3u8])),
            }),
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
        let image =
            RunnableImage::from(images::postgres::Postgres::default()).with_tag("14-alpine");
        let container = docker.run(image);
        let cache = Arc::new(Mutex::new(Cache::default()));
        let pool = setup_postgres(&container).await;
        let public_key_service = PublicKeyGrpc {
            cache: Arc::clone(&cache),
            db_pool: Arc::new(pool),
        };
        let request = PublicKeyRequest {
            public_key: Some(PublicKey {
                key: Some(Key::Secp256k1(vec![1u8, 2u8, 3u8])),
            }),
            r#impl: None,
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
        assert_eq!(cache.public_keys.len(), 1);
        assert!(cache
            .public_keys
            .contains_key(&BASE64_STANDARD.encode(&vec![1u8, 2u8, 3u8])));
        assert_eq!(
            cache
                .public_keys
                .get(&BASE64_STANDARD.encode(&vec![1u8, 2u8, 3u8]))
                .unwrap()
                .url,
            String::from("")
        );
    }

    #[tokio::test]
    async fn adds_nonempty_urls_to_remote_cache() {
        let docker = clients::Cli::default();
        let image =
            RunnableImage::from(images::postgres::Postgres::default()).with_tag("14-alpine");
        let container = docker.run(image);
        let cache = Arc::new(Mutex::new(Cache::default()));
        let pool = setup_postgres(&container).await;
        let public_key_service = PublicKeyGrpc {
            cache: Arc::clone(&cache),
            db_pool: Arc::new(pool),
        };
        let request = PublicKeyRequest {
            public_key: Some(PublicKey {
                key: Some(Key::Secp256k1(vec![1u8, 2u8, 3u8])),
            }),
            r#impl: None,
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
        assert_eq!(cache.public_keys.len(), 1);
        assert!(cache
            .public_keys
            .contains_key(&BASE64_STANDARD.encode(&vec![1u8, 2u8, 3u8])));
        assert_ne!(
            cache
                .public_keys
                .get(&BASE64_STANDARD.encode(&vec![1u8, 2u8, 3u8]))
                .unwrap()
                .url,
            String::from("")
        );
    }
}
