use crate::cache::Cache;
use crate::types::{GrpcResult, Result};
use crate::pb::{public_key::Key, PublicKey, PublicKeyRequest, PublicKeyResponse, Uuid};
use crate::pb::public_key_service_server::PublicKeyService;

use bytes::BytesMut;
use chrono::prelude::*;
use prost::Message;
use std::{io::Error, io::ErrorKind, sync::Arc, time::SystemTime};
use sqlx::{postgres::PgPool, Row};
use tonic::{Request, Response, Status};
use url::Url;

// TODO add models to domain and sql functions to datastore
// TODO convert ErrorKind into local errors

#[derive(Debug)]
pub struct PublicKeyGrpc {
    cache: Arc<Cache>,
    db_pool: Arc<PgPool>,
}

impl PublicKeyGrpc {
    pub fn new(cache: Arc<Cache>, db_pool: Arc<PgPool>) -> Self {
        Self { cache, db_pool }
    }

    async fn add_public_key(&self, public_key: PublicKeyRequest) -> Result<PublicKeyResponse> {
        let metadata = if let Some(metadata) = public_key.metadata {
            let mut buffer = BytesMut::with_capacity(metadata.encoded_len());
            metadata.encode(&mut buffer)?;
            buffer
        } else {
            BytesMut::default()
        };
        // TODO change to upsert
        // TODO change to compile time validated
        let record = sqlx::query_as(
            r#"
INSERT INTO public_key (uuid, public_key, public_key_type, url, metadata)
VALUES ($1, $2, $3::key_type, $4, $5)
RETURNING uuid, public_key, public_key_type, url, metadata, created_at, updated_at
            "#)
            .bind(uuid::Uuid::new_v4())
            .bind(match public_key.public_key.unwrap().key.unwrap() {
                Key::Secp256k1(data) => hex::encode(data),
            })
            .bind("secp256k1")
            .bind(public_key.url)
            .bind(metadata.as_ref())
            .fetch_one(self.db_pool.as_ref())
            .await?;

        Ok(record)
    }
}

#[tonic::async_trait]
impl PublicKeyService for PublicKeyGrpc {
    async fn add(
        &self,
        request: Request<PublicKeyRequest>,
    ) -> GrpcResult<Response<PublicKeyResponse>> {
        let request = request.into_inner();

        // validate public_key and url
        if let Some(ref public_key) = request.public_key {
            if public_key.key.is_none() {
                return Err(Error::new(ErrorKind::InvalidInput, "must specify key type").into());
            }
        } else {
            return Err(Error::new(ErrorKind::InvalidInput, "must specify public key").into());
        }
        Url::parse(&request.url)
            .map_err(|err| Error::new(ErrorKind::InvalidInput, err))?;

        let response = self.add_public_key(request).await?;

        Ok(Response::new(response))
    }
}

#[derive(sqlx::Type)]
#[sqlx(type_name = "key_type", rename_all = "lowercase")]
enum KeyType { Secp256k1 }

impl sqlx::FromRow<'_, sqlx::postgres::PgRow> for PublicKeyResponse {
    fn from_row(row: &sqlx::postgres::PgRow) -> std::result::Result<Self, sqlx::Error> {
        let key_bytes: Vec<u8> = hex::decode(row.try_get::<&str, _>("public_key")?)
            .map_err(|err| sqlx::Error::Decode(Box::new(err)))?;
        let public_key = match row.try_get::<KeyType, _>("public_key_type")? {
            KeyType::Secp256k1 => Key::Secp256k1(key_bytes),
        };
        let created_at: SystemTime = row.try_get::<DateTime<Utc>, _>("created_at")?.into();
        let updated_at: SystemTime = row.try_get::<DateTime<Utc>, _>("updated_at")?.into();
        let metadata: Vec<u8> = row.try_get("metadata")?;
        let metadata = if !metadata.is_empty() {
            let message = prost_types::Any::decode(metadata.as_slice())
                .map_err(|err| sqlx::Error::Decode(Box::new(err)))?;
            Some(message)
        } else {
            None
        };
        let response = PublicKeyResponse {
            uuid: Some(Uuid {
                value: row.try_get::<uuid::Uuid, _>("uuid")?.to_hyphenated().to_string()
            }),
            public_key: Some(PublicKey { key: Some(public_key) }),
            url: row.try_get("url")?,
            metadata,
            created_at: Some(created_at.into()),
            updated_at: Some(updated_at.into()),
        };

        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use crate::*;
    use crate::public_key::*;

    use testcontainers::*;
    use testcontainers::images::postgres::Postgres;
    use testcontainers::clients::Cli;

    async fn setup_postgres(container: &Container<'_, Cli, Postgres>) -> PgPool {
        let connection_string = &format!(
            "postgres://postgres:postgres@localhost:{}/postgres",
            container.get_host_port(5432).unwrap(),
        );

        let pool = PgPoolOptions::new()
            // TODO add more config fields
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
        let cache = Cache::default();
        let pool = setup_postgres(&container).await;
        let public_key_service = PublicKeyGrpc { cache: Arc::new(cache), db_pool: Arc::new(pool) };
        let request = PublicKeyRequest {
            public_key: Some(PublicKey {
                key: Some(Key::Secp256k1(vec![])),
            }),
            url: "invalidurl.com".to_owned(),
            metadata: None,
        };

        match public_key_service.add(Request::new(request)).await {
            Err(err) => {
                assert_eq!(err.code(), tonic::Code::InvalidArgument);
                assert_eq!(err.message(), "relative URL without a base".to_owned());
            },
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn missing_public_key() {
        let docker = clients::Cli::default();
        let image = images::postgres::Postgres::default().with_version(9);
        let container = docker.run(image);
        let cache = Cache::default();
        let pool = setup_postgres(&container).await;
        let public_key_service = PublicKeyGrpc { cache: Arc::new(cache), db_pool: Arc::new(pool) };
        let request = PublicKeyRequest {
            public_key: None,
            url: "http://test.com".to_owned(),
            metadata: None,
        };

        match public_key_service.add(Request::new(request)).await {
            Err(err) => {
                assert_eq!(err.code(), tonic::Code::InvalidArgument);
                assert_eq!(err.message(), "must specify public key".to_owned());
            },
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn missing_key() {
        let docker = clients::Cli::default();
        let image = images::postgres::Postgres::default().with_version(9);
        let container = docker.run(image);
        let cache = Cache::default();
        let pool = setup_postgres(&container).await;
        let public_key_service = PublicKeyGrpc { cache: Arc::new(cache), db_pool: Arc::new(pool) };
        let request = PublicKeyRequest {
            public_key: Some(PublicKey { key: None }),
            url: "http://test.com".to_owned(),
            metadata: None,
        };

        match public_key_service.add(Request::new(request)).await {
            Err(err) => {
                assert_eq!(err.code(), tonic::Code::InvalidArgument);
                assert_eq!(err.message(), "must specify key type".to_owned());
            },
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn returns_full_proto() {
        let docker = clients::Cli::default();
        let image = images::postgres::Postgres::default().with_version(9);
        let container = docker.run(image);
        let cache = Cache::default();
        let pool = setup_postgres(&container).await;
        let public_key_service = PublicKeyGrpc { cache: Arc::new(cache), db_pool: Arc::new(pool) };
        // TODO add metadata to this request
        let request = PublicKeyRequest {
            public_key: Some(PublicKey {
                key: Some(Key::Secp256k1(vec![1u8, 2u8, 3u8])),
            }),
            url: "http://test.com".to_owned(),
            metadata: None,
        };

        match public_key_service.add(Request::new(request)).await {
            Ok(result) => {
                let result = result.into_inner();
                assert!(!result.uuid.is_none());
                assert_eq!(result.public_key.unwrap().key.unwrap(), Key::Secp256k1(vec![1u8, 2u8, 3u8]));
                assert_eq!(result.url, String::from("http://test.com"));
                assert!(result.metadata.is_none());
                assert!(!result.created_at.is_none());
                assert!(!result.updated_at.is_none());
            },
            _ => unreachable!(),
        }
    }
}
