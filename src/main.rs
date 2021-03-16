#[macro_use] extern crate quick_error;

use bytes::BytesMut;
use chrono::prelude::*;
use prost::Message;
use std::{env, io::Error, io::ErrorKind, sync::Arc, time::SystemTime};
use sqlx::{migrate::Migrator, postgres::PgPool, postgres::PgPoolOptions, Executor, Row};
use tonic::{transport::Server, Code, Request, Response, Status};
use url::Url;

mod object_store {
    tonic::include_proto!("objectstore");
}

use object_store::{public_key::Key, PublicKey, PublicKeyRequest, PublicKeyResponse, Uuid};
use object_store::public_key_service_server::{PublicKeyService, PublicKeyServiceServer};

static MIGRATOR: Migrator = sqlx::migrate!();

quick_error! {
    #[derive(Debug)]
    pub enum OsError {
        AddrParseError(err: std::net::AddrParseError) {
            from()
        }
        ProstEncodeError(err: prost::EncodeError) {
            from()
        }
        SqlError(err: sqlx::Error) {
            from()
        }
        SqlMigrateError(err: sqlx::migrate::MigrateError) {
            from()
        }
        TonicTransportError(err: tonic::transport::Error) {
            from()
        }
    }
}

impl From<OsError> for Status {
    fn from(error: OsError) -> Self {
        let code = match error {
            OsError::AddrParseError(_) => Code::Internal,
            OsError::ProstEncodeError(_) => Code::Internal,
            OsError::SqlError(_) => Code::Internal,
            OsError::SqlMigrateError(_) => Code::Internal,
            OsError::TonicTransportError(_) => Code::Internal,
        };

        Status::new(code, format!("{:?}", error))
    }
}

type Result<T> = std::result::Result<T, OsError>;
type GrpcResult<T> = std::result::Result<T, Status>;

#[derive(Debug)]
struct Config {
    url: Url,
    port: u16,
    db_connection_pool_size: u32,
    db_host: String,
    db_port: u16,
    db_user: String,
    db_password: String,
    db_database: String,
    db_schema: String,
}

impl Config {
    fn new() -> Self {
        let url = env::var("OS_URL").expect("OS_URL not set");
        let url = Url::parse(url.as_ref()).expect("OS_URL could not be parsed");
        let port = env::var("OS_PORT")
            .expect("OS_PORT not set")
            .parse()
            .expect("OS_PORT could not be parsed into a u16");
        let db_connection_pool_size = env::var("DB_CONNECTION_POOL_SIZE")
            .unwrap_or("10".to_owned())
            .parse()
            .expect("DB_CONNECTION_POOL_SIZE could not be parsed into a u16");
        let db_host = env::var("DB_HOST").expect("DB_HOST not set");
        let db_port = env::var("DB_PORT")
            .expect("DB_PORT not set")
            .parse()
            .expect("DB_PORT could not be parsed into a u16");
        let db_user = env::var("DB_USER").expect("DB_USER not set");
        let db_password = env::var("DB_PASSWORD").expect("DB_PASSWORD not set");
        let db_database = env::var("DB_DATABASE").expect("DB_DATABASE not set");
        let db_schema = env::var("DB_SCHEMA").expect("DB_SCHEMA not set");

        Self { url, port, db_connection_pool_size, db_host, db_port, db_user, db_password, db_database, db_schema }
    }

    fn db_connection_string(&self) -> String {
        format!(
            "postgres://{}:{}@{}/{}",
            self.db_user,
            self.db_password,
            self.db_host,
            self.db_database,
        )
    }
}

#[derive(Debug)]
struct PublicKeyGrpc {
    db_pool: PgPool,
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

        let response = add_public_key(&self.db_pool, request)
            .await
            .map_err(Into::<Status>::into)?;

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

// TODO maybe impl under PublicKeyGrpc?
async fn add_public_key(pool: &PgPool, public_key: PublicKeyRequest) -> Result<PublicKeyResponse> {
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
INSERT INTO public_keys (uuid, public_key, public_key_type, url, metadata)
VALUES ($1, $2, $3::key_type, $4, $5)
RETURNING uuid, public_key, public_key_type, url, metadata, created_at, updated_at
        "#)
        .bind(uuid::Uuid::new_v4())
        .bind(
            match public_key.public_key.unwrap().key.unwrap() {
                Key::Secp256k1(data) => hex::encode(data),
            }
        )
        .bind("secp256k1")
        .bind(public_key.url)
        .bind(metadata.as_ref())
        .fetch_one(pool)
        .await?;

    Ok(record)
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::new();
    let schema = Arc::new(config.db_schema.clone());
    let pool = PgPoolOptions::new()
        .after_connect(move |conn| {
            let schema = Arc::clone(&schema);
            Box::pin(async move {
                conn.execute(format!("SET search_path = '{}';", &schema).as_ref()).await?;

                Ok(())
            })
        })
        // TODO add more config fields
        .max_connections(config.db_connection_pool_size)
        .connect(config.db_connection_string().as_ref())
        .await?;
    let addr = format!("[::1]:{}", &config.port).parse()?;

    MIGRATOR.run(&pool).await?;

    let public_key_service = PublicKeyGrpc { db_pool: pool };

    Server::builder()
        .add_service(PublicKeyServiceServer::new(public_key_service))
        .serve(addr)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::*;
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
        let pool = setup_postgres(&container).await;
        let public_key_service = PublicKeyGrpc { db_pool: pool };
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
        let pool = setup_postgres(&container).await;
        let public_key_service = PublicKeyGrpc { db_pool: pool };
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
        let pool = setup_postgres(&container).await;
        let public_key_service = PublicKeyGrpc { db_pool: pool };
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
        let pool = setup_postgres(&container).await;
        let public_key_service = PublicKeyGrpc { db_pool: pool };
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
