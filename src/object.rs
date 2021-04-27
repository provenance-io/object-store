use crate::dime::{Dime, Signature as DimeSignature, format_dime_bytes};
use crate::types::{GrpcResult, Result, OsError};
use crate::pb::chunk_bidi::Impl::{Chunk as ChunkEnum, MultiStreamHeader as MultiStreamHeaderEnum};
use crate::pb::MultiStreamHeader;
use crate::pb::chunk::Impl::{End, Data, Value};
// TODO change to ObjectWrapper?
use crate::pb::{Chunk, ChunkBidi, ChunkEnd, Item, Object as ObjectInner, ObjectMetadata, ObjectResponse, Sha512Request, Signature, StreamHeader, Uuid};
use crate::pb::object_service_server::ObjectService;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use chrono::prelude::*;
use futures_util::{StreamExt, TryStreamExt};
use tokio::{fs::metadata, sync::mpsc};
use std::{collections::HashMap, convert::TryInto, sync::Arc, time::SystemTime};
use sqlx::{FromRow, Row};
use sqlx::postgres::{PgConnection, PgPool, PgQueryResult};
use tonic::{Request, Response, Status, Streaming};

// TODO set encryption and signing key pair to something different locally and try this again
// TODO time slow requests
// TODO multistream header created by
// TODO tests

const DIME_FIELD_NAME: &str = "DIME";
const HASH_FIELD_NAME: &str = "HASH";
const SIGNATURE_PUBLIC_KEY_FIELD_NAME: &str = "SIGNATURE_PUBLIC_KEY";
const SIGNATURE_FIELD_NAME: &str = "SIGNATURE";
const CREATED_BY_HEADER: &str = "x-created-by";
const NOT_STORAGE_BACKED: &str = "NOT_STORAGE_BACKED";
const CHUNK_SIZE: usize = 2000000;

enum UpsertOutcome {
    Noop,
    Created,
}

impl From<PgQueryResult> for UpsertOutcome {

    fn from(query_result: PgQueryResult) -> Self {
        if query_result.rows_affected() > 0 {
            Self::Created
        } else {
            Self::Noop
        }
    }
}

#[derive(Debug)]
struct DimeProperties {
    hash: String,
    content_length: i64,
    owner_signature_base64: String,
    owner_public_key_base64: String,
}

#[derive(sqlx::FromRow, Debug)]
struct Object {
    uuid: uuid::Uuid,
    hash: String,
    unique_hash: String,
    content_length: i64,
    bucket: String,
    name: String,
    payload: Option<Vec<u8>>,
    created_at: DateTime<Utc>,
}

#[derive(sqlx::FromRow, Debug)]
struct ObjectPublicKey {
    object_uuid: uuid::Uuid,
    hash: String,
    public_key: String,
    signature: Option<String>,
    signature_public_key: Option<String>,
    created_at: DateTime<Utc>,
}

#[derive(Debug)]
struct ObjectWithPublicKeys {
    object: Object,
    public_keys: Vec<ObjectPublicKey>,
}

impl From<ObjectWithPublicKeys> for ObjectResponse {

    fn from(opk: ObjectWithPublicKeys) -> Self {
        let uuid = opk.object.uuid.to_hyphenated().to_string();
        let mut signatures = Vec::new();

        for key in &opk.public_keys {
            if key.signature.is_some() {
                // TODO fix unwrap
                signatures.push(Signature {
                    // signature: key.signature.clone().unwrap().into_bytes(),
                    signature: base64::decode(key.signature.clone().unwrap()).unwrap(),
                    // public_key: key.signature_public_key.clone().unwrap().into_bytes(),
                    public_key: base64::decode(key.signature_public_key.clone().unwrap()).unwrap(),
                });
            }
        }

        ObjectResponse {
            object: Some(ObjectInner {
                uuid: Some(Uuid { value: uuid }),
                hash: base64::decode(&opk.object.hash).unwrap(), // TODO remove unwrap
                signatures, // TODO fix
                // TODO make this config variable
                uri: format!("object://object-store.provenance.io/{}", &opk.object.hash),
                bucket: opk.object.bucket.clone(),
                name: opk.object.name.clone(),
                metadata: Some(ObjectMetadata {
                    hash: Vec::new(), // TODO get hash of whole dime?
                    length: 0_i64, // TODO this is the size of the blob in non db storage mode
                }),
                created: Some(Into::<SystemTime>::into(opk.object.created_at).into()),
                created_by: "".to_owned(), // TODO fix this
            }),
            item: Some(Item {
                bucket: opk.object.bucket,
                name: opk.object.name,
                length: 0_i64, // TODO this is the size of the blob in non db storage mode
                // TODO only "x-content-length" in legacy, but that's not really needed
                metadata: HashMap::new(),
            }),
        }
    }
}

pub struct ObjectGrpc {
    db_pool: Arc<PgPool>,
}

impl ObjectGrpc {

    pub fn new(pool: Arc<PgPool>) -> Self {
        Self { db_pool: pool }
    }

    // TODO add index to hash or (hash, public_key)
    // TODO write test to make sure this gets key with signature first
    async fn get_public_key_object_uuid(&self, hash: &str, public_key: &str) -> Result<(uuid::Uuid, Option<(String, String)>)> {
        let query_str = r#"
SELECT object_uuid, signature_public_key, signature FROM object_public_key
  WHERE hash = $1 AND public_key = $2
  ORDER BY signature_public_key NULLS LAST
        "#;
        let result = sqlx::query(query_str)
            .bind(hash)
            .bind(public_key)
            .fetch_optional(self.db_pool.as_ref())
            .await?;

        // TODO limit query to 1?

        if let Some(row) = result {
            let object_uuid = row.try_get("object_uuid")?;
            let signature_public_key: Option<String> = row.try_get("signature_public_key")?;
            let signature: Option<String> = row.try_get("signature")?;
            let pair = if signature_public_key.is_some() && signature.is_some() {
                Some((signature_public_key.unwrap(), signature.unwrap()))
            } else if signature_public_key.is_none() && signature.is_none() {
                None
            } else {
                return Err(OsError::InvalidSignatureState("Signature_public_key and signature must be both null or non null.".to_owned()))
            };

            Ok((object_uuid, pair))
        } else {
            Err(OsError::NotFound(format!("Unable to find object with public key {} and hash {}", public_key, hash)))
        }
    }

    async fn get_public_keys_by_object(&self, object_uuid: &uuid::Uuid) -> Result<Vec<ObjectPublicKey>> {
        let query_str = "SELECT * FROM object_public_key WHERE object_uuid = $1";
        let mut result = Vec::new();
        let mut query_result = sqlx::query(query_str)
            .bind(object_uuid)
            .fetch(self.db_pool.as_ref());

        while let Some(row) = query_result.try_next().await? {
            result.push(ObjectPublicKey::from_row(&row)?);
        }

        Ok(result)
    }

    async fn get_object_by_unique_hash(&self, unique_hash: &str) -> Result<Object> {
        let query_str = "SELECT * FROM object WHERE unique_hash = $1";
        let result = sqlx::query_as::<_, Object>(query_str)
            .bind(unique_hash)
            .fetch_one(self.db_pool.as_ref())
            .await?;

        Ok(result)
    }

    async fn get_object_by_uuid(&self, uuid: &uuid::Uuid) -> Result<Object> {
        let query_str = "SELECT * FROM object WHERE uuid = $1";
        let result = sqlx::query_as::<_, Object>(query_str)
            .bind(uuid)
            .fetch_one(self.db_pool.as_ref())
            .await?;

        Ok(result)
    }

    async fn put_object_public_keys(&self, conn: &mut PgConnection, dime: &Dime, properties: &DimeProperties) -> Result<()> {
        let mut object_uuids: Vec<uuid::Uuid> = Vec::new();
        let mut hashes: Vec<&str> = Vec::new();
        let mut public_keys: Vec<String> = Vec::new();
        let mut signatures: Vec<Option<&str>> = Vec::new();
        let mut signature_public_keys: Vec<Option<&str>> = Vec::new();
        let dime_owner_public_key_base64 = dime.owner_public_key_base64();

        for party in dime.unique_audience_base64() {
            object_uuids.push(dime.uuid);
            hashes.push(&properties.hash);

            if party == dime_owner_public_key_base64 {
                signatures.push(Some(properties.owner_signature_base64.as_str()));
                signature_public_keys.push(Some(properties.owner_public_key_base64.as_str()));
            } else {
                signatures.push(None);
                signature_public_keys.push(None);
            }

            public_keys.push(party);
        }

        // TODO assert object has exactly one signer

        println!("object_uuids {:?}", &object_uuids);
        println!("hashes {:?}", &hashes);
        println!("public_keys {:?}", &public_keys);
        println!("signatures {:?}", &signatures);

        let query_str = r#"
INSERT INTO object_public_key (object_uuid, hash, public_key, signature, signature_public_key)
SELECT * FROM UNNEST($1, $2, $3, $4, $5)
        "#;

        sqlx::query(query_str)
            .bind(&object_uuids)
            .bind(&hashes)
            .bind(&public_keys)
            .bind(&signatures)
            .bind(&signature_public_keys)
            .execute(conn)
            .await?;

        Ok(())
    }

    async fn put_object(&self, dime: &Dime, properties: &DimeProperties, raw_dime: Option<&Bytes>) -> Result<ObjectWithPublicKeys> {
        let mut unique_hash = dime.unique_audience_base64();
        unique_hash.sort_unstable();
        unique_hash.insert(0, String::from(&properties.hash));
        let unique_hash = unique_hash.join(";");

        // TODO can we have one path here and insert None for payload?
        let query_str = if raw_dime.is_some() {
            r#"
INSERT INTO object (uuid, hash, unique_hash, content_length, bucket, name, payload)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT ON CONSTRAINT unique_hash_cnst
DO NOTHING
            "#
        } else {
            r#"
INSERT INTO object (uuid, hash, unique_hash, content_length, bucket, name)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT ON CONSTRAINT unique_hash_cnst
DO NOTHING
            "#
        };

        let query = sqlx::query(query_str)
            .bind(&dime.uuid)
            .bind(&properties.hash)
            .bind(&unique_hash)
            .bind(&properties.content_length);
        let query = if let Some(raw_dime) = raw_dime {
            query.bind(NOT_STORAGE_BACKED)
                .bind(NOT_STORAGE_BACKED)
                .bind(raw_dime.to_vec())
        } else {
            // TODO implement
            unimplemented!("raw dime was None")
        };

        println!("in here!");

        let mut tx = self.db_pool.begin().await?;
        let result = query.execute(&mut tx).await?.into();
        println!("in here 2!");
        match result {
            UpsertOutcome::Created => self.put_object_public_keys(&mut tx, dime, properties).await,
            UpsertOutcome::Noop => Ok(()),
        }?;
        tx.commit().await?;
        println!("in here 3!");

        let object = self.get_object_by_unique_hash(unique_hash.as_str()).await?;
        println!("in here 4!");
        let object_public_keys = self.get_public_keys_by_object(&object.uuid).await?;
        println!("in here 5!");

        Ok(ObjectWithPublicKeys { object, public_keys: object_public_keys })
    }
}

#[tonic::async_trait]
impl ObjectService for ObjectGrpc {

    async fn put(
        &self,
        request: Request<Streaming<ChunkBidi>>,
    ) -> GrpcResult<Response<ObjectResponse>> {
        let mut stream = request.into_inner();
        let mut chunk_buffer = Vec::new();
        // TODO allocate exact size later?
        let mut byte_buffer = BytesMut::new();
        let mut end = false;
        let mut properties: HashMap<String, Vec<u8>> = HashMap::new();

        // TODO remove
        use prost::Message;
        while let Some(chunk) = stream.next().await {
            let mut buffer = BytesMut::with_capacity(chunk.clone()?.encoded_len());
            chunk.clone()?.encode(&mut buffer).unwrap();
            println!("received chunk! {} base64 = {}", chunk.clone()?.encoded_len(), base64::encode(&buffer));
            chunk_buffer.push(chunk?);
        }

        // check validity of stream header
        if chunk_buffer.len() < 1 {
            return Err(Status::invalid_argument("Multipart upload is empty"))
        }
        let multi_stream_header = match chunk_buffer[0].r#impl {
            Some(MultiStreamHeaderEnum(ref header)) => {
                if header.stream_count != 1 {
                    return Err(Status::invalid_argument("Multipart upload must contain a single stream"))
                } else {
                    header
                }
            },
            _ => return Err(Status::invalid_argument("Multipart upload must start with a multi stream header")),
        };

        // check validity of chunk header
        if chunk_buffer.len() < 2 {
            return Err(Status::invalid_argument("Multi stream has a header but no chunks"))
        }
        let (header_chunk_header, header_chunk_data) = match chunk_buffer[1].r#impl {
            Some(ChunkEnum(ref chunk)) => {
                let chunk_header = match chunk.header {
                    Some(ref header) => header,
                    None => return Err(Status::invalid_argument("Multi stream must start with a header")),
                };

                match chunk.r#impl {
                    Some(Data(ref data)) => (chunk_header, data),
                    Some(Value(_)) => return Err(Status::invalid_argument("Chunk header must have data and not a value")),
                    Some(End(_)) => return Err(Status::invalid_argument("Chunk header must have data and not an end of chunk")),
                    None => return Err(Status::invalid_argument("Chunk header must have data and not an empty impl")),
                }
            },
            _ => return Err(Status::invalid_argument("Chunk has no impl value")),
        };

        byte_buffer.put(header_chunk_data.as_ref());
        println!("header chunk of size = {} content length = {}", &header_chunk_data.len(), &header_chunk_header.content_length);

        // TODO refactor a lot of this fetching
        for chunk in &chunk_buffer[2..] {
            match chunk.r#impl {
                Some(ChunkEnum(ref chunk)) => {
                    match chunk.r#impl {
                        Some(Data(ref data)) => {
                            if end {
                                return Err(Status::invalid_argument("Received data chunk after an end of data chunk"))
                            } else {
                                println!("received chunk of size = {}", data.len());
                                byte_buffer.put(data.as_ref())
                            }
                        },
                        Some(Value(ref value)) => {
                            let key = chunk.header
                                .as_ref()
                                .ok_or(Status::invalid_argument("Must have stream header when impl is value"))?
                                .name
                                .clone();
                            println!("inserting property of {}:{:?}", key, &value);
                            properties.insert(key, value.to_vec());
                        },
                        Some(End(_)) => end = true,
                        None => return Err(Status::invalid_argument("Chunk header must have data and not an empty impl")),
                    }
                },
                _ => return Err(Status::invalid_argument("Non chunk detected in stream")),
            }
        }

        let hash = properties.get(HASH_FIELD_NAME)
            .map(base64::encode)
            .ok_or(Status::invalid_argument("Properties must contain \"HASH\""))?;
        let signature = properties.get(SIGNATURE_FIELD_NAME)
            .map(base64::encode)
            .ok_or(Status::invalid_argument("Properties must contain \"SIGNATURE_FIELD_NAME\""))?;
        let signature_public_key = properties.get(SIGNATURE_PUBLIC_KEY_FIELD_NAME)
            .map(base64::encode)
            .ok_or(Status::invalid_argument("Properties must contain \"SIGNATURE_PUBLIC_KEY_FIELD_NAME\""))?;
        println!("done!");
        println!("hash = {}", &hash);
        println!("signature = {}", &signature);
        println!("signature public key = {}", &signature_public_key);
        let dime_properties = DimeProperties {
            hash,
            content_length: header_chunk_header.content_length,
            // TODO can remove the base64 wording?
            owner_signature_base64: signature,
            owner_public_key_base64: signature_public_key,
        };

        let byte_buffer: Bytes = byte_buffer.into();
        println!("dime length {}", byte_buffer.len());
        // Bytes clones are cheap
        let raw_dime = byte_buffer.clone();
        let dime: Dime = byte_buffer.try_into()
            .map_err(|err| Into::<OsError>::into(err))?;

        println!("before put object dime base64 = {}", base64::encode(&raw_dime));
        // TODO none here when raw_dime threshold is met
        let response = self.put_object(&dime, &dime_properties, Some(&raw_dime)).await?;
        // TODO remove
        let temp = response.into();
        println!("after put object {:?}", temp);

        Ok(Response::new(temp))
    }

    type GetStream = tokio_stream::wrappers::ReceiverStream<GrpcResult<ChunkBidi>>;

    async fn get(
        &self,
        request: Request<Sha512Request>,
    ) -> GrpcResult<Response<Self::GetStream>> {
        let request = request.into_inner();
        let hash = base64::encode(request.hash);
        let public_key = base64::encode(&request.public_key);

        println!("hash {}\n public_key {}", &hash, &public_key);

        let (object_uuid, signature) = self.get_public_key_object_uuid(hash.as_str(), public_key.as_str()).await?;
        // TODO fix later
        let signature = signature.unwrap();
        let object = self.get_object_by_uuid(&object_uuid).await?;
        // TODO how to get self owner and fallback to any owner
        let owner_signature = DimeSignature { signature: signature.1, public_key: signature.0 };
        println!("owner signature {:?}", &owner_signature);
        // TODO fix this?
        let payload = object.payload.clone().unwrap_or_default();
        // TODO remove println
        println!("before get object dime base64 = {}", base64::encode(&payload));
        let mut payload = Bytes::copy_from_slice(payload.as_slice());
        let payload = format_dime_bytes(&mut payload, owner_signature)
            .map_err(Into::<OsError>::into)?;
        println!("after get object dime base64 = {}", base64::encode(&payload.as_ref()));

        let (tx, rx) = mpsc::channel(4);

        tokio::spawn(async move {
            // send multi stream header
            let mut metadata = HashMap::new();
            metadata.insert(CREATED_BY_HEADER.to_owned(), uuid::Uuid::nil().to_hyphenated().to_string());
            let header = MultiStreamHeader { stream_count: 1, metadata };
            let msg = ChunkBidi { r#impl: Some(MultiStreamHeaderEnum(header)) };
            tx.send(Ok(msg)).await.unwrap(); // TODO remove unwrap

            // send data chunks
            for (idx, chunk) in payload.chunks(CHUNK_SIZE).enumerate() {
                let header = if idx == 0 {
                    Some(StreamHeader { name: DIME_FIELD_NAME.to_owned(), content_length: object.content_length })
                } else {
                    None
                };
                let data_chunk = Chunk { header, r#impl: Some(Data(chunk.to_vec())) };
                let msg = ChunkBidi { r#impl: Some(ChunkEnum(data_chunk)) };
                tx.send(Ok(msg)).await.unwrap(); // TODO remove unwrap
            }

            // send end chunk
            let end = Chunk { header: None, r#impl: Some(End(ChunkEnd::default())) };
            let msg = ChunkBidi { r#impl: Some(ChunkEnum(end)) };
            tx.send(Ok(msg)).await.unwrap(); // TODO remove unwrap
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }

    type GetByUuidStream = tokio_stream::wrappers::ReceiverStream<GrpcResult<ChunkBidi>>;

    // TODO this function might not be needed
    async fn get_by_uuid(
        &self,
        request: Request<Uuid>,
    ) -> GrpcResult<Response<Self::GetByUuidStream>> {
        unimplemented!();
    }
}

// #[derive(sqlx::Type)]
// #[sqlx(type_name = "key_type", rename_all = "lowercase")]
// enum KeyType { Secp256k1 }
// 
// impl sqlx::FromRow<'_, sqlx::postgres::PgRow> for PublicKeyResponse {
//     fn from_row(row: &sqlx::postgres::PgRow) -> std::result::Result<Self, sqlx::Error> {
//         let key_bytes: Vec<u8> = hex::decode(row.try_get::<&str, _>("public_key")?)
//             .map_err(|err| sqlx::Error::Decode(Box::new(err)))?;
//         let public_key = match row.try_get::<KeyType, _>("public_key_type")? {
//             KeyType::Secp256k1 => Key::Secp256k1(key_bytes),
//         };
//         let created_at: SystemTime = row.try_get::<DateTime<Utc>, _>("created_at")?.into();
//         let updated_at: SystemTime = row.try_get::<DateTime<Utc>, _>("updated_at")?.into();
//         let metadata: Vec<u8> = row.try_get("metadata")?;
//         let metadata = if !metadata.is_empty() {
//             let message = prost_types::Any::decode(metadata.as_slice())
//                 .map_err(|err| sqlx::Error::Decode(Box::new(err)))?;
//             Some(message)
//         } else {
//             None
//         };
//         let response = PublicKeyResponse {
//             uuid: Some(Uuid {
//                 value: row.try_get::<uuid::Uuid, _>("uuid")?.to_hyphenated().to_string()
//             }),
//             public_key: Some(PublicKey { key: Some(public_key) }),
//             url: row.try_get("url")?,
//             metadata,
//             created_at: Some(created_at.into()),
//             updated_at: Some(updated_at.into()),
//         };
// 
//         Ok(response)
//     }
// }
// 
// #[cfg(test)]
// mod tests {
//     use crate::*;
//     use crate::public_key::*;
//
//     use testcontainers::*;
//     use testcontainers::images::postgres::Postgres;
//     use testcontainers::clients::Cli;
// 
//     async fn setup_postgres(container: &Container<'_, Cli, Postgres>) -> PgPool {
//         let connection_string = &format!(
//             "postgres://postgres:postgres@localhost:{}/postgres",
//             container.get_host_port(5432).unwrap(),
//         );
// 
//         let pool = PgPoolOptions::new()
//             // TODO add more config fields
//             .max_connections(5)
//             .connect(&connection_string)
//             .await
//             .unwrap();
// 
//         MIGRATOR.run(&pool).await.unwrap();
// 
//         pool
//     }
// 
//     #[tokio::test]
//     async fn invalid_url() {
//         let docker = clients::Cli::default();
//         let image = images::postgres::Postgres::default().with_version(9);
//         let container = docker.run(image);
//         let pool = setup_postgres(&container).await;
//         let public_key_service = PublicKeyGrpc { db_pool: pool };
//         let request = PublicKeyRequest {
//             public_key: Some(PublicKey {
//                 key: Some(Key::Secp256k1(vec![])),
//             }),
//             url: "invalidurl.com".to_owned(),
//             metadata: None,
//         };
// 
//         match public_key_service.add(Request::new(request)).await {
//             Err(err) => {
//                 assert_eq!(err.code(), tonic::Code::InvalidArgument);
//                 assert_eq!(err.message(), "relative URL without a base".to_owned());
//             },
//             _ => unreachable!(),
//         }
//     }
// 
//     #[tokio::test]
//     async fn missing_public_key() {
//         let docker = clients::Cli::default();
//         let image = images::postgres::Postgres::default().with_version(9);
//         let container = docker.run(image);
//         let pool = setup_postgres(&container).await;
//         let public_key_service = PublicKeyGrpc { db_pool: pool };
//         let request = PublicKeyRequest {
//             public_key: None,
//             url: "http://test.com".to_owned(),
//             metadata: None,
//         };
// 
//         match public_key_service.add(Request::new(request)).await {
//             Err(err) => {
//                 assert_eq!(err.code(), tonic::Code::InvalidArgument);
//                 assert_eq!(err.message(), "must specify public key".to_owned());
//             },
//             _ => unreachable!(),
//         }
//     }
// 
//     #[tokio::test]
//     async fn missing_key() {
//         let docker = clients::Cli::default();
//         let image = images::postgres::Postgres::default().with_version(9);
//         let container = docker.run(image);
//         let pool = setup_postgres(&container).await;
//         let public_key_service = PublicKeyGrpc { db_pool: pool };
//         let request = PublicKeyRequest {
//             public_key: Some(PublicKey { key: None }),
//             url: "http://test.com".to_owned(),
//             metadata: None,
//         };
// 
//         match public_key_service.add(Request::new(request)).await {
//             Err(err) => {
//                 assert_eq!(err.code(), tonic::Code::InvalidArgument);
//                 assert_eq!(err.message(), "must specify key type".to_owned());
//             },
//             _ => unreachable!(),
//         }
//     }
// 
//     #[tokio::test]
//     async fn returns_full_proto() {
//         let docker = clients::Cli::default();
//         let image = images::postgres::Postgres::default().with_version(9);
//         let container = docker.run(image);
//         let pool = setup_postgres(&container).await;
//         let public_key_service = PublicKeyGrpc { db_pool: pool };
//         // TODO add metadata to this request
//         let request = PublicKeyRequest {
//             public_key: Some(PublicKey {
//                 key: Some(Key::Secp256k1(vec![1u8, 2u8, 3u8])),
//             }),
//             url: "http://test.com".to_owned(),
//             metadata: None,
//         };
// 
//         match public_key_service.add(Request::new(request)).await {
//             Ok(result) => {
//                 let result = result.into_inner();
//                 assert!(!result.uuid.is_none());
//                 assert_eq!(result.public_key.unwrap().key.unwrap(), Key::Secp256k1(vec![1u8, 2u8, 3u8]));
//                 assert_eq!(result.url, String::from("http://test.com"));
//                 assert!(result.metadata.is_none());
//                 assert!(!result.created_at.is_none());
//                 assert!(!result.updated_at.is_none());
//             },
//             _ => unreachable!(),
//         }
//     }
// }
