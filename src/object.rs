use crate::{cache::{Cache, PublicKeyState}, config::Config, dime::{Dime, Signature, format_dime_bytes}, storage::{Storage, StoragePath}};
use crate::consts;
use crate::datastore;
use crate::domain::{DimeProperties, ObjectApiResponse};
use crate::types::{GrpcResult, OsError};
use crate::pb::chunk_bidi::Impl::{Chunk as ChunkEnum, MultiStreamHeader as MultiStreamHeaderEnum};
use crate::pb::MultiStreamHeader;
use crate::pb::chunk::Impl::{End, Data, Value};
use crate::pb::{Chunk, ChunkBidi, ChunkEnd, ObjectResponse, HashRequest, StreamHeader};
use crate::pb::object_service_server::ObjectService;

use bytes::{BufMut, Bytes, BytesMut};
use futures_util::StreamExt;
use linked_hash_map::LinkedHashMap;
use minitrace_macro::trace;
use prost::Message;
use tokio::sync::mpsc;
use std::{collections::HashMap, convert::TryInto, sync::{Arc, Mutex}};
use sqlx::postgres::PgPool;
use tonic::{Request, Response, Status, Streaming};

// TODO add flag for whether object-replication can be ignored for a PUT
// TODO move test packet generation to helper functions
// TODO implement mailbox only for local and unknown keys - reaper for unknown to remote like replication?

pub struct ObjectGrpc {
    pub cache: Arc<Mutex<Cache>>,
    pub config: Arc<Config>,
    pub db_pool: Arc<PgPool>,
    pub storage: Arc<Box<dyn Storage>>,
}

impl ObjectGrpc {
    pub fn new(cache: Arc<Mutex<Cache>>, config: Arc<Config>, db_pool: Arc<PgPool>, storage: Arc<Box<dyn Storage>>) -> Self
    {
        Self { cache, config, db_pool, storage }
    }
}

#[tonic::async_trait]
impl ObjectService for ObjectGrpc {
    #[trace("object::put")]
    async fn put(
        &self,
        request: Request<Streaming<ChunkBidi>>,
    ) -> GrpcResult<Response<ObjectResponse>> {
        let metadata = request.metadata().clone();
        let mut stream = request.into_inner();
        let mut chunk_buffer = Vec::new();
        let mut byte_buffer = BytesMut::new();
        let mut end = false;
        let mut properties: LinkedHashMap<String, Vec<u8>> = LinkedHashMap::new();

        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(chunk) => {
                    let mut buffer = BytesMut::with_capacity(chunk.encoded_len());
                    chunk.clone().encode(&mut buffer).unwrap();
                    chunk_buffer.push(chunk);
                },
                Err(e) => {
                    let message = format!("Error received instead of ChunkBidi - {:?}", e);

                    return Err(Status::invalid_argument(message))
                },
            }
        }

        // check validity of stream header
        if chunk_buffer.len() < 1 {
            return Err(Status::invalid_argument("Multipart upload is empty"))
        }
        match chunk_buffer[0].r#impl {
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

        for chunk in &chunk_buffer[2..] {
            match chunk.r#impl {
                Some(ChunkEnum(ref chunk)) => {
                    match chunk.r#impl {
                        Some(Data(ref data)) => {
                            if end {
                                return Err(Status::invalid_argument("Received data chunk after an end of data chunk"))
                            } else {
                                byte_buffer.put(data.as_ref())
                            }
                        },
                        Some(Value(ref value)) => {
                            let key = chunk.header
                                .as_ref()
                                .ok_or(Status::invalid_argument("Must have stream header when impl is value"))?
                                .name
                                .clone();
                            properties.insert(key, value.to_vec());
                        },
                        Some(End(_)) => end = true,
                        None => return Err(Status::invalid_argument("Chunk header must have data and not an empty impl")),
                    }
                },
                _ => return Err(Status::invalid_argument("Non chunk detected in stream")),
            }
        }

        let hash = properties.get(consts::HASH_FIELD_NAME)
            .map(base64::encode)
            .ok_or(Status::invalid_argument("Properties must contain \"HASH\""))?;
        let signature = properties.get(consts::SIGNATURE_FIELD_NAME)
            .map(base64::encode)
            .ok_or(Status::invalid_argument("Properties must contain \"SIGNATURE_FIELD_NAME\""))?;
        let public_key = properties.get(consts::SIGNATURE_PUBLIC_KEY_FIELD_NAME)
            .map(base64::encode)
            .ok_or(Status::invalid_argument("Properties must contain \"SIGNATURE_PUBLIC_KEY_FIELD_NAME\""))?;

        let owner_signature = Signature { signature, public_key };
        let mut byte_buffer: Bytes = byte_buffer.into();
        let byte_buffer = format_dime_bytes(&mut byte_buffer, owner_signature)
            .map_err(Into::<OsError>::into)?;
        // Bytes clones are cheap
        let raw_dime = byte_buffer.clone();
        let dime: Dime = byte_buffer.try_into()
            .map_err(|err| Into::<OsError>::into(err))?;
        let dime_properties = DimeProperties {
            hash,
            content_length: header_chunk_header.content_length,
            dime_length: raw_dime.len() as i64,
        };
        let mut replication_key_states = Vec::new();

        if self.config.user_auth_enabled {
            let owner_public_key = dime.owner_public_key_base64()
                .map_err(|_| Status::invalid_argument("Invalid Dime proto - owner"))?;
            let cache = self.cache.lock().unwrap();

            match cache.get_public_key_state(&owner_public_key) {
                PublicKeyState::Local => {
                    if let Some(cached_key) = cache.public_keys.get(&owner_public_key) {
                        cached_key.auth()?.authorize(&metadata)
                    } else {
                        Err(Status::internal("this should not happen - key was resolved to Local state and then could not be fetched"))
                    }
                },
                PublicKeyState::Remote => Err(Status::permission_denied(format!("remote public key {} - use the replicate route", &owner_public_key))),
                PublicKeyState::Unknown => Err(Status::permission_denied(format!("unknown public key {}", &owner_public_key))),
            }?;
        }

        if self.config.replication_enabled {
            let audience = dime.unique_audience_without_owner_base64()
                .map_err(|_| Status::invalid_argument("Invalid Dime proto - audience list"))?;
            let cache = self.cache.lock().unwrap();

            for ref party in audience {
                replication_key_states.push((party.clone(), cache.get_public_key_state(party)));
            }
        }

        // mail items should be under any reasonable threshold set, but explicitly check for
        // mail and always used database storage
        let response = if !dime.metadata.contains_key(consts::MAILBOX_KEY) &&
            dime_properties.dime_length > self.config.storage_threshold.into()
        {
            let response = datastore::put_object(&self.db_pool, &dime, &dime_properties, &properties, replication_key_states, None, self.config.replication_enabled)
                .await?;
            let storage_path = StoragePath {
                dir: response.directory.clone(),
                file: response.name.clone(),
            };

            self.storage.store(&storage_path, response.dime_length as u64, &raw_dime).await
                .map_err(Into::<OsError>::into)?;
            response.to_response(&self.config)?
        } else {
            datastore::put_object(&self.db_pool, &dime, &dime_properties, &properties, replication_key_states, Some(&raw_dime), self.config.replication_enabled)
                .await?
                .to_response(&self.config)?
        };

        Ok(Response::new(response))
    }

    type GetStream = tokio_stream::wrappers::ReceiverStream<GrpcResult<ChunkBidi>>;

    #[trace("object::get")]
    async fn get(
        &self,
        request: Request<HashRequest>,
    ) -> GrpcResult<Response<Self::GetStream>> {
        let metadata = request.metadata().clone();
        let request = request.into_inner();
        let hash = base64::encode(request.hash);
        let public_key = base64::encode(&request.public_key);

        if self.config.user_auth_enabled {
            let cache = self.cache.lock().unwrap();

            match cache.get_public_key_state(&public_key) {
                PublicKeyState::Local => {
                    if let Some(cached_key) = cache.public_keys.get(&public_key) {
                        cached_key.auth()?.authorize(&metadata)
                    } else {
                        Err(Status::internal("this should not happen - key was resolved to Local state and then could not be fetched"))
                    }
                },
                PublicKeyState::Remote => Err(Status::permission_denied(format!("remote public key {} - fetch on its own instance", &public_key))),
                PublicKeyState::Unknown => Err(Status::permission_denied(format!("unknown public key {}", &public_key))),
            }?;
        }

        let object_uuid = datastore::get_public_key_object_uuid(&self.db_pool, hash.as_str(), &public_key).await?;
        let object = datastore::get_object_by_uuid(&self.db_pool, &object_uuid).await?;
        let payload = if let Some(payload) = &object.payload {
            Bytes::copy_from_slice(payload.as_slice())
        } else {
            let storage_path = StoragePath {
                dir: object.directory.clone(),
                file: object.name.clone(),
            };
            let payload = self.storage.fetch(&storage_path, object.dime_length as u64).await
                .map_err(Into::<OsError>::into)?;

            Bytes::copy_from_slice(payload.as_slice())
        };
        let (tx, rx) = mpsc::channel(4);

        tokio::spawn(async move {
            let _ = &object;
            // send multi stream header
            let mut metadata = HashMap::new();
            metadata.insert(consts::CREATED_BY_HEADER.to_owned(), uuid::Uuid::nil().to_hyphenated().to_string());
            let header = MultiStreamHeader { stream_count: 1, metadata };
            let msg = ChunkBidi { r#impl: Some(MultiStreamHeaderEnum(header)) };
            if let Err(_) = tx.send(Ok(msg)).await {
                log::debug!("stream closed early");
                return;
            }

            // send data chunks
            for (idx, chunk) in payload.chunks(consts::CHUNK_SIZE).enumerate() {
                let header = if idx == 0 {
                    Some(StreamHeader {
                        name: consts::DIME_FIELD_NAME.to_owned(),
                        content_length: object.content_length,
                    })
                } else {
                    None
                };
                let data_chunk = Chunk { header, r#impl: Some(Data(chunk.to_vec())) };
                let msg = ChunkBidi { r#impl: Some(ChunkEnum(data_chunk)) };
                if let Err(_) = tx.send(Ok(msg)).await {
                    log::debug!("stream closed early");
                    return;
                }
            }

            // send end chunk
            let end = Chunk { header: None, r#impl: Some(End(ChunkEnd::default())) };
            let msg = ChunkBidi { r#impl: Some(ChunkEnum(end)) };
            if let Err(_) = tx.send(Ok(msg)).await {
                log::debug!("stream closed early");
                return;
            }
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }
}

#[cfg(test)]
pub mod tests {
    use std::str::FromStr;
    use std::hash::Hasher;

    use crate::MIGRATOR;
    use crate::config::DatadogConfig;
    use crate::consts::*;
    use crate::datastore::{AuthType, KeyType, MailboxPublicKey, ObjectPublicKey, PublicKey, replication_object_uuids};
    use crate::dime::Signature;
    use crate::pb::{self, Audience, Dime as DimeProto, ObjectResponse};
    use crate::object::*;
    use crate::storage::FileSystem;

    use chrono::Utc;
    use futures::stream;
    use futures_util::TryStreamExt;
    use sqlx::FromRow;
    use sqlx::postgres::PgPoolOptions;

    use serial_test::serial;
    use testcontainers::*;
    use testcontainers::images::postgres::Postgres;
    use testcontainers::clients::Cli;

    pub fn test_config() -> Config {
        let dd_config = DatadogConfig {
            agent_host: "127.0.0.1".parse().unwrap(),
            agent_port: 8126,
            service: "object-store".to_owned(),
            span_tags: Vec::default(),
        };
        Config {
            url: "0.0.0.0:6789".parse().unwrap(),
            uri_host: String::default(),
            db_connection_pool_size: 0,
            db_host: String::default(),
            db_port: 0,
            db_user: String::default(),
            db_password: String::default(),
            db_database: String::default(),
            db_schema: String::default(),
            storage_type: "file_system".to_owned(),
            storage_base_url: None,
            storage_base_path: "/tmp".to_owned(),
            storage_threshold: 5000,
            replication_enabled: true,
            replication_batch_size: 2,
            dd_config: Some(dd_config),
            backoff_min_wait: 1,
            backoff_max_wait: 1,
            logging_threshold_seconds: 1f64,
            trace_header: String::default(),
            user_auth_enabled: false,
        }
    }

    pub async fn setup_postgres(container: &Container<'_, Cli, Postgres>) -> PgPool {
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

    async fn start_server(default_config: Option<Config>) -> Arc<PgPool> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);

        tokio::spawn(async move {
            let mut cache = Cache::default();
            cache.add_public_key(PublicKey {
                uuid: uuid::Uuid::default(),
                public_key: std::str::from_utf8(&party_1().0.public_key).unwrap().to_owned(),
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
                public_key: std::str::from_utf8(&party_2().0.public_key).unwrap().to_owned(),
                public_key_type: KeyType::Secp256k1,
                url: String::from("tcp://party2:8080"),
                metadata: Vec::default(),
                auth_type: Some(AuthType::Header),
                auth_data: Some(String::from("x-test-header:test_value_2")),
                created_at: Utc::now(),
                updated_at: Utc::now(),
            });
            let cache = Mutex::new(cache);
            let config = default_config.unwrap_or(test_config());
            let url = config.url.clone();
            let docker = clients::Cli::default();
            let image = images::postgres::Postgres::default().with_version(9);
            let container = docker.run(image);
            let pool = setup_postgres(&container).await;
            let storage = FileSystem::new(config.storage_base_path.as_str());
            let object_service = ObjectGrpc {
                cache: Arc::new(cache),
                config: Arc::new(config),
                db_pool: Arc::new(pool),
                storage: Arc::new(Box::new(storage)),
            };

            tx.send(object_service.db_pool.clone()).await.unwrap();

            tonic::transport::Server::builder()
                .add_service(pb::object_service_server::ObjectServiceServer::new(object_service))
                .serve(url)
                .await
                .unwrap()
        });

        rx.recv().await.unwrap()
    }

    pub fn party_1() -> (Audience, Signature) {
        (
            Audience {
                payload_id: 0,
                public_key: base64::encode("1").into_bytes(),
                context: 0,
                tag: Vec::default(),
                ephemeral_pubkey: Vec::default(),
                encrypted_dek: Vec::default(),
            },
            Signature { public_key: "1".to_owned(), signature: "a".to_owned() },
        )
    }

    pub fn party_2() -> (Audience, Signature) {
        (
            Audience {
                payload_id: 0,
                public_key: base64::encode("2").into_bytes(),
                context: 0,
                tag: Vec::default(),
                ephemeral_pubkey: Vec::default(),
                encrypted_dek: Vec::default(),
            },
            Signature { public_key: "2".to_owned(), signature: "b".to_owned() },
        )
    }

    pub fn party_3() -> (Audience, Signature) {
        (
            Audience {
                payload_id: 0,
                public_key: base64::encode("3").into_bytes(),
                context: 0,
                tag: Vec::default(),
                ephemeral_pubkey: Vec::default(),
                encrypted_dek: Vec::default(),
            },
            Signature { public_key: "3".to_owned(), signature: "c".to_owned() },
        )
    }

    pub fn generate_dime(audience: Vec<Audience>, signatures: Vec<Signature>) -> Dime {
        let proto = DimeProto {
            uuid: None,
            owner: Some(audience.first().unwrap().clone()),
            metadata: HashMap::default(),
            audience,
            payload: Vec::default(),
            audit_fields: None,
        };

        Dime {
            uuid: uuid::Uuid::from_u128(300),
            uri: String::default(),
            proto,
            metadata: std::collections::HashMap::default(),
            signatures,
        }
    }

    pub fn hash(payload: bytes::Bytes) -> Vec<u8> {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        hasher.write(payload.to_vec().as_slice());
        let hash = hasher.finish();

        hash.to_be_bytes().to_vec()
    }

    pub async fn put_helper(dime: Dime, payload: bytes::Bytes, chunk_size: usize, extra_properties: HashMap<String, String>, grpc_metadata: Vec<(&'static str, &'static str)>) -> GrpcResult<Response<ObjectResponse>> {
        let mut packets = Vec::new();

        let mut metadata = HashMap::new();
        metadata.insert(CREATED_BY_HEADER.to_owned(), uuid::Uuid::nil().to_hyphenated().to_string());
        let header = MultiStreamHeader { stream_count: 1, metadata };
        let msg = ChunkBidi { r#impl: Some(MultiStreamHeaderEnum(header)) };

        packets.push(msg);

        let mut buffer = BytesMut::new();
        buffer.put_u32(0x44494D45 as u32);
        buffer.put_u16(1);
        buffer.put_u32(16);
        buffer.put_u128(dime.uuid.as_u128());
        let metadata = serde_json::to_vec(&dime.metadata).unwrap();
        buffer.put_u32(metadata.len().try_into().unwrap());
        buffer.put_slice(metadata.as_slice());
        buffer.put_u32(8);
        buffer.put_slice("fake_uri".as_bytes());
        let signatures = serde_json::to_vec(&dime.signatures).unwrap();
        buffer.put_u32(signatures.len().try_into().unwrap());
        buffer.put_slice(signatures.as_slice());
        let proto_len = dime.proto.encoded_len();
        let mut proto_buffer = BytesMut::with_capacity(proto_len);
        dime.proto.encode(&mut proto_buffer).unwrap();
        buffer.put_u32(proto_len.try_into().unwrap());
        buffer.put_slice(proto_buffer.as_ref());
        buffer.put_slice(payload.to_vec().as_slice());

        for (idx, chunk) in buffer.chunks(chunk_size).enumerate() {
            let header = if idx == 0 {
                Some(StreamHeader {
                    name: DIME_FIELD_NAME.to_owned(),
                    content_length: payload.len() as i64,
                })
            } else {
                None
            };
            let data_chunk = Chunk { header, r#impl: Some(Data(chunk.to_vec())) };
            let msg = ChunkBidi { r#impl: Some(ChunkEnum(data_chunk)) };
            packets.push(msg);
        }

        let header = StreamHeader { name: HASH_FIELD_NAME.to_owned(), content_length: 0 };
        let value_chunk = Chunk { header: Some(header), r#impl: Some(Value(hash(payload))) };
        let msg = ChunkBidi { r#impl: Some(ChunkEnum(value_chunk)) };
        packets.push(msg);
        let header = StreamHeader { name: SIGNATURE_FIELD_NAME.to_owned(), content_length: 0 };
        let value_chunk = Chunk { header: Some(header), r#impl: Some(Value("signature".as_bytes().to_owned())) };
        let msg = ChunkBidi { r#impl: Some(ChunkEnum(value_chunk)) };
        packets.push(msg);
        let header = StreamHeader { name: SIGNATURE_PUBLIC_KEY_FIELD_NAME.to_owned(), content_length: 0 };
        let value_chunk = Chunk { header: Some(header), r#impl: Some(Value("signature public key".as_bytes().to_owned())) };
        let msg = ChunkBidi { r#impl: Some(ChunkEnum(value_chunk)) };
        packets.push(msg);

        for (key, value) in extra_properties {
            let header = StreamHeader { name: key, content_length: 0 };
            let value_chunk = Chunk { header: Some(header), r#impl: Some(Value(value.as_bytes().to_owned())) };
            let msg = ChunkBidi { r#impl: Some(ChunkEnum(value_chunk)) };
            packets.push(msg);
        }

        let end = Chunk { header: None, r#impl: Some(End(ChunkEnd::default())) };
        let msg = ChunkBidi { r#impl: Some(ChunkEnum(end)) };
        packets.push(msg);

        // allow server to start
        tokio::time::sleep(tokio::time::Duration::from_millis(1_000)).await;

        let mut client = pb::object_service_client::ObjectServiceClient::connect("tcp://0.0.0.0:6789").await.unwrap();
        let stream = stream::iter(packets);
        let mut request = Request::new(stream);
        let metadata = request.metadata_mut();
        for (k, v) in grpc_metadata {
            metadata.insert(k, v.parse().unwrap());
        }

        client.put(request).await
    }

    pub async fn get_public_keys_by_object(db: &PgPool, object_uuid: &uuid::Uuid) -> Vec<ObjectPublicKey> {
        let query_str = "SELECT * FROM object_public_key WHERE object_uuid = $1";
        let mut result = Vec::new();
        let mut query_result = sqlx::query(query_str)
            .bind(object_uuid)
            .fetch(db);

        while let Some(row) = query_result.try_next().await.unwrap() {
            result.push(ObjectPublicKey::from_row(&row).unwrap());
        }

        result
    }

    pub async fn delete_properties(db: &PgPool, object_uuid: &uuid::Uuid) -> u64 {
        let query_str = "UPDATE object SET properties = null WHERE uuid = $1";
        let rows_affected = sqlx::query(&query_str)
            .bind(object_uuid)
            .execute(db)
            .await
            .unwrap()
            .rows_affected();

        rows_affected
    }

    pub async fn get_mailbox_keys_by_object(db: &PgPool, object_uuid: &uuid::Uuid) -> Vec<MailboxPublicKey> {
        let query_str = "SELECT * FROM mailbox_public_key WHERE object_uuid = $1";
        let mut result = Vec::new();
        let mut query_result = sqlx::query(query_str)
            .bind(object_uuid)
            .fetch(db);

        while let Some(row) = query_result.try_next().await.unwrap() {
            result.push(MailboxPublicKey::from_row(&row).unwrap());
        }

        result
    }

    // TODO test validation of sent data

    #[tokio::test]
    #[serial(grpc_server)]
    async fn simple_put() {
        let db = start_server(None).await;

        let (audience, signature) = party_1();
        let dime = generate_dime(vec![audience], vec![signature]);
        let dime_uuid = dime.uuid.clone().to_hyphenated().to_string();
        let payload: bytes::Bytes = "testing small payload".as_bytes().into();
        let payload_len = payload.len() as i64;
        let chunk_size = 500; // full payload in one packet
        let response = put_helper(dime, payload, chunk_size, HashMap::default(), Vec::default()).await;

        match response {
            Ok(response) => {
                let response = response.into_inner();
                let uuid = response.uuid.unwrap().value;
                let uuid_typed = uuid::Uuid::from_str(uuid.as_str()).unwrap();
                let object = datastore::get_object_by_uuid(&db, &uuid_typed).await.unwrap();
                let mut properties = LinkedHashMap::new();
                properties.insert(HASH_FIELD_NAME.to_owned(), base64::decode(object.hash).unwrap());
                properties.insert(SIGNATURE_FIELD_NAME.to_owned(), "signature".as_bytes().to_owned());
                properties.insert(SIGNATURE_PUBLIC_KEY_FIELD_NAME.to_owned(), "signature public key".as_bytes().to_owned());

                assert_ne!(uuid, dime_uuid);
                assert_eq!(response.name, NOT_STORAGE_BACKED);
                assert_eq!(response.metadata.unwrap().content_length, payload_len);
                assert_eq!(object.properties, properties)
            },
            _ => assert_eq!(format!("{:?}", response), ""),
        }
    }

    #[tokio::test]
    #[serial(grpc_server)]
    async fn simple_put_with_auth_failure_no_header() {
        let mut config = test_config();
        config.user_auth_enabled = true;
        start_server(Some(config)).await;

        let (audience, signature) = party_1();
        let dime = generate_dime(vec![audience], vec![signature]);
        let payload: bytes::Bytes = "testing small payload".as_bytes().into();
        let chunk_size = 500; // full payload in one packet
        let response = put_helper(dime, payload, chunk_size, HashMap::default(), Vec::default()).await;

        match response {
            Err(err) => assert_eq!(err.code(), tonic::Code::PermissionDenied),
            _ => assert_eq!(format!("{:?}", response), ""),
        }
    }

    #[tokio::test]
    #[serial(grpc_server)]
    async fn simple_put_with_auth_failure_incorrect_value() {
        let mut config = test_config();
        config.user_auth_enabled = true;
        start_server(Some(config)).await;

        let (audience, signature) = party_1();
        let dime = generate_dime(vec![audience], vec![signature]);
        let payload: bytes::Bytes = "testing small payload".as_bytes().into();
        let chunk_size = 500; // full payload in one packet
        let response = put_helper(dime, payload, chunk_size, HashMap::default(), vec![("x-test-header", "test_value_2")]).await;

        match response {
            Err(err) => assert_eq!(err.code(), tonic::Code::PermissionDenied),
            _ => assert_eq!(format!("{:?}", response), ""),
        }
    }

    #[tokio::test]
    #[serial(grpc_server)]
    async fn simple_put_with_auth_success() {
        let mut config = test_config();
        config.user_auth_enabled = true;
        let db = start_server(Some(config)).await;

        let (audience, signature) = party_1();
        let dime = generate_dime(vec![audience], vec![signature]);
        let dime_uuid = dime.uuid.clone().to_hyphenated().to_string();
        let payload: bytes::Bytes = "testing small payload".as_bytes().into();
        let payload_len = payload.len() as i64;
        let chunk_size = 500; // full payload in one packet
        let response = put_helper(dime, payload, chunk_size, HashMap::default(), vec![("x-test-header", "test_value_1")]).await;

        match response {
            Ok(response) => {
                let response = response.into_inner();
                let uuid = response.uuid.unwrap().value;
                let uuid_typed = uuid::Uuid::from_str(uuid.as_str()).unwrap();
                let object = datastore::get_object_by_uuid(&db, &uuid_typed).await.unwrap();
                let mut properties = LinkedHashMap::new();
                properties.insert(HASH_FIELD_NAME.to_owned(), base64::decode(object.hash).unwrap());
                properties.insert(SIGNATURE_FIELD_NAME.to_owned(), "signature".as_bytes().to_owned());
                properties.insert(SIGNATURE_PUBLIC_KEY_FIELD_NAME.to_owned(), "signature public key".as_bytes().to_owned());

                assert_ne!(uuid, dime_uuid);
                assert_eq!(response.name, NOT_STORAGE_BACKED);
                assert_eq!(response.metadata.unwrap().content_length, payload_len);
                assert_eq!(object.properties, properties)
            },
            _ => assert_eq!(format!("{:?}", response), ""),
        }
    }

    #[tokio::test]
    #[serial(grpc_server)]
    async fn multi_packet_file_store_put() {
        start_server(None).await;

        let (audience, signature) = party_1();
        let dime = generate_dime(vec![audience], vec![signature]);
        let dime_uuid = dime.uuid.clone().to_hyphenated().to_string();
        let payload: bytes::Bytes = "testing larger payload ".repeat(250).into_bytes().into();
        let payload_len = payload.len() as i64;
        let chunk_size = 100; // split payload into packets
        let response = put_helper(dime, payload, chunk_size, HashMap::default(), Vec::default()).await;

        match response {
            Ok(response) => {
                let response = response.into_inner();

                assert_ne!(response.uuid.unwrap().value, dime_uuid);
                assert_ne!(response.name, NOT_STORAGE_BACKED);
                assert_eq!(response.metadata.unwrap().content_length, payload_len);
            },
            _ => assert_eq!(format!("{:?}", response), ""),
        }
    }

    #[tokio::test]
    #[serial(grpc_server)]
    async fn multi_party_put() {
        let db = start_server(None).await;

        let (audience1, signature1) = party_1();
        let (audience2, signature2) = party_2();
        let (audience3, signature3) = party_3();
        let dime = generate_dime(vec![audience1, audience2, audience3], vec![signature1, signature2, signature3]);
        let payload: bytes::Bytes = "testing small payload".as_bytes().into();
        let chunk_size = 500; // full payload in one packet
        let response = put_helper(dime, payload, chunk_size, HashMap::default(), Vec::default()).await;

        match response {
            Ok(response) => {
                let response = response.into_inner();
                let uuid = response.uuid.unwrap().value;
                let uuid = uuid::Uuid::from_str(uuid.as_str()).unwrap();

                assert_eq!(get_public_keys_by_object(&db, &uuid).await.len(), 3);
                assert_eq!(get_mailbox_keys_by_object(&db, &uuid).await.len(), 0);
            },
            _ => assert_eq!(format!("{:?}", response), ""),
        }
    }

    #[tokio::test]
    #[serial(grpc_server)]
    async fn small_mailbox_put() {
        let db = start_server(None).await;

        let (audience1, signature1) = party_1();
        let (audience2, signature2) = party_2();
        let (audience3, signature3) = party_3();
        let mut dime = generate_dime(vec![audience1, audience2, audience3], vec![signature1, signature2, signature3]);
        dime.metadata.insert(MAILBOX_KEY.to_owned(), MAILBOX_FRAGMENT_REQUEST.to_owned());
        let payload: bytes::Bytes = "testing small payload".as_bytes().into();
        let chunk_size = 500; // full payload in one packet
        let response = put_helper(dime, payload, chunk_size, HashMap::default(), Vec::default()).await;

        match response {
            Ok(response) => {
                let response = response.into_inner();
                let uuid = response.uuid.unwrap().value;
                let uuid = uuid::Uuid::from_str(uuid.as_str()).unwrap();

                assert_eq!(response.name, NOT_STORAGE_BACKED);
                assert_eq!(get_public_keys_by_object(&db, &uuid).await.len(), 3);
                assert_eq!(get_mailbox_keys_by_object(&db, &uuid).await.len(), 2);
            },
            _ => assert_eq!(format!("{:?}", response), ""),
        }
    }

    #[tokio::test]
    #[serial(grpc_server)]
    async fn large_mailbox_put() {
        let db = start_server(None).await;

        let (audience1, signature1) = party_1();
        let (audience2, signature2) = party_2();
        let (audience3, signature3) = party_3();
        let mut dime = generate_dime(vec![audience1, audience2, audience3], vec![signature1, signature2, signature3]);
        dime.metadata.insert(MAILBOX_KEY.to_owned(), MAILBOX_FRAGMENT_REQUEST.to_owned());
        let payload: bytes::Bytes = "testing larger payload ".repeat(250).into_bytes().into();
        let chunk_size = 100; // split payload into packets
        let response = put_helper(dime, payload, chunk_size, HashMap::default(), Vec::default()).await;

        match response {
            Ok(response) => {
                let response = response.into_inner();
                let uuid = response.uuid.unwrap().value;
                let uuid = uuid::Uuid::from_str(uuid.as_str()).unwrap();

                assert_eq!(response.name, NOT_STORAGE_BACKED);
                assert_eq!(get_public_keys_by_object(&db, &uuid).await.len(), 3);
                assert_eq!(get_mailbox_keys_by_object(&db, &uuid).await.len(), 2);
            },
            _ => assert_eq!(format!("{:?}", response), ""),
        }
    }

    #[tokio::test]
    #[serial(grpc_server)]
    async fn simple_get() {
        start_server(None).await;

        let (audience, signature) = party_1();
        let dime = generate_dime(vec![audience.clone()], vec![signature]);
        let payload: bytes::Bytes = "testing small payload".as_bytes().into();
        let chunk_size = 500; // full payload in one packet
        let response = put_helper(dime, payload.clone(), chunk_size, HashMap::default(), Vec::default()).await;

        match response {
            Ok(response) => {
                let response = response.into_inner();

                assert_eq!(response.name, NOT_STORAGE_BACKED);
            },
            _ => assert_eq!(format!("{:?}", response), ""),
        }

        let mut client = pb::object_service_client::ObjectServiceClient::connect("tcp://0.0.0.0:6789").await.unwrap();
        let public_key = base64::decode(audience.public_key).unwrap();
        let request = HashRequest { hash: hash(payload), public_key };
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
                        },
                        _ => assert_eq!(format!("{:?}", msg), ""),
                    }
                } else {
                    assert_eq!(format!("{:?}", msg), "");
                }

                // data chunk
                let msg = response.message().await.unwrap();
                if let Some(msg) = msg {
                    match msg.clone().r#impl {
                        Some(ChunkEnum(chunk)) => {
                            match chunk.r#impl {
                                Some(Data(_)) => (),
                                _ => assert_eq!(format!("{:?}", msg), ""),
                            }
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
                        Some(ChunkEnum(chunk)) => {
                            match chunk.r#impl {
                                Some(End(_)) => (),
                                _ => assert_eq!(format!("{:?}", msg), ""),
                            }
                        },
                        _ => assert_eq!(format!("{:?}", msg), ""),
                    }
                } else {
                    assert_eq!(format!("{:?}", msg), "");
                }
            },
            _ => assert_eq!(format!("{:?}", response), ""),
        }
    }

    #[tokio::test]
    #[serial(grpc_server)]
    async fn auth_get_failure_no_key() {
        let mut config = test_config();
        config.user_auth_enabled = true;
        start_server(Some(config)).await;

        let (audience, signature) = party_1();
        let dime = generate_dime(vec![audience.clone()], vec![signature]);
        let payload: bytes::Bytes = "testing larger payload ".repeat(250).into_bytes().into();
        let chunk_size = 100; // split payload into packets
        let response = put_helper(dime, payload.clone(), chunk_size, HashMap::default(), vec![("x-test-header", "test_value_1")]).await;

        match response {
            Ok(response) => {
                let response = response.into_inner();

                assert_ne!(response.name, NOT_STORAGE_BACKED);
            },
            _ => assert_eq!(format!("{:?}", response), ""),
        }

        let mut client = pb::object_service_client::ObjectServiceClient::connect("tcp://0.0.0.0:6789").await.unwrap();
        let public_key = base64::decode(audience.public_key).unwrap();
        let request = HashRequest { hash: hash(payload), public_key };
        let response = client.get(Request::new(request)).await;

        match response {
            Err(err) => assert_eq!(err.code(), tonic::Code::PermissionDenied),
            _ => assert_eq!(format!("{:?}", response), ""),
        }
    }

    #[tokio::test]
    #[serial(grpc_server)]
    async fn auth_get_failure_invalid_key() {
        let mut config = test_config();
        config.user_auth_enabled = true;
        start_server(Some(config)).await;

        let (audience, signature) = party_1();
        let dime = generate_dime(vec![audience.clone()], vec![signature]);
        let payload: bytes::Bytes = "testing larger payload ".repeat(250).into_bytes().into();
        let chunk_size = 100; // split payload into packets
        let response = put_helper(dime, payload.clone(), chunk_size, HashMap::default(), vec![("x-test-header", "test_value_1")]).await;

        match response {
            Ok(response) => {
                let response = response.into_inner();

                assert_ne!(response.name, NOT_STORAGE_BACKED);
            },
            _ => assert_eq!(format!("{:?}", response), ""),
        }

        let mut client = pb::object_service_client::ObjectServiceClient::connect("tcp://0.0.0.0:6789").await.unwrap();
        let public_key = base64::decode(audience.public_key).unwrap();
        let mut request = Request::new(HashRequest { hash: hash(payload), public_key });
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
        let mut config = test_config();
        config.user_auth_enabled = true;
        start_server(Some(config)).await;

        let (audience, signature) = party_1();
        let dime = generate_dime(vec![audience.clone()], vec![signature]);
        let payload: bytes::Bytes = "testing larger payload ".repeat(250).into_bytes().into();
        let chunk_size = 100; // split payload into packets
        let response = put_helper(dime, payload.clone(), chunk_size, HashMap::default(), vec![("x-test-header", "test_value_1")]).await;

        match response {
            Ok(response) => {
                let response = response.into_inner();

                assert_ne!(response.name, NOT_STORAGE_BACKED);
            },
            _ => assert_eq!(format!("{:?}", response), ""),
        }

        let mut client = pb::object_service_client::ObjectServiceClient::connect("tcp://0.0.0.0:6789").await.unwrap();
        let public_key = base64::decode(audience.public_key).unwrap();
        let mut request = Request::new(HashRequest { hash: hash(payload), public_key });
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
        start_server(None).await;

        let (audience, signature) = party_1();
        let dime = generate_dime(vec![audience.clone()], vec![signature]);
        let payload: bytes::Bytes = "testing larger payload ".repeat(250).into_bytes().into();
        let chunk_size = 100; // split payload into packets
        let response = put_helper(dime, payload.clone(), chunk_size, HashMap::default(), Vec::default()).await;

        match response {
            Ok(response) => {
                let response = response.into_inner();

                assert_ne!(response.name, NOT_STORAGE_BACKED);
            },
            _ => assert_eq!(format!("{:?}", response), ""),
        }

        let mut client = pb::object_service_client::ObjectServiceClient::connect("tcp://0.0.0.0:6789").await.unwrap();
        let public_key = base64::decode(audience.public_key).unwrap();
        let request = HashRequest { hash: hash(payload), public_key };
        let response = client.get(Request::new(request)).await;

        match response {
            Ok(_) => (),
            _ => assert_eq!(format!("{:?}", response), ""),
        }
    }

    #[tokio::test]
    #[serial(grpc_server)]
    async fn multi_party_non_owner_get() {
        start_server(None).await;

        let (audience1, signature1) = party_1();
        let (audience2, signature2) = party_2();
        let (audience3, signature3) = party_3();
        let dime = generate_dime(vec![audience1, audience2, audience3.clone()], vec![signature1, signature2, signature3]);
        let payload: bytes::Bytes = "testing small payload".as_bytes().into();
        let chunk_size = 500; // full payload in one packet
        let response = put_helper(dime, payload.clone(), chunk_size, HashMap::default(), Vec::default()).await;

        match response {
            Ok(_) => (),
            _ => assert_eq!(format!("{:?}", response), ""),
        }
        let mut client = pb::object_service_client::ObjectServiceClient::connect("tcp://0.0.0.0:6789").await.unwrap();
        let public_key = base64::decode(audience3.public_key).unwrap();
        let request = HashRequest { hash: hash(payload), public_key };
        let response = client.get(Request::new(request)).await;

        match response {
            Ok(_) => (),
            _ => assert_eq!(format!("{:?}", response), ""),
        }
    }

    #[tokio::test]
    #[serial(grpc_server)]
    async fn dupe_objects_noop() {
        start_server(None).await;

        let (audience, signature) = party_1();
        let dime = generate_dime(vec![audience], vec![signature]);
        let payload: bytes::Bytes = "testing small payload".as_bytes().into();
        let chunk_size = 500; // full payload in one packet
        let response = put_helper(dime.clone(), payload.clone(), chunk_size, HashMap::default(), Vec::default()).await;
        let response_dupe = put_helper(dime, payload, chunk_size, HashMap::default(), Vec::default()).await;

        assert!(response.is_ok() && response_dupe.is_ok());

        let response_inner = response.unwrap().into_inner();
        let response_dupe_inner = response_dupe.unwrap().into_inner();

        assert_eq!(response_inner.uuid, response_dupe_inner.uuid);
    }

    #[tokio::test]
    #[serial(grpc_server)]
    async fn dupe_objects_added_audience() {
        start_server(None).await;

        let (audience, signature) = party_1();
        let (audience2, signature2) = party_2();
        let dime = generate_dime(vec![audience.clone()], vec![signature.clone()]);
        let dime2 = generate_dime(vec![audience, audience2], vec![signature, signature2]);
        let payload: bytes::Bytes = "testing small payload".as_bytes().into();
        let chunk_size = 500; // full payload in one packet
        let response = put_helper(dime.clone(), payload.clone(), chunk_size, HashMap::default(), Vec::default()).await;
        let response_dupe = put_helper(dime2, payload, chunk_size, HashMap::default(), Vec::default()).await;

        assert!(response.is_ok() && response_dupe.is_ok());

        let response_inner = response.unwrap().into_inner();
        let response_dupe_inner = response_dupe.unwrap().into_inner();

        assert_ne!(response_inner.uuid, response_dupe_inner.uuid);
    }

    #[tokio::test]
    #[serial(grpc_server)]
    async fn get_with_wrong_key() {
        start_server(None).await;

        let (audience1, signature1) = party_1();
        let (audience2, signature2) = party_2();
        let (audience3, _) = party_3();
        let dime = generate_dime(vec![audience1, audience2], vec![signature1, signature2]);
        let payload: bytes::Bytes = "testing small payload".as_bytes().into();
        let chunk_size = 500; // full payload in one packet
        let response = put_helper(dime, payload.clone(), chunk_size, HashMap::default(), Vec::default()).await;

        match response {
            Ok(_) => (),
            _ => assert_eq!(format!("{:?}", response), ""),
        }
        let mut client = pb::object_service_client::ObjectServiceClient::connect("tcp://0.0.0.0:6789").await.unwrap();
        let public_key = base64::decode(audience3.public_key).unwrap();
        let request = HashRequest { hash: hash(payload), public_key };
        let response = client.get(Request::new(request)).await;

        match response {
            Err(err) => assert_eq!(err.code(), tonic::Code::NotFound),
            _ => assert_eq!(format!("{:?}", response), ""),
        }
    }

    #[tokio::test]
    #[serial(grpc_server)]
    async fn get_nonexistent_hash() {
        start_server(None).await;

        let (audience1, _) = party_1();
        let payload: bytes::Bytes = "testing small payload".as_bytes().into();
        let mut client = pb::object_service_client::ObjectServiceClient::connect("tcp://0.0.0.0:6789").await.unwrap();
        let public_key = base64::decode(audience1.public_key).unwrap();
        let request = HashRequest { hash: hash(payload), public_key };
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
        let db = start_server(None).await;

        let (audience1, signature1) = party_1();
        let (audience2, signature2) = party_2();
        let (audience3, signature3) = party_3();
        let dime = generate_dime(vec![audience1.clone(), audience2.clone(), audience3.clone()], vec![signature1, signature2, signature3]);
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
                assert_eq!(replication_object_uuids(&db, String::from_utf8(audience1.public_key).unwrap().as_str(), 50).await.unwrap().len(), 0);
                assert_eq!(replication_object_uuids(&db, String::from_utf8(audience2.public_key).unwrap().as_str(), 50).await.unwrap().len(), 1);
                assert_eq!(replication_object_uuids(&db, String::from_utf8(audience3.public_key).unwrap().as_str(), 50).await.unwrap().len(), 1);
            },
            _ => assert_eq!(format!("{:?}", response), ""),
        }
    }

    #[tokio::test]
    #[serial(grpc_server)]
    async fn put_with_replication_different_owner() {
        let db = start_server(None).await;

        let (audience1, signature1) = party_1();
        let (audience2, signature2) = party_2();
        let (audience3, signature3) = party_3();
        let dime = generate_dime(vec![audience2.clone(), audience1.clone(), audience3.clone()], vec![signature2, signature1, signature3]);
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
                assert_eq!(replication_object_uuids(&db, String::from_utf8(audience1.public_key).unwrap().as_str(), 50).await.unwrap().len(), 0);
                assert_eq!(replication_object_uuids(&db, String::from_utf8(audience2.public_key).unwrap().as_str(), 50).await.unwrap().len(), 0);
                assert_eq!(replication_object_uuids(&db, String::from_utf8(audience3.public_key).unwrap().as_str(), 50).await.unwrap().len(), 1);
            },
            _ => assert_eq!(format!("{:?}", response), ""),
        }
    }

    #[tokio::test]
    #[serial(grpc_server)]
    async fn put_with_double_replication() {
        let db = start_server(None).await;

        let (audience1, signature1) = party_1();
        let (audience2, signature2) = party_2();
        let (audience3, signature3) = party_3();
        let dime = generate_dime(vec![audience1.clone(), audience2.clone(), audience3.clone()], vec![signature1, signature2, signature3]);
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
                assert_eq!(replication_object_uuids(&db, base64::encode(audience1.public_key).as_str(), 50).await.unwrap().len(), 0);
                assert_eq!(replication_object_uuids(&db, base64::encode(audience2.public_key).as_str(), 50).await.unwrap().len(), 0);
                assert_eq!(replication_object_uuids(&db, base64::encode(audience3.public_key).as_str(), 50).await.unwrap().len(), 0);
            },
            _ => assert_eq!(format!("{:?}", response), ""),
        }
    }

    #[tokio::test]
    #[serial(grpc_server)]
    async fn get_object_no_properties() {
        let db = start_server(None).await;

        let (audience, signature) = party_1();
        let dime = generate_dime(vec![audience.clone()], vec![signature]);
        let payload: bytes::Bytes = "testing small payload".as_bytes().into();
        let chunk_size = 500; // full payload in one packet
        let response = put_helper(dime, payload.clone(), chunk_size, HashMap::default(), Vec::default()).await;

        match response {
            Ok(response) => {
                let response = response.into_inner();
                let uuid = response.uuid.unwrap().value;
                let uuid = uuid::Uuid::from_str(uuid.as_str()).unwrap();

                assert_eq!(response.name, NOT_STORAGE_BACKED);
                assert_eq!(delete_properties(&db, &uuid).await, 1);
            },
            _ => assert_eq!(format!("{:?}", response), ""),
        }

        let mut client = pb::object_service_client::ObjectServiceClient::connect("tcp://0.0.0.0:6789").await.unwrap();
        let public_key = base64::decode(audience.public_key).unwrap();
        let request = HashRequest { hash: hash(payload), public_key };
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
                        },
                        _ => assert_eq!(format!("{:?}", msg), ""),
                    }
                } else {
                    assert_eq!(format!("{:?}", msg), "");
                }

                // data chunk
                let msg = response.message().await.unwrap();
                if let Some(msg) = msg {
                    match msg.clone().r#impl {
                        Some(ChunkEnum(chunk)) => {
                            match chunk.r#impl {
                                Some(Data(_)) => (),
                                _ => assert_eq!(format!("{:?}", msg), ""),
                            }
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
                        Some(ChunkEnum(chunk)) => {
                            match chunk.r#impl {
                                Some(End(_)) => (),
                                _ => assert_eq!(format!("{:?}", msg), ""),
                            }
                        },
                        _ => assert_eq!(format!("{:?}", msg), ""),
                    }
                } else {
                    assert_eq!(format!("{:?}", msg), "");
                }
            },
            _ => assert_eq!(format!("{:?}", response), ""),
        }
    }

    // TODO add test that has storage backed payload but the file wasn't written due to failure
    // verify that fetch returns an accurate error and also a subsequent PUT can write the file
    // TODO add test to verify owner signature is added to dime
}
