use crate::consts;
use crate::datastore;
use crate::datastore::get_object_by_uuid;
use crate::datastore::get_public_key_object_uuid;
use crate::domain::{DimeProperties, ObjectApiResponse};
use crate::pb::MultiStreamHeader;
use crate::pb::chunk::Impl::{Data, End, Value};
use crate::pb::chunk_bidi::Impl::{Chunk as ChunkEnum, MultiStreamHeader as MultiStreamHeaderEnum};
use crate::pb::object_service_server::ObjectService;
use crate::pb::{Chunk, ChunkBidi, HashRequest, ObjectResponse, StreamHeader};
use crate::proto_helpers::VecUtil;
use crate::proto_helpers::create_stream_end;
use crate::types::{GrpcResult, OsError};
use crate::{
    cache::{Cache, PublicKeyState},
    config::Config,
    dime::{Dime, Signature, format_dime_bytes},
    storage::{Storage, StoragePath},
};

use bytes::{BufMut, Bytes, BytesMut};
use fastrace_macro::trace;
use futures_util::StreamExt;
use linked_hash_map::LinkedHashMap;
use prost::Message;
use sqlx::postgres::PgPool;
use std::{
    collections::HashMap,
    convert::TryInto,
    sync::{Arc, Mutex},
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

// TODO add flag for whether object-replication can be ignored for a PUT
// TODO move test packet generation to helper functions
// TODO implement mailbox only for local and unknown keys - reaper for unknown to remote like replication?
#[derive(Debug)]
pub struct ObjectGrpc {
    cache: Arc<Mutex<Cache>>,
    config: Arc<Config>,
    db_pool: Arc<PgPool>,
    storage: Arc<Box<dyn Storage>>,
}

impl ObjectGrpc {
    pub fn new(
        cache: Arc<Mutex<Cache>>,
        config: Arc<Config>,
        db_pool: Arc<PgPool>,
        storage: Arc<Box<dyn Storage>>,
    ) -> Self {
        Self {
            cache,
            config,
            db_pool,
            storage,
        }
    }
}

#[tonic::async_trait]
impl ObjectService for ObjectGrpc {
    #[trace(name = "object::put")]
    async fn put(
        &self,
        request: Request<Streaming<ChunkBidi>>,
    ) -> GrpcResult<Response<ObjectResponse>> {
        let metadata = request.metadata().clone();
        let mut stream = request.into_inner();

        // Read stream into buffer
        let chunk_buffer = {
            let mut chunk_buffer = Vec::new();

            while let Some(chunk) = stream.next().await {
                match chunk {
                    Ok(chunk) => {
                        let mut buffer = BytesMut::with_capacity(chunk.encoded_len());
                        chunk.clone().encode(&mut buffer).unwrap();
                        chunk_buffer.push(chunk);
                    }
                    Err(e) => {
                        let message = format!("Error received instead of ChunkBidi - {:?}", e);

                        return Err(Status::invalid_argument(message));
                    }
                }
            }

            chunk_buffer
        };

        // check validity of stream header
        if chunk_buffer.is_empty() {
            return Err(Status::invalid_argument("Multipart upload is empty"));
        }

        match chunk_buffer[0].r#impl {
            Some(MultiStreamHeaderEnum(ref header)) => {
                if header.stream_count != 1 {
                    return Err(Status::invalid_argument(
                        "Multipart upload must contain a single stream",
                    ));
                } else {
                    header
                }
            }
            _ => {
                return Err(Status::invalid_argument(
                    "Multipart upload must start with a multi stream header",
                ));
            }
        };

        // check validity of chunk header
        if chunk_buffer.len() < 2 {
            return Err(Status::invalid_argument(
                "Multi stream has a header but no chunks",
            ));
        }

        let (header_chunk_header, header_chunk_data) = match chunk_buffer[1].r#impl {
            Some(ChunkEnum(ref chunk)) => {
                let chunk_header = match chunk.header {
                    Some(ref header) => header,
                    None => {
                        return Err(Status::invalid_argument(
                            "Multi stream must start with a header",
                        ));
                    }
                };

                match chunk.r#impl {
                    Some(Data(ref data)) => (chunk_header, data),
                    Some(Value(_)) => {
                        return Err(Status::invalid_argument(
                            "Chunk header must have data and not a value",
                        ));
                    }
                    Some(End(_)) => {
                        return Err(Status::invalid_argument(
                            "Chunk header must have data and not an end of chunk",
                        ));
                    }
                    None => {
                        return Err(Status::invalid_argument(
                            "Chunk header must have data and not an empty impl",
                        ));
                    }
                }
            }
            _ => return Err(Status::invalid_argument("Chunk has no impl value")),
        };

        let mut byte_buffer = BytesMut::new();
        byte_buffer.put(header_chunk_data.as_ref());

        let mut end = false;
        let mut properties: LinkedHashMap<String, Vec<u8>> = LinkedHashMap::new();
        for chunk in &chunk_buffer[2..] {
            match chunk.r#impl {
                Some(ChunkEnum(ref chunk)) => match chunk.r#impl {
                    Some(Data(ref data)) => {
                        if end {
                            return Err(Status::invalid_argument(
                                "Received data chunk after an end of data chunk",
                            ));
                        } else {
                            byte_buffer.put(data.as_ref())
                        }
                    }
                    Some(Value(ref value)) => {
                        let key = chunk
                            .header
                            .as_ref()
                            .ok_or(Status::invalid_argument(
                                "Must have stream header when impl is value",
                            ))?
                            .name
                            .clone();
                        properties.insert(key, value.to_vec());
                    }
                    Some(End(_)) => end = true,
                    None => {
                        return Err(Status::invalid_argument(
                            "Chunk header must have data and not an empty impl",
                        ));
                    }
                },
                _ => return Err(Status::invalid_argument("Non chunk detected in stream")),
            }
        }

        let hash = properties
            .get(consts::HASH_FIELD_NAME)
            .map(|f| f.encoded())
            .ok_or(Status::invalid_argument("Properties must contain \"HASH\""))?;
        let signature = properties
            .get(consts::SIGNATURE_FIELD_NAME)
            .map(|f| f.encoded())
            .ok_or(Status::invalid_argument(
                "Properties must contain \"SIGNATURE_FIELD_NAME\"",
            ))?;
        let public_key = properties
            .get(consts::SIGNATURE_PUBLIC_KEY_FIELD_NAME)
            .map(|f| f.encoded())
            .ok_or(Status::invalid_argument(
                "Properties must contain \"SIGNATURE_PUBLIC_KEY_FIELD_NAME\"",
            ))?;

        let owner_signature = Signature {
            signature,
            public_key,
        };
        let mut byte_buffer: Bytes = byte_buffer.into();
        let byte_buffer =
            format_dime_bytes(&mut byte_buffer, owner_signature).map_err(Into::<OsError>::into)?;
        // Bytes clones are cheap
        let raw_dime = byte_buffer.clone();
        let dime: Dime = byte_buffer.try_into().map_err(Into::<OsError>::into)?;
        let dime_properties = DimeProperties {
            hash,
            content_length: header_chunk_header.content_length,
            dime_length: raw_dime.len() as i64,
        };

        if self.config.user_auth_enabled {
            let owner_public_key = dime
                .owner_public_key_base64()
                .map_err(|_| Status::invalid_argument("Invalid Dime proto - owner"))?;
            let cache = self.cache.lock().unwrap();

            match cache.get_public_key_state(&owner_public_key) {
                PublicKeyState::Local => {
                    if let Some(cached_key) = cache.public_keys.get(&owner_public_key) {
                        cached_key.auth()?.authorize(&metadata)
                    } else {
                        Err(Status::internal(
                            "this should not happen - key was resolved to Local state and then could not be fetched",
                        ))
                    }
                }
                PublicKeyState::Remote => Err(Status::permission_denied(format!(
                    "remote public key {} - use the replicate route",
                    &owner_public_key
                ))),
                PublicKeyState::Unknown => Err(Status::permission_denied(format!(
                    "unknown public key {}",
                    &owner_public_key
                ))),
            }?;
        }

        let replication_key_states = {
            let mut replication_key_states = Vec::new();

            if self.config.replication_config.replication_enabled {
                let audience = dime
                    .unique_audience_without_owner_base64()
                    .map_err(|_| Status::invalid_argument("Invalid Dime proto - audience list"))?;
                let cache = self.cache.lock().unwrap();

                for ref party in audience {
                    replication_key_states.push((party.clone(), cache.get_public_key_state(party)));
                }
            };

            replication_key_states
        };

        // mail items should be under any reasonable threshold set, but explicitly check for
        // mail and always used database storage
        let is_mail = dime.metadata.contains_key(consts::MAILBOX_KEY);
        let above_storage_threshold =
            dime_properties.dime_length > self.config.storage_threshold.into();

        let response = if !is_mail && above_storage_threshold {
            let response = datastore::put_object(
                &self.db_pool,
                &dime,
                &dime_properties,
                &properties,
                replication_key_states,
                None,
                self.config.replication_config.replication_enabled,
            )
            .await?;

            let storage_path = StoragePath {
                dir: response.directory.clone(),
                file: response.name.clone(),
            };

            self.storage
                .store(&storage_path, response.dime_length as u64, &raw_dime)
                .await
                .map_err(Into::<OsError>::into)?;
            response.to_response(&self.config)?
        } else {
            datastore::put_object(
                &self.db_pool,
                &dime,
                &dime_properties,
                &properties,
                replication_key_states,
                Some(&raw_dime),
                self.config.replication_config.replication_enabled,
            )
            .await?
            .to_response(&self.config)?
        };

        Ok(Response::new(response))
    }

    type GetStream = ReceiverStream<GrpcResult<ChunkBidi>>;

    #[trace(name = "object::get")]
    async fn get(&self, request: Request<HashRequest>) -> GrpcResult<Response<Self::GetStream>> {
        let metadata = request.metadata().clone();
        let request = request.into_inner();

        let hash = request.hash.encoded();
        let public_key = request.public_key.encoded();

        if self.config.user_auth_enabled {
            let cache = self.cache.lock().unwrap();

            match cache.get_public_key_state(&public_key) {
                PublicKeyState::Local => {
                    if let Some(cached_key) = cache.public_keys.get(&public_key) {
                        cached_key.auth()?.authorize(&metadata)
                    } else {
                        Err(Status::internal(
                            "this should not happen - key was resolved to Local state and then could not be fetched",
                        ))
                    }
                }
                PublicKeyState::Remote => Err(Status::permission_denied(format!(
                    "remote public key {} - fetch on its own instance",
                    &public_key
                ))),
                PublicKeyState::Unknown => Err(Status::permission_denied(format!(
                    "unknown public key {}",
                    &public_key
                ))),
            }?;
        }

        let object = {
            let object_uuid =
                get_public_key_object_uuid(&self.db_pool, hash.as_str(), &public_key).await?;
            get_object_by_uuid(&self.db_pool, &object_uuid).await?
        };

        let payload = if let Some(payload) = &object.payload {
            Bytes::copy_from_slice(payload.as_slice())
        } else {
            let storage_path = StoragePath {
                dir: object.directory.clone(),
                file: object.name.clone(),
            };

            let payload = self
                .storage
                .fetch(&storage_path, object.dime_length as u64)
                .await
                .map_err(Into::<OsError>::into)?;

            Bytes::copy_from_slice(payload.as_slice())
        };

        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            let _ = &object;
            // send multi stream header
            let metadata = HashMap::from([(
                consts::CREATED_BY_HEADER.to_owned(),
                uuid::Uuid::nil().as_hyphenated().to_string(),
            )]);

            let header = MultiStreamHeader {
                stream_count: 1,
                metadata,
            };

            let msg = ChunkBidi {
                r#impl: Some(MultiStreamHeaderEnum(header)),
            };

            if (tx.send(Ok(msg)).await).is_err() {
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
                let data_chunk = Chunk {
                    header,
                    r#impl: Some(Data(chunk.to_vec())),
                };
                let msg = ChunkBidi {
                    r#impl: Some(ChunkEnum(data_chunk)),
                };

                if (tx.send(Ok(msg)).await).is_err() {
                    log::debug!("stream closed early");
                    return;
                }
            }

            let msg = create_stream_end();

            if (tx.send(Ok(msg)).await).is_err() {
                log::debug!("stream closed early");
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
