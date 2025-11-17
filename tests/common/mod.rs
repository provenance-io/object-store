#![allow(dead_code)]

pub mod client;
pub mod config;
pub mod containers;
pub mod data;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::{BufMut, BytesMut};
use futures::stream;
use futures_util::TryStreamExt;
use object_store::cache::Cache;
use object_store::config::Config;
use object_store::consts::{
    CREATED_BY_HEADER, DIME_FIELD_NAME, HASH_FIELD_NAME, SIGNATURE_FIELD_NAME,
    SIGNATURE_PUBLIC_KEY_FIELD_NAME,
};
use object_store::datastore::{MailboxPublicKey, ObjectPublicKey};
use object_store::dime::Dime;
use object_store::pb::chunk_bidi::Impl::{
    Chunk as ChunkEnum, MultiStreamHeader as MultiStreamHeaderEnum,
};
use object_store::pb::mailbox_service_server::MailboxServiceServer;
use object_store::pb::object_service_server::ObjectServiceServer;
use object_store::pb::{
    chunk::Impl::{Data, End, Value},
    Chunk, ChunkBidi, ChunkEnd, MultiStreamHeader, StreamHeader,
};
use object_store::replication::ReplicationState;
use object_store::AppContext;
use prost::Message;
use sqlx::{FromRow, PgPool};
use std::hash::Hasher;
use tonic::Request;

use crate::common::data::seed_cache;

pub fn put_helper(
    dime: Dime,
    payload: bytes::Bytes,
    chunk_size: usize,
    extra_properties: HashMap<String, String>,
    grpc_metadata: Vec<(&'static str, &'static str)>,
) -> Request<stream::Iter<std::vec::IntoIter<ChunkBidi>>> {
    let mut packets = Vec::new();

    let metadata = HashMap::from([(
        CREATED_BY_HEADER.to_owned(),
        uuid::Uuid::nil().as_hyphenated().to_string(),
    )]);

    let header = MultiStreamHeader {
        stream_count: 1,
        metadata,
    };
    let msg = ChunkBidi {
        r#impl: Some(MultiStreamHeaderEnum(header)),
    };

    packets.push(msg);

    let mut buffer = BytesMut::new();
    buffer.put_u32(0x44494D45_u32);
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
        let data_chunk = Chunk {
            header,
            r#impl: Some(Data(chunk.to_vec())),
        };
        let msg = ChunkBidi {
            r#impl: Some(ChunkEnum(data_chunk)),
        };
        packets.push(msg);
    }

    let header = StreamHeader {
        name: HASH_FIELD_NAME.to_owned(),
        content_length: 0,
    };
    let value_chunk = Chunk {
        header: Some(header),
        r#impl: Some(Value(hash(payload))),
    };
    let msg = ChunkBidi {
        r#impl: Some(ChunkEnum(value_chunk)),
    };
    packets.push(msg);
    let header = StreamHeader {
        name: SIGNATURE_FIELD_NAME.to_owned(),
        content_length: 0,
    };
    let value_chunk = Chunk {
        header: Some(header),
        r#impl: Some(Value("signature".as_bytes().to_owned())),
    };
    let msg = ChunkBidi {
        r#impl: Some(ChunkEnum(value_chunk)),
    };
    packets.push(msg);
    let header = StreamHeader {
        name: SIGNATURE_PUBLIC_KEY_FIELD_NAME.to_owned(),
        content_length: 0,
    };
    let value_chunk = Chunk {
        header: Some(header),
        r#impl: Some(Value("signature public key".as_bytes().to_owned())),
    };
    let msg = ChunkBidi {
        r#impl: Some(ChunkEnum(value_chunk)),
    };
    packets.push(msg);

    for (key, value) in extra_properties {
        let header = StreamHeader {
            name: key,
            content_length: 0,
        };
        let value_chunk = Chunk {
            header: Some(header),
            r#impl: Some(Value(value.as_bytes().to_owned())),
        };
        let msg = ChunkBidi {
            r#impl: Some(ChunkEnum(value_chunk)),
        };
        packets.push(msg);
    }

    let end = Chunk {
        header: None,
        r#impl: Some(End(ChunkEnd::default())),
    };
    let msg = ChunkBidi {
        r#impl: Some(ChunkEnum(end)),
    };
    packets.push(msg);

    let stream = stream::iter(packets);
    let request = {
        let mut request = Request::new(stream);
        let metadata = request.metadata_mut();
        for (k, v) in grpc_metadata {
            metadata.insert(k, v.parse().unwrap());
        }
        request
    };

    request
}

pub fn hash(payload: bytes::Bytes) -> Vec<u8> {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    hasher.write(payload.to_vec().as_slice());
    let hash = hasher.finish();

    hash.to_be_bytes().to_vec()
}

// TODO move to lib and add test
pub async fn get_public_keys_by_object(
    db: &PgPool,
    object_uuid: &uuid::Uuid,
) -> Vec<ObjectPublicKey> {
    let query_str = "SELECT * FROM object_public_key WHERE object_uuid = $1";
    let mut result = Vec::new();
    let mut query_result = sqlx::query(query_str).bind(object_uuid).fetch(db);

    while let Some(row) = query_result.try_next().await.unwrap() {
        result.push(ObjectPublicKey::from_row(&row).unwrap());
    }

    result
}

// TODO move to lib and add test
pub async fn get_mailbox_keys_by_object(
    db: &PgPool,
    object_uuid: &uuid::Uuid,
) -> Vec<MailboxPublicKey> {
    let query_str = "SELECT * FROM mailbox_public_key WHERE object_uuid = $1";
    let mut result = Vec::new();
    let mut query_result = sqlx::query(query_str).bind(object_uuid).fetch(db);

    while let Some(row) = query_result.try_next().await.unwrap() {
        result.push(MailboxPublicKey::from_row(&row).unwrap());
    }

    result
}
/// Starts a test server
/// 1. Seeds cache if remote config is supplied
/// 2. Create new [AppContext] and run [AppContext::init]
/// 3. Start server
/// 4. Init, but don't run replication
pub async fn start_test_server(
    config: Config,
    remote_config: Option<&Config>,
) -> (
    Arc<PgPool>,
    Arc<Mutex<Cache>>,
    ReplicationState,
    Arc<Config>,
) {
    let listener = tokio::net::TcpListener::bind(config.url).await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    // Update config with bound port to be used later for connecting client, etc.
    let updated_config = Arc::new(Config {
        url: local_addr,
        ..config
    });

    let context = AppContext::new(updated_config.clone()).await.unwrap();
    context.init().await;

    if let Some(remote_config) = remote_config {
        {
            let mut cache = context.cache.lock().unwrap();
            seed_cache(&mut cache, remote_config);
        };
    }

    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(MailboxServiceServer::new(context.mailbox_service))
            .add_service(ObjectServiceServer::new(context.object_service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap()
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    println!("test server running on {:?}", local_addr);

    let replication_state = ReplicationState::new(
        context.cache.clone(),
        context.config.clone(),
        context.db_pool.clone(),
        context.storage.clone(),
    );

    (
        context.db_pool,
        context.cache,
        replication_state,
        context.config,
    )
}
