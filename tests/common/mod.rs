#![allow(dead_code)]

pub mod client;
pub mod db;

use std::collections::HashMap;

use base64::{prelude::BASE64_STANDARD, Engine};
use bytes::{BufMut, BytesMut};
use chrono::Utc;
use futures::stream;
use futures_util::TryStreamExt;
use object_store::consts::{
    CREATED_BY_HEADER, DIME_FIELD_NAME, HASH_FIELD_NAME, SIGNATURE_FIELD_NAME,
    SIGNATURE_PUBLIC_KEY_FIELD_NAME,
};
use object_store::datastore::{AuthType, KeyType, MailboxPublicKey, ObjectPublicKey, PublicKey};
use object_store::pb::chunk_bidi::Impl::{
    Chunk as ChunkEnum, MultiStreamHeader as MultiStreamHeaderEnum,
};
use object_store::pb::{
    chunk::Impl::{Data, End, Value},
    Chunk, ChunkBidi, ChunkEnd, Dime as DimeProto, MultiStreamHeader, StreamHeader,
};
use object_store::{
    config::{Config, DatadogConfig},
    dime::{Dime, Signature},
    pb::Audience,
};
use prost::Message;
use sqlx::{FromRow, PgPool};
use std::hash::Hasher;
use tonic::Request;

pub fn test_config(db_port: u16) -> Config {
    let dd_config = DatadogConfig {
        agent_host: "127.0.0.1".parse().unwrap(),
        agent_port: 8126,
        service: "object-store".to_owned(),
        span_tags: Vec::default(),
    };

    Config {
        url: "0.0.0.0:0".parse().unwrap(),
        uri_host: String::default(),
        db_connection_pool_size: 1,
        db_host: "localhost".to_owned(),
        db_port,
        db_user: "postgres".to_owned(),
        db_password: "postgres".to_owned(),
        db_database: "postgres".to_owned(),
        db_schema: "public".to_owned(),
        storage_type: "file_system".to_owned(),
        storage_base_url: None,
        storage_base_path: std::env::temp_dir().to_string_lossy().to_string(),
        storage_threshold: 5000,
        replication_enabled: true,
        replication_batch_size: 2,
        dd_config: Some(dd_config),
        backoff_min_wait: 1,
        backoff_max_wait: 1,
        logging_threshold_seconds: 1f64,
        trace_header: String::default(),
        user_auth_enabled: false,
        health_service_enabled: false,
    }
}

pub fn party_1() -> (Audience, Signature) {
    (
        Audience {
            payload_id: 0,
            public_key: BASE64_STANDARD.encode("1").into_bytes(),
            context: 0,
            tag: Vec::default(),
            ephemeral_pubkey: Vec::default(),
            encrypted_dek: Vec::default(),
        },
        Signature {
            public_key: "1".to_owned(),
            signature: "a".to_owned(),
        },
    )
}

pub fn party_2() -> (Audience, Signature) {
    (
        Audience {
            payload_id: 0,
            public_key: BASE64_STANDARD.encode("2").into_bytes(),
            context: 0,
            tag: Vec::default(),
            ephemeral_pubkey: Vec::default(),
            encrypted_dek: Vec::default(),
        },
        Signature {
            public_key: "2".to_owned(),
            signature: "b".to_owned(),
        },
    )
}

pub fn party_3() -> (Audience, Signature) {
    (
        Audience {
            payload_id: 0,
            public_key: BASE64_STANDARD.encode("3").into_bytes(),
            context: 0,
            tag: Vec::default(),
            ephemeral_pubkey: Vec::default(),
            encrypted_dek: Vec::default(),
        },
        Signature {
            public_key: "3".to_owned(),
            signature: "c".to_owned(),
        },
    )
}

pub fn generate_dime(
    audience: Vec<Audience>,
    signatures: Vec<Signature>,
) -> object_store::dime::Dime {
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

pub fn put_helper(
    dime: Dime,
    payload: bytes::Bytes,
    chunk_size: usize,
    extra_properties: HashMap<String, String>,
    grpc_metadata: Vec<(&'static str, &'static str)>,
) -> Request<stream::Iter<std::vec::IntoIter<ChunkBidi>>> {
    let mut packets = Vec::new();

    let mut metadata = HashMap::new();
    metadata.insert(
        CREATED_BY_HEADER.to_owned(),
        uuid::Uuid::nil().as_hyphenated().to_string(),
    );
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

pub fn test_public_key(public_key: Vec<u8>) -> PublicKey {
    let now = Utc::now();

    PublicKey {
        uuid: uuid::Uuid::new_v4(),
        public_key: std::str::from_utf8(&public_key).unwrap().to_owned(),
        public_key_type: KeyType::Secp256k1,
        url: String::from(""),
        metadata: Vec::default(),
        auth_type: Some(AuthType::Header),
        auth_data: Some(String::from("x-test-header:test_value_1")),
        created_at: now,
        updated_at: now,
    }
}
