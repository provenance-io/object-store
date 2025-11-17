use base64::prelude::BASE64_STANDARD;
use base64::{DecodeError, Engine};

use crate::consts;
use core::result::Result;
use std::collections::HashMap;
use std::str::FromStr;

use crate::pb::chunk::Impl::{Data, End, Value};
use crate::pb::chunk_bidi::Impl::{Chunk as ChunkEnum, MultiStreamHeader as MultiStreamHeaderEnum};
use crate::pb::{public_key::Key, MultiStreamHeader, PublicKey};
use crate::pb::{Audience, Chunk, ChunkBidi, ChunkEnd, ObjectResponse, StreamHeader};

pub fn create_multi_stream_header(
    uuid: uuid::Uuid,
    stream_count: i32,
    is_replication: bool,
) -> ChunkBidi {
    let mut metadata = HashMap::from([(
        consts::CREATED_BY_HEADER.to_owned(),
        uuid.as_hyphenated().to_string(),
    )]);

    if is_replication {
        metadata.insert(
            consts::SOURCE_KEY.to_owned(),
            consts::SOURCE_REPLICATION.to_owned(),
        );
    }

    let header = MultiStreamHeader {
        stream_count,
        metadata,
    };

    ChunkBidi {
        r#impl: Some(MultiStreamHeaderEnum(header)),
    }
}

pub fn create_stream_header_field(key: String, value: Vec<u8>) -> ChunkBidi {
    let header = StreamHeader {
        name: key,
        content_length: 0,
    };

    let value_chunk = Chunk {
        header: Some(header),
        r#impl: Some(Value(value)),
    };

    ChunkBidi {
        r#impl: Some(ChunkEnum(value_chunk)),
    }
}

pub fn create_data_chunk(content_length: Option<i64>, chunk: Vec<u8>) -> ChunkBidi {
    let header = content_length.map(|len| StreamHeader {
        name: consts::DIME_FIELD_NAME.to_owned(),
        content_length: len,
    });

    let data_chunk = Chunk {
        header,
        r#impl: Some(Data(chunk)),
    };

    ChunkBidi {
        r#impl: Some(ChunkEnum(data_chunk)),
    }
}

pub fn create_stream_end() -> ChunkBidi {
    let end = Chunk {
        header: None,
        r#impl: Some(End(ChunkEnd::default())),
    };

    ChunkBidi {
        r#impl: Some(ChunkEnum(end)),
    }
}

pub trait AudienceUtil {
    fn public_key(&self) -> String;
    fn public_key_decoded(&self) -> Vec<u8>;
}
impl AudienceUtil for Audience {
    fn public_key(&self) -> String {
        String::from_utf8(self.public_key.clone()).unwrap()
    }
    fn public_key_decoded(&self) -> Vec<u8> {
        BASE64_STANDARD.decode(&self.public_key).unwrap()
    }
}

pub trait ObjectResponseUtil {
    fn uuid(&self) -> uuid::Uuid;
}
impl ObjectResponseUtil for ObjectResponse {
    fn uuid(&self) -> uuid::Uuid {
        self.uuid
            .as_ref()
            .map(|uuid| uuid::Uuid::from_str(uuid.value.as_str()).unwrap())
            .unwrap()
    }
}

// TODO: move where? (and rename?)
pub trait VecUtil {
    fn encoded(&self) -> String;
}

impl VecUtil for Vec<u8> {
    fn encoded(&self) -> String {
        BASE64_STANDARD.encode(self)
    }
}

pub trait StringUtil {
    fn decoded(&self) -> Result<Vec<u8>, DecodeError>;
}

impl StringUtil for String {
    fn decoded(&self) -> Result<Vec<u8>, DecodeError> {
        BASE64_STANDARD.decode(self)
    }
}

impl From<Vec<u8>> for Key {
    fn from(bytes: Vec<u8>) -> Self {
        Self::Secp256k1(bytes)
    }
}

impl From<Key> for PublicKey {
    fn from(key: Key) -> Self {
        Self { key: Some(key) }
    }
}

impl From<Vec<u8>> for PublicKey {
    fn from(bytes: Vec<u8>) -> Self {
        Self {
            key: Some(bytes.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use base64::{prelude::BASE64_STANDARD, Engine};

    use crate::{
        pb::{GetRequest, PublicKey},
        proto_helpers::{StringUtil, VecUtil},
    };

    #[test]
    fn encoded() {
        let request = GetRequest {
            public_key: vec![1u8, 2u8, 3u8],
            max_results: 0,
        };

        assert_eq!(
            BASE64_STANDARD.encode(&request.public_key),
            request.public_key.encoded(),
        );
    }

    #[test]
    fn decoded() {
        let x = "AQID".to_string();

        assert!(x.decoded().is_ok());
        assert_eq!(BASE64_STANDARD.decode(&x), x.decoded());
    }

    #[test]
    fn there_and_back_again() {
        let v = vec![1u8, 2u8, 3u8];

        assert_eq!(v, v.encoded().decoded().unwrap());
    }

    #[test]
    fn proto_default() {
        let default_key = PublicKey::default();
        let none_key = PublicKey { key: None };

        assert_eq!(default_key, none_key);
    }
}
