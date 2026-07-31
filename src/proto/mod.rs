pub mod util;
pub use util::*;

use crate::consts;
use std::collections::HashMap;

use crate::pb::MultiStreamHeader;
use crate::pb::chunk::Impl::{Data, End, Value};
use crate::pb::chunk_bidi::Impl::{Chunk as ChunkEnum, MultiStreamHeader as MultiStreamHeaderEnum};
use crate::pb::{Chunk, ChunkBidi, ChunkEnd, StreamHeader};

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

pub fn create_data_chunk(content_length: Option<usize>, chunk: Vec<u8>) -> ChunkBidi {
    let header = content_length.map(|len| StreamHeader {
        name: consts::DIME_FIELD_NAME.to_owned(),
        content_length: len as i64,
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

#[cfg(test)]
mod tests {
    use base64::{Engine, prelude::BASE64_STANDARD};

    use crate::{
        domain::{StringUtil, VecUtil},
        pb::{GetRequest, PublicKey},
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
