use base64::prelude::BASE64_STANDARD;
use base64::Engine;

use crate::{consts, pb};
use std::collections::HashMap;

use crate::pb::chunk::Impl::{Data, End, Value};
use crate::pb::chunk_bidi::Impl::{Chunk as ChunkEnum, MultiStreamHeader as MultiStreamHeaderEnum};
use crate::pb::MultiStreamHeader;
use crate::pb::{Chunk, ChunkBidi, ChunkEnd, StreamHeader};

pub fn create_multi_stream_header(
    uuid: uuid::Uuid,
    stream_count: i32,
    is_replication: bool,
) -> ChunkBidi {
    let mut metadata = HashMap::new();
    metadata.insert(
        consts::CREATED_BY_HEADER.to_owned(),
        uuid.as_hyphenated().to_string(),
    );
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
    let header = if let Some(content_length) = content_length {
        Some(StreamHeader {
            name: consts::DIME_FIELD_NAME.to_owned(),
            content_length,
        })
    } else {
        None
    };
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

impl pb::Audience {
    #[allow(dead_code)]
    pub fn public_key_decoded(&self) -> Vec<u8> {
        BASE64_STANDARD.decode(&self.public_key).unwrap()
    }
}
