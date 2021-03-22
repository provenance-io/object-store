use crate::dime::DimeError;
use crate::dime::Result;
use crate::pb::Dime as DimeProto;

use bytes::{Buf, Bytes};
use prost::Message;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

const MAGIC_BYTES: u32 = 0x44494D45;
const VERSION: u16 = 1;

#[derive(Serialize, Deserialize)]
#[derive(Debug, PartialEq)]
pub struct Signature {
    signature: Vec<u8>,
    #[serde(rename(serialize = "publicKey", deserialize = "publicKey"))]
    public_key: Vec<u8>,
}

#[derive(Debug, PartialEq)]
pub struct Dime {
    uuid: Uuid,
    uri: String,
    proto: DimeProto,
    metadata: HashMap<String, String>,
    signatures: Vec<Signature>,
}

impl Dime {
    // TODO convert to TryFrom trait
    fn new(buffer: &mut Bytes) -> Result<Self> {
        let size_err = Err(DimeError::BufferSizeError("Not enough bytes in buffer to parse Dime".to_owned()));

        if buffer.remaining() < 4 { return size_err }
        let magic_bytes = buffer.get_u32();
        if magic_bytes != MAGIC_BYTES {
            return Err(DimeError::InvalidMagicBytesError(format!("Invalid magic bytes of {}", magic_bytes)));
        }

        if buffer.remaining() < 2 { return size_err }
        let version = buffer.get_u16();
        if version != VERSION {
            return Err(DimeError::InvalidVersionError(version));
        }

        if buffer.remaining() < 4 { return size_err }
        let uuid_len = buffer.get_u32();
        if uuid_len != 16_u32 {
            return Err(DimeError::InvalidUuidSizeError(uuid_len));
        }
        if buffer.remaining() < uuid_len as usize { return size_err }
        let uuid: Uuid = Uuid::from_u128(buffer.get_u128());

        if buffer.remaining() < 4 { return size_err }
        let metadata_len = buffer.get_u32();
        if buffer.remaining() < metadata_len as usize { return size_err }
        let mut metadata = vec![0; metadata_len as usize];
        buffer.copy_to_slice(metadata.as_mut_slice());
        let metadata = serde_json::from_slice(metadata.as_slice())
            .map_err(|err| DimeError::SerdeDecodeError(format!("{:?}", err)))?;

        if buffer.remaining() < 4 { return size_err }
        let uri_len = buffer.get_u32();
        if buffer.remaining() < uri_len as usize { return size_err }
        let mut uri = vec![0; uri_len as usize];
        buffer.copy_to_slice(uri.as_mut_slice());
        let uri = String::from_utf8(uri)?;

        if buffer.remaining() < 4 { return size_err }
        let signature_len = buffer.get_u32();
        if buffer.remaining() < signature_len as usize { return size_err }
        let mut signature = vec![0; signature_len as usize];
        buffer.copy_to_slice(signature.as_mut_slice());
        let signatures = serde_json::from_slice(signature.as_slice())
            .map_err(|err| DimeError::SerdeDecodeError(format!("{:?}", err)))?;

        if buffer.remaining() < 4 { return size_err }
        let proto_len = buffer.get_u32();
        if buffer.remaining() < proto_len as usize { return size_err }
        let mut proto_buffer = vec![0; proto_len as usize];
        buffer.copy_to_slice(proto_buffer.as_mut_slice());
        let proto = DimeProto::decode(proto_buffer.as_slice())?;

        if buffer.remaining() != 0 { return Err(DimeError::BufferSizeError("Dime was parsed but the buffer has remaining bytes".to_owned())) }

        Ok(Self { uuid, uri, proto, metadata, signatures })
    }
}

#[cfg(test)]
mod tests {
    use crate::dime::*;
    use crate::dime::dime::*;
    use crate::pb;

    use bytes::{BytesMut, BufMut};
    use std::convert::TryInto;

    #[test]
    fn empty() {
        let buffer = Vec::default();
        let mut buffer = Bytes::copy_from_slice(buffer.as_ref());

        assert_eq!(Dime::new(&mut buffer), Err(DimeError::BufferSizeError("Not enough bytes in buffer to parse Dime".to_owned())));
    }

    #[test]
    fn magic_bytes() {
        let buffer = vec![0, 0, 1, 1];
        let mut buffer = Bytes::copy_from_slice(buffer.as_ref());

        assert_eq!(Dime::new(&mut buffer), Err(DimeError::InvalidMagicBytesError(format!("Invalid magic bytes of {}", 257))));
    }

    #[test]
    fn version() {
        let mut buffer = BytesMut::new();
        buffer.put_u32(MAGIC_BYTES);
        buffer.put_u16(5);

        assert_eq!(Dime::new(&mut buffer.into()), Err(DimeError::InvalidVersionError(5)));
    }

    #[test]
    fn uuid_len() {
        let mut buffer = BytesMut::new();
        buffer.put_u32(MAGIC_BYTES);
        buffer.put_u16(1);
        buffer.put_u32(10);

        assert_eq!(Dime::new(&mut buffer.into()), Err(DimeError::InvalidUuidSizeError(10)));
    }

    #[test]
    fn complete_parse() {
        let mut buffer = BytesMut::new();
        buffer.put_u32(MAGIC_BYTES);
        buffer.put_u16(1);
        buffer.put_u32(16);
        buffer.put_u128(300);
        let mut metadata = HashMap::new();
        metadata.insert("one".to_owned(), "1".to_owned());
        metadata.insert("two".to_owned(), "2".to_owned());
        metadata.insert("three".to_owned(), "3".to_owned());
        let json_vec = serde_json::to_vec(&metadata).unwrap();
        buffer.put_u32(json_vec.len().try_into().unwrap());
        buffer.put_slice(json_vec.as_slice());
        buffer.put_u32(10);
        buffer.put_slice(b"valid uri!");
        let mut signatures = Vec::new();
        signatures.push(Signature {
            signature: vec![1, 1, 2, 2],
            public_key: vec![2, 2, 3, 3],
        });
        signatures.push(Signature {
            signature: vec![3, 3, 4, 4],
            public_key: vec![4, 4, 5, 5],
        });
        let json_vec = serde_json::to_vec(&signatures).unwrap();
        buffer.put_u32(json_vec.len().try_into().unwrap());
        buffer.put_slice(json_vec.as_slice());

        let audience = pb::Audience {
            payload_id: 10,
            public_key: vec![1, 2, 3],
            context: 10,
            tag: vec![4, 5, 6],
            ephemeral_pubkey: vec![7, 8, 9],
            encrypted_dek: vec![10, 11, 12],
        };
        let proto = pb::Dime {
            uuid: Some(pb::Uuid { value: "a uuid?".to_owned() }),
            owner: Some(audience.clone()),
            metadata: std::collections::HashMap::default(),
            audience: vec![audience],
            payload: vec![pb::Payload { id: 1, cipher_text: vec![1, 2, 3, 4, 5, 6, 7, 8] }],
            audit_fields: None,
        };
        let proto_len = proto.encoded_len();
        let mut proto_buffer = BytesMut::with_capacity(proto_len);
        proto.encode(&mut proto_buffer).unwrap();
        buffer.put_u32(proto_len.try_into().unwrap());
        buffer.put_slice(proto_buffer.as_ref());

        let expected = Dime {
            uuid: Uuid::from_u128(300),
            uri: "valid uri!".to_owned(),
            proto,
            metadata,
            signatures,
        };
        assert_eq!(Dime::new(&mut buffer.into()), Ok(expected));
    }
}
