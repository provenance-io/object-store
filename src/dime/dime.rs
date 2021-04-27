use crate::dime::{DimeInputError, DimeOutputError};
use crate::pb::Dime as DimeProto;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use prost::Message;
use serde::{Deserialize, Serialize};
use std::{collections::{HashMap, HashSet}, convert::TryFrom};
use uuid::Uuid;

const MAGIC_BYTES: u32 = 0x44494D45;
const VERSION: u16 = 1;

#[derive(Debug, PartialEq, Deserialize, Serialize)]
#[cfg_attr(test, derive(Clone))]
pub struct Signature {
    pub signature: String,
    #[serde(rename(serialize = "publicKey", deserialize = "publicKey"))]
    pub public_key: String,
}

#[derive(Debug, PartialEq)]
#[cfg_attr(test, derive(Clone))]
pub struct Dime {
    pub uuid: Uuid,
    pub uri: String,
    pub proto: DimeProto,
    pub metadata: HashMap<String, String>,
    pub signatures: Vec<Signature>,
}

impl Dime {

    // TODO add FK in object_public_key to object
    // TODO node that input is already base64 encoded
    pub fn unique_audience_base64(&self) -> Vec<String> {
        let mut res: HashSet<String> = HashSet::new();
        for party in &self.proto.audience {
            // TODO fix unwrap
            res.insert(std::str::from_utf8(&party.public_key).unwrap().to_owned());
        }

        return res.drain().collect()
    }

    pub fn owner_public_key_base64(&self) -> String {
        // TODO fix and unwrap
        let owner_test = &self.proto.owner.clone().unwrap().public_key;

        return std::str::from_utf8(&owner_test).unwrap().to_owned()
    }
}

pub fn format_dime_bytes(input: &mut Bytes, owner_signature: Signature) -> Result<Bytes, DimeOutputError> {
    let signatures = vec![owner_signature];
    let signature_bytes = serde_json::to_vec(&signatures)
        .map_err(|err| DimeOutputError::SerdeEncodeError(format!("{:?}", err)))?;

    let mut buffer = BytesMut::with_capacity(input.remaining() + 4 + signature_bytes.len());

    // header
    buffer.put_u32(input.get_u32());
    buffer.put_u16(input.get_u16());

    // uuid
    let length = input.get_u32();
    buffer.put_u32(length);
    let mut uuid = vec![0; length as usize];
    input.copy_to_slice(uuid.as_mut_slice());
    buffer.put_slice(uuid.as_slice());

    // metadata
    let length = input.get_u32();
    buffer.put_u32(length);
    let mut metadata = vec![0; length as usize];
    input.copy_to_slice(metadata.as_mut_slice());
    buffer.put_slice(metadata.as_slice());

    // uri
    let length = input.get_u32();
    buffer.put_u32(length);
    let mut uri = vec![0; length as usize];
    input.copy_to_slice(uri.as_mut_slice());
    buffer.put_slice(uri.as_slice());

    // signatures
    let length = input.get_u32();
    input.advance(length as usize);
    buffer.put_u32(signature_bytes.len() as u32);
    buffer.put_slice(&signature_bytes);

    // dime proto
    let length = input.get_u32();
    buffer.put_u32(length);
    let mut proto = vec![0; length as usize];
    input.copy_to_slice(proto.as_mut_slice());
    buffer.put_slice(proto.as_slice());

    // remaining
    let length = input.remaining();
    let mut remaining = vec![0; length as usize];
    input.copy_to_slice(remaining.as_mut_slice());
    buffer.put_slice(remaining.as_slice());

    Ok(buffer.freeze())
}

impl TryFrom<Bytes> for Dime {
    type Error = DimeInputError;

    fn try_from(buffer: Bytes) -> Result<Self, DimeInputError> {
        let mut buffer = buffer;
        let size_err = Err(DimeInputError::BufferSizeError("Not enough bytes in buffer to parse Dime".to_owned()));

        if buffer.remaining() < 4 { return size_err }
        let magic_bytes = buffer.get_u32();
        if magic_bytes != MAGIC_BYTES {
            return Err(DimeInputError::InvalidMagicBytesError(format!("Invalid magic bytes of {}", magic_bytes)));
        }

        if buffer.remaining() < 2 { return size_err }
        let version = buffer.get_u16();
        if version != VERSION {
            return Err(DimeInputError::InvalidVersionError(version));
        }

        if buffer.remaining() < 4 { return size_err }
        let uuid_len = buffer.get_u32();
        if uuid_len != 16_u32 {
            return Err(DimeInputError::InvalidUuidSizeError(uuid_len));
        }
        if buffer.remaining() < uuid_len as usize { return size_err }
        let uuid: Uuid = Uuid::from_u128(buffer.get_u128());

        if buffer.remaining() < 4 { return size_err }
        let metadata_len = buffer.get_u32();
        if buffer.remaining() < metadata_len as usize { return size_err }
        let mut metadata = vec![0; metadata_len as usize];
        buffer.copy_to_slice(metadata.as_mut_slice());
        let metadata = serde_json::from_slice(metadata.as_slice())
            .map_err(|err| DimeInputError::SerdeDecodeError(format!("{:?}", err)))?;

        if buffer.remaining() < 4 { return size_err }
        let uri_len = buffer.get_u32();
        if buffer.remaining() < uri_len as usize { return size_err }
        let mut uri = vec![0; uri_len as usize];
        buffer.copy_to_slice(uri.as_mut_slice());
        let uri = String::from_utf8(uri)?;

        if buffer.remaining() < 4 { return size_err }
        let signature_len = buffer.get_u32();
        if buffer.remaining() < signature_len as usize { return size_err }
        let signatures = if signature_len > 0 {
            let mut signature = vec![0; signature_len as usize];
            buffer.copy_to_slice(signature.as_mut_slice());
            serde_json::from_slice(signature.as_slice())
                .map_err(|err| DimeInputError::SerdeDecodeError(format!("{:?}", err)))?
        } else {
            Vec::new()
        };

        if buffer.remaining() < 4 { return size_err }
        let proto_len = buffer.get_u32();
        if buffer.remaining() < proto_len as usize { return size_err }
        let mut proto_buffer = vec![0; proto_len as usize];
        buffer.copy_to_slice(proto_buffer.as_mut_slice());
        let proto = DimeProto::decode(proto_buffer.as_slice())?;

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
        let buffer = Bytes::copy_from_slice(buffer.as_ref());
        let actual: Result<Dime, DimeInputError> = buffer.try_into();

        assert_eq!(actual, Err(DimeInputError::BufferSizeError("Not enough bytes in buffer to parse Dime".to_owned())));
    }

    #[test]
    fn magic_bytes() {
        let buffer = vec![0, 0, 1, 1];
        let buffer = Bytes::copy_from_slice(buffer.as_ref());
        let actual: Result<Dime, DimeInputError> = buffer.try_into();

        assert_eq!(actual, Err(DimeInputError::InvalidMagicBytesError(format!("Invalid magic bytes of {}", 257))));
    }

    #[test]
    fn version() {
        let mut buffer = BytesMut::new();
        buffer.put_u32(MAGIC_BYTES);
        buffer.put_u16(5);
        let buffer: Bytes = buffer.into();
        let actual: Result<Dime, DimeInputError> = buffer.try_into();

        assert_eq!(actual, Err(DimeInputError::InvalidVersionError(5)));
    }

    #[test]
    fn uuid_len() {
        let mut buffer = BytesMut::new();
        buffer.put_u32(MAGIC_BYTES);
        buffer.put_u16(1);
        buffer.put_u32(10);
        let buffer: Bytes = buffer.into();
        let actual: Result<Dime, DimeInputError> = buffer.try_into();

        assert_eq!(actual, Err(DimeInputError::InvalidUuidSizeError(10)));
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
        let json = serde_json::to_vec(&metadata).unwrap();
        buffer.put_u32(json.len().try_into().unwrap());
        buffer.put_slice(json.as_slice());
        buffer.put_u32(10);
        buffer.put_slice(b"valid uri!");
        buffer.put_u32(0);
        // no signature slice needed since length is 0
        // buffer.put_slice();

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
        let buffer: Bytes = buffer.into();
        let actual: Result<Dime, DimeInputError> = buffer.try_into();

        let expected = Dime {
            uuid: Uuid::from_u128(300),
            uri: "valid uri!".to_owned(),
            proto,
            metadata,
            signatures: Vec::default(),
        };

        assert_eq!(actual, Ok(expected));
    }

    #[test]
    fn complete_parse_with_signatures() {
        let mut buffer = BytesMut::new();
        buffer.put_u32(MAGIC_BYTES);
        buffer.put_u16(1);
        buffer.put_u32(16);
        buffer.put_u128(300);
        let mut metadata = HashMap::new();
        metadata.insert("one".to_owned(), "1".to_owned());
        metadata.insert("two".to_owned(), "2".to_owned());
        metadata.insert("three".to_owned(), "3".to_owned());
        let json = serde_json::to_vec(&metadata).unwrap();
        buffer.put_u32(json.len().try_into().unwrap());
        buffer.put_slice(json.as_slice());
        buffer.put_u32(10);
        buffer.put_slice(b"valid uri!");
        let mut signatures= Vec::new();
        signatures.push(Signature {
            signature: "signature".to_owned(),
            public_key: "public_key".to_owned(),
        });
        signatures.push(Signature {
            signature: "signature".to_owned(),
            public_key: "public_key".to_owned(),
        });
        let json = serde_json::to_vec(&signatures).unwrap();
        buffer.put_u32(json.len().try_into().unwrap());
        buffer.put_slice(json.as_slice());

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
        let buffer: Bytes = buffer.into();
        let actual: Result<Dime, DimeInputError> = buffer.try_into();

        let expected = Dime {
            uuid: Uuid::from_u128(300),
            uri: "valid uri!".to_owned(),
            proto,
            metadata,
            signatures,
        };

        assert_eq!(actual, Ok(expected));
    }

    #[test]
    fn encode_to_decode_matches() {
        let mut buffer = BytesMut::new();
        buffer.put_u32(MAGIC_BYTES);
        buffer.put_u16(1);
        buffer.put_u32(16);
        buffer.put_u128(300);
        let mut metadata = HashMap::new();
        metadata.insert("one".to_owned(), "1".to_owned());
        metadata.insert("two".to_owned(), "2".to_owned());
        metadata.insert("three".to_owned(), "3".to_owned());
        let json = serde_json::to_vec(&metadata).unwrap();
        buffer.put_u32(json.len().try_into().unwrap());
        buffer.put_slice(json.as_slice());
        buffer.put_u32(10);
        buffer.put_slice(b"valid uri!");
        buffer.put_u32(0);
        // no signature slice needed since length is 0
        // buffer.put_slice();

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
        let mut buffer: Bytes = buffer.into();
        let signature = Signature {
            signature: "signature".to_owned(),
            public_key: "public_key".to_owned(),
        };

        let mut expected = Dime {
            uuid: Uuid::from_u128(300),
            uri: "valid uri!".to_owned(),
            proto,
            metadata,
            signatures: Vec::default(),
        };

        let actual = format_dime_bytes(&mut buffer, signature.clone());
        assert!(actual.is_ok());
        let actual: Result<Dime, DimeInputError> = actual.unwrap().try_into();
        assert!(actual.is_ok());
        expected.signatures = vec![signature];
        assert_eq!(actual.unwrap(), expected);
    }
}
