pub mod types;
pub mod util;
pub use types::*;
pub use util::*;

use crate::config::Config;
use crate::datastore::{AuthType, KeyType, PublicKey};
use crate::object::Object;
use crate::pb::public_key_response::Impl::HeaderAuth as HeaderAuthEnumResponse;
use crate::pb::{HeaderAuth, ObjectMetadata, ObjectResponse, PublicKeyResponse, public_key::Key};
use crate::proto::UuidUtil;

use chrono::Utc;
use prost::Message;
use prost_types::Timestamp;

use std::time::SystemTime;

#[derive(Debug)]
pub struct DimeProperties {
    pub hash: String,
    pub content_length: usize,
    pub dime_length: usize,
}

pub trait ObjectApiResponse {
    fn to_response(&self, config: &Config) -> Result<ObjectResponse>;
}

impl ObjectApiResponse for Object {
    fn to_response(&self, config: &Config) -> Result<ObjectResponse> {
        Ok(ObjectResponse {
            uuid: self.uuid.proto(),
            dime_uuid: self.dime_uuid.proto(),
            hash: self.hash.decoded()?,
            uri: format!("object://{}/{}", config.uri_host, self.hash),
            bucket: config.storage.base_path.clone(),
            name: self.name.clone(),
            metadata: Some(ObjectMetadata {
                sha512: Vec::new(), // TODO get hash of whole dime?
                length: self.dime_length as i64,
                content_length: self.content_length as i64,
            }),
            created: self.created_at.proto(),
        })
    }
}

pub trait PublicKeyApiResponse {
    fn to_response(&self) -> Result<PublicKeyResponse>;
}

impl PublicKeyApiResponse for PublicKey {
    fn to_response(&self) -> Result<PublicKeyResponse> {
        let public_key = {
            let key_bytes = self.public_key.decoded()?;

            match self.public_key_type {
                KeyType::Secp256k1 => Key::Secp256k1(key_bytes),
            }
        };

        let metadata = if !self.metadata.is_empty() {
            Some(prost_types::Any::decode(self.metadata.as_slice())?)
        } else {
            None
        };

        let r#impl = match self.auth_type {
            Some(AuthType::Header) => {
                let auth_data = self
                    .auth_data
                    .clone()
                    .ok_or(OsError::InvalidApplicationState(String::from(
                        "auth_type was set but no auth_data",
                    )))?;

                let (header, value) =
                    auth_data
                        .split_once(":")
                        .ok_or(OsError::InvalidApplicationState(String::from(
                            "auth_data invalid format",
                        )))?;

                Some(HeaderAuthEnumResponse(HeaderAuth {
                    header: header.to_string(),
                    value: value.to_string(),
                }))
            }
            None => None,
        };

        let response = PublicKeyResponse {
            uuid: self.uuid.proto(),
            public_key: Some(public_key.into()),
            url: self.url.clone(),
            r#impl,
            metadata,
            created_at: self.created_at.proto(),
            updated_at: self.updated_at.proto(),
        };

        Ok(response)
    }
}

// TODO move
pub trait TimestampUtil {
    fn proto(self) -> Option<Timestamp>;
}
impl TimestampUtil for chrono::DateTime<Utc> {
    fn proto(self) -> Option<Timestamp> {
        Some(Into::<SystemTime>::into(self).into())
    }
}
