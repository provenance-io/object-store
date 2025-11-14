use crate::config::Config;
use crate::datastore::{AuthType, KeyType, Object, PublicKey};
use crate::pb::public_key_response::Impl::HeaderAuth as HeaderAuthEnumResponse;
use crate::pb::{
    public_key::Key, HeaderAuth, ObjectMetadata, ObjectResponse, PublicKeyResponse, Uuid,
};
use crate::proto_helpers::StringUtil;
use crate::types::{OsError, Result};

use prost::Message;

use std::time::SystemTime;

#[derive(Debug)]
pub struct DimeProperties {
    pub hash: String,
    pub content_length: i64,
    pub dime_length: i64,
}

pub trait ObjectApiResponse {
    fn to_response(&self, config: &Config) -> Result<ObjectResponse>;
}

impl ObjectApiResponse for Object {
    fn to_response(&self, config: &Config) -> Result<ObjectResponse> {
        Ok(ObjectResponse {
            uuid: Some(Uuid {
                value: self.uuid.as_hyphenated().to_string(),
            }),
            dime_uuid: Some(Uuid {
                value: self.dime_uuid.as_hyphenated().to_string(),
            }),
            hash: self.hash.decoded()?,
            uri: format!("object://{}/{}", &config.uri_host, &self.hash),
            bucket: config.storage_base_path.clone(),
            name: self.name.clone(),
            metadata: Some(ObjectMetadata {
                sha512: Vec::new(), // TODO get hash of whole dime?
                length: self.dime_length,
                content_length: self.content_length,
            }),
            created: Some(Into::<SystemTime>::into(self.created_at).into()),
        })
    }
}

pub trait PublicKeyApiResponse {
    fn to_response(self) -> Result<PublicKeyResponse>;
}

impl PublicKeyApiResponse for PublicKey {
    fn to_response(self) -> Result<PublicKeyResponse> {
        let key_bytes: Vec<u8> = self
            .public_key
            .decoded()
            .map_err(|err| sqlx::Error::Decode(Box::new(err)))?;
        let public_key = match self.public_key_type {
            KeyType::Secp256k1 => Key::Secp256k1(key_bytes),
        };
        let created_at: SystemTime = self.created_at.into();
        let updated_at: SystemTime = self.updated_at.into();
        let metadata = if !self.metadata.is_empty() {
            let message = prost_types::Any::decode(self.metadata.as_slice())
                .map_err(|err| sqlx::Error::Decode(Box::new(err)))?;
            Some(message)
        } else {
            None
        };
        let r#impl = match self.auth_type {
            Some(AuthType::Header) => {
                let auth_data = self.auth_data.ok_or(sqlx::Error::Decode(Box::new(
                    OsError::InvalidApplicationState(String::from(
                        "auth_type was set but no auth_data",
                    )),
                )))?;
                let (header, value) =
                    auth_data
                        .split_once(":")
                        .ok_or(sqlx::Error::Decode(Box::new(
                            OsError::InvalidApplicationState(String::from(
                                "auth_data invalid format",
                            )),
                        )))?;

                Some(HeaderAuthEnumResponse(HeaderAuth {
                    header: header.to_string(),
                    value: value.to_string(),
                }))
            }
            None => None,
        };
        let response = PublicKeyResponse {
            uuid: Some(Uuid {
                value: self.uuid.as_hyphenated().to_string(),
            }),
            public_key: Some(public_key.into()),
            url: self.url,
            r#impl,
            metadata,
            created_at: Some(created_at.into()),
            updated_at: Some(updated_at.into()),
        };

        Ok(response)
    }
}
