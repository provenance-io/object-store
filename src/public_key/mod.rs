pub mod authorization;
pub mod cache;
pub mod service;
pub use authorization::*;
pub use cache::*;
pub use service::*;

use crate::db::postgres::{AuthType, KeyType};
use crate::domain::VecUtil;
use crate::domain::{OsError, Result};
use crate::pb::public_key_request::Impl::HeaderAuth as HeaderAuthEnumRequest;
use crate::pb::{PublicKeyRequest, public_key::Key};
use prost::Message;
use std::convert::TryFrom;

use bytes::BytesMut;
use chrono::prelude::*;

#[derive(Clone, Debug)]
pub struct PublicKey {
    pub uuid: uuid::Uuid,
    /// Encoded
    pub public_key: String,
    pub public_key_type: KeyType,
    pub url: String,
    pub metadata: Vec<u8>,
    pub auth_type: Option<AuthType>,
    pub auth_data: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl PublicKey {
    pub fn auth(&self) -> Result<Box<dyn Authorization + '_>> {
        match self.auth_type {
            Some(AuthType::Header) => {
                let auth_data = self
                    .auth_data
                    .as_ref()
                    .ok_or(OsError::InvalidApplicationState(String::from(
                        "auth_type was set but no auth_data",
                    )))?;
                let (header, value) =
                    auth_data
                        .split_once(":")
                        .ok_or(OsError::InvalidApplicationState(String::from(
                            "auth_data invalid format",
                        )))?;

                Ok(Box::new(HeaderAuth { header, value }))
            }
            None => Ok(Box::new(NoAuthorization::default())),
        }
    }
}

impl TryFrom<PublicKeyRequest> for PublicKey {
    type Error = OsError;

    fn try_from(request: PublicKeyRequest) -> Result<Self> {
        let (public_key_type, public_key) = match request.public_key.unwrap().key.unwrap() {
            Key::Secp256k1(data) => (KeyType::Secp256k1, data.encoded()),
        };
        let metadata = if let Some(metadata) = request.metadata {
            let mut buffer = BytesMut::with_capacity(metadata.encoded_len());
            metadata.encode(&mut buffer)?;
            buffer
        } else {
            BytesMut::default()
        };
        let (auth_type, auth_data) = match request.r#impl {
            Some(HeaderAuthEnumRequest(ref auth)) => (
                Some(AuthType::Header),
                Some(format!("{}:{}", auth.header.to_lowercase(), auth.value)),
            ),
            None => (None, None),
        };

        Ok(Self {
            uuid: uuid::Uuid::new_v4(),
            public_key,
            public_key_type,
            url: request.url,
            metadata: metadata.to_vec(),
            auth_type,
            auth_data,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        })
    }
}
