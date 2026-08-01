use base64::{Engine, prelude::BASE64_STANDARD};
use chrono::Utc;
use prost_types::Timestamp;

use crate::pb::{Audience, ObjectResponse, PublicKey, Uuid, public_key::Key};
use std::{str::FromStr, time::SystemTime};

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

pub trait TimestampUtil {
    fn proto(self) -> Option<Timestamp>;
}
impl TimestampUtil for chrono::DateTime<Utc> {
    fn proto(self) -> Option<Timestamp> {
        Some(Into::<SystemTime>::into(self).into())
    }
}

pub trait UuidUtil {
    fn proto(&self) -> Option<Uuid>;
}
impl UuidUtil for uuid::Uuid {
    fn proto(&self) -> Option<Uuid> {
        Some(Uuid {
            value: self.as_hyphenated().to_string(),
        })
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
