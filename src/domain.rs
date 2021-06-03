use crate::config::Config;
use crate::datastore::Object;
use crate::pb::{ObjectMetadata, ObjectResponse, Uuid};
use crate::types::Result;

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
            uuid: Some(Uuid { value: self.uuid.to_hyphenated().to_string() }),
            dime_uuid: Some(Uuid { value: self.dime_uuid.to_hyphenated().to_string() }),
            hash: base64::decode(&self.hash)?,
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
