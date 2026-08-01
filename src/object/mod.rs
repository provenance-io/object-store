pub mod service;
pub use service::*;

use crate::storage::StoragePath;

use chrono::prelude::*;
use linked_hash_map::LinkedHashMap;

#[derive(Debug)]
pub struct Object {
    pub uuid: uuid::Uuid,
    pub dime_uuid: uuid::Uuid,
    pub hash: String,
    pub unique_hash: String,
    pub content_length: usize,
    pub dime_length: usize,
    pub directory: String,
    pub name: String,
    pub payload: Option<Vec<u8>>,
    pub properties: LinkedHashMap<String, Vec<u8>>,
    pub created_at: DateTime<Utc>,
}

impl Object {
    pub fn storage_path(&self) -> StoragePath {
        StoragePath {
            dir: self.directory.clone(),
            file: self.name.clone(),
        }
    }
}
