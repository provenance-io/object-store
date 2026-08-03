use std::sync::Arc;

use bytes::Bytes;
use linked_hash_map::LinkedHashMap;

#[cfg(feature = "test-utils")]
use crate::db::postgres::{MailboxPublicKey, ObjectPublicKey};
use crate::{
    config::DatastoreConfig,
    db::postgres::init_postgres,
    dime::Dime,
    domain::{DimeProperties, Result},
    object::Object,
    public_key::{PublicKey, PublicKeyState},
};

#[async_trait::async_trait]
pub trait Datastore: Send + Sync + std::fmt::Debug {
    async fn get_public_key_object_uuid(&self, hash: &str, public_key: &str) -> Result<uuid::Uuid>;
    async fn get_object_by_uuid(&self, uuid: &uuid::Uuid) -> Result<Object>;
    async fn ack_object_replication(&self, uuid: &uuid::Uuid) -> Result<bool>;
    async fn reap_object_replication(&self, public_key: &str) -> Result<u64>;
    async fn replication_object_uuids(
        &self,
        public_key: &str,
        limit: i32,
    ) -> Result<Vec<(uuid::Uuid, uuid::Uuid)>>;
    async fn stream_mailbox_public_keys(
        &self,
        public_key: &str,
        limit: i32,
    ) -> Result<Vec<(uuid::Uuid, Object)>>;
    async fn ack_mailbox_public_key(
        &self,
        uuid: &uuid::Uuid,
        public_key: &Option<String>,
    ) -> Result<bool>;
    async fn put_object(
        &self,
        dime: &Dime,
        dime_properties: &DimeProperties,
        properties: &LinkedHashMap<String, Vec<u8>>,
        replication_key_states: Vec<(String, PublicKeyState)>,
        raw_dime: Option<&Bytes>,
        replication_enabled: bool,
    ) -> Result<Object>;
    async fn update_public_key(&self, public_key: PublicKey) -> Result<PublicKey>;
    async fn add_public_key(&self, public_key: PublicKey) -> Result<PublicKey>;
    async fn get_all_public_keys(&self) -> Result<Vec<PublicKey>>;

    async fn health_check(&self) -> Result<()>;

    // METHODS from test below
    #[cfg(feature = "test-utils")]
    async fn get_public_keys_by_object(&self, object_uuid: &uuid::Uuid) -> Vec<ObjectPublicKey>;
    #[cfg(feature = "test-utils")]
    async fn get_mailbox_keys_by_object(&self, object_uuid: &uuid::Uuid) -> Vec<MailboxPublicKey>;
    #[cfg(feature = "test-utils")]
    async fn delete_properties(&self, object_uuid: &uuid::Uuid) -> u64;
    #[cfg(feature = "test-utils")]
    async fn get_object_count(&self) -> i64;
}

pub async fn new_datastore(config: &DatastoreConfig) -> Result<Arc<dyn Datastore>> {
    match config {
        DatastoreConfig::Postgres(db_config) => {
            let pool = init_postgres(db_config).await?;
            Ok(pool)
        }
    }
}
