use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use sqlx::PgPool;

use crate::{
    datastore::{self, PublicKey},
    types::OsError,
};

#[derive(Debug)]
pub enum PublicKeyState {
    Local,
    Remote,
    Unknown,
}

#[derive(Clone, Debug, Default)]
pub struct Cache {
    /// keys are base64 strings
    pub public_keys: HashMap<String, PublicKey>,
}

impl Cache {
    /// populate initial cache
    pub async fn new(pool: Arc<PgPool>) -> Result<Arc<Mutex<Cache>>, OsError> {
        let mut cache = Cache::default();
        for key in datastore::get_all_public_keys(&pool).await? {
            log::debug!(
                "Adding public key {} with url {}",
                &key.public_key,
                &key.url
            );

            cache.add_public_key(key);
        }

        Ok(Arc::new(Mutex::new(cache)))
    }

    pub fn add_public_key(&mut self, key: PublicKey) -> Option<PublicKey> {
        self.public_keys.insert(key.public_key.clone(), key)
    }

    pub fn get_public_key_state(&self, public_key: &String) -> PublicKeyState {
        match self.public_keys.get(public_key) {
            Some(key) if !key.url.is_empty() => PublicKeyState::Remote,
            Some(_) => PublicKeyState::Local,
            None => PublicKeyState::Unknown,
        }
    }

    pub fn get_remote_public_keys(&self) -> Vec<(&String, &PublicKey)> {
        self.public_keys
            .iter()
            .filter(|(_, v)| !v.url.is_empty())
            .collect()
    }
}
