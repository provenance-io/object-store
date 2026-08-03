use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{domain::OsError, public_key::PublicKey};

#[derive(Debug)]
pub enum PublicKeyState {
    Local,
    Remote,
    Unknown,
}

#[derive(Clone, Debug, Default)]
pub struct PublicKeyCache {
    /// keys are base64-encoded strings
    pub public_keys: HashMap<String, PublicKey>,
}

impl PublicKeyCache {
    /// populate initial cache
    pub async fn new(keys: Vec<PublicKey>) -> Result<Arc<Mutex<PublicKeyCache>>, OsError> {
        let mut public_key_cache = PublicKeyCache::default();

        for key in keys {
            log::debug!("Adding public key {} with url {}", key.public_key, key.url);

            public_key_cache.add(key);
        }

        Ok(Arc::new(Mutex::new(public_key_cache)))
    }

    pub fn add(&mut self, key: PublicKey) -> Option<PublicKey> {
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
