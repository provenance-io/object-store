use std::collections::HashMap;

use crate::datastore::PublicKey;

#[derive(Debug)]
pub enum PublicKeyState {
    Local,
    Remote,
    Unknown,
}

#[derive(Clone, Debug, Default)]
pub struct Cache {
    // keys are base64 strings
    pub public_keys: HashMap<String, PublicKey>,
}

impl Cache {

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
        self.public_keys.iter()
            .filter(|(_, v)| !v.url.is_empty())
            .collect()
    }
}
