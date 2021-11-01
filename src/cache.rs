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
    pub local_public_keys: HashMap<String, PublicKey>,
    // keys are base64 strings, values are (key data, url) pairs
    pub remote_public_keys: HashMap<String, (PublicKey, String)>,
}

impl Cache {

    pub fn add_local_public_key(&mut self, key: PublicKey) -> Option<PublicKey> {
        self.local_public_keys.insert(key.public_key.clone(), key)
    }

    pub fn add_remote_public_key(&mut self, key: PublicKey, url: String) -> Option<(PublicKey, String)> {
        self.remote_public_keys.insert(key.public_key.clone(), (key, url))
    }

    pub fn get_public_key_state(&self, public_key: &String) -> PublicKeyState {
        if self.local_public_keys.contains_key(public_key) {
            PublicKeyState::Local
        } else if self.remote_public_keys.contains_key(public_key) {
            PublicKeyState::Remote
        } else {
            PublicKeyState::Unknown
        }
    }
}
