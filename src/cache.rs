use std::collections::{HashMap, HashSet};

pub enum PublicKeyState {
    Local,
    Remote,
    Unknown,
}

#[derive(Clone, Debug, Default)]
pub struct Cache {
    // keys are base64 strings
    pub local_public_keys: HashSet<String>,
    // keys are base64 strings, values are urls
    pub remote_public_keys: HashMap<String, String>,
}

impl Cache {

    pub fn add_local_public_key(&mut self, public_key: Vec<u8>) -> bool {
        self.local_public_keys.insert(base64::encode(&public_key))
    }

    pub fn add_remote_public_key(&mut self, public_key: Vec<u8>, url: String) -> Option<String> {
        self.remote_public_keys.insert(base64::encode(&public_key), url)
    }

    pub fn get_public_key_state(&self, public_key: &String) -> PublicKeyState {
        if self.local_public_keys.contains(public_key) {
            PublicKeyState::Local
        } else if self.remote_public_keys.contains_key(public_key) {
            PublicKeyState::Remote
        } else {
            PublicKeyState::Unknown
        }
    }
}
