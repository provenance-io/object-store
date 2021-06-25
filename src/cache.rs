use std::collections::HashSet;

pub enum PublicKeyState {
    Local,
    Remote,
    Unknown,
}

#[derive(Debug, Default)]
pub struct Cache {
    // stored as base64 strings
    pub local_public_keys: HashSet<String>,
    // stored as base64 strings
    pub remote_public_keys: HashSet<String>,
}

impl Cache {

    pub fn add_local_public_key(&mut self, public_key: Vec<u8>) -> bool {
        self.local_public_keys.insert(base64::encode(&public_key))
    }

    pub fn add_remote_public_key(&mut self, public_key: Vec<u8>) -> bool {
        self.remote_public_keys.insert(base64::encode(&public_key))
    }

    pub fn get_public_key_state(&self, public_key: &String) -> PublicKeyState {
        if self.local_public_keys.contains(public_key) {
            PublicKeyState::Local
        } else if self.remote_public_keys.contains(public_key) {
            PublicKeyState::Remote
        } else {
            PublicKeyState::Unknown
        }
    }
}
