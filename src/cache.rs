use std::collections::HashSet;

#[derive(Debug, Default)]
pub struct Cache {
    // stored as base64 strings
    pub local_public_keys: HashSet<String>,
}

impl Cache {

    pub fn add_local_public_key(&mut self, public_key: Vec<u8>) -> bool {
        self.local_public_keys.insert(base64::encode(&public_key))
    }
}
