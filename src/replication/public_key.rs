/// Newtype wrapper around string public keys
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct PublicKey(String);

impl PublicKey {
    pub fn new<S: Into<String>>(key: S) -> Self {
        Self(key.into())
    }
}

impl AsRef<str> for PublicKey {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

impl From<PublicKey> for String {
    fn from(key: PublicKey) -> String {
        key.0
    }
}

impl std::fmt::Display for PublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
