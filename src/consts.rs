// mailbox metadata fields
pub const MAILBOX_KEY: &str = "P8EAPI::TYPE";
pub const MAILBOX_FRAGMENT_REQUEST: &str = "FRAGMENT_REQUEST";
pub const MAILBOX_FRAGMENT_RESPONSE: &str = "FRAGMENT_RESPONSE";
pub const MAILBOX_ERROR_RESPONSE: &str = "ERROR_RESPONSE";

// dime value fields
pub const DIME_FIELD_NAME: &str = "DIME";
pub const HASH_FIELD_NAME: &str = "HASH";
pub const SIGNATURE_PUBLIC_KEY_FIELD_NAME: &str = "SIGNATURE_PUBLIC_KEY";
pub const SIGNATURE_FIELD_NAME: &str = "SIGNATURE";
pub const CREATED_BY_HEADER: &str = "x-created-by";

pub const CHUNK_SIZE: usize = 2000000;

pub const NOT_STORAGE_BACKED: &str = "NOT_STORAGE_BACKED";
