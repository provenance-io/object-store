use std::collections::HashMap;

use base64::{Engine, prelude::BASE64_STANDARD};
use chrono::Utc;
use object_store::cache::Cache;
use object_store::config::Config;
use object_store::datastore::{AuthType, KeyType, PublicKey};
use object_store::dime::Dime;
use object_store::pb::Dime as DimeProto;
use object_store::{dime::Signature, pb::Audience};

pub fn party_1() -> (Audience, Signature) {
    (
        Audience {
            payload_id: 0,
            public_key: BASE64_STANDARD.encode("1").into_bytes(),
            context: 0,
            tag: Vec::default(),
            ephemeral_pubkey: Vec::default(),
            encrypted_dek: Vec::default(),
        },
        Signature {
            public_key: "1".to_owned(),
            signature: "a".to_owned(),
        },
    )
}

pub fn party_2() -> (Audience, Signature) {
    (
        Audience {
            payload_id: 0,
            public_key: BASE64_STANDARD.encode("2").into_bytes(),
            context: 0,
            tag: Vec::default(),
            ephemeral_pubkey: Vec::default(),
            encrypted_dek: Vec::default(),
        },
        Signature {
            public_key: "2".to_owned(),
            signature: "b".to_owned(),
        },
    )
}

pub fn party_3() -> (Audience, Signature) {
    (
        Audience {
            payload_id: 0,
            public_key: BASE64_STANDARD.encode("3").into_bytes(),
            context: 0,
            tag: Vec::default(),
            ephemeral_pubkey: Vec::default(),
            encrypted_dek: Vec::default(),
        },
        Signature {
            public_key: "3".to_owned(),
            signature: "c".to_owned(),
        },
    )
}

pub fn generate_dime(audiences: Vec<Audience>, signatures: Vec<Signature>) -> Dime {
    let proto = DimeProto {
        uuid: None,
        owner: Some(audiences.first().unwrap().clone()),
        metadata: HashMap::default(),
        audience: audiences,
        payload: Vec::default(),
        audit_fields: None,
    };

    Dime {
        uuid: uuid::Uuid::from_u128(300),
        uri: String::default(),
        proto,
        metadata: HashMap::default(),
        signatures,
    }
}

pub fn test_public_key(public_key: Vec<u8>) -> PublicKey {
    let now = Utc::now();

    PublicKey {
        uuid: uuid::Uuid::new_v4(),
        public_key: std::str::from_utf8(&public_key).unwrap().to_owned(),
        public_key_type: KeyType::Secp256k1,
        url: String::from(""),
        metadata: Vec::default(),
        auth_type: Some(AuthType::Header),
        auth_data: Some(String::from("x-test-header:test_value_1")),
        created_at: now,
        updated_at: now,
    }
}

/// Adds two public keys to cache, one with a url of a remote server
pub fn seed_cache(cache: &mut Cache, remote_config: &Config) {
    cache.add_public_key(PublicKey {
        auth_data: Some(String::from("X-Test-Header:test_value")),
        ..test_public_key(party_1().0.public_key)
    });
    cache.add_public_key(PublicKey {
        url: String::from(format!("http://{}", remote_config.url)),
        auth_data: Some(String::from("X-Test-Header:test_value")),
        ..test_public_key(party_2().0.public_key)
    });
}
