use std::collections::HashMap;

use base64::{prelude::BASE64_STANDARD, Engine};
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

pub fn generate_dime(audience: Vec<Audience>, signatures: Vec<Signature>) -> Dime {
    let proto = DimeProto {
        uuid: None,
        owner: Some(audience.first().unwrap().clone()),
        metadata: HashMap::default(),
        audience,
        payload: Vec::default(),
        audit_fields: None,
    };

    Dime {
        uuid: uuid::Uuid::from_u128(300),
        uri: String::default(),
        proto,
        metadata: std::collections::HashMap::default(),
        signatures,
    }
}
