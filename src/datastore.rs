use crate::authorization::{Authorization, HeaderAuth, NoAuthorization};
use crate::cache::PublicKeyState;
use crate::consts::*;
use crate::dime::Dime;
use crate::domain::DimeProperties;
use crate::pb::public_key_request::Impl::HeaderAuth as HeaderAuthEnumRequest;
use crate::pb::{public_key::Key, PublicKeyRequest};
use crate::proto_helpers::VecUtil;
use crate::types::{OsError, Result};
use prost::Message;
use std::convert::TryFrom;

use bytes::{Bytes, BytesMut};
use chrono::prelude::*;
use fastrace_macro::trace;
use futures_util::TryStreamExt;
use linked_hash_map::LinkedHashMap;
use sqlx::postgres::{PgConnection, PgPool, PgQueryResult};
use sqlx::Acquire;
use sqlx::{FromRow, Row};

// TODO model public keys like the other objects and don't use grpc types directly

#[derive(Debug)]
enum UpsertOutcome {
    Noop,
    Created,
}

impl From<PgQueryResult> for UpsertOutcome {
    fn from(query_result: PgQueryResult) -> Self {
        if query_result.rows_affected() > 0 {
            Self::Created
        } else {
            Self::Noop
        }
    }
}

#[allow(
    dead_code,
    reason = "https://github.com/provenance-io/object-store/issues/47"
)]
#[derive(Debug)]
pub struct Object {
    pub uuid: uuid::Uuid,
    pub dime_uuid: uuid::Uuid,
    pub hash: String,
    pub unique_hash: String,
    pub content_length: i64,
    pub dime_length: i64,
    pub directory: String,
    pub name: String,
    pub payload: Option<Vec<u8>>,
    pub properties: LinkedHashMap<String, Vec<u8>>,
    pub created_at: DateTime<Utc>,
}

impl FromRow<'_, sqlx::postgres::PgRow> for Object {
    fn from_row(row: &sqlx::postgres::PgRow) -> std::result::Result<Self, sqlx::Error> {
        let response = Object {
            uuid: row.try_get::<uuid::Uuid, _>("uuid")?,
            dime_uuid: row.try_get::<uuid::Uuid, _>("dime_uuid")?,
            hash: row.try_get("hash")?,
            unique_hash: row.try_get("unique_hash")?,
            content_length: row.try_get("content_length")?,
            dime_length: row.try_get("dime_length")?,
            directory: row.try_get("directory")?,
            name: row.try_get("name")?,
            payload: row.try_get("payload")?,
            properties: if let Some(properties) = &row.try_get::<Option<String>, _>("properties")? {
                serde_json::from_str(properties).map_err(|e| sqlx::Error::ColumnDecode {
                    index: String::from("properties"),
                    source: Box::new(e),
                })?
            } else {
                LinkedHashMap::default()
            },
            created_at: row.try_get("created_at")?,
        };

        Ok(response)
    }
}

#[allow(
    dead_code,
    reason = "https://github.com/provenance-io/object-store/issues/47"
)]
#[derive(FromRow, Debug)]
pub struct ObjectPublicKey {
    pub object_uuid: uuid::Uuid,
    pub hash: String,
    pub public_key: String,
    pub created_at: DateTime<Utc>,
}

#[allow(
    dead_code,
    reason = "https://github.com/provenance-io/object-store/issues/47"
)]
#[derive(FromRow, Debug)]
pub struct MailboxPublicKey {
    pub uuid: uuid::Uuid,
    pub object_uuid: uuid::Uuid,
    pub public_key: String,
    pub message_type: String,
    pub created_at: DateTime<Utc>,
    pub acked_at: Option<DateTime<Utc>>,
}

#[derive(sqlx::Type, Clone, Debug)]
#[sqlx(type_name = "auth_type", rename_all = "lowercase")]
pub enum AuthType {
    Header,
}

#[derive(sqlx::Type, Clone, Debug)]
#[sqlx(type_name = "key_type", rename_all = "lowercase")]
pub enum KeyType {
    Secp256k1,
}

#[derive(Clone, Debug)]
pub struct PublicKey {
    pub uuid: uuid::Uuid,
    /// Encoded
    pub public_key: String,
    pub public_key_type: KeyType,
    pub url: String,
    pub metadata: Vec<u8>,
    pub auth_type: Option<AuthType>,
    pub auth_data: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl PublicKey {
    pub fn auth(&self) -> Result<Box<dyn Authorization + '_>> {
        match self.auth_type {
            Some(AuthType::Header) => {
                let auth_data = self
                    .auth_data
                    .as_ref()
                    .ok_or(OsError::InvalidApplicationState(String::from(
                        "auth_type was set but no auth_data",
                    )))?;
                let (header, value) =
                    auth_data
                        .split_once(":")
                        .ok_or(OsError::InvalidApplicationState(String::from(
                            "auth_data invalid format",
                        )))?;

                Ok(Box::new(HeaderAuth { header, value }))
            }
            None => Ok(Box::new(NoAuthorization::default())),
        }
    }
}

impl TryFrom<PublicKeyRequest> for PublicKey {
    type Error = OsError;

    fn try_from(request: PublicKeyRequest) -> Result<Self> {
        let (public_key_type, public_key) = match request.public_key.unwrap().key.unwrap() {
            Key::Secp256k1(data) => (KeyType::Secp256k1, data.encoded()),
        };
        let metadata = if let Some(metadata) = request.metadata {
            let mut buffer = BytesMut::with_capacity(metadata.encoded_len());
            metadata.encode(&mut buffer)?;
            buffer
        } else {
            BytesMut::default()
        };
        let (auth_type, auth_data) = match request.r#impl {
            Some(HeaderAuthEnumRequest(ref auth)) => (
                Some(AuthType::Header),
                Some(format!("{}:{}", &auth.header.to_lowercase(), &auth.value)),
            ),
            None => (None, None),
        };

        Ok(Self {
            uuid: uuid::Uuid::new_v4(),
            public_key,
            public_key_type,
            url: request.url,
            metadata: metadata.to_vec(),
            auth_type,
            auth_data,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        })
    }
}

impl sqlx::FromRow<'_, sqlx::postgres::PgRow> for PublicKey {
    fn from_row(row: &sqlx::postgres::PgRow) -> std::result::Result<Self, sqlx::Error> {
        let uuid = row.try_get("uuid")?;
        let public_key = row.try_get("public_key")?;
        let public_key_type = row.try_get("public_key_type")?;
        let url = row.try_get("url")?;
        let metadata = row.try_get("metadata")?;
        let auth_type = row.try_get("auth_type")?;
        let auth_data = row.try_get("auth_data")?;
        let created_at = row.try_get("created_at")?;
        let updated_at = row.try_get("updated_at")?;

        Ok(Self {
            uuid,
            public_key,
            public_key_type,
            url,
            metadata,
            auth_type,
            auth_data,
            created_at,
            updated_at,
        })
    }
}

#[trace(name = "datastore::get_public_key_object_uuid")]
pub async fn get_public_key_object_uuid(
    db: &PgPool,
    hash: &str,
    public_key: &str,
) -> Result<uuid::Uuid> {
    let query_str = "SELECT object_uuid FROM object_public_key WHERE hash = $1 AND public_key = $2";
    let result = sqlx::query(query_str)
        .bind(hash)
        .bind(public_key)
        .fetch_optional(db)
        .await?;

    if let Some(row) = result {
        Ok(row.try_get("object_uuid")?)
    } else {
        Err(OsError::NotFound(format!(
            "Unable to find object with public key {} and hash {}",
            public_key, hash
        )))
    }
}

#[trace(name = "datastore::get_object_by_unique_hash")]
async fn get_object_by_unique_hash(db: &PgPool, unique_hash: &str) -> Result<Object> {
    let query_str = "SELECT * FROM object WHERE md5(unique_hash) = md5($1)";
    let result = sqlx::query_as::<_, Object>(query_str)
        .bind(unique_hash)
        .fetch_one(db)
        .await?;

    Ok(result)
}

#[trace(name = "datastore::get_object_by_uuid")]
pub async fn get_object_by_uuid(db: &PgPool, uuid: &uuid::Uuid) -> Result<Object> {
    let query_str = "SELECT * FROM object WHERE uuid = $1";
    let result = sqlx::query_as::<_, Object>(query_str)
        .bind(uuid)
        .fetch_one(db)
        .await?;

    Ok(result)
}

#[trace(name = "datastore::maybe_put_replication_objects")]
async fn maybe_put_replication_objects(
    conn: &mut PgConnection,
    object_uuid: uuid::Uuid,
    replication_key_states: Vec<(String, PublicKeyState)>,
) -> Result<()> {
    let mut replication_uuids: Vec<uuid::Uuid> = Vec::new();
    let mut replication_object_uuids: Vec<uuid::Uuid> = Vec::new();
    let mut replication_public_keys: Vec<String> = Vec::new();

    for (party, replication_key_state) in replication_key_states {
        match replication_key_state {
            PublicKeyState::Local => (),
            PublicKeyState::Remote | PublicKeyState::Unknown => {
                replication_uuids.push(uuid::Uuid::new_v4());
                replication_object_uuids.push(object_uuid);
                replication_public_keys.push(party);
            }
        }
    }

    let remote_query_str = r#"
INSERT INTO object_replication (uuid, object_uuid, public_key)
SELECT * FROM UNNEST($1, $2, $3)
    "#;

    sqlx::query(remote_query_str)
        .bind(&replication_uuids)
        .bind(&replication_object_uuids)
        .bind(&replication_public_keys)
        .execute(conn.acquire().await?)
        .await?;

    Ok(())
}

#[trace(name = "datastore::put_object_public_keys")]
async fn put_object_public_keys(
    conn: &mut PgConnection,
    object_uuid: uuid::Uuid,
    dime: &Dime,
    properties: &DimeProperties,
) -> Result<()> {
    let mut object_uuids: Vec<uuid::Uuid> = Vec::new();
    let mut hashes: Vec<&str> = Vec::new();
    let mut public_keys: Vec<String> = Vec::new();

    for party in dime.unique_audience_base64()? {
        object_uuids.push(object_uuid);
        hashes.push(&properties.hash);
        public_keys.push(party);
    }

    let query_str = r#"
INSERT INTO object_public_key (object_uuid, hash, public_key)
SELECT * FROM UNNEST($1, $2, $3)
    "#;

    sqlx::query(query_str)
        .bind(&object_uuids)
        .bind(&hashes)
        .bind(&public_keys)
        .execute(conn)
        .await?;

    Ok(())
}

/// Set replicated_at to now
///
/// Returns OK(true) if one or more rows were updated
#[trace(name = "datastore::ack_object_replication")]
pub async fn ack_object_replication(db: &PgPool, uuid: &uuid::Uuid) -> Result<bool> {
    let query_str = r#"
UPDATE object_replication SET replicated_at = $1 WHERE uuid = $2
    "#;

    let rows_affected = sqlx::query(query_str)
        .bind(Utc::now())
        .bind(uuid)
        .execute(db)
        .await?
        .rows_affected();

    Ok(rows_affected > 0)
}

/// Set replicated_at to now for all objects not yet replicated for public_key
///
/// Returns OK(u64) with update count
#[trace(name = "datastore::reap_object_replication")]
pub async fn reap_object_replication(db: &PgPool, public_key: &str) -> Result<u64> {
    let query_str = r#"
UPDATE object_replication SET replicated_at = $1
  WHERE public_key = $2 AND replicated_at IS NULL
    "#;

    let rows_affected = sqlx::query(query_str)
        .bind(Utc::now())
        .bind(public_key)
        .execute(db)
        .await?
        .rows_affected();

    Ok(rows_affected)
}

/// Returns a [Result] of a [Vec] of (uuid, object_uuid) tuples for the public key that have NOT been replicated
#[trace(name = "datastore::replication_object_uuids")]
pub async fn replication_object_uuids(
    db: &PgPool,
    public_key: &str,
    limit: i32,
) -> Result<Vec<(uuid::Uuid, uuid::Uuid)>> {
    let query_str = r#"
SELECT uuid, object_uuid FROM object_replication
  WHERE public_key = $1 AND replicated_at IS NULL
  LIMIT $2
    "#;

    let mut query_result = sqlx::query(query_str)
        .bind(public_key)
        .bind(limit)
        .fetch(db);

    let result = {
        let mut result = Vec::new();

        while let Some(row) = query_result.try_next().await? {
            let uuid = row.try_get("uuid")?;
            let object_uuid = row.try_get("object_uuid")?;
            result.push((uuid, object_uuid));
        }

        result
    };

    Ok(result)
}

#[trace(name = "datastore::stream_mailbox_public_keys")]
pub async fn stream_mailbox_public_keys(
    db: &PgPool,
    public_key: &str,
    limit: i32,
) -> Result<Vec<(uuid::Uuid, Object)>> {
    let mut result = Vec::new();
    let query_str = r#"
SELECT mpk.uuid mpk_uuid, o.uuid, o.dime_uuid, hash, unique_hash, content_length, dime_length, directory, name, payload, properties, o.created_at
  FROM object AS o
  JOIN mailbox_public_key AS mpk
  ON o.uuid = mpk.object_uuid
  WHERE mpk.public_key = $1 AND mpk.acked_at IS NULL
  LIMIT $2
    "#;

    let mut query_result = sqlx::query(query_str)
        .bind(public_key)
        .bind(limit)
        .fetch(db);

    // TODO move to tokio mpsc so results can be streamed
    while let Some(row) = query_result.try_next().await? {
        let object = Object::from_row(&row)?;
        let mailbox_uuid = row.try_get("mpk_uuid")?;
        result.push((mailbox_uuid, object));
    }

    Ok(result)
}

// pub async fn stream_mailbox_public_keys(db: &PgPool, public_key: String, limit: i32) -> mpsc::Receiver<Object> {
//     let (tx, rx) = mpsc::channel(4);
//     let query_str = r#"
// SELECT o.uuid, hash, unique_hash, content_length, dime_length, directory, name, payload, o.created_at
//   FROM object AS o
//   JOIN mailbox_public_key AS mpk
//   ON o.uuid = mpk.object_uuid
//   WHERE mpk.public_key = $1 AND mpk.acked_at IS NULL
//   LIMIT $2
//     "#;
//
//     let mut query_result = sqlx::query(query_str)
//         .bind(public_key)
//         .bind(limit)
//         .fetch(db);
//
//     tokio::spawn(async move {
//         loop {
//             match query_result.try_next().await {
//                 Ok(Some(row)) => {
//                     tx.send(Object::from_row(&row).unwrap());
//                 },
//                 Ok(None) => (), // end of stream
//                 Err(e) => {
//                     // handle this better!
//                     log::info!("Error in mailbox query {:?}", e);
//                 },
//             }
//         }
//     });
//
//     rx
// }

#[trace(name = "datastore::maybe_put_mailbox_public_keys")]
async fn maybe_put_mailbox_public_keys(
    conn: &mut PgConnection,
    object_uuid: uuid::Uuid,
    dime: &Dime,
) -> Result<()> {
    let mut uuids: Vec<uuid::Uuid> = Vec::new();
    let mut object_uuids: Vec<uuid::Uuid> = Vec::new();
    let mut public_keys: Vec<String> = Vec::new();
    let mut message_types: Vec<String> = Vec::new();

    let message_type = match dime.metadata.get(MAILBOX_KEY).map(|v| v.as_str()) {
        Some(MAILBOX_FRAGMENT_REQUEST) => MAILBOX_FRAGMENT_REQUEST,
        Some(MAILBOX_FRAGMENT_RESPONSE) => MAILBOX_FRAGMENT_RESPONSE,
        Some(MAILBOX_ERROR_RESPONSE) => MAILBOX_ERROR_RESPONSE,
        _ => return Ok(()),
    };

    for party in dime.unique_audience_without_owner_base64()? {
        uuids.push(uuid::Uuid::new_v4());
        object_uuids.push(object_uuid);
        public_keys.push(party);
        message_types.push(message_type.to_owned());
    }

    let query_str = r#"
INSERT INTO mailbox_public_key (uuid, object_uuid, public_key, message_type)
SELECT * FROM UNNEST($1, $2, $3, $4)
    "#;

    sqlx::query(query_str)
        .bind(&uuids)
        .bind(&object_uuids)
        .bind(&public_keys)
        .bind(&message_types)
        .execute(conn)
        .await?;

    Ok(())
}

#[trace(name = "datastore::ack_mailbox_public_key")]
pub async fn ack_mailbox_public_key(
    db: &PgPool,
    uuid: &uuid::Uuid,
    public_key: &Option<String>,
) -> Result<bool> {
    let rows_affected = if let Some(public_key) = public_key {
        let query_str = r#"
UPDATE mailbox_public_key SET acked_at = $1 WHERE uuid = $2 AND public_key = $3
        "#;

        sqlx::query(query_str)
            .bind(Utc::now())
            .bind(uuid)
            .bind(public_key)
            .execute(db)
            .await?
            .rows_affected()
    } else {
        let query_str = r#"
UPDATE mailbox_public_key SET acked_at = $1 WHERE uuid = $2
        "#;

        sqlx::query(query_str)
            .bind(Utc::now())
            .bind(uuid)
            .execute(db)
            .await?
            .rows_affected()
    };

    Ok(rows_affected > 0)
}

#[trace(name = "datastore::put_object")]
pub async fn put_object(
    db: &PgPool,
    dime: &Dime,
    dime_properties: &DimeProperties,
    properties: &LinkedHashMap<String, Vec<u8>>,
    replication_key_states: Vec<(String, PublicKeyState)>,
    raw_dime: Option<&Bytes>,
    replication_enabled: bool,
) -> Result<Object> {
    let mut unique_hash = dime.unique_audience_base64()?;
    unique_hash.sort_unstable();
    unique_hash.insert(0, String::from(&dime_properties.hash));
    let unique_hash = unique_hash.join(";");

    let query_str = if raw_dime.is_some() {
        r#"
INSERT INTO object (uuid, dime_uuid, hash, unique_hash, content_length, dime_length, directory, name, properties, payload)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT DO NOTHING
        "#
    } else {
        r#"
INSERT INTO object (uuid, dime_uuid, hash, unique_hash, content_length, dime_length, name, properties)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT DO NOTHING
        "#
    };

    let uuid = uuid::Uuid::new_v4();
    let query = sqlx::query(query_str)
        .bind(uuid)
        .bind(dime.uuid)
        .bind(&dime_properties.hash)
        .bind(&unique_hash)
        .bind(dime_properties.content_length)
        .bind(dime_properties.dime_length);
    let query = if let Some(raw_dime) = raw_dime {
        query
            .bind(NOT_STORAGE_BACKED)
            .bind(NOT_STORAGE_BACKED)
            .bind(serde_json::to_string(properties).map_err(|e| {
                sqlx::Error::Protocol(format!("Error serializing \"properties\" {:?}", e))
            })?)
            .bind(raw_dime.to_vec())
    } else {
        query
            .bind(uuid)
            .bind(serde_json::to_string(properties).map_err(|e| {
                sqlx::Error::Protocol(format!("Error serializing \"properties\" {:?}", e))
            })?)
    };

    let mut tx = db.begin().await?;
    let result = query.execute(&mut tx).await?.into();
    match result {
        UpsertOutcome::Created => {
            put_object_public_keys(&mut tx, uuid, dime, dime_properties).await?;
            maybe_put_mailbox_public_keys(&mut tx, uuid, dime).await?;

            // objects that are saved via replication should not attempt to replicate again
            if replication_enabled
                && properties.get(SOURCE_KEY) != Some(&SOURCE_REPLICATION.as_bytes().to_owned())
            {
                maybe_put_replication_objects(&mut tx, uuid, replication_key_states).await?;
            }
        }
        UpsertOutcome::Noop => (),
    };
    tx.commit().await?;

    let object = get_object_by_unique_hash(db, unique_hash.as_str()).await?;

    Ok(object)
}

#[trace(name = "datastore::update_public_key")]
pub async fn update_public_key(db: &PgPool, public_key: PublicKey) -> Result<PublicKey> {
    // TODO change to compile time validated
    let record = sqlx::query_as(
        r#"
UPDATE public_key SET url = $2, metadata = $3, auth_type = $4::auth_type, auth_data = $5
WHERE public_key = $1
RETURNING uuid, public_key, public_key_type, auth_type, auth_data, url, metadata, created_at, updated_at
        "#)
        .bind(&public_key.public_key)
        .bind(&public_key.url)
        .bind(public_key.metadata.as_slice())
        .bind(&public_key.auth_type)
        .bind(&public_key.auth_data)
        .fetch_one(db)
        .await?;

    Ok(record)
}

#[trace(name = "datastore::add_public_key")]
pub async fn add_public_key(db: &PgPool, public_key: PublicKey) -> Result<PublicKey> {
    let record = sqlx::query_as(
        r#"
INSERT INTO public_key (uuid, public_key, public_key_type, auth_type, auth_data, url, metadata)
VALUES ($1, $2, $3::key_type, $4::auth_type, $5, $6, $7)
RETURNING uuid, public_key, public_key_type, auth_type, auth_data, url, metadata, created_at, updated_at
        "#)
        .bind(public_key.uuid)
        .bind(&public_key.public_key)
        .bind(&public_key.public_key_type)
        .bind(&public_key.auth_type)
        .bind(&public_key.auth_data)
        .bind(&public_key.url)
        .bind(public_key.metadata.as_slice())
        .fetch_one(db)
        .await;

    match record {
        Ok(record) => Ok(record),
        Err(sqlx::Error::Database(e)) => {
            if e.code() == Some(std::borrow::Cow::Borrowed("23505")) {
                update_public_key(db, public_key).await
            } else {
                Err(Into::<OsError>::into(sqlx::Error::Database(e)))
            }
        }
        Err(e) => Err(Into::<OsError>::into(e)),
    }
}

#[trace(name = "datastore::get_all_public_keys")]
pub async fn get_all_public_keys(db: &PgPool) -> Result<Vec<PublicKey>> {
    let mut result = Vec::new();
    let query_str = "SELECT uuid, public_key, public_key_type, url, metadata, auth_type, auth_data, created_at, updated_at FROM public_key";
    let mut result_set = sqlx::query(query_str).fetch(db);

    while let Some(row) = result_set.try_next().await? {
        let public_key = PublicKey::from_row(&row)?;
        result.push(public_key);
    }

    Ok(result)
}

#[trace(name = "datastore::health_check")]
pub async fn health_check(db: &PgPool) -> Result<()> {
    sqlx::query("SELECT 1").fetch_one(db).await?;

    Ok(())
}
