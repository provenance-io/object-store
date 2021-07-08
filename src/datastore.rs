use crate::cache::PublicKeyState;
use crate::consts::*;
use crate::dime::Dime;
use crate::domain::DimeProperties;
use crate::types::{Result, OsError};

use bytes::Bytes;
use chrono::prelude::*;
use futures_util::TryStreamExt;
use sqlx::Acquire;
use sqlx::postgres::{PgConnection, PgPool, PgQueryResult};
use sqlx::{FromRow, Row};

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

#[derive(FromRow, Debug)]
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
    pub created_at: DateTime<Utc>,
}

#[derive(FromRow, Debug)]
pub struct ObjectPublicKey {
    pub object_uuid: uuid::Uuid,
    pub hash: String,
    pub public_key: String,
    pub created_at: DateTime<Utc>,
}

#[derive(FromRow, Debug)]
pub struct MailboxPublicKey {
    pub uuid: uuid::Uuid,
    pub object_uuid: uuid::Uuid,
    pub public_key: String,
    pub message_type: String,
    pub created_at: DateTime<Utc>,
    pub acked_at: Option<DateTime<Utc>>,
}

pub async fn get_public_key_object_uuid(db: &PgPool, hash: &str, public_key: &str) -> Result<uuid::Uuid> {
    let query_str = "SELECT object_uuid FROM object_public_key WHERE hash = $1 AND public_key = $2";
    let result = sqlx::query(query_str)
        .bind(hash)
        .bind(public_key)
        .fetch_optional(db)
        .await?;

    if let Some(row) = result {
        Ok(row.try_get("object_uuid")?)
    } else {
        Err(OsError::NotFound(format!("Unable to find object with public key {} and hash {}", public_key, hash)))
    }
}

async fn get_object_by_unique_hash(db: &PgPool, unique_hash: &str) -> Result<Object> {
    let query_str = "SELECT * FROM object WHERE md5(unique_hash) = md5($1)";
    let result = sqlx::query_as::<_, Object>(query_str)
        .bind(unique_hash)
        .fetch_one(db)
        .await?;

    Ok(result)
}

pub async fn get_object_by_uuid(db: &PgPool, uuid: &uuid::Uuid) -> Result<Object> {
    let query_str = "SELECT * FROM object WHERE uuid = $1";
    let result = sqlx::query_as::<_, Object>(query_str)
        .bind(uuid)
        .fetch_one(db)
        .await?;

    Ok(result)
}

async fn maybe_put_replication_objects(conn: &mut PgConnection, object_uuid: uuid::Uuid, dime: &Dime, replication_key_states: &Vec<PublicKeyState>) -> Result<()> {
    let mut replication_uuids: Vec<uuid::Uuid> = Vec::new();
    let mut replication_object_uuids: Vec<uuid::Uuid> = Vec::new();
    let mut staging_object_uuids: Vec<uuid::Uuid> = Vec::new();
    let mut replication_public_keys: Vec<String> = Vec::new();
    let mut staging_public_keys: Vec<String> = Vec::new();

    for (party, replication_key_state) in dime.unique_audience_without_owner_base64()?.into_iter().zip(replication_key_states.iter()) {
        match replication_key_state {
            PublicKeyState::Local => (),
            PublicKeyState::Remote => {
                replication_uuids.push(uuid::Uuid::new_v4());
                replication_object_uuids.push(object_uuid);
                replication_public_keys.push(party);
            },
            PublicKeyState::Unknown => {
                staging_object_uuids.push(object_uuid);
                staging_public_keys.push(party);
            },
        }
    }

    let remote_query_str = r#"
INSERT INTO object_replication (uuid, object_uuid, public_key)
SELECT * FROM UNNEST($1, $2, $3)
    "#;
    let staging_query_str = r#"
INSERT INTO object_replication_staging (object_uuid, public_key)
SELECT * FROM UNNEST($1, $2)
    "#;

    sqlx::query(remote_query_str)
        .bind(&replication_uuids)
        .bind(&replication_object_uuids)
        .bind(&replication_public_keys)
        .execute(conn.acquire().await?)
        .await?;

    sqlx::query(staging_query_str)
        .bind(&staging_object_uuids)
        .bind(&staging_public_keys)
        .execute(conn)
        .await?;

    Ok(())
}

async fn put_object_public_keys(conn: &mut PgConnection, object_uuid: uuid::Uuid, dime: &Dime, properties: &DimeProperties) -> Result<()> {
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

pub async fn ack_object_replication(db: &PgPool, uuid: &uuid::Uuid) -> Result<bool> {
    let query_str = r#"
UPDATE object_replication SET replication_at = $1 WHERE uuid = $2
    "#;

    let rows_affected = sqlx::query(query_str)
        .bind(Utc::now())
        .bind(&uuid)
        .execute(db)
        .await?
        .rows_affected();

    Ok(rows_affected > 0)
}

pub async fn replication_object_uuids(db: &PgPool, public_key: &str, limit: i32) -> Result<Vec<(uuid::Uuid, uuid::Uuid)>> {
    let mut result = Vec::new();
    let query_str = r#"
SELECT uuid, object_uuid FROM object_replication
  WHERE public_key = $1 AND replicated_at IS NULL
  LIMIT $2
    "#;

    let mut query_result = sqlx::query(query_str)
        .bind(public_key)
        .bind(limit)
        .fetch(db);

    while let Some(row) = query_result.try_next().await? {
        let uuid = row.try_get("uuid")?;
        let object_uuid = row.try_get("object_uuid")?;
        result.push((uuid, object_uuid));
    }

    Ok(result)
}

pub async fn stream_mailbox_public_keys(db: &PgPool, public_key: &str, limit: i32) -> Result<Vec<(uuid::Uuid, Object)>> {
    let mut result = Vec::new();
    let query_str = r#"
SELECT mpk.uuid mpk_uuid, o.uuid, o.dime_uuid, hash, unique_hash, content_length, dime_length, directory, name, payload, o.created_at
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

async fn maybe_put_mailbox_public_keys(conn: &mut PgConnection, object_uuid: uuid::Uuid, dime: &Dime) -> Result<()> {
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

pub async fn ack_mailbox_public_key(db: &PgPool, uuid: &uuid::Uuid) -> Result<bool> {
    let query_str = r#"
UPDATE mailbox_public_key SET acked_at = $1 WHERE uuid = $2
    "#;

    let rows_affected = sqlx::query(query_str)
        .bind(Utc::now())
        .bind(&uuid)
        .execute(db)
        .await?
        .rows_affected();

    Ok(rows_affected > 0)
}

pub async fn put_object(db: &PgPool, dime: &Dime, properties: &DimeProperties, replication_key_states: &Vec<PublicKeyState>, raw_dime: Option<&Bytes>) -> Result<Object> {
    let mut unique_hash = dime.unique_audience_base64()?;
    unique_hash.sort_unstable();
    unique_hash.insert(0, String::from(&properties.hash));
    let unique_hash = unique_hash.join(";");

    let query_str = if raw_dime.is_some() {
        r#"
INSERT INTO object (uuid, dime_uuid, hash, unique_hash, content_length, dime_length, directory, name, payload)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT DO NOTHING
        "#
    } else {
        r#"
INSERT INTO object (uuid, dime_uuid, hash, unique_hash, content_length, dime_length, name)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT DO NOTHING
        "#
    };

    let uuid = uuid::Uuid::new_v4();
    let query = sqlx::query(query_str)
        .bind(&uuid)
        .bind(&dime.uuid)
        .bind(&properties.hash)
        .bind(&unique_hash)
        .bind(&properties.content_length)
        .bind(&properties.dime_length);
    let query = if let Some(raw_dime) = raw_dime {
        query.bind(NOT_STORAGE_BACKED)
            .bind(NOT_STORAGE_BACKED)
            .bind(raw_dime.to_vec())
    } else {
        query.bind(&uuid)
    };

    let mut tx = db.begin().await?;
    let result = query.execute(&mut tx).await?.into();
    match result {
        UpsertOutcome::Created => {
            put_object_public_keys(&mut tx, uuid, dime, properties).await?;
            maybe_put_mailbox_public_keys(&mut tx, uuid, dime).await?;
            maybe_put_replication_objects(&mut tx, uuid, dime, replication_key_states).await?;
        },
        UpsertOutcome::Noop => (),
    };
    tx.commit().await?;

    let object = get_object_by_unique_hash(db, unique_hash.as_str()).await?;

    Ok(object)
}

pub async fn health_check(db: &PgPool) -> Result<()> {
    sqlx::query("SELECT 1")
        .fetch_one(db)
        .await?;

    Ok(())
}
