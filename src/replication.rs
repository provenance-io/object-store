use crate::cache::Cache;
use crate::consts;
use crate::datastore;
use crate::storage::{FileSystem, StoragePath};
use crate::proto_helpers::{create_data_chunk, create_multi_stream_header, create_stream_header_field, create_stream_end};

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use chrono::prelude::*;
use futures::stream;
use sqlx::postgres::PgPool;

// TODO need to reap replication objects that were really local
// TODO backoff failed connections

// object-store=# \d object_replication;
//                   Table "object-store.object_replication"
//     Column     |           Type           | Collation | Nullable | Default
// ---------------+--------------------------+-----------+----------+---------
//  uuid          | uuid                     |           | not null |
//  object_uuid   | uuid                     |           | not null |
//  public_key    | text                     |           | not null |
//  created_at    | timestamp with time zone |           | not null | now()
//  replicated_at | timestamp with time zone |           |          |
// Indexes:
//     "object_replication_pkey" PRIMARY KEY, btree (object_uuid, public_key)
//     "object_replication_public_key_sorted_idx" btree (public_key) WHERE replicated_at IS NULL
//     "object_replication_uuid_idx" btree (uuid)
// Foreign-key constraints:
//     "fk_object_uuid_opk" FOREIGN KEY (object_uuid) REFERENCES object(uuid)

#[derive(Debug)]
pub struct ReplicationState {
    cache: Arc<Mutex<Cache>>,
    snapshot_cache: (DateTime<Utc>, Cache),
    db_pool: Arc<PgPool>,
    storage: Arc<FileSystem>,
}

impl ReplicationState {
    pub fn new(cache: Arc<Mutex<Cache>>, db_pool: Arc<PgPool>, storage: Arc<FileSystem>) -> Self {
        let snapshot_cache = cache.lock().unwrap().clone();
        let snapshot_cache = (Utc::now(), snapshot_cache);

        Self { cache, snapshot_cache, db_pool, storage }
    }
}

// TODO return Err in case we can't replicate to the remote
// this will allow replicate_iteration to clean up the working cache of connections and backoff
// for that key
pub async fn replicate_public_key(inner: &ReplicationState, public_key: &String, url: &String) {
    // X - fetch batch of replication objects
    // pull objects and create requests
    // replicate to the remote - take in this connection as an added parameter
    // X - mark replication object as replicated

    // TODO make batch size configurable
    // TODO move to ? for all await
    let batch = datastore::replication_object_uuids(&inner.db_pool, public_key, 10i32).await.unwrap();

    for (uuid, object_uuid) in batch.iter() {
        let object = datastore::get_object_by_uuid(&inner.db_pool, &object_uuid).await.unwrap();
        let payload = if let Some(payload) = &object.payload {
            Bytes::copy_from_slice(payload.as_slice())
        } else {
            let storage_path = StoragePath {
                dir: object.directory.clone(),
                file: object.name.clone(),
            };
            let payload = inner.storage.fetch(&storage_path, object.dime_length as u64).await.unwrap();
                // .map_err(Into::<OsError>::into)?;

            Bytes::copy_from_slice(payload.as_slice())
        };
        let mut packets = Vec::new();

        packets.push(create_multi_stream_header(uuid::Uuid::nil(), 1, true));
        packets.push(create_stream_header_field(consts::HASH_FIELD_NAME.to_owned(), object.hash.as_bytes().to_owned()));
        // TODO how to get this?
        // packets.push(create_stream_header_field(consts::SIGNATURE_FIELD_NAME.to_owned(), object.as_bytes.as_bytes()));
        // packets.push(create_stream_header_field(consts::SIGNATURE_PUBLIC_KEY_FIELD_NAME.to_owned(), object.as_bytes.as_bytes()));

        for (idx, chunk) in payload.chunks(consts::CHUNK_SIZE).enumerate() {
            let content_length = if idx == 0 {
                Some(object.content_length)
            } else {
                None
            };

            packets.push(create_data_chunk(content_length, chunk.to_vec()));
        }

        packets.push(create_stream_end());

        // TODO add something so we don't replicate objects that are already replicating

        let stream = stream::iter(packets);

        datastore::ack_object_replication(&inner.db_pool, &uuid).await.unwrap();
    }
}

pub async fn replicate_iteration(inner: &mut ReplicationState) {
        // update snapshot cache every 5 minutes
        let last_cache_update = &inner.snapshot_cache.0;
        if last_cache_update.time() + chrono::Duration::minutes(5) < Utc::now().time() {
            log::trace!("Updating snapshot cache");

            let snapshot_cache = inner.cache.lock().unwrap().clone();
            inner.snapshot_cache = (Utc::now(), snapshot_cache);
        }

        // TODO change to creating all of these tasks at once
        for (public_key, url) in &inner.snapshot_cache.1.remote_public_keys {
            replicate_public_key(&inner, public_key, url).await;
        }
}

pub async fn replicate(mut inner: ReplicationState) {
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        replicate_iteration(&mut inner).await;
    }
}
