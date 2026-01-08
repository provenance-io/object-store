-- no-transaction
CREATE INDEX CONCURRENTLY object_replication_status_idx
    ON object_replication(object_uuid, public_key, replicated_at);
