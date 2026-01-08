-- no-transaction
CREATE INDEX CONCURRENTLY object_replication_replicated_at_idx
    ON object_replication(replicated_at)
    WHERE replicated_at is not null;
