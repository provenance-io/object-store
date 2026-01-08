-- no-transaction
CREATE INDEX CONCURRENTLY object_public_key_public_key_idx
    ON object_public_key(public_key);
