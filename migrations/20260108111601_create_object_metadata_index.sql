-- no-transaction
CREATE INDEX CONCURRENTLY object_metadata_idx
    ON object(created_at, content_length)
    WHERE directory <> 'NOT_STORAGE_BACKED';
