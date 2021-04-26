CREATE TABLE object
(
    uuid UUID NOT NULL PRIMARY KEY,
    hash TEXT NOT NULL,
    unique_hash TEXT NOT NULL,
    content_length BIGINT NOT NULL,
    bucket TEXT NOT NULL,
    name TEXT NOT NULL,
    payload BYTEA,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX object_unique_hash_idx ON object (unique_hash);

ALTER TABLE object ADD CONSTRAINT unique_hash_cnst UNIQUE USING INDEX object_unique_hash_idx;
