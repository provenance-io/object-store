ALTER TABLE object DROP CONSTRAINT unique_hash_cnst;

CREATE UNIQUE INDEX object_unique_hash_idx ON object (md5(unique_hash));
