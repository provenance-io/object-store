CREATE TABLE object_replication
(
    uuid UUID NOT NULL,
    object_uuid UUID NOT NULL,
    public_key TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    replicated_at TIMESTAMPTZ,

    PRIMARY KEY(object_uuid, public_key),
    CONSTRAINT fk_object_uuid_opk FOREIGN KEY(object_uuid) REFERENCES object(uuid)
);

CREATE INDEX object_replication_uuid_idx ON object_replication (uuid);
CREATE INDEX object_replication_public_key_sorted_idx ON object_replication (public_key) WHERE replicated_at IS NULL;
