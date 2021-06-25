CREATE TABLE object_replication_staging
(
    object_uuid UUID NOT NULL,
    public_key TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY(public_key, object_uuid),
    CONSTRAINT fk_object_uuid_opk FOREIGN KEY(object_uuid) REFERENCES object(uuid)
);
