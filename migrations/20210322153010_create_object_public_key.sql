CREATE TABLE object_public_key
(
    object_uuid UUID NOT NULL,
    hash TEXT NOT NULL,
    public_key TEXT NOT NULL,
    signature TEXT,
    signature_public_key TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY(object_uuid, public_key),
    CONSTRAINT fk_object_uuid_opk FOREIGN KEY(object_uuid) REFERENCES object(uuid)
);
