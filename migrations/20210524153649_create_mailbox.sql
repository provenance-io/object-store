CREATE TABLE mailbox_public_key
(
    uuid UUID NOT NULL,
    object_uuid UUID NOT NULL,
    public_key TEXT NOT NULL,
    message_type TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    acked_at TIMESTAMPTZ,

    PRIMARY KEY(object_uuid, public_key, message_type),
    CONSTRAINT fk_object_uuid_opk FOREIGN KEY(object_uuid) REFERENCES object(uuid)
);

CREATE INDEX mailbox_public_key_uuid_idx ON mailbox_public_key (uuid);
CREATE INDEX mailbox_public_key_sorted_idx ON mailbox_public_key (public_key) WHERE acked_at IS NULL;
