CREATE TYPE key_type AS ENUM ('secp256k1');

CREATE TABLE public_key
(
    uuid UUID NOT NULL PRIMARY KEY,
    public_key TEXT NOT NULL,
    public_key_type key_type NOT NULL,
    url TEXT NOT NULL,
    metadata BYTEA,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX public_key_key_unq_idx ON public_key (public_key);
