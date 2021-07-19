ALTER TABLE public_key
	ADD COLUMN signing_public_key TEXT NOT NULL,
	ADD COLUMN signing_public_key_type key_type NOT NULL;

CREATE UNIQUE INDEX signing_public_key_key_unq_idx ON public_key (signing_public_key);
