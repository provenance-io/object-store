ALTER TABLE public_key
	ADD COLUMN p8e_public_key TEXT NOT NULL,
	ADD COLUMN p8e_public_key_type key_type NOT NULL;
