ALTER TABLE public_key
	DROP COLUMN signing_public_key,
	DROP COLUMN signing_public_key_type;

ALTER TABLE public_key
	ALTER COLUMN url DROP NOT NULL;
