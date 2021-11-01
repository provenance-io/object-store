CREATE TYPE auth_type AS ENUM ('header');

ALTER TABLE public_key
	ADD COLUMN auth_type auth_type,
	ADD COLUMN auth_data TEXT;
