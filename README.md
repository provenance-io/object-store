# Object Store

## Status

[![stability-release-candidate](https://img.shields.io/badge/stability-pre--release-48c9b0.svg)](https://github.com/mkenney/software-guides/blob/master/STABILITY-BADGES.md#release-candidate)
[![Latest Release][release-badge]][release-latest]
[![License][license-badge]][license-url]
[![LOC][loc-badge]][loc-report]

[release-badge]: https://img.shields.io/github/v/tag/provenance-io/object-store.svg?sort=semver
[release-latest]: https://github.com/provenance-io/object-store/releases/latest

[license-badge]: https://img.shields.io/github/license/provenance-io/object-store.svg
[license-url]: https://github.com/provenance-io/object-store/blob/main/LICENSE

[loc-badge]: https://tokei.rs/b1/github/provenance-io/object-store
[loc-report]: https://github.com/provenance-io/object-store

A object storage system with a gRPC interface and strong encryption properties. Currently, only the
[Provenance DIME](https://docs.provenance.io/p8e/overview/encrypted-object-store/dime-encryption-envelope-specification)
format is accepted. Supporting material can be found [here](https://docs.provenance.io/p8e/overview/encrypted-object-store).

This service can be used directly, but the most common case is to use this alongside the [P8e Execution Environment](https://github.com/provenance-io/p8e-scope-sdk)
in order to process Provenance scopes and memorialize them on chain.

## Features

- Strong end-to-end encryption.
- Peer-to-peer replication to parties (third party `object-store`s) you want to share data with.
- Capable of publishing traces to Datadog.

## Authentication

gRPC metadata based authentication is provided on a per key basis. When adding a public key to the database, an `auth_type` and `auth_data` can be provided. These
can either be leveraged directly as an api key or indirectly be combining it with a proxy capable of authentication and header forwarding. Setting both of these fields
to `null` and a service level config property of `USER_AUTH_ENABLED=false` disables all authentication - this can be used if the object store is meant for internal use
and not exposed publicly.

- Example authentication configuration

NOTE: Requires settings the service level configuration to `USER_AUTH_ENABLED=true`.

```
public_key=BH6YrLjN+I7JzjGCgrIWbfXicg4C4nZaMPwzmTB2Yef/aqxiJmPmpBi1JAonlTzA6c1zU/WX4RKWzAkQBd7lWbU=
public_key_type=secp256k1
auth_type=header
auth_data=x-custom-header:6eace982-f682-4b1d-9f8e-82ed9ab15813
```

With such a configuration all requests for this public key will have to contain this metadata.

## Backends

This service was designed to support many underlying storage backends. The currently supported backends are `postgres`, `google cloud storage`, and the local `file system`.
In practice, a sizeable number of objects this system stores are very small. For this reason the `postgres` backend, along with a byte threshold, is provided
so that items smaller than the threshold specified can have thier bytes stored directly in the database.

## Local Development

The minimum required environment variables can be sourced from `./bin/env`. A postgres database connection is also required. The simplest way to get up and running is to leverage the container [here](https://github.com/provenance-io/p8e-scope-sdk/tree/main/dev-tools/compose) with `docker-compose up postgres -d`.
