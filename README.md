# Object Store
An object storage system with strong encryption properties and peer-to-peer replication
[![stability-release-candidate](https://img.shields.io/badge/stability-pre--release-48c9b0.svg)](https://github.com/mkenney/software-guides/blob/master/STABILITY-BADGES.md#release-candidate)
[![Latest Release][release-badge]][release-latest]
[![License][license-badge]][license-url]

[release-badge]: https://img.shields.io/github/v/tag/provenance-io/object-store.svg?sort=semver
[release-latest]: https://github.com/provenance-io/object-store/releases/latest
[license-badge]: https://img.shields.io/github/license/provenance-io/object-store.svg
[license-url]: https://github.com/provenance-io/object-store/blob/main/LICENSE

## Features
- Strong end-to-end encryption - Currently, only the [Provenance DIME](https://developer.provenance.io/docs/pb/p8e/overview/encrypted-object-store/dime-encryption-envelope-specification/) format is accepted. Supporting material can be found [here](https://developer.provenance.io/docs/pb/p8e/overview/encrypted-object-store/).
- Peer-to-peer replication to parties (third party `object-store`s) you want to share data with.
- Configurable storage backends
- Tracing with [Datadog](https://docs.datadoghq.com).
- [Additional features](./docs/FEATURES.md)

## Overview
An object store can be used directly, but the most common case is to use this alongside the [P8e Execution Environment](https://github.com/provenance-io/p8e-scope-sdk) in order to process Provenance scopes and memorialize them on chain.

### Authentication
gRPC metadata based authentication is provided on a per key basis.
When adding a public key to the database, an `auth_type` and `auth_data` can be provided.
These can either be leveraged directly as an api key or indirectly be combining it with a proxy capable of authentication and header forwarding.
Setting both of these fields to `null` and a service level config property of `USER_AUTH_ENABLED=false` disables all authentication - this can be used if the object store is meant for internal use and not exposed publicly.

#### Example authentication configuration
NOTE: Requires settings the service level configuration to `USER_AUTH_ENABLED=true`.

```
public_key=BH6YrLjN+I7JzjGCgrIWbfXicg4C4nZaMPwzmTB2Yef/aqxiJmPmpBi1JAonlTzA6c1zU/WX4RKWzAkQBd7lWbU=
public_key_type=secp256k1
auth_type=header
auth_data=x-custom-header:6eace982-f682-4b1d-9f8e-82ed9ab15813
```

With such a configuration, all requests for this public key _must_ contain this metadata.

### Storage
This system supports different storage backends. Currently native file system and Google Cloud are supported.
In practice, many objects are very small. Therefore the `postgres` backend, along with a byte threshold, is provided where items smaller than the threshold are stored directly in the database.

## Development
The most common use case for `object-store` is to run it alongside `p8e` in order to write to the [Provenance blockchain](https://provenance.io).
The simplest way to get this up and running is [here](https://github.com/provenance-io/p8e-scope-sdk/tree/main/dev-tools/compose).

### Running Locally
A postgres connection and data directory is required to run locally.
A base set of environment variables are defined in [.cargo/config.toml](./.cargo/config.toml).
Then, specify additional config with one of the sibling config files and/or env variables.

**Run with file system storage**
```shell
cargo --config .cargo/fs.config.toml run       
```
**Run with google cloud storage and health check**
```shell
STORAGE_HEALTH_CHECK=true STORAGE_BASE_PATH=my-custom-bucket \
  cargo --config .cargo/google_cloud.config.toml run     
```

### Contributing
It is recommended to have `rust-analyzer` installed and configured for your IDE.
[Install instructions](https://rust-analyzer.github.io/book/installation.html).
Basic configuration is included for VS Code.
