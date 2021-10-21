# Object Store

## Status

[![stability-release-candidate](https://img.shields.io/badge/stability-pre--release-48c9b0.svg)](https://github.com/mkenney/software-guides/blob/master/STABILITY-BADGES.md#release-candidate)
[![Latest Release][release-badge]][release-latest]
[![License][license-badge]][license-url]
[![LOC][loc-badge]][loc-report]

[release-badge]: https://img.shields.io/github/v/tag/provenance-io/p8e-scope-sdk.svg?sort=semver
[release-latest]: https://github.com/provenance-io/p8e-scope-sdk/releases/latest

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

- Strong end-to-end encryption when combined with load balancer like `Nginx`.
- Peer-to-peer replication to parties (third party `object-store`s) you want to share data with.
- Publishes traces to Datadog.

## Backends

This service was designed to support many underlying storage backends. The currently supported backends are `postgres`, `google cloud storage`, and the local `file system`.
In practice, a sizeable number of objects this system stores are very small. For this reason the `postgres` backend, along with a byte threshold, is provided
so that items smaller than the threshold specified can have thier bytes stored directly in the database.
