# Upgrade to Rust Edition 2024
Temporary file for reference as upgrading Dependencies

## Current Dependencies
Formatted output from `cargo tree --depth 1`
### Dependencies
```
async-recursion v1.0.0 (proc-macro)
async-trait v0.1.53 (proc-macro)
base64 v0.22.1
bytes v1.3.0
chrono v0.4.23
cloud-storage v0.10.3 (https://github.com/scirner22/cloud-storage-rs.git?branch=allow-base-url#a8430c02)
env_logger v0.11.8
futures v0.3.19
futures-util v0.3.19
hex v0.4.3
linked-hash-map v0.5.6
log v0.4.28
minitrace v0.2.0 (https://github.com/tikv/minitrace-rust.git#6db0d95c)
minitrace-datadog v0.1.0 (https://github.com/tikv/minitrace-rust.git#6db0d95c)
minitrace-macro v0.1.0 (proc-macro) (https://github.com/tikv/minitrace-rust.git#6db0d95c)
minstant v0.1.2
percent-encoding v2.2.0
prost v0.9.0
prost-types v0.9.0
quick-error v2.0.1
rand v0.9.2
reqwest v0.11.8
serde v1.0.228
serde_json v1.0.91
sqlx v0.6.2
tokio v1.19.2
tokio-stream v0.1.11
tonic v0.6.2
tonic-health v0.5.0
tower v0.4.13
url v2.3.1
uuid v1.2.2
```
### Build Dependencies
```
tonic-build v0.6.2
```
### Dev Dependencies
```
testcontainers v0.14.0
```

## Updates Available
`cargo outdated -R`
```
Name               Project  Compat   Latest   Kind
----               -------  ------   ------   ----
prost              0.9.0    ---      0.14.1   Normal
prost-types        0.9.0    ---      0.14.1   Normal
tonic              0.6.2    ---      0.14.2   Normal
tonic-build        0.6.2    ---      0.14.2   Build
tonic-health       0.5.0    ---      0.14.2   Normal
tower              0.4.13   ---      0.5.2    Normal
```
### Sorted
#### Minor
```
Name               Project  Compat   Latest   Kind
----               -------  ------   ------   ----
prost              0.9.0    ---      0.14.1   Normal
prost-types        0.9.0    ---      0.14.1   Normal
tonic              0.6.2    ---      0.14.2   Normal
tonic-build        0.6.2    ---      0.14.2   Build
tonic-health       0.5.0    ---      0.14.2   Normal
tower              0.4.13   ---      0.5.2    Normal
```
