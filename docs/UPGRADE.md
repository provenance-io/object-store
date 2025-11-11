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
serial_test v0.6.0
testcontainers v0.14.0
```

## Updates Available
`cargo outdated -R`
```
Name               Project  Compat   Latest   Kind
----               -------  ------   ------   ----
async-recursion    1.0.0    1.1.1    1.1.1    Normal
async-trait        0.1.53   0.1.89   0.1.89   Normal
bytes              1.3.0    1.10.1   1.10.1   Normal
chrono             0.4.23   0.4.42   0.4.42   Normal
futures            0.3.19   0.3.31   0.3.31   Normal
futures-util       0.3.19   0.3.31   0.3.31   Normal
minitrace          0.2.0    0.6.7    0.6.7    Normal
minitrace-datadog  0.1.0    0.6.7    0.6.7    Normal
minitrace-macro    0.1.0    0.6.7    0.6.7    Normal
minstant           0.1.2    0.1.7    0.1.7    Normal
percent-encoding   2.2.0    2.3.2    2.3.2    Normal
prost              0.9.0    ---      0.14.1   Normal
prost-types        0.9.0    ---      0.14.1   Normal
reqwest            0.11.8   0.11.27  0.12.24  Normal
serde_json         1.0.91   1.0.145  1.0.145  Normal
serial_test        0.6.0    3.2.0    3.2.0    Development
sqlx               0.6.3    0.6.3    0.8.6    Normal
testcontainers     0.14.0   ---      0.25.2   Development
tokio              1.19.2   1.48.0   1.48.0   Normal
tokio-stream       0.1.11   0.1.17   0.1.17   Normal
tonic              0.6.2    ---      0.14.2   Normal
tonic-build        0.6.2    ---      0.14.2   Build
tonic-health       0.5.0    ---      0.14.2   Normal
tower              0.4.13   ---      0.5.2    Normal
url                2.3.1    2.5.7    2.5.7    Normal
uuid               1.2.2    1.18.1   1.18.1   Normal
```
### Sorted
#### Patch
```
Name               Project  Compat   Latest   Kind
----               -------  ------   ------   ----
async-trait        0.1.53   0.1.89   0.1.89   Normal
chrono             0.4.23   0.4.42   0.4.42   Normal
futures            0.3.19   0.3.31   0.3.31   Normal
futures-util       0.3.19   0.3.31   0.3.31   Normal
minstant           0.1.2    0.1.7    0.1.7    Normal
serde_json         1.0.91   1.0.145  1.0.145  Normal
tokio-stream       0.1.11   0.1.17   0.1.17   Normal
```
#### Minor
```
Name               Project  Compat   Latest   Kind
----               -------  ------   ------   ----
async-recursion    1.0.0    1.1.1    1.1.1    Normal
bytes              1.3.0    1.10.1   1.10.1   Normal
minitrace          0.2.0    0.6.7    0.6.7    Normal
minitrace-datadog  0.1.0    0.6.7    0.6.7    Normal
minitrace-macro    0.1.0    0.6.7    0.6.7    Normal
prost              0.9.0    ---      0.14.1   Normal
prost-types        0.9.0    ---      0.14.1   Normal
reqwest            0.11.8   0.11.27  0.12.24  Normal
sqlx               0.6.3    0.6.3    0.8.6    Normal
testcontainers     0.14.0   ---      0.25.2   Development
tokio              1.19.2   1.48.0   1.48.0   Normal
tonic              0.6.2    ---      0.14.2   Normal
tonic-build        0.6.2    ---      0.14.2   Build
tonic-health       0.5.0    ---      0.14.2   Normal
tower              0.4.13   ---      0.5.2    Normal
url                2.3.1    2.5.7    2.5.7    Normal
uuid               1.2.2    1.18.1   1.18.1   Normal
```
#### Major
```
Name               Project  Compat   Latest   Kind
----               -------  ------   ------   ----
serial_test        0.6.0    3.2.0    3.2.0    Development
```
### Patch upgrades
Per LLM:
Here are the major changes for each upgrade:

#### serde_json (1.0.91 → 1.0.145)
**Documentation:** https://docs.rs/serde_json
**Releases:** https://github.com/serde-rs/json/releases

- [ ] **1.0.96**: Added `Map::shift_insert` and `Map::shift_remove`
- [x] **1.0.108**: Performance improvements for string escaping
- [ ] **1.0.116+**: Better error messages with context
- [ ] **1.0.120+**: Improved handling of arbitrary precision numbers
- [ ] **1.0.132**: `Value::pointer_mut` added for mutable JSON pointer access
- [ ] **1.0.145**: Raise serde version requirement to >=1.0.220
- [x] **Throughout**: Significant performance optimizations, especially for serialization

#### tokio-stream (0.1.11 → 0.1.17)
**Documentation:** https://docs.rs/tokio-stream
**Changelog:** https://github.com/tokio-rs/tokio/blob/master/tokio-stream/CHANGELOG.md

- [ ] **0.1.12**: Added `StreamExt::take_until`
- [ ] **0.1.14**: Better integration with `tokio::sync` primitives, minimum version of Tokio 1.15
- [ ] **0.1.15**: New `StreamExt::then` combinator improvements
- [ ] **Throughout**: Bug fixes and API refinements aligned with Tokio's evolution
