# Features
## Maintenance Mode
Introduced: `v1.0.0`
Puts `object-store` in read-only mode. Objects can be retrieved and replication, if enabled, will run.
Useful if 1) a migration to another instance is needed or 2) you need a read-only replica

### Usage
Enable maintenance mode
```bash
grpcurl -plaintext -d '{"maintenance_state": true}' localhost:5000 objectstore.AdminService/SetConfig
```

Disable maintenance mode
```bash
grpcurl -plaintext -d '{"maintenance_state": false}' localhost:5000 objectstore.AdminService/SetConfig
```

Get current config
```bash
grpcurl -plaintext localhost:5000 objectstore.AdminService/GetConfig
```
