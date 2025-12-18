# Action items for updated tracing library
Note: Some require upgrades of other crates and/or enabling of features

## `MinitraceGrpcMiddleware::call`
1. `span_tags` were added if parent_id == 0. Should this be re-added or should a subset of tags always be added?
2. Set `status.description` for non-Ok status_code, are there are attributes to set?
3. Set `status.code`

## Reporter
1. Configure for console locally
