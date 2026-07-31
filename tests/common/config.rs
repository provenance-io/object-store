use std::time::Duration;

use chrono::TimeDelta;
use object_store::config::{
    Config, DatadogConfig, DbConfig, ReplicationConfig, StorageConfig, StorageType,
};

pub fn test_replication_config(
    enabled: bool,
    replication_batch_size: i32,
    backoff_min_wait: i64,
    backoff_max_wait: i64,
    snapshot_cache_refresh_frequency: TimeDelta,
) -> ReplicationConfig {
    ReplicationConfig {
        enabled,
        replication_batch_size,
        reap_unknown_keys_fixed_delay: Duration::from_secs(60 * 60),
        replicate_fixed_delay: Duration::from_secs(1),
        backoff_min_wait,
        backoff_max_wait,
        snapshot_cache_refresh_frequency,
    }
}

/// Builds a default config suitable for most tests.
///
/// Customize further in a test with struct update syntax:
/// ```no_run
/// Config {
///   health_service_enabled: true,
///   ..test_config(5432)
/// }
///
pub fn test_config(db_port: u16) -> Config {
    let dd_config = DatadogConfig {
        agent_host: "127.0.0.1".parse().unwrap(),
        agent_port: 8126,
        service: "object-store".to_owned(),
        span_tags: Vec::default(),
    };

    Config {
        url: "0.0.0.0:0".parse().unwrap(),
        uri_host: String::default(),
        db: DbConfig {
            connection_pool_size: 1,
            host: "localhost".to_owned(),
            port: db_port,
            user: "postgres".to_owned(),
            password: "postgres".to_owned(),
            database: "postgres".to_owned(),
            schema: "public".to_owned(),
        },
        storage: StorageConfig {
            storage_type: StorageType::FileSystem,
            base_url: None,
            base_path: std::env::temp_dir().to_string_lossy().to_string(),
            storage_threshold: 5000,
            health_check: false,
        },
        replication: test_replication_config(true, 2, 1, 1, chrono::Duration::minutes(5)),
        datadog: Some(dd_config),
        logging_threshold_seconds: 1,
        trace_header: String::default(),
        user_auth_enabled: false,
        health_service_enabled: false,
        maintenance_state: false.into(),
    }
}

pub fn test_config_replication(db_port: u16) -> Config {
    Config {
        replication: test_replication_config(true, 2, 0, 0, chrono::Duration::minutes(5)),
        datadog: None,
        ..test_config(db_port)
    }
}

pub fn test_config_no_replication(db_port: u16) -> Config {
    Config {
        replication: test_replication_config(false, 2, 0, 0, chrono::Duration::minutes(5)),
        datadog: None,
        ..test_config(db_port)
    }
}
