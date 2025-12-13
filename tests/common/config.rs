use object_store::config::{Config, DatadogConfig, ReplicationConfig};

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
        db_connection_pool_size: 1,
        db_host: "localhost".to_owned(),
        db_port,
        db_user: "postgres".to_owned(),
        db_password: "postgres".to_owned(),
        db_database: "postgres".to_owned(),
        db_schema: "public".to_owned(),
        storage_type: "file_system".to_owned(),
        storage_base_url: None,
        storage_base_path: std::env::temp_dir().to_string_lossy().to_string(),
        storage_threshold: 5000,
        replication_config: ReplicationConfig::new(true, 2),
        dd_config: Some(dd_config),
        backoff_min_wait: 1,
        backoff_max_wait: 1,
        logging_threshold_seconds: 1f64,
        trace_header: String::default(),
        user_auth_enabled: false,
        health_service_enabled: false,
    }
}

pub fn test_config_replication(db_port: u16) -> Config {
    Config {
        replication_config: ReplicationConfig::new(true, 2),
        dd_config: None,
        backoff_min_wait: 5,
        backoff_max_wait: 5,
        ..test_config(db_port)
    }
}

pub fn test_config_no_replication(db_port: u16) -> Config {
    Config {
        replication_config: ReplicationConfig::new(false, 2),
        dd_config: None,
        backoff_min_wait: 5,
        backoff_max_wait: 5,
        ..test_config(db_port)
    }
}
