use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use chrono::TimeDelta;
use percent_encoding::NON_ALPHANUMERIC;

mod env_var;
use crate::config::env_var::{env_var, env_var_opt, env_var_or, env_var_parse, env_var_parse_or};

#[derive(Debug)]
pub struct DatadogConfig {
    pub agent_host: IpAddr,
    pub agent_port: u16,
    pub service: String,
    pub span_tags: Vec<(&'static str, String)>,
}

#[derive(Clone, Debug)]
pub struct ReplicationConfig {
    pub replication_enabled: bool,
    pub replication_batch_size: i32,
    pub reap_unknown_keys_fixed_delay: Duration,
    pub replicate_fixed_delay: Duration,
    pub backoff_min_wait: i64,
    pub backoff_max_wait: i64,
    pub snapshot_cache_refresh_frequency: TimeDelta,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StorageType {
    FileSystem = 0,
    GoogleCloud = 1,
}

impl FromStr for StorageType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "file_system" => Ok(StorageType::FileSystem),
            "google_cloud" => Ok(StorageType::GoogleCloud),
            _ => Err(format!("Invalid storage: {}", s)),
        }
    }
}

#[derive(Clone, Debug)]
pub struct StorageConfig {
    pub storage_type: StorageType,
    pub storage_base_url: Option<String>,
    pub storage_base_path: String,
    /// Objects with size, in bytes, below this threshold will be stored in database.
    /// Larger objects will be in configured storage
    pub storage_threshold: usize,
    pub health_check: bool,
}

impl StorageConfig {
    pub fn from_env() -> Self {
        let storage_type = env_var_parse("STORAGE_TYPE");
        let storage_base_url = env_var_opt("STORAGE_BASE_URL");
        let storage_base_path = env_var("STORAGE_BASE_PATH");
        let storage_threshold = env_var_parse_or("STORAGE_THRESHOLD", 5000); // 5KB
        let health_check = env_var_parse_or("STORAGE_HEALTH_CHECK", false);

        Self {
            storage_type,
            storage_base_url,
            storage_base_path,
            storage_threshold,
            health_check,
        }
    }
}

impl ReplicationConfig {
    pub fn new(
        replication_enabled: bool,
        replication_batch_size: i32,
        backoff_min_wait: i64,
        backoff_max_wait: i64,
        snapshot_cache_refresh_frequency: TimeDelta,
    ) -> Self {
        Self {
            replication_enabled,
            replication_batch_size,
            reap_unknown_keys_fixed_delay: Duration::from_secs(60 * 60),
            replicate_fixed_delay: Duration::from_secs(1),
            backoff_min_wait,
            backoff_max_wait,
            snapshot_cache_refresh_frequency,
        }
    }
}

#[derive(Debug)]
pub struct Config {
    pub url: SocketAddr,
    pub uri_host: String,
    pub db_connection_pool_size: u16,
    pub db_host: String,
    pub db_port: u16,
    pub db_user: String,
    pub db_password: String,
    pub db_database: String,
    pub db_schema: String,
    pub storage_config: StorageConfig,
    pub replication_config: ReplicationConfig,
    /// If None, trace middleware [MinitraceGrpcMiddlewareLayer][crate::middleware::MinitraceGrpcMiddlewareLayer] disabled
    pub dd_config: Option<DatadogConfig>,
    pub logging_threshold_seconds: f64,
    pub trace_header: String,
    pub user_auth_enabled: bool,
    pub health_service_enabled: bool,
    /// Runtime maintenance mode state. When true, write operations are rejected.
    pub maintenance_state: AtomicBool,
}

const BASE_SPAN_TAGS: [(&str, &str); 3] = [
    ("component", "grpc-server"),
    ("language", "rust"),
    ("span.kind", "server"),
];

impl Config {
    pub fn from_env() -> Arc<Self> {
        let url = env_var("OS_URL");
        let uri_host = env_var("URI_HOST");
        let port: u16 = env_var_parse("OS_PORT");
        let url = format!("{}:{}", url, port)
            .parse()
            .expect("url could not be parsed");

        let db_connection_pool_size = env_var_parse_or("DB_CONNECTION_POOL_SIZE", 10);
        let db_host = env_var("DB_HOST");
        let db_port = env_var_parse("DB_PORT");
        let db_user = env_var("DB_USER");
        let db_password = env_var("DB_PASS");
        let db_database = env_var("DB_NAME");
        let db_schema = env_var("DB_SCHEMA");

        let replication_batch_size = env_var_parse_or("REPLICATION_BATCH_SIZE", 10);

        let dd_agent_enabled: bool = env_var_parse_or("DD_AGENT_ENABLED", false);

        let dd_config = if dd_agent_enabled {
            let agent_host: IpAddr =
                env_var_parse_or("DD_AGENT_HOST", Ipv4Addr::new(127, 0, 0, 1).into());
            let agent_port = env_var_parse_or("DD_AGENT_PORT", 8126);

            let service = env_var_or("DD_SERVICE", "object-store");
            let version = env_var_or("DD_VERSION", "undefined");
            let environment = env_var("DD_ENV");

            let mut span_tags = Vec::default();
            span_tags.extend(BASE_SPAN_TAGS.into_iter().map(|(k, v)| (k, v.to_string())));
            span_tags.push(("app", service.clone()));
            span_tags.push(("version", version));
            span_tags.push(("env", environment));

            Some(DatadogConfig {
                agent_host,
                agent_port,
                service,
                span_tags,
            })
        } else {
            None
        };

        let backoff_min_wait = env_var_parse_or("BACKOFF_MIN_WAIT", 30); // 30 seconds
        let backoff_max_wait = env_var_parse_or("BACKOFF_MAX_WAIT", 1920); // 32 minutes
        let replication_enabled: bool = env_var_parse_or("REPLICATION_ENABLED", false);
        let replication_cache_refresh_min: i64 =
            env_var_parse_or("REPLICATION_CACHE_REFRESH_MIN", 5);
        let replication_reap_unknown_keys_fixed_delay_seconds: u64 =
            env_var_parse_or("REPLICATION_REAP_UNKNOWN_KEYS_FIXED_DELAY_SECONDS", 3600);
        let replication_replicate_fixed_delay_seconds: u64 =
            env_var_parse_or("REPLICATION_REPLICATE_FIXED_DELAY_SECONDS", 1);

        let logging_threshold_seconds: f64 =
            env_var_parse_or::<i32>("LOGGING_THRESHOLD_SECONDS", 3).into();

        let trace_header = env_var("TRACE_HEADER");
        let user_auth_enabled = env_var_parse_or("USER_AUTH_ENABLED", false);
        let health_service_enabled = env_var_parse_or("HEALTH_SERVICE_ENABLED", true);
        let maintenance_state = env_var_parse_or("MAINTENANCE_STATE", false);

        let replication_config = ReplicationConfig {
            replication_enabled,
            replication_batch_size,
            reap_unknown_keys_fixed_delay: Duration::from_secs(
                replication_reap_unknown_keys_fixed_delay_seconds,
            ),
            replicate_fixed_delay: Duration::from_secs(replication_replicate_fixed_delay_seconds),
            backoff_min_wait,
            backoff_max_wait,
            snapshot_cache_refresh_frequency: chrono::Duration::minutes(
                replication_cache_refresh_min,
            ),
        };

        Arc::new(Self {
            url,
            uri_host,
            db_connection_pool_size,
            db_host,
            db_port,
            db_user,
            db_password,
            db_database,
            db_schema,
            storage_config: StorageConfig::from_env(),
            replication_config,
            dd_config,
            logging_threshold_seconds,
            trace_header,
            user_auth_enabled,
            health_service_enabled,
            maintenance_state: AtomicBool::new(maintenance_state),
        })
    }

    pub fn db_connection_string(&self) -> String {
        let password =
            percent_encoding::percent_encode(self.db_password.as_bytes(), NON_ALPHANUMERIC);

        format!(
            "postgres://{}:{}@{}:{}/{}",
            self.db_user, password, self.db_host, self.db_port, self.db_database,
        )
    }

    pub fn is_maintenance_state(&self) -> bool {
        self.maintenance_state.load(Ordering::Relaxed)
    }

    pub fn set_maintenance_state(&self, enabled: bool) {
        self.maintenance_state.store(enabled, Ordering::Relaxed);
    }
}
