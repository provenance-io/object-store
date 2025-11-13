use std::env;
use std::net::{IpAddr, SocketAddr};

use percent_encoding::NON_ALPHANUMERIC;

#[derive(Debug)]
pub struct DatadogConfig {
    pub agent_host: IpAddr,
    pub agent_port: u16,
    pub service: String,
    pub span_tags: Vec<(&'static str, String)>,
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
    pub storage_type: String,
    pub storage_base_url: Option<String>,
    pub storage_base_path: String,
    pub storage_threshold: u32,
    pub replication_enabled: bool,
    pub replication_batch_size: i32,
    pub dd_config: Option<DatadogConfig>,
    pub backoff_min_wait: i64,
    pub backoff_max_wait: i64,
    pub logging_threshold_seconds: f64,
    pub trace_header: String,
    pub user_auth_enabled: bool,
    pub health_service_enabled: bool,
}

const BASE_SPAN_TAGS: [(&str, &str); 3] = [
    ("component", "grpc-server"),
    ("language", "rust"),
    ("span.kind", "server"),
];

impl Config {
    pub fn from_env() -> Self {
        let url = env::var("OS_URL").expect("OS_URL not set");
        let uri_host = env::var("URI_HOST").expect("URI_HOST not set");
        let port: u16 = env::var("OS_PORT")
            .expect("OS_PORT not set")
            .parse()
            .expect("OS_PORT could not be parsed into a u16");
        let url = format!("{}:{}", &url, &port)
            .parse()
            .expect("url could not be parsed");
        let db_connection_pool_size = env::var("DB_CONNECTION_POOL_SIZE")
            .unwrap_or("10".to_owned())
            .parse()
            .expect("DB_CONNECTION_POOL_SIZE could not be parsed into a u16");
        let db_host = env::var("DB_HOST").expect("DB_HOST not set");
        let db_port = env::var("DB_PORT")
            .expect("DB_PORT not set")
            .parse()
            .expect("DB_PORT could not be parsed into a u16");
        let db_user = env::var("DB_USER").expect("DB_USER not set");
        let db_password = env::var("DB_PASS").expect("DB_PASS not set");
        let db_database = env::var("DB_NAME").expect("DB_NAME not set");
        let db_schema = env::var("DB_SCHEMA").expect("DB_SCHEMA not set");
        let storage_type = env::var("STORAGE_TYPE").expect("STORAGE_TYPE not set");
        let storage_base_url = env::var("STORAGE_BASE_URL").ok();
        let storage_base_path = env::var("STORAGE_BASE_PATH").expect("STORAGE_BASE_PATH not set");
        let storage_threshold = env::var("STORAGE_THRESHOLD")
            // default to ~ 5KB
            .unwrap_or("5000".to_owned())
            .parse()
            .expect("STORAGE_THRESHOLD could not be parsed into a u32");
        let replication_batch_size = env::var("REPLICATION_BATCH_SIZE")
            .unwrap_or("10".to_owned())
            .parse()
            .expect("REPLICATION_BATCH_SIZE could not be parsed into a u32");

        let dd_agent_enabled: bool = env::var("DD_AGENT_ENABLED")
            .unwrap_or("false".to_owned())
            .parse()
            .expect("DD_AGENT_ENABLED could not be parsed into a bool");

        let dd_config = if dd_agent_enabled {
            let agent_host = env::var("DD_AGENT_HOST")
                .unwrap_or("127.0.0.1".to_owned())
                .parse()
                .expect("DD_AGENT_HOST could not be parsed into an ip address");
            let agent_port = env::var("DD_AGENT_PORT")
                .unwrap_or("8126".to_owned())
                .parse()
                .expect("DD_AGENT_PORT could not be parsed into a u16");
            let service = env::var("DD_SERVICE").unwrap_or("object-store".to_owned());
            let version = env::var("DD_VERSION").unwrap_or("undefined".to_owned());
            let environment = env::var("DD_ENV").expect("DD_ENV not set");
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

        let backoff_min_wait = env::var("BACKOFF_MIN_WAIT")
            .unwrap_or("30".to_owned()) // 30 seconds
            .parse()
            .expect("BACKOFF_MIN_WAIT could not be parsed into a i64");
        let backoff_max_wait = env::var("BACKOFF_MAX_WAIT")
            .unwrap_or("1920".to_owned()) // 32 minutes
            .parse()
            .expect("BACKOFF_MAX_WAIT could not be parsed into a i64");
        let replication_enabled: bool = env::var("REPLICATION_ENABLED")
            .unwrap_or("false".to_owned())
            .parse()
            .expect("REPLICATION_ENABLED could not be parsed into a bool");
        let logging_threshold_seconds: i32 = env::var("LOGGING_THRESHOLD_SECONDS")
            .unwrap_or("3".to_owned())
            .parse()
            .expect("LOGGING_THRESHOLD_SECONDS could not be parsed into a i32");
        let logging_threshold_seconds: f64 = logging_threshold_seconds.into();
        let trace_header = env::var("TRACE_HEADER").expect("TRACE_HEADER not set");
        let user_auth_enabled: bool = env::var("USER_AUTH_ENABLED")
            .unwrap_or("false".to_owned())
            .parse()
            .expect("USER_AUTH_ENABLED could not be parsed into a bool");
        let health_service_enabled: bool = env::var("HEALTH_SERVICE_ENABLED")
            .unwrap_or("true".to_owned())
            .parse()
            .expect("HEALTH_SERVICE_ENABLED could not be parsed into a bool");

        Self {
            url,
            uri_host,
            db_connection_pool_size,
            db_host,
            db_port,
            db_user,
            db_password,
            db_database,
            db_schema,
            storage_type,
            storage_base_url,
            storage_base_path,
            storage_threshold,
            replication_batch_size,
            dd_config,
            backoff_min_wait,
            backoff_max_wait,
            replication_enabled,
            logging_threshold_seconds,
            trace_header,
            user_auth_enabled,
            health_service_enabled,
        }
    }

    pub fn db_connection_string(&self) -> String {
        let password =
            percent_encoding::percent_encode(self.db_password.as_bytes(), NON_ALPHANUMERIC);

        format!(
            "postgres://{}:{}@{}:{}/{}",
            self.db_user, password, self.db_host, self.db_port, self.db_database,
        )
    }
}
