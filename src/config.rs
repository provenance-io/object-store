use std::env;
use std::net::SocketAddr;

#[derive(Debug)]
pub struct Config {
    pub url: SocketAddr,
    pub db_connection_pool_size: u32,
    pub db_host: String,
    pub db_port: u16,
    pub db_user: String,
    pub db_password: String,
    pub db_database: String,
    pub db_schema: String,
}

impl Config {
    pub fn new() -> Self {
        let url = env::var("OS_URL").expect("OS_URL not set");
        let port: u16 = env::var("OS_PORT")
            .expect("OS_PORT not set")
            .parse()
            .expect("OS_PORT could not be parsed into a u16");
        let url = format!("{}:{}", &url, &port).parse().expect("url could not be parsed");
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
        let db_password = env::var("DB_PASSWORD").expect("DB_PASSWORD not set");
        let db_database = env::var("DB_DATABASE").expect("DB_DATABASE not set");
        let db_schema = env::var("DB_SCHEMA").expect("DB_SCHEMA not set");

        Self { url, db_connection_pool_size, db_host, db_port, db_user, db_password, db_database, db_schema }
    }

    pub fn db_connection_string(&self) -> String {
        format!(
            "postgres://{}:{}@{}/{}",
            self.db_user,
            self.db_password,
            self.db_host,
            self.db_database,
        )
    }
}
