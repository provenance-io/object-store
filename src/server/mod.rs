mod health;
mod trace;

use std::sync::Arc;

pub use health::*;
use tonic::transport::Server;
pub use trace::*;

use crate::{config::Config, middleware::LoggingMiddlewareLayer};

pub fn base_server(config: Arc<Config>) -> Server<LoggingMiddlewareLayer> {
    Server::builder().layer(LoggingMiddlewareLayer::new(config))
}
