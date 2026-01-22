use std::{net::AddrParseError, time::Duration};

use fastrace::collector::Config;
use fastrace_datadog::DatadogReporter;

use crate::config::DatadogConfig;

/// See all standard attributes: <https://docs.datadoghq.com/standard-attributes/>
pub fn start_trace_reporter(dd_config: &DatadogConfig) {
    let socket = dd_config.agent_addr().unwrap();

    let reporter = DatadogReporter::new(
        dd_config.agent_addr().unwrap(),
        dd_config.service.clone(),
        "all",
        "select",
    );

    log::info!("Starting Datadog reporting to agent at {}", socket);

    let config = Config::default().report_interval(Duration::from_millis(5000));

    fastrace::set_reporter(reporter, config);
}

impl DatadogConfig {
    fn agent_addr(&self) -> Result<std::net::SocketAddr, AddrParseError> {
        format!("{}:{}", self.agent_host, self.agent_port).parse()
    }
}

#[cfg(test)]
mod tests {
    use crate::config::DatadogConfig;

    #[test]
    fn addr_parse() {
        let dd_config = DatadogConfig {
            agent_host: "127.0.0.1".parse().unwrap(),
            agent_port: 8126,
            service: "object-store".to_owned(),
            span_tags: Vec::default(),
        };

        let addr = dd_config.agent_addr().unwrap();

        assert_eq!(addr.port(), 8126);
        assert_eq!(addr.ip().to_string(), "127.0.0.1");
    }
}
