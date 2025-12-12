use std::{net::AddrParseError, time::Duration};

use fastrace::collector::ConsoleReporter;

use crate::config::DatadogConfig;

// https://docs.datadoghq.com/standard-attributes/
// service	string	The unified service name for the application or service that is generating the data, used to correlate user sessions.
// db.instance/statement/user/sql.table/row_count/operation/system
// error.type/message/stack
// rpc.system/service/method

pub fn start_trace_reporter(dd_config: &DatadogConfig) {
    // TODO: header managment?
    // headers.append("Datadog-Meta-Tracer-Version", "v1.27.0".parse().unwrap());
    // headers.append("Content-Type", "application/msgpack".parse().unwrap());
    // old way posted via reqwest client to http://{}/v0.4/traces

    // TODO: configure DD/Console by env
    if false {
        fastrace_datadog::DatadogReporter::new(
            dd_config.agent_addr().unwrap(),
            dd_config.service.clone(),
            "all",
            "select",
        );
    }

    // TODO: add logging - moved from grpc
    // log::info!("Starting Datadog reporting to agent at {}", socket);

    // TODO: warn on errors?
    let config = fastrace::collector::Config::default().report_interval(Duration::from_millis(0));
    // fastrace::collector::Config::default().report_interval(Duration::from_millis(5000));
    fastrace::set_reporter(ConsoleReporter, config);
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
