use tokio::sync::mpsc::{channel, Sender};

use crate::{
    config::DatadogConfig,
    middleware::{report_datadog_traces, MinitraceSpans},
};

pub fn start_trace_reporter(dd_config: &DatadogConfig) -> Sender<MinitraceSpans> {
    let (datadog_sender, datadog_receiver) = channel::<MinitraceSpans>(10);

    tokio::spawn(report_datadog_traces(
        datadog_receiver,
        dd_config.agent_host,
        dd_config.agent_port,
        dd_config.service.clone(),
    ));

    datadog_sender
}
