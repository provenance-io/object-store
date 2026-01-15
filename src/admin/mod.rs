use std::sync::Arc;

use tonic::{Request, Response};

use crate::{
    config::Config,
    pb::{ConfigResponse, GetConfigRequest, SetConfigRequest, admin_service_server::AdminService},
    types::GrpcResult,
};

#[derive(Debug)]
pub struct AdminGrpc {
    config: Arc<Config>,
}

impl AdminGrpc {
    pub fn new(config: Arc<Config>) -> Self {
        Self { config }
    }
}

#[tonic::async_trait]
impl AdminService for AdminGrpc {
    async fn set_config(
        &self,
        request: Request<SetConfigRequest>,
    ) -> GrpcResult<Response<ConfigResponse>> {
        let request = request.into_inner();

        if let Some(maintenance_state) = request.maintenance_state {
            log::info!("Setting maintenance_state to {}", maintenance_state);
            self.config.set_maintenance_state(maintenance_state);
        }

        Ok(Response::new(ConfigResponse {
            maintenance_state: self.config.is_maintenance_state(),
        }))
    }

    async fn get_config(
        &self,
        _request: Request<GetConfigRequest>,
    ) -> GrpcResult<Response<ConfigResponse>> {
        Ok(Response::new(ConfigResponse {
            maintenance_state: self.config.is_maintenance_state(),
        }))
    }
}
