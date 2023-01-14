use crate::{api, content_storage::ContentStorage, execution_runner::ExecutionRunner};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{info, instrument};

#[derive(Debug, Default)]
pub struct CapabilitiesService {}

#[tonic::async_trait]
impl api::Capabilities for CapabilitiesService {
    #[instrument(skip_all)]
    async fn get_capabilities(
        &self,
        request: Request<api::GetCapabilitiesRequest>,
    ) -> Result<Response<api::ServerCapabilities>, Status> {
        info!("Instance: {}", request.get_ref().instance_name);
        let api_version = api::SemVer {
            major: 2,
            minor: 0,
            patch: 0,
            prerelease: String::default(),
        };

        let cache_capabilities = api::CacheCapabilities {
            digest_functions: vec![api::digest_function::Value::Sha256.into()],
            action_cache_update_capabilities: Some(api::ActionCacheUpdateCapabilities {
                update_enabled: true,
            }),
            cache_priority_capabilities: None,
            max_batch_total_size_bytes: 0,
            symlink_absolute_path_strategy: 0,
            supported_compressors: vec![],
            supported_batch_update_compressors: vec![],
        };

        let exec_caps = api::ExecutionCapabilities {
            digest_function: api::digest_function::Value::Sha256.into(),
            exec_enabled: true,
            execution_priority_capabilities: None,
            supported_node_properties: vec![],
        };

        let caps = api::ServerCapabilities {
            cache_capabilities: Some(cache_capabilities),
            execution_capabilities: Some(exec_caps),
            deprecated_api_version: None,
            low_api_version: Some(api_version.clone()),
            high_api_version: Some(api_version.clone()),
        };
        Ok(Response::new(caps))
    }
}
