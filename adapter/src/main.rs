use std::sync::Arc;

use tonic::transport::Server;

use kamu_engine_flink_adapter::adapter::FlinkODFAdapter;
use kamu_engine_flink_adapter::grpc::EngineGRPCImpl;
use opendatafabric::engine::grpc_generated::engine_server::EngineServer;

/////////////////////////////////////////////////////////////////////////////////////////

const BINARY_NAME: &str = env!("CARGO_PKG_NAME");
const VERSION: &str = env!("CARGO_PKG_VERSION");
const DEFAULT_LOGGING_CONFIG: &str = "info";

/////////////////////////////////////////////////////////////////////////////////////////

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_logging();

    let addr = "0.0.0.0:2884";

    tracing::info!(
        message = "Starting Flink engine ODF adapter",
        version = VERSION,
        addr = addr,
    );

    let addr = addr.parse()?;
    let adapter = Arc::new(FlinkODFAdapter::new().await);
    let engine_grpc = EngineGRPCImpl::new(adapter);

    Server::builder()
        .add_service(EngineServer::new(engine_grpc))
        .serve(addr)
        .await?;

    Ok(())
}

/////////////////////////////////////////////////////////////////////////////////////////

fn init_logging() {
    use tracing_bunyan_formatter::{BunyanFormattingLayer, JsonStorageLayer};
    use tracing_log::LogTracer;
    use tracing_subscriber::{layer::SubscriberExt, EnvFilter, Registry};

    // Redirect all standard logging to tracing events
    LogTracer::init().expect("Failed to set LogTracer");

    // Use configuration from RUST_LOG env var if provided
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or(EnvFilter::new(DEFAULT_LOGGING_CONFIG.to_owned()));

    // TODO: Use non-blocking writer?
    // Configure Bunyan JSON formatter
    let formatting_layer = BunyanFormattingLayer::new(BINARY_NAME.to_owned(), std::io::stdout);
    let subscriber = Registry::default()
        .with(env_filter)
        .with(JsonStorageLayer)
        .with(formatting_layer);

    tracing::subscriber::set_global_default(subscriber).expect("Failed to set subscriber");
}
