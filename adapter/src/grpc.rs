use std::sync::Arc;

use opendatafabric::serde::{EngineProtocolDeserializer, EngineProtocolSerializer};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use opendatafabric::engine::grpc_generated::engine_server::Engine as EngineGRPC;
use opendatafabric::engine::grpc_generated::{
    RawQueryRequest as RawQueryRequestGRPC, RawQueryResponse as RawQueryResponseGRPC,
    TransformRequest as TransformRequestGRPC, TransformResponse as TransformResponseGRPC,
};
use opendatafabric::serde::flatbuffers::FlatbuffersEngineProtocol;
use tracing::info;

use crate::adapter::FlinkODFAdapter;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct EngineGRPCImpl {
    adapter: Arc<FlinkODFAdapter>,
}

impl EngineGRPCImpl {
    pub fn new(adapter: Arc<FlinkODFAdapter>) -> Self {
        Self { adapter }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[tonic::async_trait]
impl EngineGRPC for EngineGRPCImpl {
    type ExecuteRawQueryStream = ReceiverStream<Result<RawQueryResponseGRPC, Status>>;
    type ExecuteTransformStream = ReceiverStream<Result<TransformResponseGRPC, Status>>;

    async fn execute_raw_query(
        &self,
        request_grpc: Request<RawQueryRequestGRPC>,
    ) -> Result<Response<Self::ExecuteRawQueryStream>, Status> {
        let span = tracing::span!(tracing::Level::INFO, "execute_raw_query");
        let _enter = span.enter();

        let request = FlatbuffersEngineProtocol
            .read_raw_query_request(&request_grpc.get_ref().flatbuffer)
            .unwrap();

        info!(message = "Got request", request = ?request);

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        let adapter = self.adapter.clone();

        tokio::spawn(async move {
            let response = adapter.execute_raw_query_impl(request).await.unwrap();

            let response_fb = FlatbuffersEngineProtocol
                .write_raw_query_response(&response)
                .unwrap();

            let response_grpc = RawQueryResponseGRPC {
                flatbuffer: response_fb.collapse_vec(),
            };

            tx.send(Ok(response_grpc)).await.unwrap();
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn execute_transform(
        &self,
        request_grpc: Request<TransformRequestGRPC>,
    ) -> Result<Response<Self::ExecuteTransformStream>, Status> {
        let span = tracing::span!(tracing::Level::INFO, "execute_transform");
        let _enter = span.enter();

        let request = FlatbuffersEngineProtocol
            .read_transform_request(&request_grpc.get_ref().flatbuffer)
            .unwrap();

        info!(message = "Got request", request = ?request);

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        let adapter = self.adapter.clone();

        tokio::spawn(async move {
            let response = adapter.execute_transform_impl(request).await.unwrap();

            let response_fb = FlatbuffersEngineProtocol
                .write_transform_response(&response)
                .unwrap();

            let response_grpc = TransformResponseGRPC {
                flatbuffer: response_fb.collapse_vec(),
            };

            tx.send(Ok(response_grpc)).await.unwrap();
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
