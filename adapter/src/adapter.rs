use std::path::{Path, PathBuf};

use opendatafabric::{
    serde::{yaml::YamlEngineProtocol, EngineProtocolDeserializer, EngineProtocolSerializer},
    ExecuteQueryRequest, ExecuteQueryResponse, ExecuteQueryResponseInternalError,
};
use tracing::{error, info};

use crate::flink::{forward_to_logging, FlinkJobManager, FlinkTaskManager};

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct FlinkODFAdapter {
    job_manager: FlinkJobManager,
    task_manager: FlinkTaskManager,
}

impl FlinkODFAdapter {
    pub async fn new() -> Self {
        let job_manager = FlinkJobManager::start().await.unwrap();
        let task_manager = FlinkTaskManager::start().await.unwrap();

        tokio::join!(job_manager.wait_to_start(), task_manager.wait_to_start());

        Self {
            job_manager,
            task_manager,
        }
    }

    pub async fn execute_query_impl(
        &self,
        request: ExecuteQueryRequest,
    ) -> Result<ExecuteQueryResponse, Box<dyn std::error::Error>> {
        let in_out_dir = PathBuf::from("/opt/engine/in-out");
        let _ = std::fs::remove_dir_all(&in_out_dir);
        let _ = std::fs::create_dir_all(&in_out_dir);

        {
            let request_path = PathBuf::from("/opt/engine/in-out/request.yaml");
            let data = YamlEngineProtocol.write_execute_query_request(&request)?;
            std::fs::write(request_path, data)?;
        }

        let mut cmd = tokio::process::Command::new("/opt/flink/bin/flink");
        cmd.current_dir("/opt/flink")
            .args(
                if let Some(prev_checkpoint_dir) = &request.prev_checkpoint_dir {
                    vec![
                        "run".to_owned(),
                        "-s".to_owned(),
                        self.get_savepoint(prev_checkpoint_dir)
                            .unwrap()
                            .to_str()
                            .unwrap()
                            .to_owned(),
                        "/opt/engine/bin/engine.flink.jar".to_owned(),
                    ]
                } else {
                    vec![
                        "run".to_owned(),
                        "/opt/engine/bin/engine.flink.jar".to_owned(),
                    ]
                },
            )
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);

        info!(message = "Submitting Flink job", cmd = ?cmd);

        let mut process = cmd.spawn()?;

        let stdout_task = forward_to_logging(process.stdout.take().unwrap(), "submit", "stdout");
        let stderr_task = forward_to_logging(process.stderr.take().unwrap(), "submit", "stderr");

        let exit_status = process.wait().await?;
        stdout_task.await?;
        stderr_task.await?;

        let response_path = PathBuf::from("/opt/engine/in-out/response.yaml");

        if response_path.exists() {
            let data = std::fs::read_to_string(&response_path)?;
            Ok(YamlEngineProtocol.read_execute_query_response(data.as_bytes())?)
        } else if !exit_status.success() {
            error!(
                message = "Job exited with non-zero code",
                code = exit_status.code().unwrap(),
            );

            Ok(ExecuteQueryResponse::InternalError(
                ExecuteQueryResponseInternalError {
                    message: format!(
                        "Engine exited with non-zero status: {}",
                        exit_status.code().unwrap()
                    ),
                    backtrace: None,
                },
            ))
        } else {
            Ok(ExecuteQueryResponse::InternalError(
                ExecuteQueryResponseInternalError {
                    message: format!("Engine did not write the response file"),
                    backtrace: None,
                },
            ))
        }
    }

    // TODO: Atomicity
    // TODO: Error handling
    fn get_savepoint(&self, checkpoint_dir: &Path) -> Result<PathBuf, std::io::Error> {
        let mut savepoints = std::fs::read_dir(checkpoint_dir)?
            .filter_map(|res| match res {
                Ok(e) => {
                    let path = e.path();
                    let name = path.file_name().unwrap().to_string_lossy();
                    if path.is_dir() && name.starts_with("savepoint-") {
                        Some(Ok(path))
                    } else {
                        None
                    }
                }
                Err(err) => Some(Err(err)),
            })
            .collect::<Result<Vec<_>, std::io::Error>>()?;

        if savepoints.len() != 1 {
            panic!(
                "Could not find a savepoint in checkpoint location: {}",
                checkpoint_dir.display()
            )
        }

        Ok(savepoints.pop().unwrap())
    }
}
