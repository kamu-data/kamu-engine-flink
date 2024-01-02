use std::path::{Path, PathBuf};

use opendatafabric::{
    serde::{yaml::YamlEngineProtocol, EngineProtocolDeserializer, EngineProtocolSerializer},
    RawQueryRequest, RawQueryResponse, RawQueryResponseInternalError, TransformRequest,
    TransformResponse, TransformResponseInternalError,
};
use tracing::{error, info};

use crate::flink::{forward_to_logging, FlinkJobManager, FlinkTaskManager};

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
#[allow(dead_code)]
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

    // TODO: Generalize with `execute_transform`
    pub async fn execute_raw_query_impl(
        &self,
        request: RawQueryRequest,
    ) -> Result<RawQueryResponse, Box<dyn std::error::Error>> {
        let in_out_dir = PathBuf::from("/opt/engine/in-out");
        let _ = std::fs::remove_dir_all(&in_out_dir);
        let _ = std::fs::create_dir_all(&in_out_dir);

        // Write request
        {
            let request_path = PathBuf::from("/opt/engine/in-out/request.yaml");
            let data = YamlEngineProtocol.write_raw_query_request(&request)?;
            std::fs::write(request_path, data)?;
        }

        let mut cmd = tokio::process::Command::new("/opt/flink/bin/flink");
        cmd.current_dir("/opt/flink")
            .args([
                "run",
                "-c",
                "dev.kamu.engine.flink.RawQueryApp",
                "/opt/engine/bin/engine.flink.jar",
            ])
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
            Ok(YamlEngineProtocol.read_raw_query_response(data.as_bytes())?)
        } else if !exit_status.success() {
            error!(
                message = "Job exited with non-zero code",
                code = exit_status.code().unwrap(),
            );

            Ok(RawQueryResponse::InternalError(
                RawQueryResponseInternalError {
                    message: format!(
                        "Engine exited with non-zero status: {}",
                        exit_status.code().unwrap()
                    ),
                    backtrace: None,
                },
            ))
        } else {
            Ok(RawQueryResponse::InternalError(
                RawQueryResponseInternalError {
                    message: format!("Engine did not write the response file"),
                    backtrace: None,
                },
            ))
        }
    }

    pub async fn execute_transform_impl(
        &self,
        mut request: TransformRequest,
    ) -> Result<TransformResponse, Box<dyn std::error::Error>> {
        let in_out_dir = PathBuf::from("/opt/engine/in-out");
        let _ = std::fs::remove_dir_all(&in_out_dir);
        let _ = std::fs::create_dir_all(&in_out_dir);

        // Prepare checkpoint
        let orig_new_checkpoint_path = request.new_checkpoint_path;
        request.new_checkpoint_path = in_out_dir.join("new-checkpoint");
        if let Some(tar_path) = &request.prev_checkpoint_path {
            let restored_checkpoint_path = in_out_dir.join("prev-checkpoint");
            Self::unpack_checkpoint(tar_path, &restored_checkpoint_path)?;
            request.prev_checkpoint_path = Some(restored_checkpoint_path);
        }

        // Write request
        {
            let request_path = PathBuf::from("/opt/engine/in-out/request.yaml");
            let data = YamlEngineProtocol.write_transform_request(&request)?;
            std::fs::write(request_path, data)?;
        }

        let mut cmd = tokio::process::Command::new("/opt/flink/bin/flink");
        cmd.current_dir("/opt/flink")
            .args(
                if let Some(prev_checkpoint_dir) = &request.prev_checkpoint_path {
                    vec![
                        "run".to_string(),
                        "-s".to_string(),
                        self.get_savepoint(prev_checkpoint_dir)
                            .unwrap()
                            .to_str()
                            .unwrap()
                            .to_string(),
                        "-c".to_string(),
                        "dev.kamu.engine.flink.TransformApp".to_string(),
                        "/opt/engine/bin/engine.flink.jar".to_string(),
                    ]
                } else {
                    vec![
                        "run".to_string(),
                        "-c".to_string(),
                        "dev.kamu.engine.flink.TransformApp".to_string(),
                        "/opt/engine/bin/engine.flink.jar".to_string(),
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
            // Pack checkpoint
            if request.new_checkpoint_path.exists()
                && request.new_checkpoint_path.read_dir()?.next().is_some()
            {
                std::fs::create_dir_all(orig_new_checkpoint_path.parent().unwrap())?;
                Self::pack_checkpoint(&request.new_checkpoint_path, &orig_new_checkpoint_path)?;
            }

            let data = std::fs::read_to_string(&response_path)?;
            Ok(YamlEngineProtocol.read_transform_response(data.as_bytes())?)
        } else if !exit_status.success() {
            error!(
                message = "Job exited with non-zero code",
                code = exit_status.code().unwrap(),
            );

            Ok(TransformResponse::InternalError(
                TransformResponseInternalError {
                    message: format!(
                        "Engine exited with non-zero status: {}",
                        exit_status.code().unwrap()
                    ),
                    backtrace: None,
                },
            ))
        } else {
            Ok(TransformResponse::InternalError(
                TransformResponseInternalError {
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

    fn unpack_checkpoint(
        prev_checkpoint_path: &Path,
        target_dir: &Path,
    ) -> Result<(), Box<dyn std::error::Error>> {
        info!(
            ?prev_checkpoint_path,
            ?target_dir,
            "Unpacking previous checkpoint"
        );
        std::fs::create_dir(target_dir)?;
        let mut archive = tar::Archive::new(std::fs::File::open(prev_checkpoint_path)?);
        archive.unpack(target_dir)?;
        Ok(())
    }

    fn pack_checkpoint(
        source_dir: &Path,
        new_checkpoint_path: &Path,
    ) -> Result<(), Box<dyn std::error::Error>> {
        info!(?source_dir, ?new_checkpoint_path, "Packing new checkpoint");
        let mut ar = tar::Builder::new(std::fs::File::create(new_checkpoint_path)?);
        ar.follow_symlinks(false);
        ar.append_dir_all(".", source_dir)?;
        ar.finish()?;
        Ok(())
    }
}
