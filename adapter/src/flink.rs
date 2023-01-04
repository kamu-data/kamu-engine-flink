use std::{error::Error, time::Duration};
use tokio::{
    io::AsyncBufReadExt,
    process::{Child, Command},
    task::JoinHandle,
};
use tracing::info;

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct FlinkJobManager {
    process: Child,
}

impl FlinkJobManager {
    pub async fn start() -> Result<Self, Box<dyn Error>> {
        info!("Starting Flink job manager");

        let mut process = Command::new("bin/jobmanager.sh")
            .current_dir("/opt/flink")
            .args(&["start-foreground"])
            .kill_on_drop(true)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()?;

        forward_to_logging(process.stdout.take().unwrap(), "job-manager", "stdout");
        forward_to_logging(process.stderr.take().unwrap(), "job-manager", "stderr");

        Ok(Self { process })
    }

    pub async fn wait_to_start(&self) {
        info!("Waiting for Flink job manager");
        wait_for_socket(6123).await;
        info!("Flink job manager started");
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct FlinkTaskManager {
    process: Child,
}

impl FlinkTaskManager {
    pub async fn start() -> Result<Self, Box<dyn Error>> {
        info!("Starting Flink task manager");

        let mut process = Command::new("bin/taskmanager.sh")
            .current_dir("/opt/flink")
            .args(&["start-foreground"])
            .kill_on_drop(true)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()?;

        forward_to_logging(process.stdout.take().unwrap(), "task-manager", "stdout");
        forward_to_logging(process.stderr.take().unwrap(), "task-manager", "stderr");

        Ok(Self { process })
    }

    pub async fn wait_to_start(&self) {
        // TODO
        // info!("Waiting for Flink task manager");
        // wait_for_socket(6121).await;
        // info!("Flink task manager started");
    }
}

///////////////////////////////////////////////////////////////////////////////

fn check_socket(port: u16) -> bool {
    use std::io::Read;
    use std::net::{TcpStream, ToSocketAddrs};

    let saddr = format!("127.0.0.1:{}", port);
    let addr = saddr.to_socket_addrs().unwrap().next().unwrap();
    let mut stream = match TcpStream::connect_timeout(&addr, Duration::from_millis(100)) {
        Ok(s) => s,
        _ => return false,
    };

    stream
        .set_read_timeout(Some(Duration::from_millis(1000)))
        .unwrap();

    let mut buf = [0; 1];
    match stream.read(&mut buf) {
        Ok(0) => false,
        Ok(_) => true,
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => true,
        Err(e) if e.kind() == std::io::ErrorKind::TimedOut => true,
        Err(_) => false,
    }
}

async fn wait_for_socket(port: u16) {
    loop {
        if check_socket(port) {
            break;
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

///////////////////////////////////////////////////////////////////////////////

pub fn forward_to_logging<R>(
    reader: R,
    process: &'static str,
    stream: &'static str,
) -> JoinHandle<()>
where
    R: tokio::io::AsyncRead + Send + 'static + Unpin,
{
    let stderr = tokio::io::BufReader::new(reader);
    tokio::spawn(async move {
        let mut line_reader = stderr.lines();
        while let Some(line) = line_reader.next_line().await.unwrap() {
            info!(message = line.as_str(), process = process, stream = stream);
        }
    })
}
