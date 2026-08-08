use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::time::timeout;

use crate::metrics::Metrics;

const MAX_REQUEST_BYTES: usize = 8 * 1024;
const IO_TIMEOUT: Duration = Duration::from_secs(2);

pub struct Server {
    listener: TcpListener,
}

impl Server {
    pub async fn bind(address: &str) -> io::Result<Self> {
        let address: SocketAddr = address
            .parse()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        Ok(Self {
            listener: TcpListener::bind(address).await?,
        })
    }

    pub async fn run(
        self,
        metrics: Metrics,
        mut shutdown: watch::Receiver<bool>,
    ) -> io::Result<()> {
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
                accepted = self.listener.accept() => {
                    let (stream, _) = accepted?;
                    match timeout(IO_TIMEOUT, serve(stream, &metrics)).await {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => tracing::warn!(error = %error, "observability request failed"),
                        Err(error) => tracing::warn!(error = %error, "observability request timed out"),
                    }
                }
            }
        }
    }
}

async fn serve(mut stream: TcpStream, metrics: &Metrics) -> io::Result<()> {
    let mut buffer = [0_u8; MAX_REQUEST_BYTES];
    let bytes_read = stream.read(&mut buffer).await?;
    let first_line = std::str::from_utf8(&buffer[..bytes_read])
        .ok()
        .and_then(|request| request.lines().next())
        .unwrap_or_default();
    let (status, content_type, body) =
        match first_line.split_whitespace().collect::<Vec<_>>().as_slice() {
            ["GET", "/healthz", _] => (
                "200 OK",
                "application/json",
                "{\"status\":\"ok\"}\n".to_owned(),
            ),
            ["GET", "/metrics", _] => (
                "200 OK",
                "text/plain; version=0.0.4; charset=utf-8",
                metrics.render_prometheus(),
            ),
            _ => (
                "404 Not Found",
                "application/json",
                "{\"status\":\"not_found\"}\n".to_owned(),
            ),
        };
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nCache-Control: no-store\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn health_response_is_liveness_only() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = Server { listener };
        let metrics = Metrics::default();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(server.run(metrics, shutdown_rx));

        let mut stream = TcpStream::connect(address).await.unwrap();
        stream
            .write_all(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("{\"status\":\"ok\"}"));
        task.abort();
    }
}
