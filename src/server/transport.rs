use std::{
    collections::HashMap,
    io,
    net::{IpAddr, SocketAddr},
    pin::Pin,
    sync::Arc,
    task::{Context as TaskContext, Poll},
    time::Duration,
};

use axum::{
    extract::{
        Request,
        connect_info::{ConnectInfo, Connected},
    },
    middleware::Next,
    response::Response,
    serve::{IncomingStream, Listener},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpListener,
    sync::{OwnedSemaphorePermit, Semaphore},
};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::{
    connection_drain::{PendingConnection, PendingConnections},
    metrics::Metrics,
};

#[derive(Debug)]
pub(super) struct BoundedTcpListener {
    pub(super) inner: TcpListener,
    pub(super) permits: Arc<Semaphore>,
    pub(super) metrics: Arc<Metrics>,
    pub(super) connections: Arc<ConnectionTracker>,
    pub(super) track_public: bool,
    pub(super) accept_cancellation: CancellationToken,
    pub(super) pending_connections: Arc<PendingConnections>,
}

impl BoundedTcpListener {
    pub(super) fn new(
        inner: TcpListener,
        max_connections: usize,
        metrics: Arc<Metrics>,
        connections: Arc<ConnectionTracker>,
        track_public: bool,
        accept_cancellation: CancellationToken,
    ) -> Self {
        Self {
            inner,
            permits: Arc::new(Semaphore::new(max_connections)),
            metrics,
            connections,
            track_public,
            accept_cancellation,
            pending_connections: Arc::new(PendingConnections::default()),
        }
    }
}

impl Listener for BoundedTcpListener {
    type Io = BoundedTcpStream;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        // Include the accept future itself so stopping accepts cannot race a socket handoff.
        let mut first_request = Some(self.pending_connections.track());
        let permit = tokio::select! {
            biased;
            () = self.accept_cancellation.cancelled() => {
                drop(first_request.take());
                std::future::pending().await
            },
            permit = Arc::clone(&self.permits).acquire_owned() => {
                permit.expect("public connection semaphore is never closed")
            }
        };
        loop {
            let accepted = tokio::select! {
                biased;
                () = self.accept_cancellation.cancelled() => {
                    drop(first_request.take());
                    std::future::pending().await
                },
                accepted = self.inner.accept() => accepted,
            };
            match accepted {
                Ok((stream, address)) => {
                    let ip = address.ip();
                    if self.track_public && !self.connections.open(ip) {
                        continue;
                    }
                    if self.track_public {
                        self.metrics.public_connection_opened();
                    }
                    return (
                        BoundedTcpStream {
                            inner: stream,
                            first_request: first_request.take().expect("accept is still active"),
                            _permit: permit,
                            metrics: Arc::clone(&self.metrics),
                            connections: Arc::clone(&self.connections),
                            ip,
                            track_public: self.track_public,
                        },
                        address,
                    );
                }
                Err(error) => {
                    warn!(%error, "failed to accept public connection");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.inner.local_addr()
    }
}

#[derive(Debug)]
pub(super) struct BoundedTcpStream {
    pub(super) inner: tokio::net::TcpStream,
    pub(super) first_request: PendingConnection,
    pub(super) _permit: OwnedSemaphorePermit,
    pub(super) metrics: Arc<Metrics>,
    pub(super) connections: Arc<ConnectionTracker>,
    pub(super) ip: IpAddr,
    pub(super) track_public: bool,
}

impl Connected<IncomingStream<'_, BoundedTcpListener>> for PendingConnection {
    fn connect_info(stream: IncomingStream<'_, BoundedTcpListener>) -> Self {
        stream.io().first_request.clone()
    }
}

pub(super) async fn connection_request_started(request: Request, next: Next) -> Response {
    if let Some(ConnectInfo(connection)) =
        request.extensions().get::<ConnectInfo<PendingConnection>>()
    {
        connection.request_started();
    }
    next.run(request).await
}

impl AsyncRead for BoundedTcpStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(context, buffer)
    }
}

impl AsyncWrite for BoundedTcpStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(context, buffer)
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(context)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(context)
    }
}

impl Drop for BoundedTcpStream {
    fn drop(&mut self) {
        if self.track_public {
            self.metrics.public_connection_closed();
            self.connections.close(self.ip);
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct ConnectionTracker {
    pub(super) state: parking_lot::Mutex<ConnectionState>,
}

#[derive(Debug, Default)]
pub(super) struct ConnectionState {
    pub(super) active: HashMap<IpAddr, usize>,
    pub(super) limits: HashMap<IpAddr, usize>,
}

pub(super) type IpCounts = Vec<(IpAddr, usize)>;

impl ConnectionTracker {
    pub(super) fn open(&self, ip: IpAddr) -> bool {
        let mut state = self.state.lock();
        let active = state.active.get(&ip).copied().unwrap_or(0);
        if state.limits.get(&ip).is_some_and(|limit| active >= *limit) {
            return false;
        }
        state.active.insert(ip, active + 1);
        true
    }

    pub(super) fn close(&self, ip: IpAddr) {
        let mut state = self.state.lock();
        if let Some(active) = state.active.get_mut(&ip) {
            *active -= 1;
            if *active == 0 {
                state.active.remove(&ip);
            }
        }
    }

    pub(super) fn snapshot(&self) -> (IpCounts, IpCounts) {
        let state = self.state.lock();
        let mut active: Vec<_> = state
            .active
            .iter()
            .map(|(&ip, &count)| (ip, count))
            .collect();
        let mut limits: Vec<_> = state
            .limits
            .iter()
            .map(|(&ip, &limit)| (ip, limit))
            .collect();
        active.sort_unstable_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        limits.sort_unstable_by_key(|(ip, _)| *ip);
        active.truncate(3);
        (active, limits)
    }

    pub(super) fn set_limit(&self, ip: IpAddr, limit: usize) {
        self.state.lock().limits.insert(ip, limit);
    }

    pub(super) fn remove_limit(&self, ip: IpAddr) -> bool {
        self.state.lock().limits.remove(&ip).is_some()
    }
}
