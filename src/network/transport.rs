//! Transport layer for network communication
//!
//! Provides TCP-based transport with optional TLS encryption

use crate::network::codec::{Codec, CodecError};
use crate::network::protocol::Message;
use crate::security::tls::{TlsConfig, TlsError};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{debug, info};

/// Error type for transport operations
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Codec error: {0}")]
    Codec(#[from] CodecError),
    #[error("TLS error: {0}")]
    Tls(#[from] TlsError),
    #[error("Connection closed")]
    ConnectionClosed,
    #[error("Connection timeout")]
    Timeout,
    #[error("Address already in use: {0}")]
    AddressInUse(SocketAddr),
}

/// Result type for transport operations
pub type TransportResult<T> = Result<T, TransportError>;

/// Transport abstraction for network communication
pub struct Transport {
    /// Local address
    local_addr: SocketAddr,
    /// TCP listener (if bound)
    listener: Option<TcpListener>,
    /// TLS configuration (optional)
    tls_config: Option<TlsConfig>,
    /// Connection pool (for future optimization)
    _connections: Arc<Mutex<dashmap::DashMap<SocketAddr, TcpStream>>>,
}

impl Transport {
    /// Create a new transport bound to the given address
    pub async fn bind(addr: SocketAddr) -> TransportResult<Self> {
        Self::bind_with_tls(addr, None).await
    }

    /// Create a new transport bound to the given address with TLS
    pub async fn bind_with_tls(
        addr: SocketAddr,
        tls_config: Option<TlsConfig>,
    ) -> TransportResult<Self> {
        let listener = TcpListener::bind(&addr).await?;
        let local_addr = listener.local_addr()?;

        if let Some(ref tls) = tls_config {
            if tls.enabled {
                info!("Transport bound to {} with TLS enabled", local_addr);
            } else {
                info!("Transport bound to {}", local_addr);
            }
        } else {
            info!("Transport bound to {}", local_addr);
        }

        Ok(Self {
            local_addr,
            listener: Some(listener),
            tls_config,
            _connections: Arc::new(Mutex::new(dashmap::DashMap::new())),
        })
    }

    /// Get the local address
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Get the TCP listener (if bound)
    pub fn listener(&self) -> Option<&TcpListener> {
        self.listener.as_ref()
    }

    /// Connect to a remote address
    pub async fn connect(&self, addr: SocketAddr) -> TransportResult<TransportConnection> {
        debug!("Connecting to {}", addr);
        let stream = TcpStream::connect(&addr).await?;
        stream.set_nodelay(true)?; // Disable Nagle's algorithm for low latency

        // If TLS is enabled, wrap the connection
        if let Some(ref tls_config) = self.tls_config {
            if tls_config.enabled {
                return self.connect_tls(addr, stream, tls_config).await;
            }
        }

        // Plain TCP connection
        let (read_half, write_half) = stream.into_split();
        let reader: Box<dyn futures::Stream<Item = Result<Message, CodecError>> + Send + Unpin> =
            Box::new(FramedRead::new(read_half, Codec::new()));
        let writer: Box<dyn futures::Sink<Message, Error = CodecError> + Send + Unpin> =
            Box::new(FramedWrite::new(write_half, Codec::new()));

        Ok(TransportConnection {
            addr,
            reader: Arc::new(Mutex::new(reader)),
            writer: Arc::new(Mutex::new(writer)),
            is_tls: false,
        })
    }

    /// Connect with TLS
    async fn connect_tls(
        &self,
        addr: SocketAddr,
        stream: TcpStream,
        tls_config: &TlsConfig,
    ) -> TransportResult<TransportConnection> {
        use rustls::ServerName;
        use tokio_rustls::TlsConnector;

        // Build client TLS config
        let client_config = tls_config.build_client_config()?;
        let connector = TlsConnector::from(client_config);

        // Get server name from address (use IP as hostname for now)
        // In production, you'd want to use the actual hostname
        let hostname = addr.ip().to_string();
        let server_name = ServerName::try_from(hostname.as_str()).map_err(|e| {
            TransportError::Tls(TlsError::Config(format!("Invalid server name: {}", e)))
        })?;

        // Perform TLS handshake
        let tls_stream = connector.connect(server_name, stream).await.map_err(|e| {
            TransportError::Tls(TlsError::Config(format!("TLS handshake failed: {}", e)))
        })?;

        // Split the TLS stream using tokio::io::split
        let (read_half, write_half) = tokio::io::split(tls_stream);
        let reader: Box<dyn futures::Stream<Item = Result<Message, CodecError>> + Send + Unpin> =
            Box::new(FramedRead::new(read_half, Codec::new()));
        let writer: Box<dyn futures::Sink<Message, Error = CodecError> + Send + Unpin> =
            Box::new(FramedWrite::new(write_half, Codec::new()));

        Ok(TransportConnection {
            addr,
            reader: Arc::new(Mutex::new(reader)),
            writer: Arc::new(Mutex::new(writer)),
            is_tls: true,
        })
    }

    /// Accept an incoming connection
    pub async fn accept(&self) -> TransportResult<TransportConnection> {
        let listener = self.listener.as_ref().ok_or_else(|| {
            TransportError::Io(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "Not bound to an address",
            ))
        })?;

        let (stream, peer_addr) = listener.accept().await?;
        stream.set_nodelay(true)?;

        debug!("Accepted connection from {}", peer_addr);

        // If TLS is enabled, wrap the connection
        if let Some(ref tls_config) = self.tls_config {
            if tls_config.enabled {
                return self.accept_tls(peer_addr, stream, tls_config).await;
            }
        }

        // Plain TCP connection
        let (read_half, write_half) = stream.into_split();
        let reader: Box<dyn futures::Stream<Item = Result<Message, CodecError>> + Send + Unpin> =
            Box::new(FramedRead::new(read_half, Codec::new()));
        let writer: Box<dyn futures::Sink<Message, Error = CodecError> + Send + Unpin> =
            Box::new(FramedWrite::new(write_half, Codec::new()));

        Ok(TransportConnection {
            addr: peer_addr,
            reader: Arc::new(Mutex::new(reader)),
            writer: Arc::new(Mutex::new(writer)),
            is_tls: false,
        })
    }

    /// Accept with TLS
    async fn accept_tls(
        &self,
        peer_addr: SocketAddr,
        stream: TcpStream,
        tls_config: &TlsConfig,
    ) -> TransportResult<TransportConnection> {
        use tokio_rustls::TlsAcceptor;

        // Build server TLS config
        let server_config = tls_config.build_server_config()?;
        let acceptor = TlsAcceptor::from(server_config);

        // Perform TLS handshake
        let tls_stream = acceptor.accept(stream).await.map_err(|e| {
            TransportError::Tls(TlsError::Config(format!("TLS handshake failed: {}", e)))
        })?;

        // Split the TLS stream using tokio::io::split
        let (read_half, write_half) = tokio::io::split(tls_stream);
        let reader: Box<dyn futures::Stream<Item = Result<Message, CodecError>> + Send + Unpin> =
            Box::new(FramedRead::new(read_half, Codec::new()));
        let writer: Box<dyn futures::Sink<Message, Error = CodecError> + Send + Unpin> =
            Box::new(FramedWrite::new(write_half, Codec::new()));

        Ok(TransportConnection {
            addr: peer_addr,
            reader: Arc::new(Mutex::new(reader)),
            writer: Arc::new(Mutex::new(writer)),
            is_tls: true,
        })
    }
}

/// A connection to a remote peer (supports both TCP and TLS)
pub struct TransportConnection {
    /// Remote address
    pub(crate) addr: SocketAddr,
    /// Framed reader for incoming messages (type-erased to support both TCP and TLS)
    #[allow(clippy::type_complexity)]
    pub(crate) reader:
        Arc<Mutex<Box<dyn futures::Stream<Item = Result<Message, CodecError>> + Send + Unpin>>>,
    /// Framed writer for outgoing messages (type-erased to support both TCP and TLS)
    #[allow(clippy::type_complexity)]
    pub(crate) writer:
        Arc<Mutex<Box<dyn futures::Sink<Message, Error = CodecError> + Send + Unpin>>>,
    /// Whether this connection uses TLS
    pub(crate) is_tls: bool,
}

impl TransportConnection {
    /// Get the remote address
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Send a message
    pub async fn send(&self, msg: Message) -> TransportResult<()> {
        use futures::SinkExt;
        let mut writer = self.writer.lock().await;
        writer.send(msg).await.map_err(TransportError::Codec)
    }

    /// Receive a message
    pub async fn recv(&self) -> TransportResult<Option<Message>> {
        use futures::StreamExt;
        let mut reader = self.reader.lock().await;
        reader
            .next()
            .await
            .transpose()
            .map_err(TransportError::Codec)
    }

    /// Check if this connection uses TLS
    pub fn is_tls(&self) -> bool {
        self.is_tls
    }

    /// Try to receive a message (non-blocking)
    pub async fn try_recv(&self) -> TransportResult<Option<Message>> {
        // For now, just call recv with a timeout
        // In the future, we could use a more sophisticated approach
        tokio::time::timeout(std::time::Duration::from_millis(1), self.recv())
            .await
            .unwrap_or(Ok(None))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::node::NodeId;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn test_transport_bind() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let transport = Transport::bind(addr).await.unwrap();
        assert!(transport.local_addr().port() > 0);
    }

    #[tokio::test]
    async fn test_transport_connect() {
        // Start a listener
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Bind transport
        let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let transport = Transport::bind(bind_addr).await.unwrap();

        // Spawn accept task
        let accept_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            stream
        });

        // Connect
        let conn = transport.connect(addr).await.unwrap();
        assert_eq!(conn.addr(), addr);

        // Wait for accept
        accept_task.await.unwrap();
    }

    #[tokio::test]
    async fn test_connection_send_recv() {
        // Bind transport (this creates a listener)
        let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let transport = Transport::bind(bind_addr).await.unwrap();
        let server_addr = transport.local_addr();

        // Spawn server task that accepts connections
        // We need to use Arc to share the transport properly
        let transport_arc = Arc::new(transport);
        let server_transport = Arc::clone(&transport_arc);
        let server_task = tokio::spawn(async move {
            let conn = server_transport.accept().await.unwrap();
            let msg = conn.recv().await.unwrap().unwrap();
            conn.send(msg).await.unwrap();
        });

        // Give server time to start accepting
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Connect and send/receive
        let conn = transport_arc.connect(server_addr).await.unwrap();
        let ping = Message::ping(NodeId::new(1), 12345);
        conn.send(ping.clone()).await.unwrap();

        let pong = conn.recv().await.unwrap();
        assert!(pong.is_some());

        server_task.await.unwrap();
    }
}

impl Clone for Transport {
    fn clone(&self) -> Self {
        Self {
            local_addr: self.local_addr,
            listener: None, // Can't clone TcpListener, but we can still use connect()
            tls_config: self.tls_config.clone(),
            _connections: Arc::clone(&self._connections),
        }
    }
}

impl Clone for TransportConnection {
    fn clone(&self) -> Self {
        Self {
            addr: self.addr,
            reader: Arc::clone(&self.reader),
            writer: Arc::clone(&self.writer),
            is_tls: self.is_tls,
        }
    }
}
