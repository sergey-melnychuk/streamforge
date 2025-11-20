//! Network layer for cluster communication
//!
//! Provides RPC, transport, and protocol implementations for distributed coordination

pub mod protocol;
pub mod codec;
pub mod transport;
pub mod rpc;

pub use protocol::{Message, MessageType, RpcRequest, RpcResponse};
pub use codec::Codec;
pub use transport::Transport;
pub use rpc::{RpcClient, RpcServer, RpcHandler};

