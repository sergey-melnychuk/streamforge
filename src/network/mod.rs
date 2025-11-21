//! Network layer for cluster communication
//!
//! Provides RPC, transport, and protocol implementations for distributed coordination

pub mod codec;
pub mod protocol;
pub mod rpc;
pub mod transport;

pub use codec::Codec;
pub use protocol::{Message, MessageType, RpcRequest, RpcResponse};
pub use rpc::{RpcClient, RpcHandler, RpcServer};
pub use transport::Transport;
