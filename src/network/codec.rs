//! Message codec for serialization/deserialization
//!
//! Uses bincode for efficient binary serialization

use crate::network::protocol::Message;
use bytes::{Buf, BufMut, BytesMut};
use std::io;
use tokio_util::codec::{Decoder, Encoder};

/// Error type for codec operations
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("Deserialization error: {0}")]
    Deserialization(String),
    #[error("Invalid message length: {0}")]
    InvalidLength(usize),
}

/// Codec for encoding/decoding messages
///
/// Uses length-prefixed bincode serialization:
/// [4 bytes: length][N bytes: serialized message]
pub struct Codec;

impl Codec {
    /// Create a new codec
    pub fn new() -> Self {
        Self
    }

    /// Serialize a message to bytes
    pub fn encode_message(msg: &Message) -> Result<Vec<u8>, CodecError> {
        bincode::serialize(msg).map_err(|e| CodecError::Serialization(e.to_string()))
    }

    /// Deserialize a message from bytes
    pub fn decode_message(bytes: &[u8]) -> Result<Message, CodecError> {
        bincode::deserialize(bytes).map_err(|e| CodecError::Deserialization(e.to_string()))
    }
}

impl Default for Codec {
    fn default() -> Self {
        Self::new()
    }
}

impl Encoder<Message> for Codec {
    type Error = CodecError;

    fn encode(&mut self, item: Message, dst: &mut BytesMut) -> Result<(), Self::Error> {
        // Serialize the message
        let encoded = Self::encode_message(&item)?;
        let len = encoded.len();

        // Check length fits in u32
        if len > u32::MAX as usize {
            return Err(CodecError::InvalidLength(len));
        }

        // Write length prefix (4 bytes, big-endian)
        dst.reserve(4 + len);
        dst.put_u32(len as u32);
        dst.put_slice(&encoded);

        Ok(())
    }
}

impl Decoder for Codec {
    type Item = Message;
    type Error = CodecError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // Need at least 4 bytes for length prefix
        if src.len() < 4 {
            return Ok(None);
        }

        // Read length prefix (big-endian)
        let len = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;

        // Check for reasonable length (prevent DoS)
        if len > 10 * 1024 * 1024 {
            // 10MB max message size
            return Err(CodecError::InvalidLength(len));
        }

        // Need full message
        if src.len() < 4 + len {
            src.reserve(4 + len - src.len());
            return Ok(None);
        }

        // Extract message bytes
        src.advance(4);
        let message_bytes = src.split_to(len).freeze();

        // Deserialize
        let message = Self::decode_message(&message_bytes)?;
        Ok(Some(message))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::discovery::GossipMessage;
    use crate::distributed::node::{NodeId, NodeMetadata};
    use std::net::SocketAddr;

    fn create_test_node(id: u64, port: u16) -> NodeMetadata {
        let node_id = NodeId::new(id);
        let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
        NodeMetadata::new(node_id, addr)
    }

    #[test]
    fn test_encode_decode_gossip() {
        let node = create_test_node(1, 8080);
        let msg = Message::gossip(NodeId::new(1), GossipMessage::Join { node: node.clone() });

        let encoded = Codec::encode_message(&msg).unwrap();
        let decoded = Codec::decode_message(&encoded).unwrap();

        match (msg, decoded) {
            (
                Message::Gossip {
                    from: f1,
                    content: c1,
                },
                Message::Gossip {
                    from: f2,
                    content: c2,
                },
            ) => {
                assert_eq!(f1, f2);
                match (c1, c2) {
                    (GossipMessage::Join { node: n1 }, GossipMessage::Join { node: n2 }) => {
                        assert_eq!(n1.id, n2.id);
                        assert_eq!(n1.address, n2.address);
                    }
                    _ => panic!("Message type mismatch"),
                }
            }
            _ => panic!("Message type mismatch"),
        }
    }

    #[test]
    fn test_codec_roundtrip() {
        let mut codec = Codec::new();
        let mut buffer = BytesMut::new();

        let _node = create_test_node(1, 8080);
        let msg = Message::gossip(
            NodeId::new(1),
            GossipMessage::Heartbeat {
                node_id: NodeId::new(2),
            },
        );

        // Encode
        codec.encode(msg.clone(), &mut buffer).unwrap();

        // Decode
        let decoded = codec.decode(&mut buffer).unwrap().unwrap();

        match (msg, decoded) {
            (
                Message::Gossip {
                    from: f1,
                    content: c1,
                },
                Message::Gossip {
                    from: f2,
                    content: c2,
                },
            ) => {
                assert_eq!(f1, f2);
                match (c1, c2) {
                    (
                        GossipMessage::Heartbeat { node_id: n1 },
                        GossipMessage::Heartbeat { node_id: n2 },
                    ) => {
                        assert_eq!(n1, n2);
                    }
                    _ => panic!("Message type mismatch"),
                }
            }
            _ => panic!("Message type mismatch"),
        }
    }

    #[test]
    fn test_codec_partial_decode() {
        let mut codec = Codec::new();
        let mut buffer = BytesMut::new();

        let node = create_test_node(1, 8080);
        let msg = Message::gossip(NodeId::new(1), GossipMessage::Join { node });

        // Encode
        codec.encode(msg, &mut buffer).unwrap();

        // Try to decode with partial data
        let mut partial = BytesMut::new();
        partial.put_u32(1000); // Length prefix only
        assert!(codec.decode(&mut partial).unwrap().is_none());

        // Decode with full data
        let decoded = codec.decode(&mut buffer).unwrap();
        assert!(decoded.is_some());
    }
}
