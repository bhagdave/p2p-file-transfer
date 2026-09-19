use anyhow::{Context, Result};
use libp2p::{
    PeerId,
    Multiaddr,
};
use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Write},
    path::Path,
};
use tokio::sync::mpsc;
use crate::network::P2PNode;
use crate::auth::Authenticator;

#[derive(Debug)]
pub enum TransferState {
    Idle,
    Authenticating,
    SendingMetadata,
    SendingData,
    ReceivingMetadata,
    ReceivingData,
    Complete,
    Error(String),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FileMetadata {
    pub name: String,
    pub size: u64,
    pub hash: String,
}

#[derive(Debug)]
pub struct TransferSession {
    pub peer_id: PeerId,
    pub state: TransferState,
    pub metadata: Option<FileMetadata>,
    pub bytes_sent: u64,
    pub bytes_received: u64,
}

pub struct FileTransferProtocol {
    node: P2PNode,
    authenticator: Authenticator,
    sessions: HashMap<PeerId, TransferSession>,
    tx: mpsc::Sender<ProtocolEvent>,
    rx: mpsc::Receiver<ProtocolEvent>,
}

#[derive(Debug)]
pub enum ProtocolEvent {
    AuthChallenge {
        peer_id: PeerId,
        challenge: Vec<u8>,
    },
    AuthResponse {
        peer_id: PeerId,
        response: Vec<u8>,
    },
    AuthSuccess {
        peer_id: PeerId,
    },
    AuthFailed {
        peer_id: PeerId,
    },
    FileMetadata {
        peer_id: PeerId,
        metadata: FileMetadata,
    },
    FileChunk {
        peer_id: PeerId,
        chunk: Vec<u8>,
        offset: u64,
    },
    TransferComplete {
        peer_id: PeerId,
        file_name: String,
    },
    TransferError {
        peer_id: PeerId,
        error: String,
    },
}

impl FileTransferProtocol {
    pub fn new(node: P2PNode, authenticator: Authenticator) -> Self {
        let (tx, rx) = mpsc::channel(100);
        Self {
            node,
            authenticator,
            sessions: HashMap::new(),
            tx,
            rx,
        }
    }

    pub async fn send_file(&mut self, file_path: &str) -> Result<()> {
        let path = Path::new(file_path);
        if !path.exists() {
            return Err(anyhow::anyhow!("File not found: {}", file_path));
        }

        let metadata = self.compute_file_metadata(path).await?;
        println!("File: {}", metadata.name);
        println!("Size: {} bytes", metadata.size);
        println!("Hash: {}", metadata.hash);

        println!("Waiting for receiver to connect...");
        let peer_id = self.wait_for_peer().await?;

        self.authenticate_with_peer(peer_id).await?;

        self.send_metadata(peer_id, &metadata).await?;

        self.send_file_data(peer_id, path, &metadata).await?;

        println!("Transfer complete!");
        Ok(())
    }

    pub async fn receive_file(&mut self, sender_address: &str) -> Result<()> {
        let address: Multiaddr = sender_address
            .parse()
            .context("Invalid address format")?;

        println!("Connecting to sender...");
        self.node.connect_to(&address)?;

        let peer_id = self.wait_for_peer().await?;

        self.authenticate_with_peer(peer_id).await?;

        let metadata = self.receive_metadata(peer_id).await?;
        println!("Receiving: {}", metadata.name);
        println!("Size: {} bytes", metadata.size);

        self.receive_file_data(peer_id, &metadata).await?;

        println!("Transfer complete!");
        Ok(())
    }

    async fn wait_for_peer(&mut self) -> Result<PeerId> {
        loop {
            match self.rx.recv().await {
                Some(ProtocolEvent::AuthChallenge { peer_id, .. }) => {
                    return Ok(peer_id);
                }
                _ => continue,
            }
        }
    }

    async fn authenticate_with_peer(&mut self, peer_id: PeerId) -> Result<()> {
        println!("Authenticating with peer...");

        let challenge = self.authenticator.generate_challenge();
        self.send_auth_challenge(peer_id, &challenge).await?;

        let response = self.wait_for_auth_response(peer_id).await?;

        if self.authenticator.verify_response(&challenge, &response) {
            println!("Authentication successful!");
            Ok(())
        } else {
            Err(anyhow::anyhow!("Authentication failed"))
        }
    }

    async fn send_auth_challenge(&mut self, _peer_id: PeerId, _challenge: &[u8]) -> Result<()> {
        // TODO: Implement custom protocol for auth challenge
        Ok(())
    }

    async fn wait_for_auth_response(&mut self, peer_id: PeerId) -> Result<Vec<u8>> {
        loop {
            match self.rx.recv().await {
                Some(ProtocolEvent::AuthResponse {
                    peer_id: received_id,
                    response,
                }) if received_id == peer_id => {
                    return Ok(response);
                }
                _ => continue,
            }
        }
    }

    async fn compute_file_metadata(&self, path: &Path) -> Result<FileMetadata> {
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .context("Invalid file path")?;

        let size = std::fs::metadata(path)?
            .len();

        let hash = self.compute_file_hash(path).await?;

        Ok(FileMetadata { name, size, hash })
    }

    async fn compute_file_hash(&self, path: &Path) -> Result<String> {
        let mut file = File::open(path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        use sha2::{Sha256, Digest};
        let mut hasher = Sha256::new();
        hasher.update(&buffer);
        let result = hasher.finalize();

        Ok(hex::encode(result))
    }

    async fn send_metadata(&mut self, peer_id: PeerId, metadata: &FileMetadata) -> Result<()> {
        let json = serde_json::to_string(metadata)?;
        let data = json.into_bytes();
        self.node.send_data(peer_id, data)?;
        Ok(())
    }

    async fn receive_metadata(&mut self, peer_id: PeerId) -> Result<FileMetadata> {
        loop {
            match self.rx.recv().await {
                Some(ProtocolEvent::FileMetadata {
                    peer_id: received_id,
                    metadata,
                }) if received_id == peer_id => {
                    return Ok(metadata);
                }
                _ => continue,
            }
        }
    }

    async fn send_file_data(
        &mut self,
        peer_id: PeerId,
        path: &Path,
        metadata: &FileMetadata,
    ) -> Result<()> {
        let mut file = File::open(path)?;
        let mut buffer = [0u8; 65536]; // 64KB chunks
        let mut total_sent: u64 = 0;

        loop {
            let bytes_read = file.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }

            let chunk = &buffer[..bytes_read];
            self.node.send_data(peer_id, chunk.to_vec())?;

            total_sent += bytes_read as u64;

            let progress = (total_sent as f64 / metadata.size as f64) * 100.0;
            println!(
                "Sending: {:.2}% ({}/{} bytes)",
                progress, total_sent, metadata.size
            );
        }

        self.node.send_data(peer_id, vec![])?;
        Ok(())
    }

    async fn receive_file_data(
        &mut self,
        peer_id: PeerId,
        metadata: &FileMetadata,
    ) -> Result<()> {
        let mut file = File::create(&metadata.name)?;
        let mut total_received: u64 = 0;

        loop {
            match self.rx.recv().await {
                Some(ProtocolEvent::FileChunk {
                    peer_id: received_id,
                    chunk,
                    ..
                }) if received_id == peer_id => {
                    if chunk.is_empty() {
                        break;
                    }

                    file.write_all(&chunk)?;
                    total_received += chunk.len() as u64;

                    let progress = (total_received as f64 / metadata.size as f64) * 100.0;
                    println!(
                        "Receiving: {:.2}% ({}/{} bytes)",
                        progress, total_received, metadata.size
                    );
                }
                _ => continue,
            }
        }

        // Verify file hash
        let received_hash = self.compute_file_hash(Path::new(&metadata.name)).await?;
        if received_hash == metadata.hash {
            println!("File hash verified!");
        } else {
            return Err(anyhow::anyhow!(
                "File hash mismatch: expected {}, got {}",
                metadata.hash,
                received_hash
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_metadata_serde() {
        let metadata = FileMetadata {
            name: "test_file.txt".to_string(),
            size: 1024,
            hash: "abcdef0123456789abcdef0123456789".to_string(),
        };

        let json = serde_json::to_string(&metadata).expect("Failed to serialize");
        let deserialized: FileMetadata = serde_json::from_str(&json).expect("Failed to deserialize");

        assert_eq!(deserialized.name, metadata.name);
        assert_eq!(deserialized.size, metadata.size);
        assert_eq!(deserialized.hash, metadata.hash);
    }

    #[test]
    fn test_file_metadata_fields() {
        let metadata = FileMetadata {
            name: "document.pdf".to_string(),
            size: 2048576,
            hash: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".to_string(),
        };

        assert_eq!(metadata.name, "document.pdf");
        assert_eq!(metadata.size, 2048576);
        assert_eq!(metadata.hash.len(), 64);
    }

    #[test]
    fn test_transfer_state_variants() {
        let idle = TransferState::Idle;
        let complete = TransferState::Complete;
        let error = TransferState::Error("connection lost".to_string());

        match error {
            TransferState::Error(msg) => assert_eq!(msg, "connection lost"),
            _ => panic!("Expected error state"),
        }

        match idle {
            TransferState::Idle => (),
            _ => panic!("Expected idle state"),
        }

        match complete {
            TransferState::Complete => (),
            _ => panic!("Expected complete state"),
        }
    }

    #[test]
    fn test_transfer_session_creation() {
        let peer_id = libp2p::PeerId::random();
        let session = TransferSession {
            peer_id,
            state: TransferState::Idle,
            metadata: None,
            bytes_sent: 0,
            bytes_received: 0,
        };

        assert_eq!(session.peer_id, peer_id);
        assert_eq!(session.bytes_sent, 0);
        assert_eq!(session.bytes_received, 0);
        assert!(session.metadata.is_none());
    }

    #[test]
    fn test_transfer_session_with_metadata() {
        let peer_id = libp2p::PeerId::random();
        let metadata = FileMetadata {
            name: "large_file.bin".to_string(),
            size: 10_000_000,
            hash: "hash_value".to_string(),
        };

        let session = TransferSession {
            peer_id,
            state: TransferState::SendingData,
            metadata: Some(metadata.clone()),
            bytes_sent: 5_000_000,
            bytes_received: 0,
        };

        assert_eq!(session.bytes_sent, 5_000_000);
        assert!(session.metadata.is_some());
        assert_eq!(session.metadata.unwrap().size, 10_000_000);
    }

    #[test]
    fn test_protocol_event_auth_challenge() {
        let peer_id = libp2p::PeerId::random();

        let challenge_event = ProtocolEvent::AuthChallenge {
            peer_id,
            challenge: vec![1, 2, 3, 4],
        };

        match challenge_event {
            ProtocolEvent::AuthChallenge { challenge, .. } => {
                assert_eq!(challenge, vec![1, 2, 3, 4]);
            }
            _ => panic!("Expected AuthChallenge event"),
        }
    }

    #[test]
    fn test_protocol_event_file_metadata() {
        let peer_id = libp2p::PeerId::random();
        let metadata = FileMetadata {
            name: "test.txt".to_string(),
            size: 100,
            hash: "hash123".to_string(),
        };

        let event = ProtocolEvent::FileMetadata {
            peer_id,
            metadata: metadata.clone(),
        };

        match event {
            ProtocolEvent::FileMetadata { metadata: m, .. } => {
                assert_eq!(m.name, "test.txt");
                assert_eq!(m.size, 100);
            }
            _ => panic!("Expected FileMetadata event"),
        }
    }

    #[test]
    fn test_transfer_state_debug() {
        let state = TransferState::SendingMetadata;
        let debug_str = format!("{:?}", state);
        assert!(debug_str.contains("SendingMetadata"));
    }

    #[test]
    fn test_file_metadata_clone() {
        let metadata1 = FileMetadata {
            name: "file.txt".to_string(),
            size: 512,
            hash: "abc123def456".to_string(),
        };

        let metadata2 = metadata1.clone();
        assert_eq!(metadata1.name, metadata2.name);
        assert_eq!(metadata1.size, metadata2.size);
        assert_eq!(metadata1.hash, metadata2.hash);
    }
}
