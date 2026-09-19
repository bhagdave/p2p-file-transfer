use anyhow::{Context, Result};
use libp2p::PeerId;
use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Write},
    path::Path,
};
use tokio::net::{TcpStream, TcpListener};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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
    pub node: P2PNode,
    authenticator: Authenticator,
    sessions: HashMap<PeerId, TransferSession>,
    tx: mpsc::Sender<ProtocolEvent>,
    pub rx: mpsc::Receiver<ProtocolEvent>,
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

        // Start listening on a random TCP port
        let listener = TcpListener::bind("0.0.0.0:0").await?;
        let local_addr = listener.local_addr()?;

        println!("\nFile ready to send!");
        println!("Share this with the receiver:");
        println!("  Address: /ip4/127.0.0.1/tcp/{}", local_addr.port());
        println!();

        let mut node_for_run = P2PNode::new().await?;
        tokio::spawn(async move {
            let _ = node_for_run.run().await;
        });

        let file_data = std::fs::read(path)?;
        let metadata_clone = metadata.clone();

        println!("Waiting for receiver to connect...");

        let (mut socket, peer_addr) = listener.accept().await?;
        println!("Receiver connected from: {}", peer_addr);

        // Auth
        println!("Authenticating receiver...");
        let challenge = self.authenticator.generate_challenge();
        socket.write_all(&challenge).await?;

        let mut response = vec![0u8; 32];
        socket.read_exact(&mut response).await?;

        if !self.authenticator.verify_response(&challenge, &response) {
            println!("Authentication failed - rejecting connection");
            return Err(anyhow::anyhow!("Authentication failed"));
        }
        println!("Authentication successful!");

        let metadata_json = serde_json::to_string(&metadata_clone)?;
        let mut msg = (metadata_json.len() as u32).to_le_bytes().to_vec();
        msg.extend(metadata_json.as_bytes());
        socket.write_all(&msg).await?;

        println!("Sending file data...");
        socket.write_all(&(file_data.len() as u64).to_le_bytes()).await?;
        socket.write_all(&file_data).await?;

        println!("File sent! {} bytes transferred", file_data.len());
        println!("Transfer complete!");
        Ok(())
    }

    pub async fn receive_file(&mut self, sender_address: &str) -> Result<()> {
        println!("Connecting to sender...");

        let parts: Vec<&str> = sender_address.split('/').collect();
        let ip = if parts.len() > 2 { parts[2] } else { "127.0.0.1" };
        let port_str = if parts.len() > 4 { parts[4] } else { "9000" };
        let port: u16 = port_str.parse()?;

        let target_addr = format!("{}:{}", ip, port);

        let mut socket = TcpStream::connect(&target_addr).await
            .context(format!("Failed to connect to {}", target_addr))?;

        println!("Connected to sender at {}", target_addr);

        let mut size_buf = [0u8; 4];
        socket.read_exact(&mut size_buf).await?;
        let metadata_size = u32::from_le_bytes(size_buf) as usize;

        let mut metadata_buf = vec![0u8; metadata_size];
        socket.read_exact(&mut metadata_buf).await?;

        let metadata: FileMetadata = serde_json::from_slice(&metadata_buf)?;

        // prevent path traversal attacks
        let safe_name = Path::new(&metadata.name)
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| anyhow::anyhow!("Invalid filename"))?;

        if safe_name.starts_with('.') || safe_name.contains("..") {
            return Err(anyhow::anyhow!("Invalid filename: path traversal detected"));
        }

        let downloads_dir = Path::new("./downloads");
        std::fs::create_dir_all(downloads_dir)?;
        let out_path = downloads_dir.join(safe_name);

        println!("Receiving: {}", metadata.name);
        println!("Size: {} bytes", metadata.size);
        println!("Saving to: {}", out_path.display());

        let mut file_size_buf = [0u8; 8];
        socket.read_exact(&mut file_size_buf).await?;
        let file_size = u64::from_le_bytes(file_size_buf) as usize;

        let mut file_data = vec![0u8; file_size];
        let mut total_read = 0;

        println!("Receiving file data...");
        while total_read < file_size {
            let n = socket.read(&mut file_data[total_read..]).await?;
            if n == 0 {
                break;
            }
            total_read += n;

            let progress = (total_read as f64 / file_size as f64) * 100.0;
            println!("Progress: {:.2}% ({}/{})", progress, total_read, file_size);
        }

        let mut file = File::create(&out_path)?;
        file.write_all(&file_data)?;

        println!("File received! {} bytes written to {}", file_data.len(), out_path.display());
        println!("Transfer complete!");
        Ok(())
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
