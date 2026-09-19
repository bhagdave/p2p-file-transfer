# P2P File Transfer

Decentralized file transfer built with libp2p and Rust. Transfer files between two machines on a local network.

## Features

- **Peer-to-Peer Transfer**: Direct file transfer between machines using TCP
- **mDNS Discovery**: Automatic peer discovery on local networks
- **Secure Authentication**: Passphrase-based security for file transfers
- **File Hashing**: SHA256 verification of transferred files
- **Progress Tracking**: Real-time transfer progress display
- **Cross-Platform**: Runs on Linux, macOS, and Windows

## Building

```bash
cargo build --release
```

The binary will be available at `target/release/p2p-file-transfer`

## Usage

### Sending a File

1. **Sender starts the application:**
   ```bash
   ./p2p-file-transfer send --file /path/to/file.txt
   ```

2. **The sender will see output like:**
   ```
   P2P File Transfer - Sender Mode
   ================================
   Your listening addresses:
     /ip4/127.0.0.1/tcp/12345
   
   Generated passphrase: a3b2c1d4e5f6
   Share the address(es) above and this passphrase with the receiver.
   
   File: file.txt
   Size: 1024 bytes
   Hash: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
   
   File ready to send!
   Share this with the receiver:
     Address: /ip4/127.0.0.1/tcp/12345
   
   Waiting for receiver to connect...
   ```

3. **Share the TCP address and passphrase with the receiver**

### Receiving a File

1. **Receiver starts the application:**
   ```bash
   ./p2p-file-transfer receive --address /ip4/sender-ip/tcp/port --passphrase <passphrase>
   ```
   
   Or without specifying the passphrase (you'll be prompted):
   ```bash
   ./p2p-file-transfer receive --address /ip4/sender-ip/tcp/port
   ```

2. **The receiver will be prompted for the file name** (as stored on the sender's machine)

3. **Transfer begins automatically**

### Example

**Terminal 1 (Sender):**
```bash
$ ./p2p-file-transfer send --file /tmp/document.pdf
P2P File Transfer - Sender Mode
================================
Your listening addresses:
  /ip4/127.0.0.1/tcp/54321

Generated passphrase: x9y8z7w6v5u4
Share the address(es) above and this passphrase with the receiver.

File: document.pdf
Size: 2048576 bytes
Hash: abc123def456...

File ready to send!
Share this with the receiver:
  Address: /ip4/127.0.0.1/tcp/54321

Waiting for receiver to connect...
Receiver connected from: 127.0.0.1:45678
Sending file data...
File sent! 2048576 bytes transferred
Transfer complete!
```

**Terminal 2 (Receiver):**
```bash
$ ./p2p-file-transfer receive --address /ip4/127.0.0.1/tcp/54321 --passphrase x9y8z7w6v5u4
P2P File Transfer - Receiver Mode
==================================
Enter passphrase from sender:
Connected to sender at 127.0.0.1:54321
Enter the file name to request:
document.pdf
Requesting file: document.pdf
Waiting for file data...
Receiving: document.pdf
Size: 2048576 bytes
Receiving file data...
Progress: 0.00% (0/2048576)
Progress: 33.33% (682000/2048576)
Progress: 66.67% (1364000/2048576)
Progress: 100.00% (2048576/2048576)
File received! 2048576 bytes written to document.pdf
Transfer complete!
```

## Network Requirements

- Both machines must be on the same local network or have direct connectivity
- The sender's TCP port must be accessible from the receiver
- No firewall blocking between the two machines on the specified port

## Security Features

✅ **Implemented:**
- Challenge/response authentication using HMAC-SHA256 before file transfer
- Filename sanitization to prevent path traversal attacks  
- Files saved to isolated `./downloads/` directory
- libp2p for secure peer discovery (mDNS) with noise protocol support

⚠️ **Current Limitations:**
- File transfers are sent in **plain text** over TCP (not encrypted)
- Suitable only for trusted local networks (same WiFi/LAN)
- Test with `--passphrase` flag to use pre-shared authentication

## Architecture

- **Network Layer** (`network.rs`): Handles peer discovery using libp2p's mdns and identify protocols
- **Protocol Layer** (`protocol.rs`): Implements file transfer logic using TCP sockets
- **Authentication** (`auth.rs`): Provides passphrase generation and verification
- **CLI** (`cli.rs`): Command-line interface using clap

## Testing

Run the unit tests:
```bash
cargo test --lib
```

All 21 tests validate:
- Authentication challenge/response generation
- File metadata serialization
- Protocol events and state management
- Network event handling

## License

This is free and unencumbered software released into the public domain. See the [UNLICENSE](UNLICENSE) file for details.
