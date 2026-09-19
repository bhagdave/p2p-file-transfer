// This is free and unencumbered software released into the public domain.
// See the UNLICENSE file for details.

mod cli;
mod network;
mod protocol;
mod auth;

use anyhow::Result;
use clap::Parser;
use cli::Cli;
use network::P2PNode;
use protocol::FileTransferProtocol;
use auth::Authenticator;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        cli::Command::Send { file, .. } => {
            let node = P2PNode::new().await?;
            let addresses = node.listen_addresses();

            println!("P2P File Transfer - Sender Mode");
            println!("================================");
            println!("Your listening addresses:");
            for addr in addresses {
                println!("  {}", addr);
            }
            println!();

            let passphrase = generate_passphrase();
            println!("Generated passphrase: {}", passphrase);
            println!("Share the address(es) above and this passphrase with the receiver.");
            println!();

            let authenticator = Authenticator::new(passphrase);
            let mut protocol = FileTransferProtocol::new(node, authenticator);

            protocol.send_file(&file).await?;
        }
        cli::Command::Receive { address, passphrase } => {
            println!("P2P File Transfer - Receiver Mode");
            println!("==================================");

            let passphrase = match passphrase {
                Some(p) => p,
                None => {
                    println!("Enter passphrase from sender: ");
                    rpassword::read_password()?
                }
            };

            let node = P2PNode::new().await?;
            let authenticator = Authenticator::new(passphrase);
            let mut protocol = FileTransferProtocol::new(node, authenticator);

            protocol.receive_file(&address).await?;
        }
    }

    Ok(())
}

fn generate_passphrase() -> String {
    use rand::Rng;
    let chars: Vec<char> = "abcdefghijklmnopqrstuvwxyz0123456789-+!?".chars().collect();
    let mut rng = rand::thread_rng();
    (0..12)
        .map(|_| chars[rng.gen_range(0..chars.len())])
        .collect()
}
