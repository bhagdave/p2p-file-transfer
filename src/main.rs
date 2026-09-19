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
            let passphrase = generate_passphrase();
            println!("Generated passphrase: {}", passphrase);
            println!("Share this passphrase with the receiver.");

            let node = P2PNode::new().await?;
            let authenticator = Authenticator::new(passphrase);
            let mut protocol = FileTransferProtocol::new(node, authenticator);

            protocol.send_file(&file).await?;
        }
        cli::Command::Receive { address, passphrase } => {
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
