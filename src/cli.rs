use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "p2p-file-transfer")]
#[command(about = "Secure P2P file transfer on local network")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Send a file to another machine
    Send {
        /// Path to the file to send
        #[arg(short, long)]
        file: String,

        /// Your local address (auto-detected)
        #[arg(short, long)]
        address: Option<String>,
    },
    /// Receive a file from another machine
    Receive {
        /// Address of the sender
        #[arg(short, long)]
        address: String,

        /// Passphrase provided by sender
        #[arg(short, long)]
        passphrase: Option<String>,
    },
}
