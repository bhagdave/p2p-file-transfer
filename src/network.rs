use anyhow::Result;
use libp2p::{
    identity,
    mdns,
    noise,
    tcp,
    yamux,
    swarm::{Swarm, SwarmEvent},
    core::upgrade::Version,
    Transport,
    Multiaddr,
    PeerId,
};
use std::time::Duration;
use tokio::sync::mpsc;
use futures::prelude::*;

#[derive(Debug, Clone)]
pub enum NetworkEvent {
    PeerConnected(PeerId),
    PeerDisconnected(PeerId),
    Error(String),
}

#[derive(libp2p::swarm::NetworkBehaviour)]
pub struct Behaviour {
    mdns: mdns::tokio::Behaviour,
    identify: libp2p::identify::Behaviour,
}

pub struct P2PNode {
    pub swarm: Swarm<Behaviour>,
    pub peer_id: PeerId,
    pub tx: mpsc::Sender<NetworkEvent>,
    pub rx: mpsc::Receiver<NetworkEvent>,
}

impl P2PNode {
    pub async fn new() -> Result<Self> {
        let keypair = identity::Keypair::generate_ed25519();
        let peer_id = PeerId::from(keypair.public());

        let transport = tcp::tokio::Transport::new(tcp::Config::default());

        let upgrade_transport = transport
            .upgrade(Version::V1)
            .authenticate(noise::Config::new(&keypair).expect("noise config"))
            .multiplex(yamux::Config::default())
            .boxed();

        let behaviour = Behaviour {
            mdns: mdns::tokio::Behaviour::new(mdns::Config::default(), peer_id)?,
            identify: libp2p::identify::Behaviour::new(
                libp2p::identify::Config::new(
                    "/p2p-file-transfer/1.0.0".to_string(),
                    keypair.public(),
                ),
            ),
        };

        let config = libp2p::swarm::Config::with_tokio_executor();
        let mut swarm = Swarm::new(upgrade_transport, behaviour, peer_id, config);

        let listen_addr = Multiaddr::empty()
            .with(libp2p::multiaddr::Protocol::Ip4(std::net::Ipv4Addr::UNSPECIFIED))
            .with(libp2p::multiaddr::Protocol::Tcp(0));
        swarm.listen_on(listen_addr)?;

        let (tx, rx) = mpsc::channel(100);

        Ok(Self {
            swarm,
            peer_id,
            tx,
            rx,
        })
    }

    pub fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    pub fn listen_addresses(&self) -> Vec<Multiaddr> {
        self.swarm.listeners().cloned().collect()
    }

    pub fn connect_to(&mut self, address: &Multiaddr) -> Result<()> {
        self.swarm.dial(address.clone())?;
        Ok(())
    }

    pub async fn run(&mut self) -> Result<()> {
        loop {
            tokio::select! {
                event = self.swarm.select_next_some() => {
                    self.handle_swarm_event(event).await;
                }
                _ = tokio::time::sleep(Duration::from_secs(1)) => {
                    // Periodic check
                }
            }
        }
    }

    async fn handle_swarm_event(&mut self, event: SwarmEvent<BehaviourEvent>) {
        match event {
            SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                println!("Connected to peer: {}", peer_id);
                let _ = self.tx.send(NetworkEvent::PeerConnected(peer_id)).await;
            }
            SwarmEvent::ConnectionClosed { peer_id, .. } => {
                println!("Disconnected from peer: {}", peer_id);
                let _ = self.tx.send(NetworkEvent::PeerDisconnected(peer_id)).await;
            }
            SwarmEvent::NewListenAddr { address, .. } => {
                println!("Listening on: {}", address);
            }
            SwarmEvent::ListenerError { listener_id, error, .. } => {
                println!("Listen error on listener {}: {}", listener_id, error);
            }
            SwarmEvent::Dialing { peer_id: Some(peer_id), .. } => {
                println!("Dialing peer: {}", peer_id);
            }
            SwarmEvent::Dialing { peer_id: None, .. } => {
                // Dialing without known peer
            }
            SwarmEvent::Behaviour(_event) => {
                // Handle behaviour events
            }
            _ => {
                // Handle other event types
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_network_event_creation() {
        let peer_id = libp2p::PeerId::random();
        let event = NetworkEvent::PeerConnected(peer_id);

        match event {
            NetworkEvent::PeerConnected(id) => assert_eq!(id, peer_id),
            _ => panic!("Expected PeerConnected"),
        }
    }

    #[test]
    fn test_network_event_clone() {
        let event1 = NetworkEvent::Error("test error".to_string());
        let event2 = event1.clone();

        match (event1, event2) {
            (NetworkEvent::Error(e1), NetworkEvent::Error(e2)) => {
                assert_eq!(e1, e2);
            }
            _ => panic!("Expected errors"),
        }
    }
}
