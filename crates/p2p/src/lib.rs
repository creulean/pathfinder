#![deny(rust_2018_idioms)]

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use futures::channel::mpsc::{Receiver as ResponseReceiver, Sender as ResponseSender};
use ipnet::IpNet;
use libp2p::gossipsub::IdentTopic;
use libp2p::identity::Keypair;
use libp2p::kad::RecordKey;
use libp2p::swarm;
use libp2p::{Multiaddr, PeerId, Swarm};
use p2p_proto::block::{
    BlockBodiesRequest, BlockBodiesResponse, BlockHeadersRequest, BlockHeadersResponse, NewBlock,
};
use p2p_proto::event::{EventsRequest, EventsResponse};
use p2p_proto::receipt::{ReceiptsRequest, ReceiptsResponse};
use p2p_proto::transaction::{TransactionsRequest, TransactionsResponse};
use pathfinder_common::{BlockHash, BlockNumber, ChainId};
use peers::Peer;
use tokio::sync::{mpsc, oneshot};

mod behaviour;
pub mod client;
mod main_loop;
mod peers;
mod sync;
#[cfg(test)]
mod test_utils;
#[cfg(test)]
mod tests;
mod transport;

pub use client::peer_agnostic::PeerData;
pub use libp2p;
pub use sync::protocol::PROTOCOLS;

use client::peer_aware::Client;
use main_loop::MainLoop;

pub use behaviour::{kademlia_protocol_name, IDENTIFY_PROTOCOL_NAME};

pub fn new(keypair: Keypair, cfg: Config, chain_id: ChainId) -> (Client, EventReceiver, MainLoop) {
    let local_peer_id = keypair.public().to_peer_id();

    let (behaviour, relay_transport) = behaviour::Behaviour::new(&keypair, chain_id, cfg.clone());

    let swarm = Swarm::new(
        transport::create(&keypair, relay_transport),
        behaviour,
        local_peer_id,
        // libp2p v0.52 related change: `libp2p::swarm::keep_alive`` has been deprecated and
        // it is advised to set the idle connection timeout to maximum value instead.
        //
        // TODO but ultimately do we really need keep_alive?
        // 1. sync status message was removed in the latest spec, but as we used it partially to
        //    maintain connection with peers, we're using keep alive instead
        // 2. I'm not sure if we really need keep alive, as connections should be closed when not used
        //    because they consume resources, and in general we should be managing connections in a wiser manner,
        //    the deprecated `libp2p::swarm::keep_alive::Behaviour` was supposed to be mostly used for testing anyway.
        swarm::Config::with_tokio_executor().with_idle_connection_timeout(Duration::MAX),
    );

    let (command_sender, command_receiver) = mpsc::channel(1);
    let (event_sender, event_receiver) = mpsc::channel(1);

    (
        Client::new(command_sender, local_peer_id),
        event_receiver,
        MainLoop::new(swarm, command_receiver, event_sender, cfg, chain_id),
    )
}

/// P2P limitations.
#[derive(Debug, Clone)]
pub struct Config {
    /// A direct (not relayed) peer can only connect once in this period.
    pub direct_connection_timeout: Duration,
    /// A relayed peer can only connect once in this period.
    pub relay_connection_timeout: Duration,
    /// Maximum number of direct (non-relayed) peers.
    pub max_inbound_direct_peers: usize,
    /// Maximum number of relayed peers.
    pub max_inbound_relayed_peers: usize,
    /// How long to prevent evicted peers from reconnecting.
    pub eviction_timeout: Duration,
    pub ip_whitelist: Vec<IpNet>,
    pub bootstrap: BootstrapConfig,
}

impl Config {
    pub fn new(
        max_inbound_direct_peers: usize,
        max_inbound_relay_peers: usize,
        bootstrap: BootstrapConfig,
    ) -> Self {
        Self {
            direct_connection_timeout: Duration::from_secs(30),
            relay_connection_timeout: Duration::from_secs(10),
            max_inbound_direct_peers,
            max_inbound_relayed_peers: max_inbound_relay_peers,
            ip_whitelist: vec!["::/0".parse().unwrap(), "0.0.0.0/0".parse().unwrap()],
            bootstrap,
            eviction_timeout: Duration::from_secs(15 * 60),
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub struct BootstrapConfig {
    pub start_offset: Duration,
    pub period: Duration,
}

impl Default for BootstrapConfig {
    fn default() -> Self {
        Self {
            start_offset: Duration::from_secs(5),
            period: Duration::from_secs(10 * 60),
        }
    }
}

pub type HeadTx = tokio::sync::watch::Sender<Option<(BlockNumber, BlockHash)>>;
pub type HeadRx = tokio::sync::watch::Receiver<Option<(BlockNumber, BlockHash)>>;

type EmptyResultSender = oneshot::Sender<anyhow::Result<()>>;

#[derive(Debug)]
enum Command {
    StarListening {
        addr: Multiaddr,
        sender: EmptyResultSender,
    },
    Dial {
        peer_id: PeerId,
        addr: Multiaddr,
        sender: EmptyResultSender,
    },
    Disconnect {
        peer_id: PeerId,
        sender: EmptyResultSender,
    },
    ProvideCapability {
        capability: String,
        sender: EmptyResultSender,
    },
    GetCapabilityProviders {
        capability: String,
        sender: mpsc::Sender<anyhow::Result<HashSet<PeerId>>>,
    },
    SubscribeTopic {
        topic: IdentTopic,
        sender: EmptyResultSender,
    },
    SendHeadersSyncRequest {
        peer_id: PeerId,
        request: BlockHeadersRequest,
        sender: oneshot::Sender<anyhow::Result<ResponseReceiver<BlockHeadersResponse>>>,
    },
    SendBodiesSyncRequest {
        peer_id: PeerId,
        request: BlockBodiesRequest,
        sender: oneshot::Sender<anyhow::Result<ResponseReceiver<BlockBodiesResponse>>>,
    },
    SendTransactionsSyncRequest {
        peer_id: PeerId,
        request: TransactionsRequest,
        sender: oneshot::Sender<anyhow::Result<ResponseReceiver<TransactionsResponse>>>,
    },
    SendReceiptsSyncRequest {
        peer_id: PeerId,
        request: ReceiptsRequest,
        sender: oneshot::Sender<anyhow::Result<ResponseReceiver<ReceiptsResponse>>>,
    },
    SendEventsSyncRequest {
        peer_id: PeerId,
        request: EventsRequest,
        sender: oneshot::Sender<anyhow::Result<ResponseReceiver<EventsResponse>>>,
    },
    PublishPropagationMessage {
        topic: IdentTopic,
        new_block: NewBlock,
        sender: EmptyResultSender,
    },
    /// For testing purposes only
    _Test(TestCommand),
}

#[derive(Debug)]
pub enum TestCommand {
    GetPeersFromDHT(oneshot::Sender<HashSet<PeerId>>),
    GetConnectedPeers(oneshot::Sender<HashMap<PeerId, Peer>>),
}

#[derive(Debug)]
pub enum Event {
    SyncPeerConnected {
        peer_id: PeerId,
    },
    InboundHeadersSyncRequest {
        from: PeerId,
        request: BlockHeadersRequest,
        channel: ResponseSender<BlockHeadersResponse>,
    },
    InboundBodiesSyncRequest {
        from: PeerId,
        request: BlockBodiesRequest,
        channel: ResponseSender<BlockBodiesResponse>,
    },
    InboundTransactionsSyncRequest {
        from: PeerId,
        request: TransactionsRequest,
        channel: ResponseSender<TransactionsResponse>,
    },
    InboundReceiptsSyncRequest {
        from: PeerId,
        request: ReceiptsRequest,
        channel: ResponseSender<ReceiptsResponse>,
    },
    InboundEventsSyncRequest {
        from: PeerId,
        request: EventsRequest,
        channel: ResponseSender<EventsResponse>,
    },
    BlockPropagation {
        from: PeerId,
        new_block: NewBlock,
    },
    /// For testing purposes only
    Test(TestEvent),
}

#[derive(Debug)]
pub enum TestEvent {
    NewListenAddress(Multiaddr),
    PeriodicBootstrapCompleted(Result<PeerId, PeerId>),
    StartProvidingCompleted(Result<RecordKey, RecordKey>),
    ConnectionEstablished { outbound: bool, remote: PeerId },
    ConnectionClosed { remote: PeerId },
    Subscribed { remote: PeerId, topic: String },
    PeerAddedToDHT { remote: PeerId },
    Dummy,
}

pub type EventReceiver = mpsc::Receiver<Event>;
