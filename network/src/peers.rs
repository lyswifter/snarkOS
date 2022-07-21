// Copyright (C) 2019-2022 Aleo Systems Inc.
// This file is part of the snarkOS library.

// The snarkOS library is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// The snarkOS library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with the snarkOS library. If not, see <https://www.gnu.org/licenses/>.

use crate::{OutboundRouter, Peer, State};
use snarkos_environment::{
    network::{Data, DisconnectReason, Message},
    Environment,
};
use snarkvm::dpc::prelude::*;

#[cfg(any(feature = "test", feature = "prometheus"))]
use snarkos_metrics as metrics;

use anyhow::Result;
use rand::{prelude::IteratorRandom, rngs::OsRng, thread_rng, Rng};
use std::{
    collections::{HashMap, HashSet},
    net::{SocketAddr, IpAddr, Ipv4Addr},
    sync::Arc,
    time::{Duration, Instant, SystemTime}, 
};
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot, RwLock},
    task,
    time::timeout,
};

use std::fs::File;
use std::io::BufReader;
use std::io::prelude::*;
use dirs::home_dir;

/// Shorthand for the parent half of the `Peers` message channel.
pub type PeersRouter<N, E> = mpsc::Sender<PeersRequest<N, E>>;
/// Shorthand for the child half of the `Peers` message channel.
pub type PeersHandler<N, E> = mpsc::Receiver<PeersRequest<N, E>>;

/// Shorthand for the parent half of the connection result channel.
pub(crate) type ConnectionResult = oneshot::Sender<Result<()>>;

///
/// An enum of requests that the `Peers` struct processes.
///
#[derive(Debug)]
pub enum PeersRequest<N: Network, E: Environment> {
    /// Connect := (peer_ip, connection_result)
    Connect(SocketAddr, ConnectionResult),
    /// Heartbeat
    Heartbeat,
    /// MessagePropagate := (peer_ip, message)
    MessagePropagate(SocketAddr, Message<N, E>),
    /// MessageSend := (peer_ip, message)
    MessageSend(SocketAddr, Message<N, E>),
    /// PeerConnecting := (stream, peer_ip)
    PeerConnecting(TcpStream, SocketAddr),
    /// PeerConnected := (peer_ip, peer_nonce, outbound_router)
    PeerConnected(SocketAddr, u64, OutboundRouter<N, E>),
    /// PeerDisconnected := (peer_ip)
    PeerDisconnected(SocketAddr),
    /// PeerRestricted := (peer_ip)
    PeerRestricted(SocketAddr),
    /// SendPeerResponse := (peer_ip, rtt_start)
    /// Note: rtt_start is for the request/response cycle for sharing peers.
    SendPeerResponse(SocketAddr, Option<Instant>),
    /// ReceivePeerResponse := (\[peer_ip\])
    ReceivePeerResponse(Vec<SocketAddr>),
}

///
/// A list of peers connected to the node server.
///
pub struct Peers<N: Network, E: Environment> {
    /// The peers router of the node.
    peers_router: PeersRouter<N, E>,
    /// The local nonce for this node session.
    local_nonce: u64,
    /// The map connected peer IPs to their nonce and outbound message router.
    connected_peers: RwLock<HashMap<SocketAddr, (u64, OutboundRouter<N, E>)>>,
    /// The set of candidate peer IPs.
    candidate_peers: RwLock<HashSet<SocketAddr>>,
    /// The set of restricted peer IPs.
    restricted_peers: RwLock<HashMap<SocketAddr, Instant>>,
    /// The map of peers to their first-seen port number, number of attempts, and timestamp of the last inbound connection request.
    seen_inbound_connections: RwLock<HashMap<SocketAddr, ((u16, u32), SystemTime)>>,
    /// The map of peers to the timestamp of their last outbound connection request.
    seen_outbound_connections: RwLock<HashMap<SocketAddr, SystemTime>>,
    /// The shared state of the owning node.
    state: Arc<State<N, E>>,
}

impl<N: Network, E: Environment> Peers<N, E> {
    ///
    /// Initializes a new instance of `Peers` and its corresponding handler.
    ///
    pub async fn new(local_nonce: Option<u64>, state: Arc<State<N, E>>) -> (Self, mpsc::Receiver<PeersRequest<N, E>>) {
        // Initialize an mpsc channel for sending requests to the `Peers` struct.
        let (peers_router, peers_handler) = mpsc::channel(1024);

        // Sample the nonce.
        let local_nonce = match local_nonce {
            Some(nonce) => nonce,
            None => thread_rng().gen(),
        };

        // Initialize the peers.
        let peers = Self {
            peers_router,
            local_nonce,
            connected_peers: Default::default(),
            candidate_peers: Default::default(),
            restricted_peers: Default::default(),
            seen_inbound_connections: Default::default(),
            seen_outbound_connections: Default::default(),
            state,
        };

        (peers, peers_handler)
    }

    /// Returns an instance of the peers router.
    pub fn router(&self) -> &PeersRouter<N, E> {
        &self.peers_router
    }

    ///
    /// Returns `true` if the node is connected to the given IP.
    ///
    pub async fn is_connected_to(&self, ip: SocketAddr) -> bool {
        self.connected_peers.read().await.contains_key(&ip)
    }

    ///
    /// Returns `true` if the given IP is restricted.
    ///
    pub async fn is_restricted(&self, ip: SocketAddr) -> bool {
        match self.restricted_peers.read().await.get(&ip) {
            Some(timestamp) => timestamp.elapsed().as_secs() < E::RADIO_SILENCE_IN_SECS,
            None => false,
        }
    }

    ///
    /// Returns the list of connected peers.
    ///
    pub async fn connected_peers(&self) -> Vec<SocketAddr> {
        self.connected_peers.read().await.keys().copied().collect()
    }

    ///
    /// Returns the list of candidate peers.
    ///
    pub async fn candidate_peers(&self) -> HashSet<SocketAddr> {
        self.candidate_peers.read().await.clone()
    }

    pub async fn is_ignoreed_peers(&self, ipa: SocketAddr) -> bool {
        let ip_addr = ipa.ip();

        let mut local_path = home_dir().unwrap();

        local_path.push("aleo_ignore_address");

        // read local files
        let file = File::open(local_path);

        match file {
            Ok(f) => {
                let  buf_reader = BufReader::new(f);

                for line in buf_reader.lines() {
                    let line_str = line.unwrap();

                    let ip_v4 = line_str.parse::<Ipv4Addr>();

                    let ip_addr_str = IpAddr::V4(ip_v4.unwrap());

                    if ip_addr == ip_addr_str {
                        return true
                    }
                }
                
                return false
            }
            Err(_) => {
                return false
            }
        }
    }

    ///
    /// Returns the set of connected sync nodes.
    ///
    pub async fn connected_sync_nodes(&self) -> HashSet<SocketAddr> {
        let sync_nodes = E::sync_nodes();
        self.connected_peers
            .read()
            .await
            .keys()
            .filter(|addr| sync_nodes.contains(addr))
            .copied()
            .collect()
    }

    ///
    /// Returns the number of connected sync nodes.
    ///
    pub async fn number_of_connected_sync_nodes(&self) -> usize {
        let sync_nodes = E::sync_nodes();
        self.connected_peers
            .read()
            .await
            .keys()
            .filter(|addr| sync_nodes.contains(addr))
            .count()
    }

    ///
    /// Returns the number of connected peers.
    ///
    pub async fn number_of_connected_peers(&self) -> usize {
        self.connected_peers.read().await.len()
    }

    ///
    /// Returns the number of candidate peers.
    ///
    pub async fn number_of_candidate_peers(&self) -> usize {
        self.candidate_peers.read().await.len()
    }

    ///
    /// Returns the number of restricted peers.
    ///
    pub async fn number_of_restricted_peers(&self) -> usize {
        self.restricted_peers.read().await.len()
    }

    ///
    /// Returns the list of nonces for the connected peers.
    ///
    pub(crate) async fn connected_nonces(&self) -> Vec<u64> {
        self.connected_peers
            .read()
            .await
            .values()
            .map(|(peer_nonce, _)| *peer_nonce)
            .collect()
    }

    ///
    /// Performs the given `request` to the peers.
    /// All requests must go through this `update`, so that a unified view is preserved.
    ///
    pub(super) async fn update(&self, request: PeersRequest<N, E>) {
        match request {
            PeersRequest::Connect(peer_ip, connection_result) => {
                // Ensure the peer IP is not this node.
                if peer_ip == self.state.local_ip
                    || (peer_ip.ip().is_unspecified() || peer_ip.ip().is_loopback()) && peer_ip.port() == self.state.local_ip.port()
                {
                    debug!("Skipping connection request to {} (attempted to self-connect)", peer_ip);
                }
                // Ensure the node does not surpass the maximum number of peer connections.
                else if self.number_of_connected_peers().await >= E::MAXIMUM_NUMBER_OF_PEERS {
                    debug!("Skipping connection request to {} (maximum peers reached)", peer_ip);
                }
                // Ensure the peer is a new connection.
                else if self.is_connected_to(peer_ip).await {
                    debug!("Skipping connection request to {} (already connected)", peer_ip);
                }
                // Ensure the peer is not restricted.
                else if self.is_restricted(peer_ip).await {
                    debug!("Skipping connection request to {} (restricted)", peer_ip);
                } else if self.is_ignoreed_peers(peer_ip).await {
                    debug!("Skipping giving peers for {} (ignored)", peer_ip);
                }
                // Attempt to open a TCP stream.
                else {
                    // Lock seen_outbound_connections for further processing.
                    let mut seen_outbound_connections = self.seen_outbound_connections.write().await;

                    // Ensure the node respects the connection frequency limit.
                    let last_seen = seen_outbound_connections.entry(peer_ip).or_insert(SystemTime::UNIX_EPOCH);
                    let elapsed = last_seen.elapsed().unwrap_or(Duration::MAX).as_secs();
                    if elapsed < E::RADIO_SILENCE_IN_SECS {
                        trace!("Skipping connection request to {} (tried {} secs ago)", peer_ip, elapsed);
                    } else {
                        debug!("Connecting to {}...", peer_ip);
                        // Update the last seen timestamp for this peer.
                        seen_outbound_connections.insert(peer_ip, SystemTime::now());

                        // Release the lock over seen_outbound_connections.
                        drop(seen_outbound_connections);

                        // Initialize the peer handler.
                        match timeout(Duration::from_millis(E::CONNECTION_TIMEOUT_IN_MILLIS), TcpStream::connect(peer_ip)).await {
                            Ok(stream) => match stream {
                                Ok(stream) => {
                                    Peer::handler(
                                        stream,
                                        self.local_nonce,
                                        self.connected_nonces().await,
                                        Some(connection_result),
                                        self.state.clone(),
                                    )
                                    .await
                                }
                                Err(error) => {
                                    trace!("Failed to connect to '{}': '{:?}'", peer_ip, error);
                                    self.candidate_peers.write().await.remove(&peer_ip);
                                }
                            },
                            Err(error) => {
                                error!("Unable to reach '{}': '{:?}'", peer_ip, error);
                                self.candidate_peers.write().await.remove(&peer_ip);
                            }
                        };
                    }
                }
            }
            PeersRequest::Heartbeat => {
                // Obtain the number of connected peers.
                let number_of_connected_peers = self.number_of_connected_peers().await;
                // Ensure the number of connected peers is below the maximum threshold.
                if number_of_connected_peers > E::MAXIMUM_NUMBER_OF_PEERS {
                    debug!("Exceeded maximum number of connected peers");

                    // Determine the peers to disconnect from.
                    let num_excess_peers = number_of_connected_peers.saturating_sub(E::MAXIMUM_NUMBER_OF_PEERS);
                    let peer_ips_to_disconnect = self
                        .connected_peers
                        .read()
                        .await
                        .iter()
                        .filter(|(peer_ip, _)| {
                            !E::sync_nodes().contains(peer_ip)
                                && !E::beacon_nodes().contains(peer_ip)
                                && !E::trusted_nodes().contains(peer_ip)
                        })
                        .take(num_excess_peers)
                        .map(|(&peer_ip, _)| peer_ip)
                        .collect::<Vec<SocketAddr>>();

                    // Proceed to send disconnect requests to these peers.
                    for peer_ip in peer_ips_to_disconnect {
                        info!("Disconnecting from {} (exceeded maximum connections)", peer_ip);
                        self.send(peer_ip, Message::Disconnect(DisconnectReason::TooManyPeers)).await;
                        // Add an entry for this `Peer` in the restricted peers.
                        self.restricted_peers.write().await.insert(peer_ip, Instant::now());
                    }
                }

                // TODO (howardwu): This logic can be optimized and unified with the context around it.
                // Determine if the node is connected to more sync nodes than expected.
                let connected_sync_nodes = self.connected_sync_nodes().await;
                let number_of_connected_sync_nodes = connected_sync_nodes.len();
                let num_excess_sync_nodes = number_of_connected_sync_nodes.saturating_sub(1);
                if num_excess_sync_nodes > 0 {
                    debug!("Exceeded maximum number of sync nodes");

                    // Proceed to send disconnect requests to these peers.
                    for peer_ip in connected_sync_nodes
                        .iter()
                        .copied()
                        .choose_multiple(&mut OsRng::default(), num_excess_sync_nodes)
                    {
                        info!("Disconnecting from {} (exceeded maximum connections)", peer_ip);
                        self.send(peer_ip, Message::Disconnect(DisconnectReason::TooManyPeers)).await;
                        // Add an entry for this `Peer` in the restricted peers.
                        self.restricted_peers.write().await.insert(peer_ip, Instant::now());
                    }
                }

                // Ensure that the trusted nodes are connected.
                if !E::trusted_nodes().is_empty() {
                    let connected_peers = self.connected_peers().await.into_iter().collect::<HashSet<_>>();
                    let trusted_nodes = E::trusted_nodes();
                    let disconnected_trusted_nodes = trusted_nodes.difference(&connected_peers).copied();
                    for peer_ip in disconnected_trusted_nodes {
                        // Initialize the connection process.
                        let (router, handler) = oneshot::channel();
                        let request = PeersRequest::Connect(peer_ip, router);
                        if let Err(error) = self.peers_router.send(request).await {
                            warn!("Failed to transmit the request: '{}'", error);
                        }

                        // Do not wait for the result of each connection.
                        // Procure a resource id to register the task with, as it might be terminated at any point in time.
                        let resource_id = E::resources().procure_id();
                        E::resources().register_task(
                            Some(resource_id),
                            task::spawn(async move {
                                let _ = handler.await;

                                E::resources().deregister(resource_id);
                            }),
                        );
                    }
                }

                // Skip if the number of connected peers is above the minimum threshold.
                match number_of_connected_peers < E::MINIMUM_NUMBER_OF_PEERS {
                    true => {
                        if number_of_connected_peers > 0 {
                            trace!("Sending requests for more peer connections");
                            // Request more peers if the number of connected peers is below the threshold.
                            for peer_ip in self.connected_peers().await.iter().choose_multiple(&mut OsRng::default(), 3) {
                                self.send(*peer_ip, Message::PeerRequest).await;
                            }
                        }
                    }
                    false => return,
                };

                // Add the sync nodes to the list of candidate peers.
                if number_of_connected_sync_nodes == 0 {
                    self.add_candidate_peers(E::sync_nodes().iter()).await;
                }

                // Add the beacon nodes to the list of candidate peers.
                self.add_candidate_peers(E::beacon_nodes().iter()).await;

                // Attempt to connect to more peers if the number of connected peers is below the minimum threshold.
                // Select the peers randomly from the list of candidate peers.
                let midpoint_number_of_peers = E::MINIMUM_NUMBER_OF_PEERS.saturating_add(E::MAXIMUM_NUMBER_OF_PEERS) / 2;
                for peer_ip in self
                    .candidate_peers()
                    .await
                    .iter()
                    .copied()
                    .choose_multiple(&mut OsRng::default(), midpoint_number_of_peers)
                {
                    // Ensure this node is not connected to more than the permitted number of sync nodes.
                    if E::sync_nodes().contains(&peer_ip) && number_of_connected_sync_nodes >= 1 {
                        continue;
                    }

                    if !self.is_connected_to(peer_ip).await {
                        trace!("Attempting connection to {}...", peer_ip);

                        // Initialize the connection process.
                        let (router, handler) = oneshot::channel();
                        let request = PeersRequest::Connect(peer_ip, router);
                        if let Err(error) = self.peers_router.send(request).await {
                            warn!("Failed to transmit the request: '{}'", error);
                        }
                        // Do not wait for the result of each connection.
                        // Procure a resource id to register the task with, as it might be terminated at any point in time.
                        let resource_id = E::resources().procure_id();
                        E::resources().register_task(
                            Some(resource_id),
                            task::spawn(async move {
                                let _ = handler.await;

                                E::resources().deregister(resource_id);
                            }),
                        );
                    }
                }
            }
            PeersRequest::MessagePropagate(sender, message) => {
                self.propagate(sender, message).await;
            }
            PeersRequest::MessageSend(sender, message) => {
                self.send(sender, message).await;
            }
            PeersRequest::PeerConnecting(stream, peer_ip) => {
                // Ensure the peer IP is not this node.
                if peer_ip == self.state.local_ip
                    || (peer_ip.ip().is_unspecified() || peer_ip.ip().is_loopback()) && peer_ip.port() == self.state.local_ip.port()
                {
                    debug!("Skipping connection request to {} (attempted to self-connect)", peer_ip);
                }
                // Ensure the node does not surpass the maximum number of peer connections.
                else if self.number_of_connected_peers().await >= E::MAXIMUM_NUMBER_OF_PEERS {
                    debug!("Dropping connection request from {} (maximum peers reached)", peer_ip);
                }
                // Ensure the node is not already connected to this peer.
                else if self.is_connected_to(peer_ip).await {
                    debug!("Dropping connection request from {} (already connected)", peer_ip);
                }
                // Ensure the peer is not restricted.
                else if self.is_restricted(peer_ip).await {
                    debug!("Dropping connection request from {} (restricted)", peer_ip);
                }
                // Spawn a handler to be run asynchronously.
                else {
                    // Sanitize the port from the peer, if it is a remote IP address.
                    let (peer_lookup, peer_port) = match peer_ip.ip().is_loopback() {
                        // Loopback case - Do not sanitize, merely pass through.
                        true => (peer_ip, peer_ip.port()),
                        // Remote case - Sanitize, storing u16::MAX for the peer IP address to dedup the peer next time.
                        false => (SocketAddr::new(peer_ip.ip(), u16::MAX), peer_ip.port()),
                    };

                    // Lock seen_inbound_connections for further processing.
                    let mut seen_inbound_connections = self.seen_inbound_connections.write().await;

                    // Fetch the inbound tracker entry for this peer.
                    let ((initial_port, num_attempts), last_seen) = seen_inbound_connections
                        .entry(peer_lookup)
                        .or_insert(((peer_port, 0), SystemTime::UNIX_EPOCH));
                    let elapsed = last_seen.elapsed().unwrap_or(Duration::MAX).as_secs();

                    // Reset the inbound tracker entry for this peer, if the predefined elapsed time has passed.
                    if elapsed > E::RADIO_SILENCE_IN_SECS {
                        // Reset the initial port for this peer.
                        *initial_port = peer_port;
                        // Reset the number of attempts for this peer.
                        *num_attempts = 0;
                        // Reset the last seen timestamp for this peer.
                        *last_seen = SystemTime::now();
                    }

                    // Ensure the connecting peer has not surpassed the connection attempt limit.
                    if *num_attempts > E::MAXIMUM_CONNECTION_FAILURES {
                        trace!("Dropping connection request from {} (tried {} secs ago)", peer_ip, elapsed);
                        // Add an entry for this `Peer` in the restricted peers.
                        self.restricted_peers.write().await.insert(peer_ip, Instant::now());
                    } else {
                        debug!("Received a connection request from {}", peer_ip);
                        // Update the number of attempts for this peer.
                        *num_attempts += 1;

                        // Release the lock over seen_inbound_connections.
                        drop(seen_inbound_connections);

                        // Initialize the peer handler.
                        Peer::handler(stream, self.local_nonce, self.connected_nonces().await, None, self.state.clone()).await;
                    }
                }
            }
            PeersRequest::PeerConnected(peer_ip, peer_nonce, outbound) => {
                // Add an entry for this `Peer` in the connected peers.
                self.connected_peers.write().await.insert(peer_ip, (peer_nonce, outbound));
                // Remove an entry for this `Peer` in the candidate peers, if it exists.
                self.candidate_peers.write().await.remove(&peer_ip);

                #[cfg(any(feature = "test", feature = "prometheus"))]
                {
                    let number_of_connected_peers = self.number_of_connected_peers().await;
                    let number_of_candidate_peers = self.number_of_candidate_peers().await;
                    metrics::gauge!(metrics::peers::CONNECTED, number_of_connected_peers as f64);
                    metrics::gauge!(metrics::peers::CANDIDATE, number_of_candidate_peers as f64);
                }
            }
            PeersRequest::PeerDisconnected(peer_ip) => {
                // Remove an entry for this `Peer` in the connected peers, if it exists.
                self.connected_peers.write().await.remove(&peer_ip);
                // Add an entry for this `Peer` in the candidate peers.
                self.candidate_peers.write().await.insert(peer_ip);

                #[cfg(any(feature = "test", feature = "prometheus"))]
                {
                    let number_of_connected_peers = self.number_of_connected_peers().await;
                    let number_of_candidate_peers = self.number_of_candidate_peers().await;
                    metrics::gauge!(metrics::peers::CONNECTED, number_of_connected_peers as f64);
                    metrics::gauge!(metrics::peers::CANDIDATE, number_of_candidate_peers as f64);
                }
            }
            PeersRequest::PeerRestricted(peer_ip) => {
                // Remove an entry for this `Peer` in the connected peers, if it exists.
                self.connected_peers.write().await.remove(&peer_ip);
                // Add an entry for this `Peer` in the restricted peers.
                self.restricted_peers.write().await.insert(peer_ip, Instant::now());

                #[cfg(any(feature = "test", feature = "prometheus"))]
                {
                    let number_of_connected_peers = self.number_of_connected_peers().await;
                    let number_of_restricted_peers = self.number_of_restricted_peers().await;
                    metrics::gauge!(metrics::peers::CONNECTED, number_of_connected_peers as f64);
                    metrics::gauge!(metrics::peers::RESTRICTED, number_of_restricted_peers as f64);
                }
            }
            PeersRequest::SendPeerResponse(recipient, rtt_start) => {
                // Send a `PeerResponse` message.
                let connected_peers = self.connected_peers().await;
                self.send(recipient, Message::PeerResponse(connected_peers, rtt_start)).await;
            }
            PeersRequest::ReceivePeerResponse(peer_ips) => {
                self.add_candidate_peers(peer_ips.iter()).await;

                #[cfg(any(feature = "test", feature = "prometheus"))]
                {
                    let number_of_candidate_peers = self.number_of_candidate_peers().await;
                    metrics::gauge!(metrics::peers::CANDIDATE, number_of_candidate_peers as f64);
                }
            }
        }
    }

    ///
    /// Adds the given peer IPs to the set of candidate peers.
    ///
    /// This method skips adding any given peers if the combined size exceeds the threshold,
    /// as the peer providing this list could be subverting the protocol.
    ///
    async fn add_candidate_peers<'a, T: ExactSizeIterator<Item = &'a SocketAddr> + IntoIterator>(&self, peers: T) {
        // Acquire the candidate peers write lock.
        let mut candidate_peers = self.candidate_peers.write().await;
        // Ensure the combined number of peers does not surpass the threshold.
        for peer_ip in peers.take(E::MAXIMUM_CANDIDATE_PEERS.saturating_sub(candidate_peers.len())) {
            // Ensure the peer is not self and is a new candidate peer.
            let is_self = *peer_ip == self.state.local_ip
                || (peer_ip.ip().is_unspecified() || peer_ip.ip().is_loopback()) && peer_ip.port() == self.state.local_ip.port();
            if !is_self && !self.is_connected_to(*peer_ip).await {
                // Proceed to insert each new candidate peer IP.
                candidate_peers.insert(*peer_ip);
            }
        }
    }

    ///
    /// Sends the given message to specified peer.
    ///
    async fn send(&self, peer: SocketAddr, message: Message<N, E>) {
        let target_peer = self.connected_peers.read().await.get(&peer).cloned();
        match target_peer {
            Some((_, outbound)) => {
                if let Err(error) = outbound.send(message).await {
                    trace!("Outbound channel failed: {}", error);
                    self.connected_peers.write().await.remove(&peer);

                    #[cfg(any(feature = "test", feature = "prometheus"))]
                    {
                        let number_of_connected_peers = self.number_of_connected_peers().await;
                        metrics::gauge!(metrics::peers::CONNECTED, number_of_connected_peers as f64);
                    }
                }
            }
            None => warn!("Attempted to send to a non-connected peer {}", peer),
        }
    }

    ///
    /// Sends the given message to every connected peer, excluding the sender.
    ///
    async fn propagate(&self, sender: SocketAddr, mut message: Message<N, E>) {
        // Perform ahead-of-time, non-blocking serialization just once for applicable objects.
        if let Message::UnconfirmedBlock(_, _, ref mut data) = message {
            let serialized_block = Data::serialize(data.clone()).await.expect("Block serialization is bugged");
            let _ = std::mem::replace(data, Data::Buffer(serialized_block));
        }

        // Iterate through all peers that are not the sender, sync node, or beacon node.
        for peer in self
            .connected_peers()
            .await
            .iter()
            .filter(|peer_ip| *peer_ip != &sender && !E::sync_nodes().contains(peer_ip) && !E::beacon_nodes().contains(peer_ip))
            .copied()
            .collect::<Vec<_>>()
        {
            self.send(peer, message.clone()).await;
        }
    }

    ///
    /// Removes the addresses of all known peers.
    ///
    #[cfg(feature = "test")]
    pub async fn reset_known_peers(&self) {
        self.candidate_peers.write().await.clear();
        self.restricted_peers.write().await.clear();
        self.seen_inbound_connections.write().await.clear();
        self.seen_outbound_connections.write().await.clear();
    }
}
