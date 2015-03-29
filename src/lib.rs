#![feature(core)]
#![feature(io)]
#![feature(old_io)]
#![feature(std_misc)]
#![feature(collections)]

extern crate rustc_serialize;
extern crate time;
extern crate uuid;
extern crate rand;

use std::collections::{HashMap, HashSet};
use std::collections::hash_map::Entry;
use std::old_io::net::ip::{SocketAddr, ToSocketAddr};
use std::old_io::net::udp::UdpSocket;
use std::old_io::timer::Timer;
use std::old_io::{IoError, IoErrorKind};
use std::default::Default;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread;

use rustc_serialize::{Decodable, Decoder, Encodable, Encoder};
use rustc_serialize::json;
use time::Duration;
use uuid::Uuid;

mod member;
mod memberlist;

use member::StateChange;
use memberlist::MemberList;

pub use member::{Member, MemberState};

pub type ClusterEvent = (Vec<Member>, MemberEvent);
type WaitList = HashMap<SocketAddr, Vec<SocketAddr>>;

#[derive(Debug)]
pub enum MemberEvent {
    MemberJoined(Member),
    MemberWentUp(Member),
    MemberSuspectedDown(Member),
    MemberWentDown(Member),
    MemberLeft(Member),
}

pub struct Cluster {
    pub events: Receiver<ClusterEvent>,
    comm: Sender<InternalRequest>,
}

pub struct ClusterConfig {
    pub cluster_key: Vec<u8>,
    pub ping_interval: Duration,
    pub network_mtu: usize,
    pub ping_request_host_count: usize,
    pub ping_timeout: Duration,
    pub listen_addr: SocketAddr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EncSocketAddr(SocketAddr);

#[derive(RustcEncodable, RustcDecodable, Debug, Clone, PartialEq, Eq)]
enum Request {
    Ping,
    Ack,
    PingRequest(EncSocketAddr),
    AckHost(Member),
}

#[derive(Debug, Clone)]
struct TargetedRequest {
    request: Request,
    target: SocketAddr,
}

#[derive(Clone)]
enum InternalRequest {
    AddSeed(SocketAddr),
    Respond(SocketAddr, Message),
    LeaveCluster,
    Exit(Sender<()>),
}

struct State {
    host_key: Uuid,
    config: ClusterConfig,
    members: MemberList,
    seed_queue: Vec<SocketAddr>,
    pending_responses: Vec<(time::Tm, SocketAddr, Vec<StateChange>)>,
    state_changes: Vec<StateChange>,
    wait_list: WaitList,
    socket: UdpSocket,
    request_tx: Sender<TargetedRequest>,
    event_tx: Sender<ClusterEvent>,
}

#[derive(RustcEncodable, RustcDecodable, Debug, Clone)]
struct Message {
    sender: Uuid,
    cluster_key: Vec<u8>,
    request: Request,
    state_changes: Vec<StateChange>,
}

pub fn start_cluster(host_key: Uuid, config: ClusterConfig) -> Cluster {
    let (event_tx, event_rx) = channel();
    let (internal_tx, internal_rx) = channel();

    let runner_tx = internal_tx.clone();
    thread::spawn(move || {
        run_main_loop(host_key, config, event_tx, runner_tx, internal_rx);
    });

    return Cluster { events: event_rx, comm: internal_tx };
}

fn run_main_loop(host_key: Uuid, config: ClusterConfig, event_tx: Sender<ClusterEvent>, internal_tx: Sender<InternalRequest>, internal_rx: Receiver<InternalRequest>) {
    let (request_tx, request_rx) = channel();
    let socket = UdpSocket::bind(config.listen_addr).unwrap();
    let stop_listener_cell = Arc::new(Mutex::new(false));

    let listener_tx = internal_tx.clone();
    let listener_socket = socket.clone();
    let network_mtu = config.network_mtu;
    let listener_cell = stop_listener_cell.clone();
    let listen_thread = thread::spawn(move || {
        run_udp_listen_loop(listener_cell, listener_socket, listener_tx, network_mtu);
    });

    let me = Member::myself(host_key.clone());

    let mut state = State {
        host_key: host_key,
        config: config,
        members: MemberList::new(me.clone()),
        seed_queue: Vec::new(),
        pending_responses: Vec::new(),
        state_changes: vec![StateChange::new(me)],
        wait_list: HashMap::new(),
        socket: socket,
        request_tx: request_tx,
        event_tx: event_tx,
    };

    let mut timer = Timer::new().unwrap();
    let ping_interval = std::time::Duration::microseconds(
        state.config.ping_interval.num_microseconds().unwrap());
    let periodic = timer.periodic(ping_interval);

    let mut exit_tx = None;

    loop {
        select!(
            _ = periodic.recv() => {
                state.enqueue_seed_nodes();
                state.enqueue_random_ping();
            },
            request = request_rx.recv() => {
                state.prune_timed_out_responses();
                state.process_request(request.unwrap());
            },
            internal_message = internal_rx.recv() => {
                exit_tx = state.process_internal_request(internal_message.unwrap());
            }
        );

        if exit_tx.is_some() {
            break;
        }
    }

    {
        let mut lock = (*stop_listener_cell).lock().unwrap();

        *lock = true;
    }

    listen_thread.join();

    if let Some(exit_tx) = exit_tx {
        exit_tx.send(()).unwrap();
    }
}

fn run_udp_listen_loop(stop_signal: Arc<Mutex<bool>>, mut listener_socket: UdpSocket, listener_tx: Sender<InternalRequest>, network_mtu: usize) {
    listener_socket.set_read_timeout(Some(100));

    loop {
        {
            let lock = (*stop_signal).lock().unwrap();
            if *lock {
                return;
            }
        }

        let mut buf = vec![0; network_mtu];

        match listener_socket.recv_from(&mut buf) {
            Ok((size, src_addr)) => {
                let message = json::decode(&*String::from_utf8_lossy(&buf[..size]));
                listener_tx.send(InternalRequest::Respond(src_addr, message.unwrap())).unwrap();
            },
            Err(IoError { kind: IoErrorKind::TimedOut, desc: _, detail: _ }) => (),
            Err(e) => {
                println!("Error while reading: {:?}", e);
            }
        };
    }
}

impl Cluster {
    pub fn add_seed_node<A: ToSocketAddr>(&self, addr: A) {
        self.comm.send(InternalRequest::AddSeed(addr.to_socket_addr().unwrap())).unwrap();
    }

    pub fn leave_cluster(&self) {
        self.comm.send(InternalRequest::LeaveCluster).unwrap();
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        let (tx, rx) = channel();

        self.comm.send(InternalRequest::Exit(tx)).unwrap();

        rx.recv().unwrap();
    }
}

impl State {
    fn process_request(&mut self, request: TargetedRequest) {
        use Request::*;

        let timeout = time::now_utc() + self.config.ping_timeout;
        let should_add_pending = request.request == Ping;
        let message = build_message(&self.host_key, &self.config.cluster_key, request.request, self.state_changes.clone(), self.config.network_mtu);

        if should_add_pending {
            self.pending_responses.push((timeout, request.target.clone(), message.state_changes.clone()));
        }

        let encoded = json::encode(&message).unwrap();

        assert!(encoded.len() < self.config.network_mtu);

        self.socket.send_to(encoded.as_bytes(), request.target).unwrap();
    }

    fn enqueue_seed_nodes(&self) {
        for seed_node in &self.seed_queue {
            self.request_tx.send(TargetedRequest { request: Request::Ping, target: seed_node.clone() }).unwrap();
        }
    }

    fn enqueue_random_ping(&mut self) {
        if let Some(member) = self.members.next_random_member() {
            self.request_tx.send(TargetedRequest { request: Request::Ping, target: member.remote_host().unwrap() }).unwrap();
        }
    }

    fn prune_timed_out_responses(&mut self) {
        let now = time::now_utc();

        let (remaining, expired): (Vec<_>, Vec<_>) = self.pending_responses
            .iter()
            .cloned()
            .partition(| &(t, _, _) | t < now);

        let expired_hosts: HashSet<SocketAddr> = expired
            .iter()
            .map(| &(_, a, _) | a)
            .collect();

        self.pending_responses = remaining;

        let (suspect, down) = self.members.time_out_nodes(expired_hosts);

        enqueue_state_change(&mut self.state_changes, &down);
        enqueue_state_change(&mut self.state_changes, &suspect);

        for member in suspect {
            self.send_ping_requests(&member);
            self.send_member_event(MemberEvent::MemberSuspectedDown(member.clone()));
        }

        for member in down {
            self.send_member_event(MemberEvent::MemberWentDown(member.clone()));
        }
    }

    fn send_ping_requests(&self, target: &Member) {
        if let Some(target_host) = target.remote_host() {
            for relay in self.members.hosts_for_indirect_ping(self.config.ping_request_host_count, &target_host) {
                self.request_tx.send(TargetedRequest {
                    request: Request::PingRequest(EncSocketAddr::from_addr(&target_host)),
                    target: relay,
                }).unwrap();
            }
        }
    }

    fn process_internal_request(&mut self, message: InternalRequest) -> Option<Sender<()>> {
        use InternalRequest::*;

        match message {
            AddSeed(addr) => self.seed_queue.push(addr),
            Respond(src_addr, message) => self.respond_to_message(src_addr, message),
            LeaveCluster => {
                let myself = self.members.leave();
                enqueue_state_change(&mut self.state_changes, &[myself]);
            },
            Exit(tx) => return Some(tx),
        };

        None
    }

    fn respond_to_message(&mut self, src_addr: SocketAddr, message: Message) {
        use Request::*;

        if message.cluster_key != self.config.cluster_key {
            println!("ERROR: Mismatching cluster keys, ignoring message");
        }
        else {
            self.apply_state_changes(message.state_changes, src_addr);
            remove_potential_seed(&mut self.seed_queue, src_addr);

            self.ensure_node_is_member(src_addr, message.sender);

            let response = match message.request {
                Ping => Some(TargetedRequest { request: Ack, target: src_addr }),
                Ack => {
                    self.ack_response(src_addr);
                    self.mark_node_alive(src_addr);
                    None
                },
                PingRequest(dest_addr) => {
                    let EncSocketAddr(dest_addr) = dest_addr;
                    add_to_wait_list(&mut self.wait_list, &dest_addr, &src_addr);
                    Some(TargetedRequest { request: Ping, target: dest_addr })
                },
                AckHost(member) => {
                    self.ack_response(member.remote_host().unwrap());
                    self.mark_node_alive(member.remote_host().unwrap());
                    None
                }
            };

            match response {
                Some(response) => self.request_tx.send(response).unwrap(),
                None => (),
            };
        }
    }

    fn ack_response(&mut self, src_addr: SocketAddr) {
        let mut to_remove = Vec::new();

        for &(ref t, ref addr, ref state_changes) in self.pending_responses.iter() {
            if src_addr != *addr {
                continue;
            }

            to_remove.push((t.clone(), addr.clone(), state_changes.clone()));

            self.state_changes
                .retain(|os| !state_changes.iter().any(| is | is.member().host_key() == os.member().host_key()))
        }

        self.pending_responses.retain(|op| !to_remove.iter().any(|ip| ip == op));
    }

    fn ensure_node_is_member(&mut self, src_addr: SocketAddr, sender: Uuid) {
        if self.members.has_member(&src_addr) {
            return;
        }

        let new_member = Member::new(sender, src_addr, 0, MemberState::Alive);

        self.members.add_member(new_member.clone());
        enqueue_state_change(&mut self.state_changes, &[new_member.clone()]);
        self.send_member_event(MemberEvent::MemberJoined(new_member));
    }

    fn send_member_event(&self, event: MemberEvent) {
        use MemberEvent::*;

        match event {
            MemberJoined(_) => {},
            MemberWentUp(ref m) => assert_eq!(m.state(), MemberState::Alive),
            MemberWentDown(ref m) => assert_eq!(m.state(), MemberState::Down),
            MemberSuspectedDown(ref m) => assert_eq!(m.state(), MemberState::Suspect),
            MemberLeft(ref m) => assert_eq!(m.state(), MemberState::Left),
        };

        self.event_tx.send((self.members.available_nodes(), event)).unwrap();
    }

    fn apply_state_changes(&mut self, state_changes: Vec<StateChange>, from: SocketAddr) {
        let (new, changed) = self.members.apply_state_changes(state_changes, &from);

        enqueue_state_change(&mut self.state_changes, &new);
        enqueue_state_change(&mut self.state_changes, &changed);

        for member in new {
            self.send_member_event(MemberEvent::MemberJoined(member));
        }

        for member in changed {
            self.send_member_event(determine_member_event(member));
        }
    }

    fn mark_node_alive(&mut self, src_addr: SocketAddr) {
        if let Some(member) = self.members.mark_node_alive(&src_addr) {
            match self.wait_list.get_mut(&src_addr) {
                Some(mut wait_list) => {
                    for remote in wait_list.drain() {
                        self.request_tx.send(TargetedRequest { request: Request::AckHost(member.clone()), target: remote }).unwrap();
                    }
                },
                None => ()
            };

            enqueue_state_change(&mut self.state_changes, &[member.clone()]);
            self.send_member_event(MemberEvent::MemberWentUp(member.clone()));
        }
    }
}

fn build_message(sender: &Uuid, cluster_key: &Vec<u8>, request: Request, state_changes: Vec<StateChange>, network_mtu: usize) -> Message {
    let mut message = Message {
        sender: sender.clone(),
        cluster_key: cluster_key.clone(),
        request: request.clone(),
        state_changes: Vec::new(),
    };

    for i in 0..state_changes.len() + 1 {
        message = Message {
            sender: sender.clone(),
            cluster_key: cluster_key.clone(),
            request: request.clone(),
            state_changes: (&state_changes[..i]).iter().cloned().collect(),
        };

        let encoded = json::encode(&message).unwrap();
        if encoded.len() >= network_mtu {
            return message;
        }
    }

    return message;
}

fn add_to_wait_list(wait_list: &mut WaitList, wait_addr: &SocketAddr, notify_addr: &SocketAddr) {
    match wait_list.entry(*wait_addr) {
        Entry::Occupied(mut entry) => { entry.get_mut().push(notify_addr.clone()); },
        Entry::Vacant(entry) => { entry.insert(vec![notify_addr.clone()]); }
    };
}

fn remove_potential_seed(seed_queue: &mut Vec<SocketAddr>, src_addr: SocketAddr) {
    seed_queue.retain(|&addr| addr != src_addr)
}

fn determine_member_event(member: Member) -> MemberEvent {
    use member::MemberState::*;
    use MemberEvent::*;

    return match member.state() {
        Alive => MemberWentUp(member),
        Suspect => MemberSuspectedDown(member),
        Down => MemberWentDown(member),
        Left => MemberLeft(member),
    };
}

fn enqueue_state_change(state_changes: &mut Vec<StateChange>, members: &[Member]) {
    for member in members {
        for state_change in state_changes.iter_mut() {
            if state_change.member().host_key() == member.host_key() {
                state_change.update(member.clone());
                return;
            }
        }

        state_changes.push(StateChange::new(member.clone()));
    }
}

impl Decodable for EncSocketAddr {
    fn decode<D: Decoder>(d: &mut D) -> Result<Self, D::Error> {
        match d.read_str() {
            Ok(s) => match FromStr::from_str(&s) {
                Ok(addr) => Ok(EncSocketAddr(addr)),
                Err(e) => Err(d.error(format!("{:?}", e).as_slice())),
            },
            Err(e) => Err(e),
        }
    }
}

impl Encodable for EncSocketAddr {
    fn encode<E: Encoder>(&self, e: &mut E) -> Result<(), E::Error> {
        let &EncSocketAddr(addr) = self;
        format!("{}", addr).encode(e)
    }
}

impl EncSocketAddr {
    fn from_addr(addr: &SocketAddr) -> Self {
        EncSocketAddr(addr.clone())
    }
}

impl Default for ClusterConfig {
    fn default() -> Self {
        ClusterConfig {
            cluster_key: "default".as_bytes().to_vec(),
            ping_interval: Duration::seconds(1),
            network_mtu: 512,
            ping_request_host_count: 3,
            ping_timeout: Duration::seconds(3),
            listen_addr: "127.0.0.1:2552".to_socket_addr().unwrap(),
        }
    }
}
