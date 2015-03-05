#![feature(core)]
#![feature(old_io)]
#![feature(std_misc)]
#![feature(collections)]

extern crate "rustc-serialize" as rustc_serialize;
extern crate time;
extern crate uuid;
extern crate rand;

use std::collections::{HashMap, HashSet};
use std::collections::hash_map::Entry;
use std::old_io::net::ip::{SocketAddr, ToSocketAddr};
use std::old_io::net::udp::UdpSocket;
use std::old_io::timer::Timer;
use std::str::FromStr;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::time::Duration;
use std::thread;

use rustc_serialize::{Decodable, Decoder, Encodable, Encoder};
use rustc_serialize::json;
use uuid::Uuid;

mod member;
mod memberlist;

use member::{Member, MemberState, StateChange};
use memberlist::MemberList;

pub type ClusterEvent = (Vec<Member>, MemberEvent);
type WaitList = HashMap<SocketAddr, Vec<SocketAddr>>;

#[derive(Debug)]
pub enum MemberEvent {
    MemberJoined(Member),
    MemberWentUp(Member),
    MemberSuspectedDown(Member),
    MemberWentDown(Member),
}

pub struct Cluster {
    pub events: Receiver<ClusterEvent>,
    comm: Sender<InternalRequest>,
}

#[derive(Debug, Clone)]
struct EncSocketAddr(SocketAddr);

#[derive(RustcEncodable, RustcDecodable, Debug, Clone)]
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

#[derive(Debug, Clone)]
enum InternalRequest {
    AddSeed(SocketAddr),
    Respond(SocketAddr, Message),
}

struct State {
    host_key: Uuid,
    cluster_key: Vec<u8>,
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

pub fn start_cluster<A: ToSocketAddr>(host_key: Uuid, cluster_key: &str, listen_addr: A) -> Cluster {
    let (event_tx, event_rx) = channel();
    let (request_tx, request_rx) = channel();
    let (internal_tx, internal_rx) = channel();
    let socket = UdpSocket::bind(listen_addr).unwrap();
    let cluster_key = cluster_key.as_bytes().to_vec();

    let timeout_tx = request_tx.clone();
    let worker_socket = socket.clone();
    thread::spawn(move || {
        let me = Member::myself(host_key.clone());

        let mut state = State {
            host_key: host_key,
            cluster_key: cluster_key,
            members: MemberList::new(me.clone()),
            seed_queue: Vec::new(),
            pending_responses: Vec::new(),
            state_changes: vec![StateChange::new(me)],
            wait_list: HashMap::new(),
            socket: worker_socket,
            request_tx: timeout_tx,
            event_tx: event_tx,
        };

        let mut timer = Timer::new().unwrap();
        let periodic = timer.periodic(Duration::seconds(1));

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
                    state.process_internal_request(internal_message.unwrap());
                }
            );
        }
    });

    let listener_tx = internal_tx.clone();
    let mut listener_socket = socket.clone();
    thread::spawn(move || {
        loop {
            let mut buf = [0; 512];

            match listener_socket.recv_from(&mut buf) {
                Ok((size, src_addr)) => {
                    let message = json::decode(&*String::from_utf8_lossy(&buf[..size]));
                    listener_tx.send(InternalRequest::Respond(src_addr, message.unwrap())).unwrap();
                },
                Err(e) => {
                    println!("Error while reading: {:?}", e);
                }
            };
        }
    });

    return Cluster { events: event_rx, comm: internal_tx };
}

impl Cluster {
    pub fn add_seed_node<A: ToSocketAddr>(&self, addr: A) {
        self.comm.send(InternalRequest::AddSeed(addr.to_socket_addr().unwrap())).unwrap();
    }
}

impl State {
    fn process_request(&mut self, request: TargetedRequest) {
        use Request::*;

        let timeout = time::now_utc() + Duration::seconds(3);
        let should_add_pending = match request.request { Ping => true, _ => false };
        let message = build_message(&self.host_key, &self.cluster_key, request.request, self.state_changes.clone());

        if should_add_pending {
            self.pending_responses.push((timeout, request.target.clone(), message.state_changes.clone()));
        }

        let encoded = json::encode(&message).unwrap();

        assert!(encoded.len() < 512);

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

        enqueue_state_change(&mut self.state_changes, down.as_slice());
        enqueue_state_change(&mut self.state_changes, suspect.as_slice());

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
            for relay in self.members.hosts_for_indirect_ping(&target_host) {
                self.request_tx.send(TargetedRequest {
                    request: Request::PingRequest(EncSocketAddr::from_addr(&target_host)),
                    target: relay,
                }).unwrap();
            }
        }
    }

    fn process_internal_request(&mut self, message: InternalRequest) {
        use InternalRequest::*;

        match message {
            AddSeed(addr) => self.seed_queue.push(addr),
            Respond(src_addr, message) => self.respond_to_message(src_addr, message),
        };
    }

    fn respond_to_message(&mut self, src_addr: SocketAddr, message: Message) {
        use Request::*;

        if message.cluster_key != self.cluster_key {
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
        };

        self.event_tx.send((self.members.to_vec(), event)).unwrap();
    }

    fn apply_state_changes(&mut self, state_changes: Vec<StateChange>, from: SocketAddr) {
        let (new, changed) = self.members.apply_state_changes(state_changes, &from);

        enqueue_state_change(&mut self.state_changes, new.as_slice());
        enqueue_state_change(&mut self.state_changes, changed.as_slice());

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

fn build_message(sender: &Uuid, cluster_key: &Vec<u8>, request: Request, state_changes: Vec<StateChange>) -> Message {
    let mut message = Message {
        sender: sender.clone(),
        cluster_key: cluster_key.clone(),
        request: request.clone(),
        state_changes: Vec::new(),
    };

    for i in range(0, state_changes.len() + 1) {
        message = Message {
            sender: sender.clone(),
            cluster_key: cluster_key.clone(),
            request: request.clone(),
            state_changes: (&state_changes[..i]).iter().cloned().collect(),
        };

        let encoded = json::encode(&message).unwrap();
        if encoded.len() >= 512 {
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
            Ok(s) => match FromStr::from_str(s.as_slice()) {
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
