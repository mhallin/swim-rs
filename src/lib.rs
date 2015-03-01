#![feature(core)]
#![feature(old_io)]
#![feature(std_misc)]
#![feature(collections)]

extern crate "rustc-serialize" as rustc_serialize;
extern crate time;
extern crate uuid;
extern crate rand;

use std::fmt::{Debug, Formatter};
use std::cmp::Ordering;
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
use rand::Rng;

#[derive(Debug)]
pub enum MemberListEvent {
    MemberJoined(Member),
    MemberWentUp(Member),
    MemberSuspectedDown(Member),
    MemberWentDown(Member),
}

pub struct MemberList {
    pub events: Receiver<(Vec<Member>, MemberListEvent)>,
    comm: Sender<InternalRequest>,
}

#[derive(RustcEncodable, RustcDecodable, Debug, Clone, PartialOrd, Ord, PartialEq, Eq, Copy)]
pub enum MemberState {
    Alive,
    Suspect,
    Down,
}

#[derive(Debug, Clone)]
struct EncSocketAddr {
    addr: SocketAddr,
}

#[derive(RustcEncodable, RustcDecodable, Debug, Clone)]
enum Request {
    Ping,
    PingRequest(EncSocketAddr),
    Ack,
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

#[derive(Clone, PartialEq, Eq)]
pub struct Member {
    host_key: Uuid,
    remote_host: Option<SocketAddr>,
    incarnation: u64,
    member_state: MemberState,
    last_state_change: time::Tm,
}

struct State {
    host_key: Uuid,
    cluster_key: Vec<u8>,
    members: Vec<Member>,
    seed_queue: Vec<SocketAddr>,
    pending_responses: Vec<(time::Tm, SocketAddr, Vec<StateChange>)>,
    state_changes: Vec<StateChange>,
    wait_list: HashMap<SocketAddr, Vec<SocketAddr>>,
    socket: UdpSocket,
    periodic_index: usize,
}

#[derive(RustcEncodable, RustcDecodable, Debug, Clone, PartialOrd, Ord, PartialEq, Eq)]
struct StateChange {
    member: Member,
}

#[derive(RustcEncodable, RustcDecodable, Debug, Clone)]
struct Message {
    sender: Uuid,
    cluster_key: Vec<u8>,
    request: Request,
    state_changes: Vec<StateChange>,
}

pub fn start_member_list<A: ToSocketAddr>(host_key: Uuid, cluster_key: &str, listen_addr: A) -> MemberList {
    let (event_tx, event_rx) = channel();
    let (request_tx, request_rx) = channel();
    let (internal_tx, internal_rx) = channel();
    let socket = UdpSocket::bind(listen_addr).unwrap();
    let cluster_key = cluster_key.as_bytes().to_vec();

    let timeout_tx = request_tx.clone();
    let worker_socket = socket.clone();
    thread::spawn(move || {
        let me = Member {
            host_key: host_key.clone(),
            remote_host: None,
            incarnation: 0,
            member_state: MemberState::Alive,
            last_state_change: time::now_utc(),
        };

        let mut state = State {
            host_key: host_key,
            cluster_key: cluster_key,
            members: vec![me.clone()],
            seed_queue: Vec::new(),
            pending_responses: Vec::new(),
            state_changes: vec![StateChange { member: me }],
            wait_list: HashMap::new(),
            socket: worker_socket,
            periodic_index: 0,
        };

        let mut timer = Timer::new().unwrap();
        let periodic = timer.periodic(Duration::seconds(1));

        loop {
            select!(
                _ = periodic.recv() => {
                    enqueue_seed_nodes(&state.seed_queue, &timeout_tx);
                    enqueue_random_ping(&mut state, &timeout_tx);
                },
                request = request_rx.recv() => {
                    prune_timed_out_responses(&mut state, &event_tx, &timeout_tx);
                    process_request(&mut state, request.unwrap());
                },
                internal_message = internal_rx.recv() => {
                    process_internal_request(&mut state, internal_message.unwrap(), &event_tx, &timeout_tx);
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

    return MemberList { events: event_rx, comm: internal_tx };
}

impl MemberList {
    pub fn add_seed_node<A: ToSocketAddr>(&self, addr: A) {
        self.comm.send(InternalRequest::AddSeed(addr.to_socket_addr().unwrap())).unwrap();
    }
}

fn process_request(state: &mut State, request: TargetedRequest) {
    use Request::*;

    let timeout = time::now_utc() + Duration::seconds(3);
    let should_add_pending = match request.request { Ping => true, _ => false };
    let message = build_message(&state.host_key, &state.cluster_key, request.request, state.state_changes.clone());

    if should_add_pending {
        state.pending_responses.push((timeout, request.target.clone(), message.state_changes.clone()));
    }

    let encoded = json::encode(&message).unwrap();

    assert!(encoded.len() < 512);

    state.socket.send_to(encoded.as_bytes(), request.target).unwrap();
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

fn enqueue_seed_nodes(seed_nodes: &[SocketAddr], tx: &Sender<TargetedRequest>) {
    for seed_node in seed_nodes {
        tx.send(TargetedRequest { request: Request::Ping, target: seed_node.clone() }).unwrap();
    }
}

fn enqueue_random_ping(state: &mut State, tx: &Sender<TargetedRequest>) {
    if state.periodic_index == 0 {
        rand::thread_rng().shuffle(&mut state.members);
    }

    let other_members: Vec<_> = state.members.iter().filter(|&m| m.remote_host.is_some()).collect();

    if other_members.len() == 0 {
        return;
    }

    state.periodic_index = (state.periodic_index + 1) % other_members.len();
    let member = &other_members[state.periodic_index];
    tx.send(TargetedRequest { request: Request::Ping, target: member.remote_host.unwrap().clone() }).unwrap();
}

fn prune_timed_out_responses(state: &mut State, event_tx: &Sender<(Vec<Member>, MemberListEvent)>, request_tx: &Sender<TargetedRequest>) {
    let now = time::now_utc();

    let (remaining, expired): (Vec<_>, Vec<_>) = state.pending_responses
        .iter()
        .cloned()
        .partition(| &(t, _, _) | t < now);

    let expired_hosts: HashSet<SocketAddr> = expired
        .iter()
        .map(| &(_, a, _) | a)
        .collect();

    state.pending_responses = remaining;

    let mut suspect_members = Vec::new();
    let mut down_members = Vec::new();

    for mut member in state.members.iter_mut() {
        if member.remote_host.is_none() {
            continue;
        }

        if !expired_hosts.contains(&member.remote_host.unwrap()) {
            continue;
        }

        if member.member_state == MemberState::Alive {
            set_member_state(&mut member, MemberState::Suspect);
            suspect_members.push(member.clone());
        }
        else if member.member_state == MemberState::Suspect && member.last_state_change + Duration::seconds(3) < now {
            set_member_state(&mut member, MemberState::Down);
            down_members.push(member.clone());
        }
    }

    enqueue_state_change(state, down_members.as_slice());
    enqueue_state_change(state, suspect_members.as_slice());

    for member in suspect_members {
        send_ping_requests(state, &member, request_tx);
        send_member_list_event(state.members.clone(), event_tx, MemberListEvent::MemberSuspectedDown(member.clone()));
    }

    for member in down_members {
        send_member_list_event(state.members.clone(), event_tx, MemberListEvent::MemberWentDown(member.clone()));
    }
}

fn send_ping_requests(state: &State, target: &Member, request_tx: &Sender<TargetedRequest>) {
    let mut possible_members: Vec<_> = state.members
        .iter()
        .filter(|m|
            m.member_state == MemberState::Alive
            && m.remote_host.is_some()
            && m.remote_host != target.remote_host)
        .collect();

    rand::thread_rng().shuffle(&mut possible_members);

    for relay in possible_members.iter().take(3) {
        println!("Sending indirect ping through {:?}", relay);

        request_tx.send(TargetedRequest {
            request: Request::PingRequest(EncSocketAddr::from_addr(&target.remote_host.unwrap())),
            target: relay.remote_host.unwrap(),
        }).unwrap();
    }
}

fn set_member_state(member: &mut Member, state: MemberState) {
    if member.member_state != state {
        member.member_state = state;
        member.last_state_change = time::now_utc();
    }
}

fn send_member_list_event(members: Vec<Member>, event_tx: &Sender<(Vec<Member>, MemberListEvent)>, event: MemberListEvent) {
    use MemberListEvent::*;

    match event {
        MemberJoined(_) => {},
        MemberWentUp(ref m) => assert_eq!(m.member_state, MemberState::Alive),
        MemberWentDown(ref m) => assert_eq!(m.member_state, MemberState::Down),
        MemberSuspectedDown(ref m) => assert_eq!(m.member_state, MemberState::Suspect),
    };

    event_tx.send((members, event)).unwrap();
}

fn process_internal_request(state: &mut State, message: InternalRequest, event_tx: &Sender<(Vec<Member>, MemberListEvent)>, request_tx: &Sender<TargetedRequest>) {
    use InternalRequest::*;
    use Request::*;

    match message {
        AddSeed(addr) => {
            state.seed_queue.push(addr);
        },
        Respond(src_addr, message) => {
            if message.cluster_key != state.cluster_key {
                println!("ERROR: Mismatching cluster keys, ignoring message");
            }
            else {
                apply_state_changes(state, message.state_changes, src_addr, event_tx);
                remove_potential_seed(state, src_addr);

                ensure_node_is_member(state, src_addr, event_tx, message.sender);

                let response = match message.request {
                    Ping => Some(TargetedRequest { request: Ack, target: src_addr }),
                    PingRequest(dest_addr) => {
                        add_to_wait_list(state, &dest_addr.addr, &src_addr);
                        Some(TargetedRequest { request: Ping, target: dest_addr.addr })
                    },
                    Ack => {
                        ack_response(state, src_addr);
                        mark_node_alive(state, src_addr, event_tx);
                        None
                    },
                };

                match response {
                    Some(response) => request_tx.send(response).unwrap(),
                    None => (),
                };
            }
        },
    };
}

fn add_to_wait_list(state: &mut State, wait_addr: &SocketAddr, notify_addr: &SocketAddr) {
    match state.wait_list.entry(*wait_addr) {
        Entry::Occupied(mut entry) => { entry.get_mut().push(notify_addr.clone()); },
        Entry::Vacant(entry) => { entry.insert(vec![notify_addr.clone()]); }
    };
}

fn remove_potential_seed(state: &mut State, src_addr: SocketAddr) {
    state.seed_queue.retain(|&addr| addr != src_addr)
}

fn ack_response(state: &mut State, src_addr: SocketAddr) {
    let mut to_remove = Vec::new();

    for &(ref t, ref addr, ref state_changes) in state.pending_responses.iter() {
        if src_addr != *addr {
            continue;
        }

        to_remove.push((t.clone(), addr.clone(), state_changes.clone()));

        state.state_changes
            .retain(|os| !state_changes.iter().any(| is | is.member.host_key == os.member.host_key))
    }

    state.pending_responses.retain(|op| !to_remove.iter().any(|ip| ip == op));
}

fn ensure_node_is_member(state: &mut State, src_addr: SocketAddr, event_tx: &Sender<(Vec<Member>, MemberListEvent)>, sender: Uuid) {
    if state.members.iter().any(|ref m| m.remote_host == Some(src_addr)) {
        return;
    }

    let new_member = Member {
        host_key: sender,
        remote_host: Some(src_addr),
        incarnation: 0,
        member_state: MemberState::Alive,
        last_state_change: time::now_utc(),
    };

    state.members.push(new_member.clone());
    enqueue_state_change(state, &[new_member.clone()]);
    send_member_list_event(state.members.clone(), event_tx, MemberListEvent::MemberJoined(new_member));
}

fn mark_node_alive(state: &mut State, src_addr: SocketAddr, event_tx: &Sender<(Vec<Member>, MemberListEvent)>) {
    let mut found_member = None;

    for mut member in state.members.iter_mut() {
        if member.remote_host == Some(src_addr) {
            if member.member_state != MemberState::Alive {
                set_member_state(&mut member, MemberState::Alive);

                found_member = Some(member.clone());
            }
        }
    }

    if found_member.is_some() {
        let member = found_member.unwrap();
        enqueue_state_change(state, &[member.clone()]);
        send_member_list_event(state.members.clone(), event_tx, MemberListEvent::MemberWentUp(member.clone()));
    }
}

fn apply_state_changes(state: &mut State, state_changes: Vec<StateChange>, from: SocketAddr, event_tx: &Sender<(Vec<Member>, MemberListEvent)>) {
    let mut current_members: HashMap<Uuid, Member>
        = state.members.iter().map(|ref m| (m.host_key.clone(), (*m).clone())).collect();

    let mut changed_nodes = Vec::new();
    let mut new_nodes = Vec::new();

    for state_change in state_changes {
        let new_member_data = state_change.member;
        let old_member_data = current_members.entry(new_member_data.host_key.clone());

        if new_member_data.host_key == state.host_key {
            if new_member_data.member_state != MemberState::Alive {
                reincarnate_self(state);
            }
        }
        else {
            match old_member_data {
                Entry::Occupied(mut entry) => {
                    let mut new_member = most_recent_member_data(&new_member_data, entry.get()).clone();
                    new_member.remote_host = new_member.remote_host.or(entry.get().remote_host);

                    if new_member.member_state != entry.get().member_state {
                        entry.insert(new_member.clone());
                        enqueue_state_change(state, &[new_member.clone()]);
                        changed_nodes.push(new_member);
                    }
                },
                Entry::Vacant(entry) => {
                    let new_member = Member {
                        remote_host: Some(new_member_data.remote_host.unwrap_or(from)),
                        .. new_member_data
                    };

                    entry.insert(new_member.clone());
                    enqueue_state_change(state, &[new_member.clone()]);
                    new_nodes.push(new_member);
                }
            };
        }
    }

    state.members = current_members.values().cloned().collect();

    for member in new_nodes {
        send_member_list_event(state.members.clone(), event_tx, MemberListEvent::MemberJoined(member));
    }

    for member in changed_nodes {
        send_member_list_event(state.members.clone(), event_tx, determine_member_list_event(member));
    }
}

fn determine_member_list_event(member: Member) -> MemberListEvent {
    use MemberState::*;
    use MemberListEvent::*;

    return match member.member_state {
        Alive => MemberWentUp(member),
        Suspect => MemberSuspectedDown(member),
        Down => MemberWentDown(member),
    };
}

fn reincarnate_self(state: &mut State) {
    let mut found_member = None;

    for member in state.members.iter_mut() {
        if member.remote_host == None {
            member.incarnation += 1;
            found_member = Some(member.clone());
        }
    }

    let member = found_member.unwrap();
    enqueue_state_change(state, &[member.clone()]);
}

fn enqueue_state_change(state: &mut State, members: &[Member]) {
    for member in members {
        for state_change in &mut state.state_changes {
            if state_change.member.host_key == member.host_key {
                state_change.member = member.clone();
                return;
            }
        }

        state.state_changes.push(StateChange { member: member.clone() });
    }
}

fn most_recent_member_data<'a>(lhs: &'a Member, rhs: &'a Member) -> &'a Member {
    use MemberState::*;

    let lhs_overrides = match (lhs.member_state, lhs.incarnation, rhs.member_state, rhs.incarnation) {
        (Alive, i, Suspect, j) => i > j,
        (Alive, i, Alive, j) => i > j,
        (Suspect, i, Suspect, j) => i > j,
        (Suspect, i, Alive, j) => i >= j,
        (Down, _, Alive, _) => true,
        (Down, _, Suspect, _) => true,
        _ => false,
    };

    return if lhs_overrides { lhs } else { rhs };
}

impl Decodable for EncSocketAddr {
    fn decode<D: Decoder>(d: &mut D) -> Result<Self, D::Error> {
        match d.read_str() {
            Ok(s) => match FromStr::from_str(s.as_slice()) {
                Ok(addr) => Ok(EncSocketAddr { addr: addr }),
                Err(e) => Err(d.error(format!("{:?}", e).as_slice())),
            },
            Err(e) => Err(e),
        }
    }
}

impl Encodable for EncSocketAddr {
    fn encode<E: Encoder>(&self, e: &mut E) -> Result<(), E::Error> {
        format!("{}", self.addr).encode(e)
    }
}

impl EncSocketAddr {
    fn from_addr(addr: &SocketAddr) -> Self {
        EncSocketAddr { addr: addr.clone() }
    }
}

impl Decodable for Member {
    fn decode<D: Decoder>(d: &mut D) -> Result<Self, D::Error> {
        d.read_struct("m", 5, |d| {
            let host_key = try!(d.read_struct_field("h", 0, |d| Decodable::decode(d)));
            let remote_host = try!(d.read_struct_field("r", 1, |d| {
                d.read_option(|d, b| {
                    if b {
                        match d.read_str() {
                            Ok(s) => Ok(Some(FromStr::from_str(s.as_slice()).unwrap())),
                            Err(e) => Err(e),
                        }
                    }
                    else {
                        Ok(None)
                    }
                })
            }));
            let incarnation = try!(d.read_struct_field("i", 2, |d| Decodable::decode(d)));
            let member_state = try!(d.read_struct_field("m", 3, |d| Decodable::decode(d)));
            let (sec, nsec) = try!(d.read_struct_field("t", 4, |d| Decodable::decode(d)));

            Ok(Member {
                host_key: host_key,
                remote_host: remote_host,
                incarnation: incarnation,
                member_state: member_state,
                last_state_change: time::at_utc(time::Timespec::new(sec, nsec)),
            })
        })
    }
}

impl Encodable for Member {
    fn encode<E: Encoder>(&self, e: &mut E) -> Result<(), E::Error> {
        e.emit_struct("m", 5, |e| {
            try!(e.emit_struct_field("h", 0, |e| self.host_key.encode(e)));
            try!(e.emit_struct_field("r", 1, |e| {
                e.emit_option(|e| {
                    match self.remote_host {
                        Some(host) => e.emit_option_some(|e| format!("{}", host).encode(e)),
                        None => e.emit_option_none()
                    }
                })
            }));
            try!(e.emit_struct_field("i", 2, |e| self.incarnation.encode(e)));
            try!(e.emit_struct_field("m", 3, |e| self.member_state.encode(e)));
            e.emit_struct_field("t", 4, |e| {
                let spec = self.last_state_change.to_timespec();
                (spec.sec, spec.nsec).encode(e)
            })
        })
    }
}

impl PartialOrd for Member {
    fn partial_cmp(&self, rhs: &Member) -> Option<Ordering> {
        let t1 = (
            self.host_key.as_bytes(),
            format!("{:?}", self.remote_host),
            self.incarnation,
            self.member_state.clone(),
        );

        let t2 = (
            rhs.host_key.as_bytes(),
            format!("{:?}", rhs.remote_host),
            rhs.incarnation,
            rhs.member_state.clone(),
        );

        return t1.partial_cmp(&t2);
    }
}

impl Ord for Member {
    fn cmp(&self, rhs: &Member) -> Ordering {
        return self.partial_cmp(rhs).unwrap();
    }
}

impl Debug for Member {
    fn fmt(&self, formatter: &mut Formatter) -> std::fmt::Result {
        formatter.write_fmt(format_args!(
            "Member[{}, {}] => (state: {:?}, {} ms, host: {})",
            self.incarnation,
            self.host_key.to_hyphenated_string(),
            self.member_state,
            (time::get_time() - self.last_state_change.to_timespec()).num_milliseconds(),
            match self.remote_host {
                Some(host) => format!("{}", host),
                None => String::from_str("(myself)"),
            }))
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use rustc_serialize::json;
    use uuid;
    use time;
    use super::{Member, MemberState};

    #[test]
    fn test_member_encode_decode() {
        let member = Member {
            host_key: uuid::Uuid::new_v4(),
            remote_host: Some(FromStr::from_str("127.0.0.1:2552").unwrap()),
            incarnation: 123,
            member_state: MemberState::Alive,
            last_state_change: time::at_utc(time::Timespec::new(123, 456)),
        };

        let encoded = json::encode(&member).unwrap();
        let decoded = json::decode(&encoded).unwrap();

        assert_eq!(decoded, member);
    }
}
