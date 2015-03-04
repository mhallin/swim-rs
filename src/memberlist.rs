use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::old_io::net::ip::SocketAddr;
use std::time::Duration;

use rand;
use rand::Rng;
use uuid::Uuid;

use member;
use member::{Member, MemberState, StateChange};

pub struct MemberList {
    members: Vec<Member>,
    periodic_index: usize,
}

impl MemberList {
    pub fn new(me: Member) -> Self {
        MemberList {
            members: vec![me],
            periodic_index: 0,
        }
    }

    pub fn to_vec(&self) -> Vec<Member> {
        self.members.clone()
    }

    pub fn to_map(&self) -> HashMap<Uuid, Member> {
        self.members.iter().map(|ref m| (m.host_key().clone(), (*m).clone())).collect()
    }

    fn mut_myself(&mut self) -> &mut Member {
        for member in self.members.iter_mut() {
            if member.is_myself() {
                return member;
            }
        }

        panic!("Could not find myself as member");
    }

    pub fn reincarnate_self(&mut self) -> Member {
        let myself = self.mut_myself();
        myself.reincarnate();

        myself.clone()
    }

    pub fn next_random_member(&mut self) -> Option<Member> {
        if self.periodic_index == 0 {
            rand::thread_rng().shuffle(&mut self.members);
        }

        let other_members: Vec<_> = self.members.iter().filter(|&m| m.is_remote()).collect();

        if other_members.len() == 0 {
            None
        }
        else {
            self.periodic_index = (self.periodic_index + 1) % other_members.len();
            Some(other_members[self.periodic_index].clone())
        }
    }

    pub fn time_out_nodes(&mut self, expired_hosts: HashSet<SocketAddr>) -> (Vec<Member>, Vec<Member>) {
        let mut suspect_members = Vec::new();
        let mut down_members = Vec::new();

        for mut member in self.members.iter_mut() {
            if let Some(remote_host) = member.remote_host() {
                if !expired_hosts.contains(&remote_host) {
                    continue;
                }

                if member.state() == MemberState::Alive {
                    member.set_state(MemberState::Suspect);
                    suspect_members.push(member.clone());
                }
                else if member.state() == MemberState::Suspect && member.state_change_older_than(Duration::seconds(3)) {
                    member.set_state(MemberState::Down);
                    down_members.push(member.clone());
                }
            }
        }

        (suspect_members, down_members)
    }

    pub fn mark_node_alive(&mut self, src_addr: &SocketAddr) -> Option<Member> {
        for mut member in self.members.iter_mut() {
            if member.remote_host() == Some(*src_addr) && member.state() != MemberState::Alive {
                member.set_state(MemberState::Alive);

                return Some(member.clone())
            }
        }

        None
    }

    pub fn apply_state_changes(&mut self, state_changes: Vec<StateChange>, from: &SocketAddr) -> (Vec<Member>, Vec<Member>) {
        let mut current_members = self.to_map();

        let mut changed_nodes = Vec::new();
        let mut new_nodes = Vec::new();

        let my_host_key = self.mut_myself().host_key();

        for state_change in state_changes {
            let new_member_data = state_change.member();
            let old_member_data = current_members.entry(new_member_data.host_key());

            if new_member_data.host_key() == my_host_key {
                if new_member_data.state() != MemberState::Alive {
                    let myself = self.reincarnate_self();
                    changed_nodes.push(myself.clone());
                }
            }
            else {
                match old_member_data {
                    Entry::Occupied(mut entry) => {
                        let new_member = member::most_recent_member_data(&new_member_data, entry.get()).clone();
                        let new_host = new_member.remote_host().or(entry.get().remote_host()).unwrap();
                        let new_member = new_member.member_by_changing_host(new_host);

                        if new_member.state() != entry.get().state() {
                            entry.insert(new_member.clone());
                            changed_nodes.push(new_member);
                        }
                    },
                    Entry::Vacant(entry) => {
                        let new_host = new_member_data.remote_host().unwrap_or(*from);
                        let new_member = new_member_data.member_by_changing_host(new_host);

                        entry.insert(new_member.clone());
                        new_nodes.push(new_member);
                    }
                };
            }
        }

        self.members = current_members.values().cloned().collect();

        (new_nodes, changed_nodes)
    }

    pub fn hosts_for_indirect_ping(&self, target: &SocketAddr) -> Vec<SocketAddr> {
        let mut possible_members: Vec<_> = self.members
            .iter()
            .filter(|m|
                m.state() == MemberState::Alive
                && m.is_remote()
                && m.remote_host() != Some(*target))
            .map(|m| m.remote_host().unwrap())
            .collect();

        rand::thread_rng().shuffle(&mut possible_members);

        possible_members.iter().take(3).cloned().collect()
    }

    pub fn has_member(&self, remote_host: &SocketAddr) -> bool {
        self.members.iter().any(|ref m| m.remote_host() == Some(*remote_host))
    }

    pub fn add_member(&mut self, member: Member) {
        self.members.push(member)
    }

}
