use std::fmt;
use std::fmt::{Debug, Formatter};
use std::net::SocketAddr;
use std::str::FromStr;
use std::cmp::Ordering;

use rustc_serialize::{Decodable, Decoder, Encodable, Encoder};
use time;
use time::Duration;
use uuid::Uuid;


#[derive(RustcEncodable, RustcDecodable, Debug, Clone, PartialOrd, Ord, PartialEq, Eq, Copy)]
pub enum MemberState {
    Alive,
    Suspect,
    Down,
    Left,
}

#[derive(Clone, PartialEq, Eq)]
pub struct Member {
    host_key: Uuid,
    remote_host: Option<SocketAddr>,
    incarnation: u64,
    member_state: MemberState,
    last_state_change: time::Tm,
}

#[derive(RustcEncodable, RustcDecodable, Debug, Clone, PartialOrd, Ord, PartialEq, Eq)]
pub struct StateChange {
    member: Member,
}

impl Member {
    pub fn new(host_key: Uuid, remote_host: SocketAddr, incarnation: u64, known_state: MemberState) -> Self {
        Member {
            host_key: host_key, remote_host: Some(remote_host), incarnation: incarnation,
            member_state: known_state, last_state_change: time::now_utc(),
        }
    }

    pub fn myself(host_key: Uuid) -> Self {
        Member {
            host_key: host_key, remote_host: None, incarnation: 0,
            member_state: MemberState::Alive, last_state_change: time::now_utc(),
        }
    }

    pub fn host_key(&self) -> Uuid {
        self.host_key.clone()
    }

    pub fn remote_host(&self) -> Option<SocketAddr> {
        self.remote_host
    }

    pub fn is_remote(&self) -> bool {
        self.remote_host.is_some()
    }

    pub fn is_myself(&self) -> bool {
        self.remote_host.is_none()
    }

    pub fn state_change_older_than(&self, duration: Duration) -> bool {
        self.last_state_change + duration < time::now_utc()
    }

    pub fn state(&self) -> MemberState {
        self.member_state
    }

    pub fn set_state(&mut self, state: MemberState) {
        if self.member_state != state {
            self.member_state = state;
            self.last_state_change = time::now_utc();
        }
    }

    pub fn member_by_changing_host(&self, remote_host: SocketAddr) -> Member {
        Member {
            remote_host: Some(remote_host),
            .. self.clone()
        }
    }

    pub fn reincarnate(&mut self) {
        self.incarnation += 1
    }
}

impl StateChange {
    pub fn new(member: Member) -> StateChange {
        StateChange { member: member }
    }

    pub fn member(&self) -> &Member {
        &self.member
    }

    pub fn update(&mut self, member: Member) {
        self.member = member
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
                            Ok(s) => Ok(Some(FromStr::from_str(&s).unwrap())),
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
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        formatter.write_fmt(format_args!(
            "Member[{}, {}] => (state: {:?}, {} ms, host: {})",
            self.incarnation,
            self.host_key.to_hyphenated_string(),
            self.member_state,
            (time::get_time() - self.last_state_change.to_timespec()).num_milliseconds(),
            match self.remote_host {
                Some(host) => format!("{}", host),
                None => String::from("(myself)"),
            }))
    }
}

pub fn most_recent_member_data<'a>(lhs: &'a Member, rhs: &'a Member) -> &'a Member {
    use member::MemberState::*;

    let lhs_overrides = match (lhs.member_state, lhs.incarnation, rhs.member_state, rhs.incarnation) {
        (Alive, i, Suspect, j) => i > j,
        (Alive, i, Alive, j) => i > j,
        (Suspect, i, Suspect, j) => i > j,
        (Suspect, i, Alive, j) => i >= j,
        (Down, _, Alive, _) => true,
        (Down, _, Suspect, _) => true,
        (Left, _, _, _) => true,
        _ => false,
    };

    return if lhs_overrides { lhs } else { rhs };
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
        let decoded : Member = json::decode(&encoded).unwrap();

        assert_eq!(decoded, member);
    }
}
