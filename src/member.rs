use std::fmt;
use std::fmt::{Debug, Formatter};
use std::old_io::net::ip::SocketAddr;
use std::str::FromStr;
use std::cmp::Ordering;

use rustc_serialize::{Decodable, Decoder, Encodable, Encoder};
use time;
use uuid::Uuid;


#[derive(RustcEncodable, RustcDecodable, Debug, Clone, PartialOrd, Ord, PartialEq, Eq, Copy)]
pub enum MemberState {
    Alive,
    Suspect,
    Down,
}

#[derive(Clone, PartialEq, Eq)]
pub struct Member {
    pub host_key: Uuid,
    pub remote_host: Option<SocketAddr>,
    pub incarnation: u64,
    pub member_state: MemberState,
    pub last_state_change: time::Tm,
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
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
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

pub fn most_recent_member_data<'a>(lhs: &'a Member, rhs: &'a Member) -> &'a Member {
    use member::MemberState::*;

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

