#![feature(core)]
#![feature(io)]
#![feature(path)]
#![feature(path_ext)]
#![feature(plugin)]
#![plugin(docopt_macros)]

extern crate "rustc-serialize" as rustc_serialize;
extern crate docopt;
extern crate uuid;

extern crate swim;

use std::io::{Read, Write};
use std::default::Default;
use std::fs::File;
use std::fs::PathExt;
use std::path::Path;

use uuid::Uuid;

docopt!(Args derive Debug, "
SWIMer - Utility for testing the SWIM protocol implementation.

Usage: swimmer <data-folder> <cluster-key> <listen-addr> [<seed-node>]
");

fn main() {
    let args: Args = Args::docopt().decode().unwrap_or_else(|e| e.exit());
    let root_folder = Path::new(&args.arg_data_folder);
    let host_key = read_host_key(&root_folder);

    println!("Swimmer main: {:?}", args);
    println!("Host key: {}", host_key.to_hyphenated_string());

    let config = swim::ClusterConfig {
        cluster_key: args.arg_cluster_key.as_bytes().to_vec(),
        .. Default::default()
    };

    let cluster = swim::start_cluster(
        host_key, config, args.arg_listen_addr.as_slice());

    if args.arg_seed_node.len() > 0 {
        cluster.add_seed_node(args.arg_seed_node.as_slice());
    }

    println!("Starting event poller");
    for (members, event) in cluster.events.iter() {
        println!("");
        println!(" CLUSTER EVENT ");
        println!("===============");
        println!("{:?}", event);
        println!("");

        for member in members {
            println!("  {:?}", member);
        }
    }
    println!("Stopping event poller");
}

fn read_host_key(root_folder: &Path) -> Uuid {
    let host_key_path = root_folder.join("host_key");

    if host_key_path.exists() {
        let mut host_key_contents = Vec::new();
        File::open(&host_key_path).unwrap().read_to_end(&mut host_key_contents).unwrap();

        return Uuid::from_bytes(host_key_contents.as_slice()).unwrap();
    }

    let host_key = Uuid::new_v4();
    let mut host_key_file = File::create(&host_key_path).unwrap();
    host_key_file.write_all(host_key.as_bytes()).unwrap();

    return host_key;
}
