extern crate log;
extern crate mysql;

use edna_cryptdb::helpers::*;
use log::{error, warn};
use msql_srv::MysqlIntermediary;
use std::collections::{HashMap, HashSet};
use std::net;
use std::thread;

fn main() {
    env_logger::init();
    let dryrun = false;
    let schema = if !dryrun{
        std::fs::read_to_string(
            "/data/repository/applications/websubmit-rs/cryptdb-server/src/schema.sql",
        )
        .unwrap()
    } else {
        std::fs::read_to_string(
            "/data/repository/applications/websubmit-rs/cryptdb-server/src/schema_nocrypto.sql",
        )
        .unwrap()
    };
    let port = 62292;
    let listener = net::TcpListener::bind("127.0.0.1:62292").unwrap();
    warn!("Listener on port {}", port);
    let mut encmap = HashMap::new();
    let mut enccols = HashSet::new();
    enccols.insert("answer".to_string());
    enccols.insert("email".to_string());
    enccols.insert("submitted_at".to_string());
    encmap.insert("answers".to_string(), enccols);

    let mut enccols = HashSet::new();
    enccols.insert("apikey".to_string());
    enccols.insert("email".to_string());
    enccols.insert("is_admin".to_string());
    enccols.insert("is_anon".to_string());
    encmap.insert("users".to_string(), enccols);

    let host = "127.0.0.1:3306";
    init_db(
        false, // in-memory
        "tester",
        "pass",
        host,
        "myclass_cryptdb",
        &schema,
    );
    error!("initialized db");

    let proxy = edna_cryptdb::proxy::Proxy::new(
        "127.0.0.1:3306",
        "tester",
        "pass",
        "myclass_cryptdb",
        encmap,
        dryrun,
    );
    while let Ok((s, _)) = listener.accept() {
        s.set_nodelay(true).expect("Connot disable nagle");
        let my_proxy = proxy.clone();
        let _ = thread::spawn(move || {
            MysqlIntermediary::run_on_tcp(my_proxy, s).unwrap();
        });
    }
}
