extern crate log;
extern crate mysql;
extern crate rand;
use edna_cryptdb::{helpers, EdnaClient};
use std::net;

fn init_logger() {
    let _ = env_logger::builder()
        // Include all events in tests
        //.filter_level(log::LevelFilter::Warn)
        .filter_level(log::LevelFilter::Error)
        // Ensure events are captured by `cargo test`
        .is_test(true)
        // Ignore errors initializing the logger if tests race to configure it
        .try_init();
}

fn main() {
    init_logger();
    let dbname = "ednatester";
    // setup initial (unencrypted) db
    helpers::init_db(true, "tester", "pass", "127.0.0.1", &dbname, "schema");

    // create proxy
    let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();

    // create db pool to proxy
    let url = format!("mysql://127.0.0.1:{}", port);
    let _edna = EdnaClient::new(&url, true, false);
}
