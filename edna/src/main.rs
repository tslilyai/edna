extern crate log;
extern crate mysql;
extern crate rand;
use edna::{helpers, EdnaClient};

fn init_logger() {
    let _ = env_logger::builder()
        // Include all events in tests
        .filter_level(log::LevelFilter::Warn)
        //.filter_level(log::LevelFilter::Error)
        // Ensure events are captured by `cargo test`
        .is_test(true)
        // Ignore errors initializing the logger if tests race to configure it
        .try_init();
}

fn main() {
    init_logger();
    let dbname = "ednatester";
    helpers::init_db(true, "tester", "pass", "127.0.0.1", &dbname, "schema");
    let _edna = EdnaClient::new("tester", "pass", "127.0.0.1", dbname, true, true, false);
}
