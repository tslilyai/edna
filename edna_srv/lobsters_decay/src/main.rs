extern crate chrono;
extern crate clap;

use chrono::{Duration, Local};
use clap::{App, Arg};
use edna;
use log::warn;
use mysql::from_value;
use mysql::prelude::*;
use mysql::Opts;
use reqwest;
use serde::*;
use serde_json::json;
use std::collections::HashMap;

pub const LOBSTERS_APP: &'static str = "lobsters";
pub const HOTCRP_APP: &'static str = "hotcrp";
const SERVER: &'static str = "http://localhost:3000";

#[derive(Serialize, Deserialize)]
pub struct ApplyDisguiseResponse {
    pub locators: HashMap<edna::UID, Vec<edna::records::Locator>>,
}

fn init_logger() {
    let _ = env_logger::builder()
        // Include all events in tests
        .filter_level(log::LevelFilter::Warn)
        // Ensure events are captured by `cargo test`
        .is_test(true)
        // Ignore errors initializing the logger if tests race to configure it
        .try_init();
}

pub fn main() {
    init_logger();

    let matches = App::new("Edna API server")
        .arg(
            Arg::with_name("database")
                .short("d")
                .long("database-name")
                .default_value("testdb")
                .help("The MySQL database to use")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("host")
                .short("h")
                .long("host")
                .default_value("mariadb")
                .help("The MySQL host server to use")
                .takes_value(true),
        )
        .get_matches();
    let mut db = mysql::Conn::new(
        Opts::from_url(&format!(
            "mysql://root:password@{}/{}",
            matches.value_of("host").unwrap(),
            matches.value_of("database").unwrap(),
        ))
        .unwrap(),
    )
    .unwrap();
    assert_eq!(db.ping(), true);

    // get all users
    let mut users: Vec<(u64, String)> = vec![];
    let dt = Local::now().naive_local() - Duration::days(365);

    warn!("Got date {}", dt.to_string());
    let res = db
        .query_iter(&format!(
            //"SELECT id, email FROM users WHERE `last_login` < '{}';", dt.to_string()
            "SELECT id, email FROM users WHERE `id` > 2;"
        ))
        .expect("Could not select inactive users?");
    for r in res {
        let r = r.unwrap().unwrap();
        let uid: u64 = from_value(r[0].clone());
        let email: String = from_value(r[1].clone());
        warn!("got id res {}", uid);
        users.push((uid, email));
    }

    warn!("getting client");
    let client = reqwest::blocking::Client::new();

    let postdata = json!({
        "decrypt_cap": [],
        "locators": [],
    });

    for (u, email) in &users {
        warn!("Decaying user {}", u);
        // we don't need any capabilities
        let endpoint = format!("{}/apply_disguise/lobsters/1/{}", SERVER, u);
        let response = client
            .post(&endpoint)
            .header("Content-Type", "application/json")
            .body(postdata.to_string())
            .send()
            .unwrap();
        let strbody = response.text().unwrap();
        warn!("Decay strbody response: {}", strbody);
        let body: ApplyDisguiseResponse = serde_json::from_str(&strbody).unwrap();
        let l = if let Some(l) = body.locators.get(&u.to_string()) {
            serde_json::to_string(l).unwrap()
        } else {
            let v: Vec<u64> = vec![];
            serde_json::to_string(&v).unwrap()
        };
        // TODO send email with locators
        warn!(
            "Sending email to {}: Undecay your account with l {} at /users/undecay_account",
            email, l
        );
    }
}
