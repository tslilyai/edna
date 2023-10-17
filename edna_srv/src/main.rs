extern crate clap;
#[macro_use]
extern crate rocket;
extern crate slog;
extern crate slog_term;
#[macro_use]
extern crate serde_derive;

mod apiproxy;
mod tests;

use clap::{App, Arg};
use edna::{helpers, EdnaClient};
use rocket::{Build, Rocket};
//use rocket_okapi::{openapi, openapi_get_routes};
use rocket_okapi::openapi;
use std::fs;
use std::sync::{Arc, Mutex};

fn init_logger() {
    let _ = env_logger::builder()
        // Include all events in tests
        .filter_level(log::LevelFilter::Warn)
        // Ensure events are captured by `cargo test`
        .is_test(true)
        // Ignore errors initializing the logger if tests race to configure it
        .try_init();
}

#[openapi]
#[get("/")]
fn index() -> &'static str {
    "Edna API server\n"
}

fn rocket(
    prime: bool,
    user: &str,
    pass: &str,
    host: &str,
    db: &str,
    schema_file: &str,
    in_memory: bool,
) -> Rocket<Build> {
    let schemastr = fs::read_to_string(schema_file).unwrap();
    if prime {
        helpers::init_db(true, user, pass, host, db, &schemastr);
        error!("Primed!");
    }
    let edna_client = EdnaClient::new(user, pass, host, db, in_memory, false, false);
    rocket::build()
        .manage(Arc::new(Mutex::new(edna_client)))
        // NOTE: just for openapi generation
        /*.mount(
            "/",
            openapi_get_routes![
                index,
                apiproxy::register_principal,
                apiproxy::start_disguise,
                apiproxy::end_disguise,
                apiproxy::start_reveal,
                apiproxy::end_reveal,
                apiproxy::apply_disguise,
                apiproxy::reveal_disguise,
                apiproxy::get_pseudoprincipals_of,
                apiproxy::get_records_of_disguise,
                apiproxy::cleanup_records_of_disguise,
                apiproxy::save_diff_record,
                apiproxy::save_pseudoprincipal_record,
                //apiproxy::create_pseudoprincipal
            ],
        )*/
        .mount("/", routes![index])
        .mount("/", routes![apiproxy::register_principal])
        .mount("/", routes![apiproxy::start_disguise])
        .mount("/", routes![apiproxy::end_disguise])
        .mount("/", routes![apiproxy::start_reveal])
        .mount("/", routes![apiproxy::end_reveal])
        .mount("/", routes![apiproxy::apply_disguise])
        .mount("/", routes![apiproxy::reveal_disguise])
        .mount("/", routes![apiproxy::get_pseudoprincipals_of])
        .mount("/", routes![apiproxy::get_records_of_disguise])
        .mount("/", routes![apiproxy::cleanup_records_of_disguise])
        .mount("/", routes![apiproxy::save_diff_record])
        .mount("/", routes![apiproxy::save_pseudoprincipal_record])
}

#[rocket::main]
async fn main() {
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
                .default_value("127.0.0.1")
                .help("The MySQL server host to use")
                .takes_value(true),
        )
        .arg(Arg::with_name("prime").help("Prime the database"))
        .arg(
            Arg::with_name("user")
                .long("user")
                .default_value("tslilyai")
                .help("MySQL user")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("pass")
                .long("pass")
                .default_value("pass")
                .help("MySQL password")
                .takes_value(true),
        )
        .arg(Arg::with_name("test").help("Run the test"))
        .arg(
            Arg::with_name("schema")
                .short("s")
                .default_value("../applications/lobsters/schema.sql")
                .takes_value(true)
                .long("schema")
                .help("File containing SQL schema to use"),
        )
        .arg(
            Arg::with_name("in-memory")
                .long("memory")
                .help("Use in-memory tables."),
        )
        .get_matches();

    if matches.is_present("test") {
        tests::test_lobsters_disguise().await;
        tests::test_hotcrp_disguise().await;
        return;
    }

    let my_rocket = rocket(
        matches.is_present("prime"),
        matches.value_of("user").unwrap(),
        matches.value_of("pass").unwrap(),
        matches.value_of("host").unwrap(),
        matches.value_of("database").unwrap(),
        matches.value_of("schema").unwrap(),
        matches.is_present("in-memory"),
    );
    my_rocket.launch().await.expect("Failed to launch rocket");
}
