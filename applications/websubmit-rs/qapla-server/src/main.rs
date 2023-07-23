extern crate clap;
extern crate mysql;
#[macro_use]
extern crate rocket;
extern crate lettre;
extern crate lettre_email;
#[macro_use]
extern crate slog;
extern crate slog_term;
#[macro_use]
extern crate serde_derive;

mod admin;
mod apikey;
mod args;
mod backend;
mod config;
mod disguises;
mod email;
mod login;
mod privacy;
mod questions;

pub const APIKEY_FILE: &'static str = "apikey.txt";
pub const ADMIN_EMAIL: &'static str = "malte@cs.brown.edu";

use backend::MySqlBackend;
//use mysql::from_value;
use mysql::prelude::*;
use mysql::{Opts, Value};
use rocket::http::ContentType;
use rocket::http::CookieJar;
use rocket::http::Status;
use rocket::local::blocking::Client;
use rocket::response::Redirect;
use rocket::{Build, Rocket, State};
use rocket_dyn_templates::Template;
use std::cmp::min;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time;
use std::time::Duration;

pub fn new_logger() -> slog::Logger {
    use slog::Drain;
    use slog::Logger;
    use slog_term::term_full;
    Logger::root(Mutex::new(term_full()).fuse(), o!())
}

#[get("/")]
fn index(cookies: &CookieJar<'_>, backend: &State<Arc<Mutex<MySqlBackend>>>) -> Redirect {
    if let Some(cookie) = cookies.get("apikey") {
        let apikey: String = cookie.value().parse().ok().unwrap();
        // TODO validate API key
        let anonkey: String = cookies
            .get("anonkey")
            .unwrap()
            .value()
            .parse()
            .ok()
            .unwrap();
        match apikey::check_api_key(&*backend, &apikey, &anonkey) {
            Ok(_user) => Redirect::to("/leclist"),
            Err(_) => Redirect::to("/login"),
        }
    } else {
        Redirect::to("/login")
    }
}

fn rocket(args: &args::Args) -> Rocket<Build> {
    let backend = Arc::new(Mutex::new(
        MySqlBackend::new(&format!("{}", args.class), Some(new_logger()), args).unwrap(),
    ));

    rocket::build()
        .attach(Template::fairing())
        .manage(backend)
        .manage(args.config.clone())
        .mount("/", routes![index])
        .mount(
            "/questions",
            routes![questions::questions, questions::questions_submit],
        )
        .mount("/apikey/login", routes![apikey::check])
        .mount("/apikey/generate", routes![apikey::generate])
        .mount("/answers", routes![questions::answers])
        .mount("/leclist", routes![questions::leclist])
        .mount("/login", routes![login::login])
        .mount("/delete", routes![privacy::gdpr_delete])
        .mount("/restore", routes![privacy::restore, privacy::gdpr_restore])
        .mount(
            "/anon/auth",
            routes![privacy::edit_as_pseudoprincipal_auth_request],
        )
        .mount("/anon/edit", routes![privacy::edit_as_pseudoprincipal])
        .mount(
            "/admin/anonymize",
            routes![privacy::anonymize, privacy::anonymize_answers],
        )
        .mount(
            "/admin/lec/add",
            routes![admin::lec_add, admin::lec_add_submit],
        )
        .mount("/admin/users", routes![admin::get_registered_users])
        .mount(
            "/admin/lec",
            routes![admin::lec, admin::addq, admin::editq, admin::editq_submit],
        )
}

#[rocket::main]
async fn main() {
    let args = args::parse_args();
    let my_rocket = rocket(&args);
    if args.benchmark {
        thread::spawn(move || {
            run_benchmark(&args, my_rocket);
        })
        .join()
        .expect("Thread panicked")
    } else {
        let _ = my_rocket.launch().await.expect("Failed to launch rocket");
    }
}

fn run_benchmark(args: &args::Args, rocket: Rocket<Build>) {
    let mut account_durations = vec![];
    let mut edit_durations = vec![];
    let mut delete_durations = vec![];
    let mut restore_durations = vec![];
    let mut anon_durations = vec![];
    let mut edit_durations_nonanon = vec![];
    let mut delete_durations_nonanon = vec![];
    let mut restore_durations_nonanon = vec![];
    let mut leclist_durations = vec![];
    let mut answers_durations = vec![];
    let mut questions_durations = vec![];

    let client = Client::tracked(rocket).expect("valid rocket instance");
    let mut db = mysql::Conn::new(
        Opts::from_url(&format!(
            "mysql://{}:{}@127.0.0.1:3306/{}",
            args.config.mysql_user, args.config.mysql_pass, args.class
        ))
        .unwrap(),
    )
    .unwrap();

    let mut user2apikey = HashMap::new();
    let log = new_logger();

    // create all users
    let nusers = args.nusers;
    for u in 0..nusers {
        let email = format!("{}@mail.edu", u);
        let postdata = serde_urlencoded::to_string(&vec![("email", email.clone())]).unwrap();
        let start = time::Instant::now();
        let response = client
            .post("/apikey/generate")
            .body(postdata)
            .header(ContentType::Form)
            .dispatch();
        account_durations.push(start.elapsed());
        assert_eq!(response.status(), Status::Ok);

        // get api key
        let file = File::open(format!("{}.{}", email, APIKEY_FILE)).unwrap();
        let mut buf_reader = BufReader::new(file);
        let mut apikey = String::new();
        buf_reader.read_to_string(&mut apikey).unwrap();
        debug!(log, "Got email {} with apikey {}", &email, apikey);
        user2apikey.insert(email.clone(), apikey);
    }

    /************
     * admin read
     *************/
    {
        // login as the admin
        let postdata = serde_urlencoded::to_string(&vec![
            ("email", config::ADMIN.0),
            ("key", config::ADMIN.1),
        ])
        .unwrap();
        let response = client
            .post("/apikey/login")
            .body(postdata)
            .header(ContentType::Form)
            .dispatch();
        assert_eq!(response.status(), Status::SeeOther);

        for _ in 0..200 {
            // admin read
            info!(log, "ADMIN READ LECS");
            let start = time::Instant::now();
            let response = client.get(format!("/leclist")).dispatch();
            assert_eq!(response.status(), Status::Ok);
            leclist_durations.push(start.elapsed());

            // answers
            info!(log, "ADMIN READ ANS");
            let start = time::Instant::now();
            let response = client.get(format!("/answers/{}", 0)).dispatch();
            assert_eq!(response.status(), Status::Ok);
            answers_durations.push(start.elapsed());
        }
    }

    /***********************************
     * editing nonanon data
     ***********************************/
    for u in 0..min(args.nusers, 200) {
        // login
        let email = format!("{}@mail.edu", u);
        let apikey = user2apikey.get(&email).unwrap();
        let postdata = serde_urlencoded::to_string(&vec![("key", apikey)]).unwrap();
        let response = client
            .post("/apikey/login")
            .body(postdata)
            .header(ContentType::Form)
            .dispatch();
        assert_eq!(response.status(), Status::SeeOther);

        // questions
        info!(log, "READ QS {}", email);
        let start = time::Instant::now();
        let response = client.get(format!("/questions/{}", 0)).dispatch();
        assert_eq!(response.status(), Status::Ok);
        questions_durations.push(start.elapsed());

        // editing
        info!(log, "EDIT NONANON {}", email);
        let start = time::Instant::now();
        let response = client.get(format!("/questions/{}", 0)).dispatch();
        assert_eq!(response.status(), Status::Ok);

        let mut answers = vec![];
        answers.push((
            format!("answers.{}", 0),
            format!("new_answer_user_{}_lec_{}", u, 0),
        ));
        let postdata = serde_urlencoded::to_string(&answers).unwrap();
        debug!(log, "Posting to questions for lec 0 answers {}", postdata);
        let response = client
            .post(format!("/questions/{}", 0)) // testing lecture 0 for now
            .body(postdata)
            .header(ContentType::Form)
            .dispatch();
        assert_eq!(response.status(), Status::SeeOther);
        edit_durations_nonanon.push(start.elapsed());
    }

    /***********************************
     * gdpr deletion (no composition)
     ***********************************/
    for u in 0..min(20, args.nusers) {
        let email = format!("{}@mail.edu", u);
        info!(log, "GDPR DELETE {}", email);
        let apikey = user2apikey.get(&email).unwrap();

        // login as the user
        let postdata = serde_urlencoded::to_string(&vec![("key", apikey)]).unwrap();
        let response = client
            .post("/apikey/login")
            .body(postdata)
            .header(ContentType::Form)
            .dispatch();
        assert_eq!(response.status(), Status::SeeOther);

        let start = time::Instant::now();
        let response = client.post("/delete").header(ContentType::Form).dispatch();
        assert_eq!(response.status(), Status::SeeOther);
        delete_durations_nonanon.push(start.elapsed());
    }

    /***********************************
     * gdpr restore (without composition)
     ***********************************/
    for u in 0..min(20, args.nusers) {
        let email = format!("{}@mail.edu", u);
        info!(log, "GDPR RESTORE {}", email);
        let start = time::Instant::now();
        let apikey = user2apikey.get(&email).unwrap();
        let postdata =
            serde_urlencoded::to_string(&vec![("email", &email), ("apikey", &apikey)]).unwrap();
        let response = client
            .post("/restore")
            .body(postdata)
            .header(ContentType::Form)
            .dispatch();
        assert_eq!(response.status(), Status::SeeOther);
        restore_durations_nonanon.push(start.elapsed());
    }

    /**********************************
     * anonymization
     ***********************************/
    // login as the admin
    let postdata = serde_urlencoded::to_string(&vec![("key", config::ADMIN.1)]).unwrap();
    let response = client
        .post("/apikey/login")
        .body(postdata)
        .header(ContentType::Form)
        .dispatch();
    assert_eq!(response.status(), Status::SeeOther);

    // anonymize
    info!(log, "ADMIN ANON");
    let start = time::Instant::now();
    let response = client.post("/admin/anonymize").dispatch();
    anon_durations.push(start.elapsed());
    assert_eq!(response.status(), Status::SeeOther);

    // get records
    for u in 0..min(20, args.nusers) {
        let email = format!("{}@mail.edu", u);

        // check results of anonymization: user has no answers
        for l in 0..args.nlec {
            let keys: Vec<Value> = vec![l.into(), email.clone().into()];
            let res = db
                .exec_iter(
                    "SELECT answers.* FROM answers JOIN users on answers.email= users.email WHERE answers.lec = ? AND answers.`email` = ? and users.is_deleted=0;",
                    keys,
                )
                .unwrap();
            let mut rows = vec![];
            for row in res {
                let rowvals = row.unwrap().unwrap();
                let vals: Vec<Value> = rowvals.iter().map(|v| v.clone().into()).collect();
                rows.push(vals);
            }
            assert_eq!(rows.len(), 0);
        }
    }

    /***********************************
     * editing anonymized data
     ***********************************/
    for u in 0..min(20, args.nusers) {
        let email = format!("{}@mail.edu", u);
        info!(log, "EDIT ANON {}", email);
        let apikey = user2apikey.get(&email).unwrap();
        let start = time::Instant::now();

        // render template to get edit creds
        let response = client.get(format!("/anon/auth")).dispatch();
        assert_eq!(response.status(), Status::Ok);
        info!(
            log,
            "Auth edit anon request: {}mus",
            start.elapsed().as_micros()
        );

        // set creds, post to edit anon's answer to lecture 0
        let anon_start = time::Instant::now();
        let postdata = serde_urlencoded::to_string(&vec![
            ("email", &email),
            ("apikey", apikey),
            ("lec_id", &format!("0")),
        ])
        .unwrap();
        let response = client
            .post("/anon/edit")
            .body(postdata)
            .header(ContentType::Form)
            .dispatch();
        assert_eq!(response.status(), Status::Ok);
        info!(
            log,
            "Perform edit anon request: {}mus",
            anon_start.elapsed().as_micros()
        );

        // update answers to lecture 0
        let anon_start = time::Instant::now();
        let mut answers = vec![];
        answers.push((
            format!("answers.{}", 0),
            format!("new_answer_user_{}_lec_{}", u, 0),
        ));
        let postdata = serde_urlencoded::to_string(&answers).unwrap();
        debug!(log, "Posting to questions for lec 0 answers {}", postdata);
        let response = client
            .post(format!("/questions/{}", 0)) // testing lecture 0 for now
            .body(postdata)
            .header(ContentType::Form)
            .dispatch();
        assert_eq!(response.status(), Status::SeeOther);
        info!(
            log,
            "Update answers as anon request: {}mus",
            anon_start.elapsed().as_micros()
        );

        edit_durations.push(start.elapsed());

        // logged out
        let response = client.get(format!("/leclist")).dispatch();
        assert_eq!(response.status(), Status::Unauthorized);

        // check answers for users for lecture 0
        /*let res = db
            .query_iter("SELECT answer FROM answers WHERE lec = 0 AND q = 0;")
            .unwrap();
        for row in res {
            let rowvals = row.unwrap().unwrap();
            let answer: String = from_value(rowvals[0].clone());
            assert!(answer.contains("new_answer"));
        }*/
    }

    /***********************************
     * gdpr deletion (with composition)
     ***********************************/
    for u in 0..min(20, args.nusers) {
        let email = format!("{}@mail.edu", u);
        info!(log, "GDPR DELETE COMP {}", email);
        let apikey = user2apikey.get(&email).unwrap();

        // login as the user
        let postdata = serde_urlencoded::to_string(&vec![("key", apikey)]).unwrap();
        let response = client
            .post("/apikey/login")
            .body(postdata)
            .header(ContentType::Form)
            .dispatch();
        assert_eq!(response.status(), Status::SeeOther);

        let start = time::Instant::now();
        let response = client.post("/delete").header(ContentType::Form).dispatch();
        assert_eq!(response.status(), Status::SeeOther);
        delete_durations.push(start.elapsed());
    }
    // check results of delete: no answers or users exist
    /*let res = db.query_iter("SELECT * FROM answers JOIN users ON answers.email = users.email WHERE users.is_deleted=0;").unwrap();
    let mut rows = vec![];
    for row in res {
        let rowvals = row.unwrap().unwrap();
        let answer: String = from_value(rowvals[0].clone());
        rows.push(answer);
    }
    assert_eq!(rows.len(), 0);
    let res = db
        .query_iter("SELECT * FROM users WHERE users.is_deleted=0;")
        .unwrap();
    let mut rows = vec![];
    for row in res {
        let rowvals = row.unwrap().unwrap();
        let answer: String = from_value(rowvals[0].clone());
        rows.push(answer);
    }
    assert_eq!(rows.len(), 1); // the admin*/

    /***********************************
     * gdpr restore (with composition)
     ***********************************/
    for u in 0..min(20, args.nusers) {
        let email = format!("{}@mail.edu", u);
        info!(log, "GDPR RESTORE COMP {}", email);
        let start = time::Instant::now();
        let apikey = user2apikey.get(&email).unwrap();
        let postdata =
            serde_urlencoded::to_string(&vec![("email", &email), ("apikey", apikey)]).unwrap();
        let response = client
            .post("/restore")
            .body(postdata)
            .header(ContentType::Form)
            .dispatch();
        assert_eq!(response.status(), Status::SeeOther);
        restore_durations.push(start.elapsed());
    }

    // database is back in anonymized form
    // check answers for lecture 0
    /*let res = db
        .query_iter("SELECT answer FROM answers JOIN users ON answers.email= users.email WHERE users.is_deleted=0 AND lec=0 AND q = 0;")
        .unwrap();
    let mut rows = vec![];
    for row in res {
        let rowvals = row.unwrap().unwrap();
        let answer: String = from_value(rowvals[0].clone());
        assert!(answer.contains("new_answer"));
        rows.push(answer);
    }
    assert_eq!(rows.len(), args.nusers as usize);

    let res = db
        .query_iter("SELECT * FROM users WHERE users.is_deleted=0;")
        .unwrap();
    let mut rows = vec![];
    for row in res {
        let rowvals = row.unwrap().unwrap();
        let answer: String = from_value(rowvals[0].clone());
        rows.push(answer);
    }
    assert_eq!(
        rows.len(),
        1 + args.nusers as usize * (args.nlec as usize + 1)
    );*/

    print_stats(
        args,
        account_durations,
        anon_durations,
        leclist_durations,
        answers_durations,
        questions_durations,
        edit_durations,
        delete_durations,
        restore_durations,
        edit_durations_nonanon,
        delete_durations_nonanon,
        restore_durations_nonanon,
    );
}

fn print_stats(
    args: &args::Args,
    account_durations: Vec<Duration>,
    anon_durations: Vec<Duration>,
    leclist_durations: Vec<Duration>,
    answers_durations: Vec<Duration>,
    questions_durations: Vec<Duration>,
    edit_durations: Vec<Duration>,
    delete_durations: Vec<Duration>,
    restore_durations: Vec<Duration>,
    edit_durations_nonanon: Vec<Duration>,
    delete_durations_nonanon: Vec<Duration>,
    restore_durations_nonanon: Vec<Duration>,
) {
    let filename = format!(
        "../../../results/websubmit_results/qapla_stats_{}lec_{}users.csv",
        args.nlec, args.nusers
    );

    // print out stats
    let mut f = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&filename)
        .unwrap();
    writeln!(
        f,
        "{}",
        account_durations
            .iter()
            .map(|d| d.as_micros().to_string())
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
    writeln!(
        f,
        "{}",
        anon_durations
            .iter()
            .map(|d| d.as_micros().to_string())
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
    writeln!(
        f,
        "{}",
        leclist_durations
            .iter()
            .map(|d| d.as_micros().to_string())
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
    writeln!(
        f,
        "{}",
        answers_durations
            .iter()
            .map(|d| d.as_micros().to_string())
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
    writeln!(
        f,
        "{}",
        questions_durations
            .iter()
            .map(|d| d.as_micros().to_string())
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
    writeln!(
        f,
        "{}",
        edit_durations
            .iter()
            .map(|d| d.as_micros().to_string())
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
    writeln!(
        f,
        "{}",
        delete_durations
            .iter()
            .map(|d| d.as_micros().to_string())
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
    writeln!(
        f,
        "{}",
        restore_durations
            .iter()
            .map(|d| d.as_micros().to_string())
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
    writeln!(
        f,
        "{}",
        edit_durations_nonanon
            .iter()
            .map(|d| d.as_micros().to_string())
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
    writeln!(
        f,
        "{}",
        delete_durations_nonanon
            .iter()
            .map(|d| d.as_micros().to_string())
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
    writeln!(
        f,
        "{}",
        restore_durations_nonanon
            .iter()
            .map(|d| d.as_micros().to_string())
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
}
