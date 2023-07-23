extern crate libc;
extern crate log;
extern crate mysql;
extern crate rand;

use edna::{helpers, EdnaClient, TableInfo};
use log::{info, warn};
use mysql::from_value;
use mysql::prelude::*;
use mysql::Opts;
use serde_json;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::str::FromStr;
use std::time::Duration;
use std::*;
use structopt::StructOpt;

mod datagen;
mod disguises;

const SCHEMA: &'static str = include_str!("schema.sql");
const DBNAME: &'static str = &"test_hotcrp";
const TABLEINFO_JSON: &'static str = include_str!("./disguises/table_info.json");

#[derive(StructOpt)]
pub struct Cli {
    #[structopt(long = "prime")]
    prime: bool,
    #[structopt(long = "baseline")]
    baseline: bool,
    // Generates nusers_nonpc+nusers_pc users
    #[structopt(long = "nusers_nonpc", default_value = "400")]
    nusers_nonpc: usize,
    #[structopt(long = "nusers_pc", default_value = "50")]
    nusers_pc: usize,
    // Generates npapers_rej+npapers_accept papers.
    #[structopt(long = "npapers_rej", default_value = "400")]
    npapers_rej: usize,
    #[structopt(long = "npapers_acc", default_value = "50")]
    npapers_accept: usize,
    #[structopt(long = "mysql_user", default_value = "root")]
    mysql_user: String,
    #[structopt(long = "mysql_pass", default_value = "pass")]
    mysql_pass: String,
    #[structopt(long = "dryrun")]
    dryrun: bool,
}

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
    let args = Cli::from_args();

    if args.baseline {
        run_baseline(&args);
    } else {
        run_edna(&args);
    }
}

fn get_stats(db: &mut mysql::PooledConn) {
    let mut users = vec![];
    let res = db
        .query_iter(format!(r"SELECT contactId FROM ContactInfo"))
        .unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 1);
        users.push(helpers::mysql_val_to_u64(&vals[0]).unwrap());
    }
    for u in users {
        let mut nobjs = 1;
        let table_infos: HashMap<String, TableInfo> = serde_json::from_str(TABLEINFO_JSON).unwrap();
        for (table, info) in table_infos.iter() {
            for owner_col in &info.owner_fk_cols {
                let res = db
                    .query_iter(format!(
                        r"SELECT COUNT(*) FROM {} WHERE {} = {};",
                        table, owner_col, u
                    ))
                    .unwrap();
                for row in res {
                    let vals = row.unwrap().unwrap();
                    assert_eq!(vals.len(), 1);
                    nobjs += helpers::mysql_val_to_u64(&vals[0]).unwrap();
                }
            }
        }
        println!("{}\t{}", u, nobjs);
    }
}

fn run_edna(args: &Cli) {
    let mut account_durations = vec![];
    let mut edit_durations = vec![];
    let mut delete_durations = vec![];
    let mut anon_durations = vec![];
    let mut restore_durations = vec![];
    let mut read_durations = vec![];

    let mut edit_durations_preanon = vec![];
    let mut delete_durations_preanon = vec![];
    let mut restore_durations_preanon = vec![];

    let nusers = args.nusers_nonpc + args.nusers_pc;
    let host = "127.0.0.1:3306";

    if args.prime {
        info!("Initializing DB");
        helpers::init_db(
            false,
            &args.mysql_user,
            &args.mysql_pass,
            host,
            DBNAME,
            SCHEMA,
        );
    }
    let url = format!(
        "mysql://{}:{}@{}/{}",
        args.mysql_user, args.mysql_pass, host, DBNAME,
    );

    let pool = mysql::Pool::new(Opts::from_url(&url).unwrap()).unwrap();
    let mut db = pool.get_conn().unwrap();
    if args.prime {
        datagen::populate_database(&mut db, &args).unwrap();
    }
    warn!("database populated!");

    let mut edna = EdnaClient::new(
        &args.mysql_user,
        &args.mysql_pass,
        host,
        DBNAME,
        true,
        false,
        args.dryrun,
    );

    get_stats(&mut db);

    for uid in 1..nusers + 1 {
        let start = time::Instant::now();
        edna.register_principal(&uid.to_string(), uid.to_string());
        datagen::insert_single_user(&mut db).unwrap();
        account_durations.push(start.elapsed());
    }

    let mut user2rid = HashMap::new();
    // edit/delete/restore for pc members
    for u in args.nusers_nonpc + 2.. args.nusers_nonpc + args.nusers_pc {
        // edit
        let start = time::Instant::now();
        let rids = datagen::reviews::get_reviews(u as u64, &mut db).unwrap();
        read_durations.push(start.elapsed());
        datagen::reviews::update_review(rids[0], &mut db).unwrap();
        edit_durations_preanon.push(start.elapsed());
        user2rid.insert(u as u64, rids[0]);

        // delete
        let start = time::Instant::now();
        let did =
            disguises::gdpr_disguise::apply(&mut edna, u as u64, u.to_string(), false).unwrap();
        delete_durations_preanon.push(start.elapsed());

        // restore
        let start = time::Instant::now();
        disguises::gdpr_disguise::reveal(u as u64, did, &mut edna, u.to_string()).unwrap();
        restore_durations_preanon.push(start.elapsed());
    }

    // anonymize
    let start = time::Instant::now();
    // anonymization doesn't produce diff records that we'll reuse later
    let _anondid = disguises::universal_anon_disguise::apply(&mut edna).unwrap();
    anon_durations.push(start.elapsed());

    // edit/delete/restore for pc members
    for u in args.nusers_nonpc + 2..args.nusers_nonpc + args.nusers_pc {
        // edit after anonymization, for fairness only edit the one review
        let rid = user2rid.get(&(u as u64)).unwrap();
        let start = time::Instant::now();
        let pps = edna.get_pseudoprincipals(u.to_string(), Some(u.to_string()), None);
        for pp in pps {
            let rids = datagen::reviews::get_reviews(u64::from_str(&pp).unwrap(), &mut db).unwrap();
            if rids.len() > 0 && rids[0] == *rid {
                datagen::reviews::update_review(rids[0], &mut db).unwrap();
            }
        }
        edit_durations.push(start.elapsed());

        // delete
        let start = time::Instant::now();
        let gdprdid =
            disguises::gdpr_disguise::apply(&mut edna, u as u64, u.to_string(), true).unwrap();
        delete_durations.push(start.elapsed());

        // restore
        let start = time::Instant::now();
        disguises::gdpr_disguise::reveal(u as u64, gdprdid, &mut edna, u.to_string()).unwrap();
        restore_durations.push(start.elapsed());
    }
    print_stats(
        nusers as u64,
        account_durations,
        anon_durations,
        read_durations,
        edit_durations,
        delete_durations,
        restore_durations,
        edit_durations_preanon,
        delete_durations_preanon,
        restore_durations_preanon,
        false,
        args.dryrun,
    );
}

fn run_baseline(args: &Cli) {
    let mut account_durations = vec![];
    let mut edit_durations = vec![];
    let mut delete_durations = vec![];
    let mut anon_durations = vec![];
    let mut read_durations = vec![];

    let nusers = args.nusers_nonpc + args.nusers_pc;
    let host = "127.0.0.1:3306";
    if args.prime {
        helpers::init_db(
            false,
            &args.mysql_user,
            &args.mysql_pass,
            host,
            DBNAME,
            SCHEMA,
        );
    }
    let url = format!(
        "mysql://{}:{}@{}/{}",
        args.mysql_user, args.mysql_pass, host, DBNAME,
    );
    let pool = mysql::Pool::new(Opts::from_url(&url).unwrap()).unwrap();
    let mut db = pool.get_conn().unwrap();
    let mut db1 = pool.get_conn().unwrap();
    if args.prime {
        datagen::populate_database(&mut db, &args).unwrap();
    }
    warn!("database populated!");
    for _ in 0..10 {
        let start = time::Instant::now();
        datagen::insert_single_user(&mut db).unwrap();
        account_durations.push(start.elapsed());
    }

    // baseline edit/delete/restore for pc members
    for u in args.nusers_nonpc + 2..args.nusers_nonpc + args.nusers_pc {
        // edit
        let start = time::Instant::now();
        let rids = datagen::reviews::get_reviews(u as u64, &mut db).unwrap();
        read_durations.push(start.elapsed());
        datagen::reviews::update_review(rids[0], &mut db).unwrap();
        edit_durations.push(start.elapsed());

        // delete
        let start = time::Instant::now();
        db.query_drop(&format!("DELETE FROM ContactInfo WHERE contactId = {}", u))
            .unwrap();
        db.query_drop(&format!("DELETE FROM PaperWatch WHERE contactId = {}", u))
            .unwrap();
        db.query_drop(&format!(
            "DELETE FROM PaperReviewPreference WHERE contactId = {}",
            u
        ))
        .unwrap();
        db.query_drop(&format!("DELETE FROM Capability WHERE contactId = {}", u))
            .unwrap();
        db.query_drop(&format!(
            "DELETE FROM PaperConflict WHERE contactId = {}",
            u
        ))
        .unwrap();
        db.query_drop(&format!(
            "DELETE FROM TopicInterest WHERE contactId = {}",
            u
        ))
        .unwrap();
        db.query_drop(&format!(
            "DELETE FROM ReviewRating WHERE contactId = {}",
            u
        ))
        .unwrap();

        let mut count = 0;

        // decorrelate papers
        let res = db.query_iter(&format!("SELECT paperId FROM Paper WHERE leadContactId = {}", u)).unwrap();
        for row in res {
            count += 1;
            let pid: u64 = from_value(row.unwrap().unwrap()[0].clone());
            db1.query_drop(&format!(
                "UPDATE Paper SET leadContactId = {} WHERE PaperId = {}",
                u + nusers ,
                pid
            ))
            .unwrap();
        }
        datagen::insert_users_anon(count, &mut db1).unwrap();
        count = 0;

        // decorrelate papers
        let res = db.query_iter(&format!("SELECT paperId FROM Paper WHERE shepherdContactId = {}", u)).unwrap();
        for row in res {
            count += 1;
            let pid: u64 = from_value(row.unwrap().unwrap()[0].clone());
            db1.query_drop(&format!(
                "UPDATE Paper SET shepherdContactId = {} WHERE PaperId = {}",
                u + nusers ,
                pid
            ))
            .unwrap();
        }
        datagen::insert_users_anon(count, &mut db1).unwrap();
        count = 0;

        // decorrelate papers
        let res = db.query_iter(&format!("SELECT paperId FROM Paper WHERE managerContactId = {}", u)).unwrap();
        for row in res {
            count += 1;
            let pid: u64 = from_value(row.unwrap().unwrap()[0].clone());
            db1.query_drop(&format!(
                "UPDATE Paper SET managerContactId = {} WHERE PaperId = {}",
                u + nusers ,
                pid
            ))
            .unwrap();
        }
        datagen::insert_users_anon(count, &mut db1).unwrap();
        count = 0;

        // decorrelate reviews
        let res = db
            .query_iter(&format!(
                "SELECT reviewId FROM PaperReview WHERE contactId = {}",
                u
            ))
            .unwrap();
        for row in res {
            count += 1;
            let rid: u64 = from_value(row.unwrap().unwrap()[0].clone());
            db1.query_drop(&format!(
                "UPDATE PaperReview SET contactId = {} WHERE ReviewId = {}",
                u + nusers ,
                rid
            ))
            .unwrap();
        }
        datagen::insert_users_anon(count, &mut db1).unwrap();
        count = 0;

        // decorrelate comments
        let res = db
            .query_iter(&format!(
                "SELECT commentId FROM PaperComment WHERE contactId = {}",
                u
            ))
            .unwrap();
        for row in res {
            count += 1;
            let rid: u64 = from_value(row.unwrap().unwrap()[0].clone());
            db1.query_drop(&format!(
                "UPDATE PaperComment SET contactId = {} WHERE commentId = {}",
                u + nusers ,
                rid
            ))
            .unwrap();
        }
        datagen::insert_users_anon(count, &mut db1).unwrap();
        count = 0;


        // decorrelate paper review requested
        let res = db
            .query_iter(&format!(
                "SELECT paperId, email FROM PaperReviewRefused WHERE requestedBy = {}",
                u
            ))
            .unwrap();
        for row in res {
            count += 1;
            let vals = row.unwrap().unwrap();
            let pid: u64 = from_value(vals[0].clone());
            let email: String = from_value(vals[1].clone());
            db1.query_drop(&format!(
                "UPDATE PaperReviewRefused SET requestedBy = {} WHERE paperId= {} and contactId = {}",
                u + nusers ,
                pid,email 
            ))
            .unwrap();
        }
        datagen::insert_users_anon(count, &mut db1).unwrap();
        count = 0;

        // decorrelate paper review refused
        let res = db
            .query_iter(&format!(
                "SELECT paperId, email FROM PaperReviewRefused WHERE refusedBy = {}",
                u
            ))
            .unwrap();
        for row in res {
            count += 1;
            let vals = row.unwrap().unwrap();
            let pid: u64 = from_value(vals[0].clone());
            let email: String = from_value(vals[1].clone());
            db1.query_drop(&format!(
                "UPDATE PaperReviewRefused SET refusedBy = {} WHERE paperId= {} and contactId = {}",
                u + nusers ,
                pid,email 
            ))
            .unwrap();
        }
        datagen::insert_users_anon(count, &mut db1).unwrap();
        count = 0;

        // decorrelate comments
        let res = db
            .query_iter(&format!(
                "SELECT commentId FROM PaperComment WHERE contactId = {}",
                u
            ))
            .unwrap();
        for row in res {
            count += 1;
            let rid: u64 = from_value(row.unwrap().unwrap()[0].clone());
            db1.query_drop(&format!(
                "UPDATE PaperComment SET contactId = {} WHERE commentId = {}",
                u + nusers,
                rid
            ))
            .unwrap();
        }
        datagen::insert_users_anon(count, &mut db1).unwrap();
        count = 0;
    
        // decorrelate action log
        let res = db.query_iter(&format!("SELECT * FROM ActionLog WHERE contactId = {}", u)).unwrap();
        for row in res {
            count += 1;
            let aid: u64 = from_value(row.unwrap().unwrap()[0].clone());
            db1.query_drop(&format!(
                "UPDATE ActionLog SET contactId = {} WHERE logId = {}",
                u + nusers ,
               aid 
            ))
            .unwrap();
        }
        datagen::insert_users_anon(count, &mut db1).unwrap();
        count = 0;

        // decorrelate action log
        let res = db.query_iter(&format!("SELECT * FROM ActionLog WHERE destContactId = {}", u)).unwrap();
        for row in res {
            count += 1;
            let aid: u64 = from_value(row.unwrap().unwrap()[0].clone());
            db1.query_drop(&format!(
                "UPDATE ActionLog SET destContactId = {} WHERE logId = {}",
                u + nusers ,
                aid 
            ))
            .unwrap();
        }
        datagen::insert_users_anon(count, &mut db1).unwrap();
        count = 0;

        // decorrelate action log
        let res = db.query_iter(&format!("SELECT * FROM ActionLog WHERE trueContactId = {}", u)).unwrap();
        for row in res {
            count += 1;
            let aid: u64 = from_value(row.unwrap().unwrap()[0].clone());
            db1.query_drop(&format!(
                "UPDATE ActionLog SET trueContactId = {} WHERE logId = {}",
                u + nusers ,
               aid 
            ))
            .unwrap();
        }
        datagen::insert_users_anon(count, &mut db1).unwrap();
        delete_durations.push(start.elapsed());
    }

    // anonymize all users!
    let start = time::Instant::now();
    db1.query_drop(&format!(
        "UPDATE ContactInfo SET email = 'randemail' WHERE roles & 1",
    ))
    .unwrap();

    // decorrelate paper watches
    let res = db
        .query_iter(&format!("SELECT * FROM PaperWatch"))
        .unwrap();

    // simulate updates, count number of rows
    let mut count = 0;
    for row in res {
        let pid: u64 = from_value(row.unwrap().unwrap()[0].clone());
        db1.query_drop(&format!(
            "UPDATE PaperWatch SET contactId = {} WHERE paperWatchId = {}",
            nusers,
            pid 
        ))
        .unwrap();
        count += 1;
    }
    datagen::insert_users_anon(count, &mut db1).unwrap();
    count = 0;

    // decorrelate paper review pref
    let res = db
        .query_iter(&format!(
            "SELECT * FROM PaperReviewPreference" 
        ))
        .unwrap();
    for row in res {
        let prp: u64 = from_value(row.unwrap().unwrap()[0].clone());
        count += 1;
        db1.query_drop(&format!(
            "UPDATE PaperReviewPreference SET contactId = {} WHERE paperRevPrefId= {}",
            nusers ,
            prp 
        ))
        .unwrap();
    }
    datagen::insert_users_anon(count, &mut db1).unwrap();
    count = 0;

    // decorrelate papersConflicts
    let res = db.query_iter(&format!("SELECT paperConflictId FROM PaperConflict")).unwrap();
    for row in res {
        count += 1;
        let pid: u64 = from_value(row.unwrap().unwrap()[0].clone());
        db1.query_drop(&format!(
            "UPDATE PaperConflict SET contactId = {} WHERE paperConflictId = {}",
            nusers,
            pid
        ))
        .unwrap();
    }
    datagen::insert_users_anon(count, &mut db1).unwrap();
    count = 0;

    // decorrelate topicinterests 
    let res = db.query_iter(&format!("SELECT topicInterestId FROM TopicInterest")).unwrap();
    for row in res {
        count += 1;
        let pid: u64 = from_value(row.unwrap().unwrap()[0].clone());
        db1.query_drop(&format!(
            "UPDATE TopicInterest SET contactId = {} WHERE topicInterestId = {}",
            nusers,
            pid
        ))
        .unwrap();
    }
    datagen::insert_users_anon(count, &mut db1).unwrap();
    count = 0;

    // decorrelate papers
    let res = db.query_iter(&format!("SELECT paperId FROM Paper")).unwrap();
    for row in res {
        count += 1;
        let pid: u64 = from_value(row.unwrap().unwrap()[0].clone());
        db1.query_drop(&format!(
            "UPDATE Paper SET leadContactId = {} WHERE PaperId = {}",
            nusers,
            pid
        ))
        .unwrap();
        db1.query_drop(&format!(
            "UPDATE Paper SET shepherdContactId = {} WHERE PaperId = {}",
            nusers,
            pid
        ))
        .unwrap();
        db1.query_drop(&format!(
            "UPDATE Paper SET managerContactId = {} WHERE PaperId = {}",
            nusers,
            pid
        ))
        .unwrap();
    }
    datagen::insert_users_anon(count, &mut db1).unwrap();
    count = 0;

    // decorrelate reviews
    let res = db
        .query_iter(&format!(
            "SELECT reviewId FROM PaperReview",
        ))
        .unwrap();
    for row in res {
        count += 1;
        let rid: u64 = from_value(row.unwrap().unwrap()[0].clone());
        db1.query_drop(&format!(
            "UPDATE PaperReview SET contactId = {} WHERE ReviewId = {}",
            nusers,
            rid
        ))
        .unwrap();
    }
    datagen::insert_users_anon(count, &mut db1).unwrap();
    count = 0;

    // decorrelate comments
    let res = db
        .query_iter(&format!(
            "SELECT commentId FROM PaperComment",
        ))
        .unwrap();
    for row in res {
        count += 1;
        let rid: u64 = from_value(row.unwrap().unwrap()[0].clone());
        db1.query_drop(&format!(
            "UPDATE PaperComment SET contactId = {} WHERE commentId = {}",
            nusers,
            rid
        ))
        .unwrap();
    }
    datagen::insert_users_anon(count, &mut db1).unwrap();
    count = 0;

    // decorrelate paper review requested
    let res = db
        .query_iter(&format!(
            "SELECT paperId, email FROM PaperReviewRefused",
        ))
        .unwrap();
    for row in res {
        count += 1;
        let vals = row.unwrap().unwrap();
        let pid: u64 = from_value(vals[0].clone());
        let email: String = from_value(vals[1].clone());
        db1.query_drop(&format!(
            "UPDATE PaperReviewRefused SET requestedBy = {} WHERE paperId= {} and contactId = {}",
            nusers,
            pid,email 
        ))
        .unwrap();
        db1.query_drop(&format!(
            "UPDATE PaperReviewRefused SET refusedBy = {} WHERE paperId= {} and contactId = {}",
            nusers,
            pid,email 
        ))
        .unwrap();
    }
    datagen::insert_users_anon(count, &mut db1).unwrap();
    count = 0;

    // decorrelate comments
    let res = db
        .query_iter(&format!(
            "SELECT commentId FROM PaperComment",
        ))
        .unwrap();
    for row in res {
        count += 1;
        let rid: u64 = from_value(row.unwrap().unwrap()[0].clone());
        db1.query_drop(&format!(
            "UPDATE PaperComment SET contactId = {} WHERE commentId = {}",
            nusers,
            rid
        ))
        .unwrap();
    }
    datagen::insert_users_anon(count, &mut db1).unwrap();
    count = 0;

    // decorrelate review rating
    let res = db
        .query_iter(&format!(
            "SELECT * FROM ReviewRating",
        ))
        .unwrap();
    for row in res {
        count += 1;
        let rid: u64 = from_value(row.unwrap().unwrap()[0].clone());
        db1.query_drop(&format!(
            "UPDATE ReviewRating SET contactId = {} WHERE ratingId = {}",
            nusers,
            rid 
        ))
        .unwrap();
    }
    datagen::insert_users_anon(count, &mut db1).unwrap();
    count = 0;

    // decorrelate action log
    let res = db.query_iter(&format!("SELECT * FROM ActionLog")).unwrap();
    for row in res {
        count += 1;
        let aid: u64 = from_value(row.unwrap().unwrap()[0].clone());
        db1.query_drop(&format!(
            "UPDATE ActionLog SET contactId = {} WHERE logId = {}",
            nusers,
           aid 
        ))
        .unwrap();
        db1.query_drop(&format!(
            "UPDATE ActionLog SET trueContactId = {} WHERE logId = {}",
            nusers,
           aid 
        ))
        .unwrap();
    }
    datagen::insert_users_anon(count, &mut db1).unwrap();
    anon_durations.push(start.elapsed());

    print_stats(
        nusers as u64,
        account_durations,
        anon_durations,
        read_durations,
        edit_durations,
        delete_durations,
        vec![],
        vec![],
        vec![],
        vec![],
        true,
        args.dryrun,
    );
}

fn print_stats(
    nusers: u64,
    account_durations: Vec<Duration>,
    anon_durations: Vec<Duration>,
    read_durations: Vec<Duration>,
    edit_durations: Vec<Duration>,
    delete_durations: Vec<Duration>,
    restore_durations: Vec<Duration>,
    edit_durations_preanon: Vec<Duration>,
    delete_durations_preanon: Vec<Duration>,
    restore_durations_preanon: Vec<Duration>,
    baseline: bool,
    dryrun: bool,
) {
    let filename = if baseline {
        format!(
            "../../results/hotcrp_results/hotcrp_disguise_stats_{}users_baseline.csv",
            nusers
        )
    } else if dryrun {
        format!(
            "../../results/hotcrp_results/hotcrp_disguise_stats_{}users_nocrypto.csv",
            nusers
        )
    } else {
        format!("../../results/hotcrp_results/hotcrp_disguise_stats_{}users.csv", nusers)
    };
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
        edit_durations_preanon
            .iter()
            .map(|d| d.as_micros().to_string())
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
    writeln!(
        f,
        "{}",
        delete_durations_preanon
            .iter()
            .map(|d| d.as_micros().to_string())
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
    writeln!(
        f,
        "{}",
        restore_durations_preanon
            .iter()
            .map(|d| d.as_micros().to_string())
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
    writeln!(
        f,
        "{}",
        read_durations
            .iter()
            .map(|d| d.as_micros().to_string())
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
}
