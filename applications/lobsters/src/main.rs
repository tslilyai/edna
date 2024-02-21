extern crate libc;
extern crate log;
extern crate mysql;
extern crate rand;

use chrono::Local;
use edna::{helpers, EdnaClient};
use log::{error, warn};
use mysql::prelude::*;
use mysql::Opts;
use rand::Rng;
use std::cmp::min;
use std::convert::TryInto;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time;
use std::time::Duration;
use std::*;
use structopt::StructOpt;

mod datagen;
mod disguises;
mod migrations;
mod queriers;
mod reveal_bench;
mod updates_bench;
include!("statistics.rs");

const TOTAL_TIME: u128 = 500000;
const SCHEMA: &'static str = include_str!("../schema.sql");
const TABLEINFO_JSON: &'static str = include_str!("./disguises/table_info.json");
const PPGEN_JSON: &'static str = include_str!("./disguises/pp_gen.json");
const DECAY_JSON: &'static str = include_str!("./disguises/data_decay.json");
const GDPR_JSON: &'static str = include_str!("./disguises/gdpr_disguise.json");
const HOBBY_JSON: &'static str = include_str!("./disguises/hobby_anon.json");

#[derive(StructOpt)]
struct Cli {
    #[structopt(long = "scale", default_value = "1")]
    scale: f64,
    #[structopt(long = "filename", default_value = "")]
    filename: String,
    #[structopt(long = "nconcurrent", default_value = "1")]
    nconcurrent: usize,
    #[structopt(long = "disguiser", default_value = "cheap")]
    disguiser: String,
    #[structopt(long = "prime")]
    prime: bool,
    #[structopt(long = "test", default_value = "")]
    test: String,
    #[structopt(long = "txn")]
    use_txn: bool,
    #[structopt(long = "dryrun")]
    dryrun: bool,
}

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
    let args = Cli::from_args();
    let scale = args.scale;
    let prime = args.prime;
    let sampler = datagen::Sampler::new(scale);
    let nusers = sampler.nusers();

    let dbname: String = "lobsters_edna".to_string();
    let host = "127.0.0.1:3306";
    let url = format!("mysql://{}:{}@{}/{}", "tester", "pass", host, dbname);
    let pool = mysql::Pool::new(Opts::from_url(&url).unwrap()).unwrap();
    let mut db = pool.get_conn().unwrap();

    if prime {
        helpers::init_db(false, "tester", "pass", host, &dbname, SCHEMA);
        datagen::gen_data(&sampler, &mut db);
    }
    let mut edna = EdnaClient::new("tester", "pass", host, &dbname, false, false, args.dryrun);

    if args.prime {
        // don't run benchmarks if we're just priming
        error!("PRIMING???");
        return;
    }

    if args.test.contains("baseline") {
        match args.test.as_str() {
            "baseline_stats" => run_baseline_stats_test(&mut db, &sampler),
            "baseline_delete" => run_baseline_delete_test(&sampler, &mut db),
            "baseline_decay" => run_baseline_decay_test(&sampler, &mut db),
            "baseline_hobby_anon" => run_baseline_hobby_anon_test(&sampler, &mut db),
            _ => (),
        }
        return;
    }

    error!("Registering {} users", nusers);
    for u in 0..nusers {
        let user_id = u as u64 + 1;
        edna.register_principal(&user_id.to_string(), user_id.to_string());
    }
    if args.test == "updates" {
        updates_bench::run_updates_test(&mut edna, &mut db, args.use_txn, nusers as usize);
        return;
    }
    if args.test == "reveal" {
        reveal_bench::run_disguise_reveal_test(&mut edna, &mut db, args.use_txn, nusers as usize);
        return;
    }

    let mut run_concurrent = false;
    match args.test.as_str() {
        "storage" => run_sizes_test(&mut edna, &sampler),
        //"stats" => run_single_edna_test(&sampler, &mut edna, &url),
        "stats" => run_stats_test(&mut edna, &sampler, &url, args.use_txn, args.dryrun),
        _ => run_concurrent = true,
    }
    if !run_concurrent {
        return;
    }

    // otherwise run the concurrent test
    let delete_durations = Arc::new(Mutex::new(vec![]));
    let restore_durations = Arc::new(Mutex::new(vec![]));
    let op_durations = Arc::new(Mutex::new(vec![]));
    let operations = Arc::new(Mutex::new(vec![]));

    let mut user_stories = 0;
    let mut user_comments = 0;
    let mut user_votes = 0;
    let user_to_disguise: i64 = if args.disguiser == "cheap" {
        error!("Disguising cheap /random user");
        0
    } else if args.disguiser == "expensive" {
        nusers as i64
    } else {
        -1
    };

    if user_to_disguise > 0 {
        let res = db
            .query_iter(format!(
                r"SELECT COUNT(*) FROM stories WHERE user_id={};",
                user_to_disguise
            ))
            .unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 1);
            user_stories = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        }
        let res = db
            .query_iter(format!(
                r"SELECT COUNT(*) FROM votes WHERE user_id={};",
                user_to_disguise
            ))
            .unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 1);
            user_votes = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        }
        let res = db
            .query_iter(format!(
                r"SELECT COUNT(*) FROM comments WHERE user_id={};",
                user_to_disguise
            ))
            .unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 1);
            user_comments = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        }
        error!(
            "Going to disguise user with {} stories, {} comments, {} votes ({} total)",
            user_stories,
            user_comments,
            user_votes,
            user_stories + user_votes + user_comments
        );
    }

    let mut threads = vec![];
    let arc_sampler = Arc::new(sampler);
    let barrier = Arc::new(Barrier::new(args.nconcurrent + 1));
    for _ in 0..args.nconcurrent {
        let c = barrier.clone();
        let my_op_durations = op_durations.clone();
        let my_operations = operations.clone();
        let my_arc_sampler = arc_sampler.clone();
        let mypool = pool.clone();
        threads.push(thread::spawn(move || {
            let mut db = mypool.get_conn().unwrap();
            run_normal_thread(
                user_to_disguise,
                &mut db,
                my_arc_sampler,
                my_op_durations,
                my_operations,
                c,
            )
        }));
    }
    error!("Waiting for barrier!");
    barrier.wait();

    let arc_edna = Arc::new(Mutex::new(edna));
    let ndisguises = if user_to_disguise >= 0 {
        run_disguising(
            &args,
            arc_edna,
            user_to_disguise.try_into().unwrap(),
            nusers,
            delete_durations.clone(),
            restore_durations.clone(),
        )
        .unwrap()
    } else {
        0
    };
    warn!("Finished running disguising!");

    for j in threads {
        j.join().expect("Could not join?");
    }

    print_concurrent_stats(
        &args,
        ndisguises,
        &op_durations.lock().unwrap(),
        &delete_durations.lock().unwrap(),
        &restore_durations.lock().unwrap(),
        &operations.lock().unwrap(),
    );
}

fn run_normal_thread(
    disguiser: i64,
    db: &mut mysql::PooledConn,
    sampler: Arc<datagen::Sampler>,
    op_durations: Arc<Mutex<Vec<(Duration, Duration)>>>,
    operations: Arc<Mutex<Vec<String>>>,
    barrier: Arc<Barrier>,
) {
    let nstories = sampler.nstories();
    let mut ncomments = sampler.ncomments();
    let res = db
        .query_iter(format!(
            r"SELECT COUNT(*) FROM comments WHERE id < {};",
            ncomments,
        ))
        .unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 1);
        ncomments = helpers::mysql_val_to_u64(&vals[0]).unwrap() as u32;
    }
    let nusers = sampler.nusers() as u64;

    let mut rng = rand::thread_rng();
    let max_id = i32::MAX as u32;
    let mut my_op_durations = vec![];

    barrier.wait();

    let mut my_operations = vec![];
    let overall_start = time::Instant::now();
    while overall_start.elapsed().as_millis() < TOTAL_TIME {
        let start = time::Instant::now();
        // XXX: assume a uniform distribution instead of assuming high-use users mostly
        // submit requests
        //let user_id = sampler.user(&mut rng) as u64;
        let mut user_id = rng.gen_range(0, nusers);
        while disguiser > 0 && user_id as i64 == disguiser {
            user_id = rng.gen_range(0, nusers);
        }
        let user = Some(user_id);

        // randomly pick next request type based on relative frequency
        let mut seed: isize = rng.gen_range(0, 100000);
        let seed = &mut seed;
        let mut pick = |f| {
            let applies = *seed <= f;
            *seed -= f;
            applies
        };

        let mut op = "read_story";
        if pick(55842) {
            // XXX: we're assuming here that stories with more votes are viewed more
            let story = sampler.story_for_vote(&mut rng) as u64;
            let _ = queriers::stories::read_story(db, user, story).unwrap();
        } else if pick(30105) {
            op = "frontpage";
            let _ = queriers::frontpage::query_frontpage(db, user).unwrap();
        } else if pick(6702) {
            // XXX: we're assuming that users who vote a lot are also "popular"
            op = "prof";
            queriers::user::get_profile(db, user_id).unwrap();
        } else if pick(4674) {
            op = "getcomments";
            queriers::comment::get_comments(db, user).unwrap();
        } else if pick(967) {
            op = "getrecent";
            queriers::recent::recent(db, user).unwrap();
        } else if pick(630) {
            op = "votecomment";
            let comment = rng.gen_range(0, ncomments); //sampler.comment_for_vote(&mut rng);
            queriers::vote::vote_on_comment(db, user, comment as u64, true).unwrap();
        } else if pick(475) {
            op = "votestory";
            let story = sampler.story_for_vote(&mut rng);
            queriers::vote::vote_on_story(db, user, story as u64, true).unwrap();
        } else if pick(316) {
            // comments without a parent
            op = "postcomment";
            let id = rng.gen_range(ncomments, max_id);
            let story = sampler.story_for_comment(&mut rng);
            queriers::comment::post_comment(db, user, id as u64, story as u64, None).unwrap();
        } else if pick(87) {
            op = "login";
            queriers::user::login(db, user_id).unwrap();
        } else if pick(71) {
            // comments with a parent
            op = "postcommentparent";
            let id = rng.gen_range(ncomments, max_id);
            let story = sampler.story_for_comment(&mut rng);
            // we need to pick a comment that's on the chosen story
            // we know that every nth comment from prepopulation is to the same story
            let comments_per_story = ncomments / nstories;
            let parent = min(
                story + nstories * rng.gen_range(0, comments_per_story),
                max_id,
            );
            queriers::comment::post_comment(db, user, id.into(), story as u64, Some(parent as u64))
                .unwrap();
        } else if pick(54) {
            op = "votecomment";
            let comment = sampler.comment_for_vote(&mut rng);
            queriers::vote::vote_on_comment(db, user, comment as u64, false).unwrap();
        } else if pick(53) {
            op = "poststory";
            let id = rng.gen_range(nstories, max_id);
            queriers::stories::post_story(db, user, id as u64, format!("benchmark {}", id))
                .unwrap();
        } else {
            op = "votestory";
            let story = sampler.story_for_vote(&mut rng);
            queriers::vote::vote_on_story(db, user, story as u64, false).unwrap();
        }
        my_op_durations.push((overall_start.elapsed(), start.elapsed()));
        my_operations.push(format!("{}, {}", op, start.elapsed().as_micros()));
    }

    op_durations.lock().unwrap().append(&mut my_op_durations);
    operations.lock().unwrap().append(&mut my_operations);
}

fn run_disguising_thread(
    edna: Arc<Mutex<EdnaClient>>,
    uid: u64,
    my_delete_durations: &mut Vec<(Duration, Duration)>,
    my_restore_durations: &mut Vec<(Duration, Duration)>,
    overall_start: time::Instant,
    use_txn: bool,
) {
    // UNSUB
    let mut edna_locked = edna.lock().unwrap();
    let start = time::Instant::now();
    let did = edna_locked
        .apply_disguise(
            uid.to_string(),
            GDPR_JSON,
            TABLEINFO_JSON,
            PPGEN_JSON,
            None, //Some(uid.to_string()),
            None,
            use_txn,
        )
        .unwrap();
    my_delete_durations.push((overall_start.elapsed(), start.elapsed()));
    warn!("Ran unsub: {}", start.elapsed().as_micros());

    // RESUB
    let start = time::Instant::now();
    edna_locked
        .reveal_disguise(
            uid.to_string(),
            did,
            TABLEINFO_JSON,
            PPGEN_JSON,
            Some(edna::RevealPPType::Restore),
            true, // allow partial row reveals
            Some(uid.to_string()),
            None,
            use_txn,
        )
        .unwrap();
    drop(edna_locked);
    my_restore_durations.push((overall_start.elapsed(), start.elapsed()));
    warn!("Ran resub: {}", start.elapsed().as_micros());
}

fn run_disguising(
    args: &Cli,
    edna: Arc<Mutex<EdnaClient>>,
    uid: u64,
    nusers: u32,
    delete_durations: Arc<Mutex<Vec<(Duration, Duration)>>>,
    restore_durations: Arc<Mutex<Vec<(Duration, Duration)>>>,
) -> Result<u64, mysql::Error> {
    let overall_start = time::Instant::now();
    let mut nexec = 0;
    let mut rng = rand::thread_rng();
    // if we're doing random
    let mut uid = uid;
    if uid == 0 {
        uid = rng.gen_range(0, nusers) as u64;
    }
    while overall_start.elapsed().as_millis() < TOTAL_TIME {
        // wait between each round
        let my_edna = edna.clone();
        let mut my_delete_durations = vec![];
        let mut my_restore_durations = vec![];
        run_disguising_thread(
            my_edna,
            uid,
            &mut my_delete_durations,
            &mut my_restore_durations,
            overall_start,
            args.use_txn,
        );

        delete_durations
            .lock()
            .unwrap()
            .append(&mut my_delete_durations);
        restore_durations
            .lock()
            .unwrap()
            .append(&mut my_restore_durations);
        nexec += 1;
    }
    warn!("Disguising ran {} times", nexec);
    Ok(nexec)
}

fn run_sizes_test(edna: &mut EdnaClient, sampler: &datagen::Sampler) {
    let dbname = "lobsters_edna";
    let filename = format!("../../results/lobsters_results/lobsters_disguise_storage_stats.csv");
    let mut file = File::create(filename).unwrap();
    let nusers = sampler.nusers();
    let mut rng = rand::thread_rng();
    let bytes = edna.get_space_overhead(dbname);
    file.write(format!("TOTAL START: {} {}\n", bytes.0, bytes.1).as_bytes())
        .unwrap();

    for _ in 0..1 {
        let mut users = vec![];
        let mut dids = vec![];
        for _i in 0..(nusers / 10) as usize {
            let user_id = rng.gen_range(0, nusers) as u64 + 1;
            users.push(user_id);

            // DECAY
            let did = edna
                .apply_disguise(
                    user_id.to_string(),
                    DECAY_JSON,
                    TABLEINFO_JSON,
                    PPGEN_JSON,
                    None, //Some(user_id.to_string()),
                    None,
                    false,
                )
                .unwrap();
            dids.push(did);
        }
        let bytes = edna.get_space_overhead(dbname);
        file.write(format!("TOTAL DECOR: {} {}\n", bytes.0, bytes.1).as_bytes())
            .unwrap();

        // restore all the users
        for (i, u) in users.iter().enumerate() {
            let did = dids[i];

            // UNDECAY
            edna.reveal_disguise(
                u.to_string(),
                did,
                TABLEINFO_JSON,
                PPGEN_JSON,
                Some(edna::RevealPPType::Restore),
                true, // allow partial row reveals
                Some(u.to_string()),
                None,
                false,
            )
            .unwrap();
        }

        let bytes = edna.get_space_overhead(dbname);
        file.write(format!("TOTAL RESTORE: {} {}\n", bytes.0, bytes.1).as_bytes())
            .unwrap();
        file.flush().unwrap();
    }
    file.flush().unwrap();
}

fn run_stats_test(
    edna: &mut EdnaClient,
    sampler: &datagen::Sampler,
    url: &str,
    use_txn: bool,
    dryrun: bool,
) {
    error!("Running stats test");
    let pool = mysql::Pool::new(Opts::from_url(url).unwrap()).unwrap();
    let mut db = pool.get_conn().unwrap();

    let filename = if dryrun {
        format!("../../results/lobsters_results/lobsters_disguise_stats_basic_dryrun.csv",)
    } else {
        format!("../../results/lobsters_results/lobsters_disguise_stats_basic.csv")
    };
    let mut file = File::create(filename).unwrap();
    file.write("create_edna, read_story, read_frontpage\n".as_bytes())
        .unwrap();

    let mut rng = rand::thread_rng();
    let mut next_user_id = sampler.nusers() + 2;
    for u in 0..sampler.nusers() {
        // now sample every 10 users (just temporary)
        if u % 10 > 0 {
            continue;
        }
        let user_id = u as u64 + 1;
        let start = time::Instant::now();
        db.query_drop(&format!(
            "INSERT INTO `users` (`id`, `username`, `last_login`) VALUES ({}, {}, '{}')",
            next_user_id,
            next_user_id,
            Local::now().naive_local().to_string()
        ))
        .unwrap();
        edna.register_principal(&next_user_id.to_string(), next_user_id.to_string());
        file.write(format!("{}, ", start.elapsed().as_micros()).as_bytes())
            .unwrap();
        next_user_id += 1;

        // READS
        let start = time::Instant::now();
        let story = sampler.story_for_vote(&mut rng) as u64;
        queriers::stories::read_story(&mut db, Some(user_id), story).unwrap();
        file.write(format!("{}, ", start.elapsed().as_micros()).as_bytes())
            .unwrap();
        let start = time::Instant::now();
        queriers::frontpage::query_frontpage(&mut db, Some(user_id)).unwrap();
        file.write(format!("{}\n", start.elapsed().as_micros()).as_bytes())
            .unwrap();
    }
    file.flush().unwrap();

    let filename = if dryrun {
        format!("../../results/lobsters_results/lobsters_disguise_stats_dryrun.csv",)
    } else {
        format!("../../results/lobsters_results/lobsters_disguise_stats.csv")
    };
    let mut file = File::create(filename).unwrap();
    file.write(
        "uid, ndata, decay, undecay, delete, restore, category_anon, category_reveal\n".as_bytes(),
    )
    .unwrap();

    for u in 0..sampler.nusers() {
        if u % 10 > 0 {
            continue;
        }
        let user_id = u as u64 + 1;
        let mut user_stories = 0;
        let mut user_comments = 0;
        let res = db
            .query_iter(format!(
                r"SELECT COUNT(*) FROM stories WHERE user_id={};",
                user_id
            ))
            .unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 1);
            user_stories = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        }
        let res = db
            .query_iter(format!(
                r"SELECT COUNT(*) FROM comments WHERE user_id={};",
                user_id
            ))
            .unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 1);
            user_comments = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        }
        file.write(format!("{}, {}, ", user_id, user_stories + user_comments).as_bytes())
            .unwrap();

        // DECAY
        error!("Decaying user {}", u);
        let start = time::Instant::now();
        let did = edna
            .apply_disguise(
                user_id.to_string(),
                DECAY_JSON,
                TABLEINFO_JSON,
                PPGEN_JSON,
                None, //Some(user_id.to_string()),
                None,
                use_txn,
            )
            .unwrap();
        file.write(format!("{}, ", start.elapsed().as_micros()).as_bytes())
            .unwrap();

        // checks
        let res = db
            .query_iter(format!(
                r"SELECT COUNT(*) FROM stories WHERE user_id={};",
                user_id
            ))
            .unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 1);
            assert_eq!(helpers::mysql_val_to_u64(&vals[0]).unwrap(), 0);
        }
        let res = db
            .query_iter(format!(
                r"SELECT COUNT(*) FROM comments WHERE user_id={};",
                user_id
            ))
            .unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 1);
            assert_eq!(helpers::mysql_val_to_u64(&vals[0]).unwrap(), 0);
        }

        // UNDECAY
        error!("Undecaying user {}", u);
        let start = time::Instant::now();
        edna.reveal_disguise(
            user_id.to_string(),
            did,
            TABLEINFO_JSON,
            PPGEN_JSON,
            Some(edna::RevealPPType::Restore),
            true, // allow partial row reveals
            Some(user_id.to_string()),
            None,
            use_txn,
        )
        .unwrap();
        file.write(format!("{}, ", start.elapsed().as_micros()).as_bytes())
            .unwrap();

        // checks
        let res = db
            .query_iter(format!(
                r"SELECT COUNT(*) FROM stories WHERE user_id={};",
                user_id
            ))
            .unwrap();

        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 1);
            assert_eq!(helpers::mysql_val_to_u64(&vals[0]).unwrap(), user_stories);
        }
        let res = db
            .query_iter(format!(
                r"SELECT COUNT(*) FROM comments WHERE user_id={};",
                user_id
            ))
            .unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 1);
            assert_eq!(helpers::mysql_val_to_u64(&vals[0]).unwrap(), user_comments);
        }

        // UNSUB
        error!("UNSUB user {}", u);
        let start = time::Instant::now();
        let did = edna
            .apply_disguise(
                user_id.to_string(),
                GDPR_JSON,
                TABLEINFO_JSON,
                PPGEN_JSON,
                None, //Some(user_id.to_string()),
                None,
                use_txn,
            )
            .unwrap();
        file.write(format!("{}, ", start.elapsed().as_micros()).as_bytes())
            .unwrap();
        // checks
        let res = db
            .query_iter(format!(
                r"SELECT COUNT(*) FROM stories WHERE user_id={};",
                user_id
            ))
            .unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 1);
            assert_eq!(helpers::mysql_val_to_u64(&vals[0]).unwrap(), 0);
        }
        let res = db
            .query_iter(format!(
                r"SELECT COUNT(*) FROM comments WHERE user_id={};",
                user_id
            ))
            .unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 1);
            assert_eq!(helpers::mysql_val_to_u64(&vals[0]).unwrap(), 0);
        }

        // RESUB
        error!("RESUB user {}", u);
        let start = time::Instant::now();
        edna.reveal_disguise(
            user_id.to_string(),
            did,
            TABLEINFO_JSON,
            PPGEN_JSON,
            Some(edna::RevealPPType::Restore),
            true, // allow partial row reveals
            Some(user_id.to_string()),
            None,
            use_txn,
        )
        .unwrap();
        file.write(format!("{}, ", start.elapsed().as_micros()).as_bytes())
            .unwrap();
        // checks
        let res = db
            .query_iter(format!(
                r"SELECT COUNT(*) FROM stories WHERE user_id={};",
                user_id
            ))
            .unwrap();

        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 1);
            assert_eq!(helpers::mysql_val_to_u64(&vals[0]).unwrap(), user_stories);
        }
        let res = db
            .query_iter(format!(
                r"SELECT COUNT(*) FROM comments WHERE user_id={};",
                user_id
            ))
            .unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 1);
            assert_eq!(helpers::mysql_val_to_u64(&vals[0]).unwrap(), user_comments);
        }

        // CATEGORY ANON
        {
            error!("CATEGORY ANON APPLY user {}", u);
            let from = "INNER JOIN taggings ON taggings.story_id = stories.id \
                        INNER JOIN tags ON taggings.tag_id = tags.id";
            let nstories: u64 = db
                .query_first(format!(
                    "SELECT COUNT(*) FROM stories {} WHERE stories.user_id={} AND tags.tag = 'starwars';",
                    from, user_id
                ))
                .unwrap()
                .unwrap();
            let ncomments: u64 = db
                .query_first(format!(
                    "SELECT COUNT(*) FROM comments \
                    INNER JOIN stories ON comments.story_id = stories.id {} \
                    WHERE comments.user_id={} AND tags.tag = 'starwars';",
                    from, user_id
                ))
                .unwrap()
                .unwrap();
            let nvotes: u64 = db
                .query_first(format!(
                    "SELECT COUNT(*) FROM votes \
                    INNER JOIN stories ON votes.story_id = stories.id {} \
                    WHERE votes.user_id={} AND tags.tag = 'starwars';",
                    from, user_id
                ))
                .unwrap()
                .unwrap();

            error!(
                "{} has {} starwars contributions",
                user_id,
                nstories + ncomments + nvotes
            );
            let start = time::Instant::now();
            let did = edna
                .apply_disguise(
                    user_id.to_string(),
                    HOBBY_JSON,
                    TABLEINFO_JSON,
                    PPGEN_JSON,
                    None, //Some(user_id.to_string()),
                    None,
                    use_txn,
                )
                .unwrap();
            file.write(format!("{}, ", start.elapsed().as_micros()).as_bytes())
                .unwrap();

            // checks
            let stories_count: u64 = db
                .query_first(format!(
                    r"SELECT COUNT(*) FROM stories {} WHERE stories.user_id={} AND tags.tag = 'starwars';",
                    from, user_id
                ))
                .unwrap()
                .unwrap();
            assert_eq!(stories_count, 0);
            let comments_count: u64 = db
                .query_first(format!(
                    "SELECT COUNT(*) FROM comments \
                    INNER JOIN stories ON comments.story_id = stories.id {} \
                    WHERE comments.user_id={} AND tags.tag = 'starwars';",
                    from, user_id
                ))
                .unwrap()
                .unwrap();
            assert_eq!(comments_count, 0);
            let votes_count: u64 = db
                .query_first(format!(
                    "SELECT COUNT(*) FROM votes \
                    INNER JOIN stories ON votes.story_id = stories.id {} \
                    WHERE votes.user_id={} AND tags.tag = 'starwars';",
                    from, user_id
                ))
                .unwrap()
                .unwrap();
            assert_eq!(votes_count, 0);

            // CATEGORY ANON REVEAL
            error!("CATEGORY ANON REVEAL user {}", u);
            let start = time::Instant::now();
            edna.reveal_disguise(
                user_id.to_string(),
                did,
                TABLEINFO_JSON,
                PPGEN_JSON,
                Some(edna::RevealPPType::Restore),
                true, // allow partial row reveals
                Some(user_id.to_string()),
                None,
                use_txn,
            )
            .unwrap();
            file.write(format!("{}\n", start.elapsed().as_micros()).as_bytes())
                .unwrap();
            // checks
            let stories_count: u64 = db
                .query_first(format!(
                    "SELECT COUNT(*) FROM stories {} WHERE stories.user_id={} AND tags.tag = 'starwars';",
                    from, user_id
                ))
                .unwrap()
                .unwrap();
            assert_eq!(stories_count, nstories);
            let comments_count: u64 = db
                .query_first(format!(
                    "SELECT COUNT(*) FROM comments \
                    INNER JOIN stories ON comments.story_id = stories.id {} \
                    WHERE comments.user_id={} AND tags.tag = 'starwars';",
                    from, user_id
                ))
                .unwrap()
                .unwrap();
            assert_eq!(comments_count, ncomments);
            let votes_count: u64 = db
                .query_first(format!(
                    "SELECT COUNT(*) FROM votes \
                    INNER JOIN stories ON votes.story_id = stories.id {} \
                    WHERE votes.user_id={} AND tags.tag = 'starwars';",
                    from, user_id
                ))
                .unwrap()
                .unwrap();
            assert_eq!(votes_count, nvotes);
        }
    }
    file.flush().unwrap();
}

fn run_baseline_stats_test(db: &mut mysql::PooledConn, sampler: &datagen::Sampler) {
    // READS
    let filename =
        format!("../../results/lobsters_results/lobsters_disguise_stats_baseline_stats.csv");
    let mut file = File::create(filename).unwrap();

    file.write("create, read_story, read_frontpage\n".as_bytes())
        .unwrap();

    let mut next_user_id = sampler.nusers() + 2;
    let mut rng = rand::thread_rng();
    for u in 0..sampler.nusers() {
        if u % 10 > 0 {
            continue;
        }
        let start = time::Instant::now();
        db.query_drop(&format!(
            "INSERT INTO `users` (`id`, `username`, `last_login`) VALUES ({}, {}, '{}')",
            next_user_id,
            next_user_id,
            Local::now().naive_local().to_string()
        ))
        .unwrap();
        next_user_id += 1;
        file.write(format!("{}, ", start.elapsed().as_micros()).as_bytes())
            .unwrap();

        let start = time::Instant::now();
        let story = sampler.story_for_vote(&mut rng) as u64;
        queriers::stories::read_story(db, Some(u as u64 + 1), story).unwrap();
        file.write(format!("{}, ", start.elapsed().as_micros()).as_bytes())
            .unwrap();
        let start = time::Instant::now();
        queriers::frontpage::query_frontpage(db, Some(u as u64 + 1)).unwrap();
        file.write(format!("{}\n", start.elapsed().as_micros()).as_bytes())
            .unwrap();
    }
    file.flush().unwrap();
}

fn run_baseline_delete_test(sampler: &datagen::Sampler, db: &mut mysql::PooledConn) {
    let filename =
        format!("../../results/lobsters_results/lobsters_disguise_stats_baseline_delete.csv");
    let mut file = File::create(filename).unwrap();
    for u in 0..sampler.nusers() {
        // now sample every 10 users (just temporary)
        if u % 10 > 0 {
            continue;
        }

        let start = time::Instant::now();
        disguises::baseline::apply_delete(u as u64 + 1, db).unwrap();
        file.write(format!("{}\n", start.elapsed().as_micros()).as_bytes())
            .unwrap();
    }
    file.flush().unwrap();
}

fn run_baseline_decay_test(sampler: &datagen::Sampler, db: &mut mysql::PooledConn) {
    let filename =
        format!("../../results/lobsters_results/lobsters_disguise_stats_baseline_decay.csv");
    let mut file = File::create(filename).unwrap();
    for u in 0..sampler.nusers() {
        // now sample every 10 users (just temporary)
        if u % 10 > 0 {
            continue;
        }

        // baseline decay
        let start = time::Instant::now();
        disguises::baseline::apply_decay(u as u64 + 1, db).unwrap();
        file.write(format!("{}\n", start.elapsed().as_micros()).as_bytes())
            .unwrap();
    }
    file.flush().unwrap();
}

fn run_baseline_hobby_anon_test(sampler: &datagen::Sampler, db: &mut mysql::PooledConn) {
    let filename =
        format!("../../results/lobsters_results/lobsters_disguise_stats_baseline_hobbyanon.csv");
    let mut file = File::create(filename).unwrap();

    for u in 0..sampler.nusers() {
        // now sample every 10 users (just temporary)
        if u % 10 > 0 {
            continue;
        }
        let user_id = u as u64 + 1;
        let from = "INNER JOIN taggings ON taggings.story_id = stories.id \
                        INNER JOIN tags ON taggings.tag_id = tags.id";
        let nstories: u64 = db
            .query_first(format!(
                "SELECT COUNT(*) FROM stories {} WHERE stories.user_id={} AND tags.tag = 'starwars';",
                from, user_id
            ))
            .unwrap()
            .unwrap();
        let ncomments: u64 = db
            .query_first(format!(
                "SELECT COUNT(*) FROM comments \
                INNER JOIN stories ON comments.story_id = stories.id {} \
                WHERE comments.user_id={} AND tags.tag = 'starwars';",
                from, user_id
            ))
            .unwrap()
            .unwrap();
        let nvotes: u64 = db
            .query_first(format!(
                "SELECT COUNT(*) FROM votes \
                INNER JOIN stories ON votes.story_id = stories.id {} \
                WHERE votes.user_id={} AND tags.tag = 'starwars';",
                from, user_id
            ))
            .unwrap()
            .unwrap();

        error!(
            "{} has {} starwars contributions",
            user_id,
            nstories + ncomments + nvotes
        );

        let start = time::Instant::now();
        disguises::baseline::apply_hobby_anon(user_id, db).unwrap();
        warn!("{}mus", start.elapsed().as_micros());
        file.write(format!("{}\n", start.elapsed().as_micros()).as_bytes())
            .unwrap();
        // checks
        {
            let stories_count: u64 = db
                .query_first(format!(
                    r"SELECT COUNT(*) FROM stories {} WHERE stories.user_id={} AND tags.tag = 'starwars';",
                    from, user_id
                ))
                .unwrap()
                .unwrap();
            assert_eq!(stories_count, 0);
            let comments_count: u64 = db
                .query_first(format!(
                    "SELECT COUNT(*) FROM comments \
                    INNER JOIN stories ON comments.story_id = stories.id {} \
                    WHERE comments.user_id={} AND tags.tag = 'starwars';",
                    from, user_id
                ))
                .unwrap()
                .unwrap();
            assert_eq!(comments_count, 0);
            let votes_count: u64 = db
                .query_first(format!(
                    "SELECT COUNT(*) FROM votes \
                    INNER JOIN stories ON votes.story_id = stories.id {} \
                    WHERE votes.user_id={} AND tags.tag = 'starwars';",
                    from, user_id
                ))
                .unwrap()
                .unwrap();
            assert_eq!(votes_count, 0);
        }
    }
    file.flush().unwrap();
}

fn print_concurrent_stats(
    args: &Cli,
    ndisguises: u64,
    edit_durations: &Vec<(Duration, Duration)>,
    delete_durations: &Vec<(Duration, Duration)>,
    restore_durations: &Vec<(Duration, Duration)>,
    operations: &Vec<String>,
) {
    error!("Finished {} disguises", ndisguises);

    let filename = format!(
        "../../results/lobsters_results/concurrent_disguise_stats_{}.csv",
        args.filename
    );

    // print out stats
    let mut f = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&filename)
        .unwrap();

    writeln!(f, "{}", ndisguises).unwrap();

    writeln!(
        f,
        "{}",
        edit_durations
            .iter()
            .map(|d| format!(
                "{}:{}",
                d.0.as_millis().to_string(),
                d.1.as_micros().to_string()
            ))
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
    writeln!(
        f,
        "{}",
        delete_durations
            .iter()
            .map(|d| format!(
                "{}:{}",
                d.0.as_millis().to_string(),
                d.1.as_micros().to_string()
            ))
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
    writeln!(
        f,
        "{}",
        restore_durations
            .iter()
            .map(|d| format!(
                "{}:{}",
                d.0.as_millis().to_string(),
                d.1.as_micros().to_string()
            ))
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();

    let filename = format!(
        "../../results/lobsters_results/concurrent_op_stats_{}.csv",
        args.filename
    );

    // print out stats
    let mut f = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&filename)
        .unwrap();

    for ln in operations {
        writeln!(f, "{}", ln).unwrap();
    }
}
