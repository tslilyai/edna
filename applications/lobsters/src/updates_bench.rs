use crate::migrations::*;
use edna::{helpers, EdnaClient};
use log::warn;
use mysql::prelude::*;
use std::fs::OpenOptions;
use std::io::Write;
use std::time;
use std::time::Duration;

const TABLEINFO_JSON: &'static str = include_str!("./disguises/table_info.json");
const PPGEN_JSON: &'static str = include_str!("./disguises/pp_gen.json");
const TABLEINFO_JSON_UPDATED: &'static str = include_str!("./migrations/table_info_updated.json");
const PPGEN_JSON_UPDATED: &'static str = include_str!("./migrations/pp_gen_updated.json");

const GDPR_JSON: &'static str = include_str!("./disguises/gdpr_disguise.json");

fn check_counts(user_stories: u64, user_comments: u64, db: &mut mysql::PooledConn, uid: usize) {
    let res = db
        .query_iter(format!(
            r"SELECT COUNT(*) FROM stories WHERE user_id={};",
            uid
        ))
        .unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 1);
        assert_eq!(user_stories, helpers::mysql_val_to_u64(&vals[0]).unwrap());
    }
    let res = db
        .query_iter(format!(
            r"SELECT COUNT(*) FROM comments WHERE user_id={};",
            uid
        ))
        .unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 1);
        assert_eq!(user_comments, helpers::mysql_val_to_u64(&vals[0]).unwrap());
    }
}

pub fn run_updates_test(
    edna: &mut EdnaClient,
    db: &mut mysql::PooledConn,
    use_txn: bool,
    uid: usize,
) {
    let mut delete_durations = vec![];
    let mut updated_restore_durations = vec![];
    let mut restore_durations = vec![];

    let mut user_stories = 0;
    let mut user_comments = 0;
    let res = db
        .query_iter(format!(
            r"SELECT COUNT(*) FROM stories WHERE user_id={};",
            uid
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
            uid
        ))
        .unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 1);
        user_comments = helpers::mysql_val_to_u64(&vals[0]).unwrap();
    }

    // UNSUB
    let start = time::Instant::now();
    let did = edna
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
    delete_durations.push(start.elapsed());
    warn!("Ran unsub 1: {}", start.elapsed().as_micros());

    // RESUB
    let start = time::Instant::now();
    edna.reveal_disguise(
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
    restore_durations.push(start.elapsed());
    warn!("Ran resub no updates: {}", start.elapsed().as_micros());

    // check state of db
    check_counts(user_stories, user_comments, db, uid);

    // UNSUB
    let start = time::Instant::now();
    let did = edna
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
    delete_durations.push(start.elapsed());
    warn!("Ran unsub 2: {}", start.elapsed().as_micros());

    // apply schema updates!
    normalize_url::apply(db);
    addusersettingshowemail::apply(db);
    story_text::apply(db);
    warn!(
        "apply all normalize url schema update: {}mus",
        start.elapsed().as_micros()
    );

    // record one-by-one, so they count as separate updates in Edna
    edna.record_update(normalize_url::update);
    edna.record_update(addusersettingshowemail::update);
    edna.record_update(story_text::update);

    // check state of db
    check_counts(user_stories, user_comments, db, uid);
    let res = db
        .query_iter(format!(r"SELECT COUNT(*) FROM story_texts JOIN stories on story_texts.id = stories.id WHERE stories.user_id = {};", uid))
        .unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 1);
        assert_eq!(user_stories, helpers::mysql_val_to_u64(&vals[0]).unwrap());
    }

    // RESUB
    let start = time::Instant::now();
    edna.reveal_disguise(
        uid.to_string(),
        did,
        TABLEINFO_JSON_UPDATED,
        PPGEN_JSON_UPDATED,
        Some(edna::RevealPPType::Restore),
        true, // allow partial row reveals
        Some(uid.to_string()),
        None,
        use_txn,
    )
    .unwrap();
    updated_restore_durations.push(start.elapsed());
    warn!("Ran resub updates: {}", start.elapsed().as_micros());

    // check state of db
    check_counts(user_stories, user_comments, db, uid);
    let res = db
        .query_iter(format!(r"SELECT COUNT(*) FROM story_texts JOIN stories on story_texts.id = stories.id WHERE stories.user_id = {};", uid))
        .unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 1);
        assert_eq!(user_stories, helpers::mysql_val_to_u64(&vals[0]).unwrap());
    }
    print_update_stats(
        &delete_durations,
        &restore_durations,
        &updated_restore_durations,
    );
}
fn print_update_stats(
    delete_durations: &Vec<Duration>,
    restore_durations: &Vec<Duration>,
    updated_restore_durations: &Vec<Duration>,
) {
    let filename = format!("../../results/lobsters_results/update_stats.csv",);

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
        delete_durations
            .iter()
            .map(|d| format!("{}", d.as_micros().to_string()))
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
    writeln!(
        f,
        "{}",
        restore_durations
            .iter()
            .map(|d| format!("{}", d.as_micros().to_string()))
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
    writeln!(
        f,
        "{}",
        updated_restore_durations
            .iter()
            .map(|d| format!("{}", d.as_micros().to_string()))
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
}
