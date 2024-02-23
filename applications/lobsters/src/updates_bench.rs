use crate::migrations::*;
use edna::{helpers, EdnaClient};
use log::warn;
use mysql::prelude::*;
use std::fs::OpenOptions;
use std::io::Write;
use std::thread::sleep;
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

pub fn run_simple_reveal(
    edna: &mut EdnaClient,
    db: &mut mysql::PooledConn,
    use_txn: bool,
    uid: usize,
) {
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

    helpers::query_drop("OPTIMIZE TABLE stories", db).unwrap();
    helpers::query_drop("OPTIMIZE TABLE story_texts", db).unwrap();
    sleep(Duration::from_secs(30));

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
    let elapsed = start.elapsed();

    // check state of db
    check_counts(user_stories, user_comments, db, uid);

    let filename = format!("../../results/lobsters_results/reveal_stats.csv",);
    // print out stats
    let mut f = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(&filename)
        .unwrap();

    writeln!(
        f,
        "{},{},{}",
        user_stories,
        user_comments,
        elapsed.as_micros().to_string()
    )
    .unwrap();
}

pub fn run_updates_test(
    edna: &mut EdnaClient,
    db: &mut mysql::PooledConn,
    use_txn: bool,
    uid: usize,
) {
    let mut user_stories = 0;
    let mut user_comments = 0;
    let mut story_count = 0;
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
    let res = db.query_iter(r"SELECT COUNT(*) FROM stories").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        story_count = helpers::mysql_val_to_u64(&vals[0]).unwrap();
    }

    // UNSUB
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

    // apply schema updates!
    let start = time::Instant::now();
    //normalize_url::apply(db);
    addusersettingshowemail::apply(db);
    story_text::apply(db);
    warn!(
        "apply all schema updates: {}mus",
        start.elapsed().as_micros()
    );
    sleep(Duration::from_secs(30));

    // record one-by-one, so they count as separate updates in Edna
    //edna.record_update(normalize_url::update);
    edna.record_update(addusersettingshowemail::update);
    edna.record_update(story_text::update);

    let mut story_text_count = 0;
    let res = db.query_iter(r"SELECT COUNT(*) FROM story_texts").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        story_text_count = helpers::mysql_val_to_u64(&vals[0]).unwrap();
    }
    assert_eq!(story_text_count, story_count);

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
    let elapsed = start.elapsed();
    warn!("Ran resub updates: {}", elapsed.as_micros());

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
    let mut story_text_count = 0;
    let res = db.query_iter(r"SELECT COUNT(*) FROM story_texts").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        story_text_count = helpers::mysql_val_to_u64(&vals[0]).unwrap();
    }
    assert_eq!(story_text_count, story_count);

    let filename = format!("../../results/lobsters_results/update_stats.csv",);
    // print out stats
    let mut f = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(&filename)
        .unwrap();

    writeln!(
        f,
        "{},{},{}",
        user_stories,
        user_comments,
        elapsed.as_micros().to_string()
    )
    .unwrap();
}
