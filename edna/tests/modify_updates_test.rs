extern crate log;
extern crate mysql;

use edna::helpers;
use edna::{RowVal, TableRow};
use log::warn;
use mysql::prelude::*;
use mysql::Opts;
use std::collections::HashSet;
use std::str::FromStr;
use std::*;

const SCHEMA: &'static str = include_str!("./schema.sql");
const PPGEN_JSON: &'static str = include_str!("./disguises/pp_gen.json");
const ANON_JSON: &'static str = include_str!("./disguises/universal_anon_disguise.json");
const GDPR_JSON: &'static str = include_str!("./disguises/gdpr_disguise.json");
const TABLEINFO_JSON: &'static str = include_str!("./disguises/table_info.json");

const USER_ITERS: u64 = 2;
const NSTORIES: u64 = 2;

fn init_logger() {
    let _ = env_logger::builder()
        // Include all events in tests
        .filter_level(log::LevelFilter::Debug)
        // Ensure events are captured by `cargo test`
        .is_test(true)
        // Ignore errors initializing the logger if tests race to configure it
        .try_init();
}

// pub type UpdateFn = Arc<Mutex<dyn Fn(Vec<TableRow>) -> Vec<TableRow> + Send + Sync>>;
fn update_strings(rows: Vec<TableRow>) -> Vec<TableRow> {
    let mut new_rows = vec![];
    for row in &rows {
        let mut new_row = vec![];
        for rv in &row.row {
            let v = rv.value();
            new_row.push(RowVal::new(rv.column(), v.replace("o", "i")));
        }
        new_rows.push(TableRow {
            table: row.table.to_string(),
            row: new_row,
        });
    }
    new_rows
}

fn apply_update_strings(db: &mut mysql::Conn) {
    let res = helpers::get_query_rows_str_q(r"SELECT * FROM users", db).unwrap();
    let mut table_rows = vec![];
    for row in res {
        table_rows.push(TableRow {
            table: "users".to_string(),
            row: row.clone(),
        });
    }

    let res = helpers::get_query_rows_str_q(r"SELECT * FROM stories", db).unwrap();
    for row in res {
        table_rows.push(TableRow {
            table: "stories".to_string(),
            row: row.clone(),
        });
    }

    let res = helpers::get_query_rows_str_q(r"SELECT * FROM moderations", db).unwrap();
    for row in res {
        table_rows.push(TableRow {
            table: "moderations".to_string(),
            row: row.clone(),
        });
    }

    let new_rows = update_strings(table_rows);

    db.query_drop("DELETE FROM moderations WHERE TRUE").unwrap();
    db.query_drop("DELETE FROM stories WHERE TRUE").unwrap();
    db.query_drop("DELETE FROM users WHERE TRUE").unwrap();

    // insert new rows into database
    for table_row in new_rows {
        helpers::query_drop(
            &format!(
                "INSERT INTO {} ({}) VALUES ({})",
                table_row.table,
                table_row
                    .row
                    .iter()
                    .map(|rv| rv.column())
                    .collect::<Vec<String>>()
                    .join(","),
                table_row
                    .row
                    .iter()
                    .map(|rv| {
                        let v = rv.value();
                        match u64::from_str(&v) {
                            Err(_) => format!("'{}'", v),
                            Ok(_) => v,
                        }
                    })
                    .collect::<Vec<String>>()
                    .join(",")
            ),
            db,
        )
        .unwrap();
    }
}

#[test]
fn test_modify_update_app_anon_gdpr_rev_gdpr_anon_disguises() {
    init_logger();
    let dbname = "testRevComposeModifyUpdate".to_string();
    helpers::init_db(true, "tester", "pass", "127.0.0.1", &dbname, SCHEMA);
    let mut edna =
        edna::EdnaClient::new("tester", "pass", "127.0.0.1", &dbname, true, false, false);
    let mut db = mysql::Conn::new(
        Opts::from_url(&format!("mysql://tester:pass@127.0.0.1/{}", dbname)).unwrap(),
    )
    .unwrap();
    assert_eq!(db.ping(), true);

    let mut user_shares = vec![];

    // INITIALIZATION
    for u in 1..USER_ITERS + 1 {
        // insert user into DB
        db.query_drop(format!(
            r"INSERT INTO users (id, username) VALUES ({}, 'hello{}');",
            u, u
        ))
        .unwrap();

        // insert a bunch of data for each user
        for s in 0..NSTORIES {
            let story_id = (u * NSTORIES) + s;
            db.query_drop(format!(
                r"INSERT INTO stories (id, user_id) VALUES ({}, {});",
                story_id, u
            ))
            .unwrap();
            db.query_drop(format!(r"INSERT INTO moderations (moderator_user_id, story_id, user_id, action) VALUES ({}, {}, {}, 'bad story!');", u, story_id, u)).unwrap();
        }

        // register user in Edna
        let user_share = edna.register_principal(&u.to_string(), String::from("password"));
        user_shares.push(user_share.clone());
    }

    // APPLY ANON DISGUISE
    let anon_did = edna
        .apply_disguise(
            "NULL".to_string(),
            ANON_JSON,
            TABLEINFO_JSON,
            PPGEN_JSON,
            None,
            None,
            false,
        )
        .unwrap();

    // APPLY GDPR DISGUISES
    let mut gdpr_dids = vec![];
    for u in 1..USER_ITERS + 1 {
        let did = edna
            .apply_disguise(
                u.to_string(),
                GDPR_JSON,
                TABLEINFO_JSON,
                PPGEN_JSON,
                None,
                Some(user_shares[u as usize - 1].clone()),
                false,
            )
            .unwrap();
        gdpr_dids.push(did);
    }

    // APPLY MODIFY UPDATE (note: currently not in a txn)
    apply_update_strings(&mut db);
    edna.record_update(update_strings);

    // check: nothing has an o
    {
        for u in 1..USER_ITERS + 1 {
            let res = db
                .query_iter(format!(
                    r"SELECT action FROM moderations WHERE moderator_user_id={} OR user_id={}",
                    u, u
                ))
                .unwrap();
            for row in res {
                let vals = row.unwrap().unwrap();
                assert_eq!(vals.len(), 1);
                let note = helpers::mysql_val_to_string(&vals[0]);
                assert!(!note.contains("o"), "{}", note);
            }
        }
    }

    // REVERSE ANON DISGUISE WITH NO DIFFS
    edna.reveal_disguise(
        String::from("NULL"),
        anon_did,
        TABLEINFO_JSON,
        PPGEN_JSON,
        Some(edna::RevealPPType::Restore),
        true, // allow partial row reveals
        None,
        None,
        false,
    )
    .unwrap();

    // CHECK DISGUISE RESULTS: nothing restored
    {
        // users removed
        for u in 1..USER_ITERS + 1 {
            let mut results = vec![];
            let res = db
                .query_iter(format!(
                    r"SELECT * FROM users WHERE users.username='helli{}'",
                    u
                ))
                .unwrap();
            for row in res {
                let vals = row.unwrap().unwrap();
                assert_eq!(vals.len(), 3);
                let id = helpers::mysql_val_to_string(&vals[0]);
                let username = helpers::mysql_val_to_string(&vals[1]);
                let karma = helpers::mysql_val_to_string(&vals[2]);
                results.push((id, username, karma));
            }
            assert_eq!(results.len(), 0);
        }
        // no correlated moderations
        for u in 1..USER_ITERS + 1 {
            let mut results = vec![];
            let res = db
                .query_iter(format!(
                    r"SELECT id FROM moderations WHERE moderator_user_id={} OR user_id={}",
                    u, u
                ))
                .unwrap();
            for row in res {
                let vals = row.unwrap().unwrap();
                assert_eq!(vals.len(), 1);
                let id = helpers::mysql_val_to_string(&vals[0]);
                results.push(id);
            }
            assert_eq!(results.len(), 0);
        }
        // stories removed
        let mut stories_results = vec![];
        let res = db
            .query_iter(format!(r"SELECT user_id FROM stories"))
            .unwrap();
        for _ in res {
            stories_results.push(1);
        }
        assert_eq!(stories_results.len() as u64, 0);

        // moderations have pseudoprincipals as owners
        let mut pseudoprincipals = HashSet::new();
        let res = db
            .query_iter(format!(
                r"SELECT moderator_user_id, user_id FROM moderations"
            ))
            .unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 2);
            let moderator_user_id = helpers::mysql_val_to_u64(&vals[0]).unwrap();
            let user_id = helpers::mysql_val_to_u64(&vals[1]).unwrap();
            assert!(pseudoprincipals.insert(user_id));
            assert!(pseudoprincipals.insert(moderator_user_id));
            assert!(user_id >= USER_ITERS + 1);
            assert!(moderator_user_id >= USER_ITERS + 1);
        }

        // check that all pseudoprincipals exist
        for u in pseudoprincipals {
            let res = db
                .query_iter(format!(r"SELECT * FROM users WHERE id={}", u))
                .unwrap();
            for row in res {
                let vals = row.unwrap().unwrap();
                assert_eq!(vals.len(), 3);
                let username = helpers::mysql_val_to_string(&vals[1]);
                assert_eq!(username.len(), 30);
            }
        }
    }
    warn!("Successfully reversed anon no diffs");

    // REVERSE GDPR DISGUISES
    for u in 1..USER_ITERS + 1 {
        edna.reveal_disguise(
            u.to_string(),
            gdpr_dids[u as usize - 1],
            TABLEINFO_JSON,
            PPGEN_JSON,
            Some(edna::RevealPPType::Restore),
            true, // allow partial row reveals
            None,
            Some(user_shares[u as usize - 1].clone()),
            false,
        )
        .unwrap();
        warn!("Reversed GDPR for {}", u);
    }

    // CHECK DISGUISE RESULTS: everything restored but still anon
    // users exist
    {
        for u in 1..USER_ITERS + 1 {
            let mut results = vec![];
            let res = db
                .query_iter(format!(r"SELECT id FROM stories WHERE user_id={}", u))
                .unwrap();
            for row in res {
                let vals = row.unwrap().unwrap();
                assert_eq!(vals.len(), 1);
                let id = helpers::mysql_val_to_string(&vals[0]);
                results.push(id);
            }
            assert_eq!(results.len(), 0);
        }

        // no correlated moderations
        for u in 1..USER_ITERS + 1 {
            let mut results = vec![];
            let res = db
                .query_iter(format!(
                    r"SELECT id FROM moderations WHERE moderator_user_id={} OR user_id={}",
                    u, u
                ))
                .unwrap();
            for row in res {
                let vals = row.unwrap().unwrap();
                assert_eq!(vals.len(), 1);
                let id = helpers::mysql_val_to_string(&vals[0]);
                results.push(id);
            }
            assert_eq!(results.len(), 0);
        }

        let mut pseudoprincipals = HashSet::new();

        // stories have pseudoprincipals as owners
        let mut stories_results = vec![];
        let res = db
            .query_iter(format!(r"SELECT user_id FROM stories"))
            .unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 1);
            let user_id = helpers::mysql_val_to_u64(&vals[0]).unwrap();
            assert!(pseudoprincipals.insert(user_id));
            assert!(user_id >= USER_ITERS + 1);
            stories_results.push(user_id);
        }
        assert_eq!(
            stories_results.len() as u64,
            (USER_ITERS + 1 - 1) * NSTORIES
        );

        // moderations have pseudoprincipals as owners
        let res = db
            .query_iter(format!(
                r"SELECT moderator_user_id, user_id FROM moderations"
            ))
            .unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 2);
            let moderator_user_id = helpers::mysql_val_to_u64(&vals[0]).unwrap();
            let user_id = helpers::mysql_val_to_u64(&vals[1]).unwrap();
            assert!(user_id >= USER_ITERS + 1);
            assert!(moderator_user_id >= USER_ITERS + 1);
        }

        // check that all pseudoprincipals exist
        for u in pseudoprincipals {
            let res = db
                .query_iter(format!(r"SELECT * FROM users WHERE id={}", u))
                .unwrap();
            for row in res {
                let vals = row.unwrap().unwrap();
                assert_eq!(vals.len(), 3);
                let username = helpers::mysql_val_to_string(&vals[1]);
                assert_eq!(username.len(), 30);
            }
        }
    }

    // REVERSE DISGUISE WITH USER DIFFS
    for u in 1..USER_ITERS + 1 {
        edna.reveal_disguise(
            u.to_string(),
            anon_did,
            TABLEINFO_JSON,
            PPGEN_JSON,
            Some(edna::RevealPPType::Restore),
            true, // allow partial row reveals
            None,
            Some(user_shares[u as usize - 1].clone()),
            false,
        )
        .unwrap();

        // CHECK DISGUISE RESULTS: stories have been restored too
        // stories recorrelated
        let mut results = vec![];
        let res = db
            .query_iter(format!(r"SELECT id FROM stories WHERE user_id={}", u))
            .unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 1);
            let id = helpers::mysql_val_to_string(&vals[0]);
            results.push(id);
        }
        assert_eq!(results.len(), NSTORIES as usize);

        // moderations recorrelated
        let mut results = vec![];
        let res = db
            .query_iter(format!(
                r"SELECT id FROM moderations WHERE moderator_user_id={} OR user_id={}",
                u, u
            ))
            .unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 1);
            let id = helpers::mysql_val_to_string(&vals[0]);
            results.push(id);
        }
        assert_eq!(results.len(), NSTORIES as usize);
    }

    // CHECK AFTER ALL USERS HAVE REVERSED
    // stories have no pseudoprincipals as owners
    let mut stories_results = vec![];
    let res = db
        .query_iter(format!(r"SELECT user_id FROM stories"))
        .unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 1);
        let user_id = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        assert!(user_id < USER_ITERS + 1);
        stories_results.push(user_id);
    }
    assert_eq!(
        stories_results.len() as u64,
        (USER_ITERS + 1 - 1) * NSTORIES
    );

    // moderations have no pseudoprincipals as owners
    let res = db
        .query_iter(format!(
            r"SELECT moderator_user_id, user_id FROM moderations"
        ))
        .unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 2);
        let moderator_user_id = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        let user_id = helpers::mysql_val_to_u64(&vals[1]).unwrap();
        assert!(user_id < USER_ITERS + 1);
        assert!(moderator_user_id < USER_ITERS + 1);
    }

    // pseudoprincipals are all gone
    let res = db
        .query_iter(format!(r"SELECT id, username FROM users"))
        .unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 2);
        let id = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        let username = helpers::mysql_val_to_string(&vals[1]);
        assert!(id < USER_ITERS + 1);
        assert!(!username.contains("o"), "{}", username);
    }

    drop(db);
}
