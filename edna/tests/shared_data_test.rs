extern crate log;
extern crate mysql;

use edna::helpers;
use log::warn;
use mysql::prelude::*;
use mysql::Opts;
use std::*;

const SCHEMA: &'static str = include_str!("./schema.sql");
const GUISEGEN_JSON: &'static str = include_str!("./disguises/guise_gen.json");
const ANON_JSON: &'static str = include_str!("./disguises/universal_anon_disguise.json");
const REMOVE_ALL_JSON: &'static str = include_str!("./disguises/universal_remove_disguise.json");
const GDPR_REMOVE_JSON: &'static str = include_str!("./disguises/gdpr_disguise_remove.json");
const TABLEINFO_JSON: &'static str = include_str!("./disguises/table_info.json");
const USER_ITERS: u64 = 3;
const NSTORIES: u64 = 2;
const ADMIN: u64 = 100;

fn init_logger() {
    let _ = env_logger::builder()
        // Include all events in tests
        .filter_level(log::LevelFilter::Debug)
        // Ensure events are captured by `cargo test`
        .is_test(true)
        // Ignore errors initializing the logger if tests race to configure it
        .try_init();
}

// test remove of shared data after anon for pps
#[test]
fn test_remove_shared_after_anon() {
    init_logger();
    let dbname = "testRemoveSharedAnon".to_string();
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
    db.query_drop(format!(
        r"INSERT INTO users (id, username) VALUES ({}, 'hello{}');",
        ADMIN, ADMIN
    ))
    .unwrap();
    // register user in Edna
    let admin_user_share = edna.register_principal(&ADMIN.to_string(), String::from("password"));
    for u in 1..USER_ITERS + 1 {
        // insert user into DB
        db.query_drop(format!(
            r"INSERT INTO users (id, username) VALUES ({}, 'hello{}');",
            u, u
        ))
        .unwrap();

        // insert a bunch of data for each user
        for s in 0..NSTORIES {
            db.query_drop(format!(
                r"INSERT INTO stories (id, user_id) VALUES ({}, {});",
                u * NSTORIES + s,
                u
            ))
            .unwrap();
            db.query_drop(format!(r"INSERT INTO moderations (moderator_user_id, story_id, user_id, action) VALUES ({}, {}, {}, 'bad story!');", ADMIN, s*u + s, u)).unwrap();
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
            GUISEGEN_JSON,
            None,
            None,
            false,
        )
        .unwrap();

    // CHECK DISGUISE RESULTS
    // moderations are decorrelated from all users
    let mut results = vec![];
    let res = db
        .query_iter(format!(
            r"SELECT moderator_user_id, user_id FROM moderations"
        ))
        .unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        let mod_id = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        let id = helpers::mysql_val_to_u64(&vals[1]).unwrap();
        assert!(id >= USER_ITERS + 1);
        assert!(mod_id >= USER_ITERS && mod_id != ADMIN);
        results.push(id);
    }
    assert_eq!(results.len(), USER_ITERS as usize * NSTORIES as usize);

    // APPLY ADMIN DISGUISE
    let admin_did = edna
        .apply_disguise(
            ADMIN.to_string(),
            GDPR_REMOVE_JSON,
            TABLEINFO_JSON,
            GUISEGEN_JSON,
            None,
            None,
            false,
        )
        .unwrap();

    // admin doesn't exist
    let mut results = vec![];
    let res = db
        .query_iter(format!(r"SELECT id FROM users WHERE id = {}", ADMIN))
        .unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        let id = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        results.push(id);
    }
    assert_eq!(results.len(), 0);

    // admin restore with both locators
    edna.reveal_disguise(
        ADMIN.to_string(),
        admin_did,
        TABLEINFO_JSON,
        GUISEGEN_JSON,
        None,
        Some(admin_user_share.clone()),
        false,
    )
    .unwrap();
    edna.reveal_disguise(
        ADMIN.to_string(),
        anon_did,
        TABLEINFO_JSON,
        GUISEGEN_JSON,
        None,
        Some(admin_user_share.clone()),
        false,
    )
    .unwrap();
    let mut results = vec![];
    let res = db
        .query_iter(format!(
            r"SELECT moderator_user_id, user_id FROM moderations"
        ))
        .unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 2);
        let mod_id = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        let id = helpers::mysql_val_to_u64(&vals[1]).unwrap();
        assert_eq!(mod_id, ADMIN);
        assert!(id >= USER_ITERS + 1);
        results.push(id);
    }
    assert_eq!(results.len(), USER_ITERS as usize * NSTORIES as usize);
    drop(db);
}

#[test]
fn test_remove_one_shared() {
    init_logger();
    let dbname = "testRemoveOneSharedDisguise".to_string();
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
    db.query_drop(format!(
        r"INSERT INTO users (id, username) VALUES ({}, 'hello{}');",
        ADMIN, ADMIN
    ))
    .unwrap();
    // register user in Edna
    let admin_user_share = edna.register_principal(&ADMIN.to_string(), String::from("password"));
    for u in 1..USER_ITERS + 1 {
        // insert user into DB
        db.query_drop(format!(
            r"INSERT INTO users (id, username) VALUES ({}, 'hello{}');",
            u, u
        ))
        .unwrap();

        // insert a bunch of data for each user
        for s in 0..NSTORIES {
            db.query_drop(format!(
                r"INSERT INTO stories (id, user_id) VALUES ({}, {});",
                u * NSTORIES + s,
                u
            ))
            .unwrap();
            db.query_drop(format!(r"INSERT INTO moderations (moderator_user_id, story_id, user_id, action) VALUES ({}, {}, {}, 'bad story!');", ADMIN, NSTORIES*u + s, u)).unwrap();
        }

        // register user in Edna
        let user_share = edna.register_principal(&u.to_string(), String::from("password"));
        user_shares.push(user_share.clone());
    }

    // APPLY ADMIN DISGUISE
    let admin_did = edna
        .apply_disguise(
            ADMIN.to_string(),
            GDPR_REMOVE_JSON,
            TABLEINFO_JSON,
            GUISEGEN_JSON,
            None,
            None,
            false,
        )
        .unwrap();

    // CHECK DISGUISE RESULTS
    // moderations are decorrelated from admin
    let mut results = vec![];
    let res = db
        .query_iter(format!(
            r"SELECT moderator_user_id, user_id FROM moderations"
        ))
        .unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        let mod_id = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        let id = helpers::mysql_val_to_u64(&vals[1]).unwrap();
        assert!(id < USER_ITERS + 1);
        assert!(mod_id >= USER_ITERS + 1);
        results.push(id);
    }
    assert_eq!(results.len(), USER_ITERS as usize * NSTORIES as usize);

    // APPLY USER 1 DISGUISE
    let user_did = edna
        .apply_disguise(
            1.to_string(),
            GDPR_REMOVE_JSON,
            TABLEINFO_JSON,
            GUISEGEN_JSON,
            None,
            None,
            false,
        )
        .unwrap();

    // CHECK DISGUISE RESULTS
    // moderations are removed
    let mut results = vec![];
    let res = db
        .query_iter(format!(
            r"SELECT moderator_user_id, user_id FROM moderations WHERE user_id = {}",
            1
        ))
        .unwrap();
    for _row in res {
        results.push(1);
    }
    assert_eq!(results.len(), 0);

    // all users are pseudoprincipals
    let res = db
        .query_iter(format!(r"SELECT id FROM users WHERE id = {}", 1))
        .unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        let id = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        warn!("Got user ID {}", id);
        assert!(false);
    }

    // we clear all records even if disguises fails, so if admin fails here, they won't be able to
    // restore their ownership later on
    /*// admin restore
    edna.reveal_disguise(
        ADMIN.to_string(),
        admin_did,
        TABLEINFO_JSON,
        GUISEGEN_JSON,
        None,
        Some(admin_user_share.clone()),
        false,
    )
    .unwrap();

    let mut results = vec![];
    let res = db
        .query_iter(format!(
            r"SELECT moderator_user_id, user_id FROM moderations"
        ))
        .unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 2);
        let mod_id = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        let id = helpers::mysql_val_to_u64(&vals[1]).unwrap();
        assert_eq!(mod_id, ADMIN);
        warn!("Got user ID {}", id);
        assert!(id != 1);
        results.push(id);
    }
    // cannot restore entry yet because the story doesn't exist...
    assert_eq!(results.len(), (USER_ITERS as usize - 1) * NSTORIES as usize);*/

    // users restore
    edna.reveal_disguise(
        1.to_string(),
        user_did,
        TABLEINFO_JSON,
        GUISEGEN_JSON,
        None,
        Some(user_shares[0].clone()),
        false,
    )
    .unwrap();
    let mut results = vec![];
    let res = db
        .query_iter(format!(
            r"SELECT moderator_user_id, user_id FROM moderations WHERE user_id = {}",
            1
        ))
        .unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 2);
        let mod_id = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        let id = helpers::mysql_val_to_u64(&vals[1]).unwrap();
        // NOTE: the admin can't have recorrelated w/the moderation bc the story didn't exist yet
        assert!(mod_id > ADMIN);
        assert!(id == 1);
        results.push(id.to_string());
    }
    assert_eq!(results.len(), NSTORIES as usize);
    let mut results = vec![];
    let res = db
        .query_iter(format!(
            r"SELECT moderator_user_id, user_id FROM moderations"
        ))
        .unwrap();
    for _row in res {
        results.push(1);
    }
    assert_eq!(results.len(), USER_ITERS as usize * NSTORIES as usize);

    // admin restore again
    edna.reveal_disguise(
        ADMIN.to_string(),
        admin_did,
        TABLEINFO_JSON,
        GUISEGEN_JSON,
        None,
        Some(admin_user_share.clone()),
        false,
    )
    .unwrap();

    let mut results = vec![];
    let res = db
        .query_iter(format!(
            r"SELECT moderator_user_id, user_id FROM moderations"
        ))
        .unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 2);
        let mod_id = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        let id = helpers::mysql_val_to_u64(&vals[1]).unwrap();
        assert_eq!(mod_id, ADMIN);
        warn!("Got user ID {}", id);
        assert!(id < ADMIN);
        results.push(id);
    }
    assert_eq!(results.len(), USER_ITERS as usize * NSTORIES as usize);

    // all pseudoprincipals for user 1 removed
    let mut results = vec![];
    let res = db.query_iter(format!(r"SELECT id FROM users")).unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        let id = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        if id > ADMIN {
            results.push(id);
        }
    }
    assert_eq!(results.len(), (USER_ITERS as usize - 1) * NSTORIES as usize);

    // TRY AGAIN IN OPPOSITE REVEAL ORDER

    // APPLY ADMIN DISGUISE
    let admin_did = edna
        .apply_disguise(
            ADMIN.to_string(),
            GDPR_REMOVE_JSON,
            TABLEINFO_JSON,
            GUISEGEN_JSON,
            None,
            None,
            false,
        )
        .unwrap();

    // CHECK DISGUISE RESULTS
    // moderations are decorrelated from admin
    let mut results = vec![];
    let res = db
        .query_iter(format!(
            r"SELECT moderator_user_id, user_id FROM moderations"
        ))
        .unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        let mod_id = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        let id = helpers::mysql_val_to_u64(&vals[1]).unwrap();
        assert!(id < USER_ITERS + 1);
        assert!(mod_id >= USER_ITERS + 1);
        results.push(id);
    }
    assert_eq!(results.len(), USER_ITERS as usize * NSTORIES as usize);

    // APPLY USER 1 DISGUISE
    let user_did = edna
        .apply_disguise(
            1.to_string(),
            GDPR_REMOVE_JSON,
            TABLEINFO_JSON,
            GUISEGEN_JSON,
            None,
            None,
            false,
        )
        .unwrap();

    // CHECK DISGUISE RESULTS
    // moderations are removed
    let mut results = vec![];
    let res = db
        .query_iter(format!(
            r"SELECT moderator_user_id, user_id FROM moderations WHERE user_id = {}",
            1
        ))
        .unwrap();
    for _row in res {
        results.push(1);
    }
    assert_eq!(results.len(), 0);

    // user 1 restore
    edna.reveal_disguise(
        1.to_string(),
        user_did,
        TABLEINFO_JSON,
        GUISEGEN_JSON,
        None,
        Some(user_shares[0].clone()),
        false,
    )
    .unwrap();
    let mut results = vec![];
    let res = db
        .query_iter(format!(
            r"SELECT moderator_user_id, user_id FROM moderations WHERE user_id = {}",
            1
        ))
        .unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 2);
        let mod_id = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        let id = helpers::mysql_val_to_u64(&vals[1]).unwrap();
        assert!(mod_id != ADMIN);
        assert!(id == 1);
        results.push(id);
    }
    assert_eq!(results.len(), NSTORIES as usize);

    // admin restore
    edna.reveal_disguise(
        ADMIN.to_string(),
        admin_did,
        TABLEINFO_JSON,
        GUISEGEN_JSON,
        None,
        Some(admin_user_share),
        false,
    )
    .unwrap();
    let mut results = vec![];
    let res = db
        .query_iter(format!(
            r"SELECT moderator_user_id, user_id FROM moderations"
        ))
        .unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 2);
        let mod_id = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        let id = helpers::mysql_val_to_u64(&vals[1]).unwrap();
        assert_eq!(mod_id, ADMIN);
        assert!(id == 1 || id <= USER_ITERS + 1);
        results.push(id);
    }
    assert_eq!(results.len(), USER_ITERS as usize * NSTORIES as usize);

    // all pseudoprincipals removed
    let mut results = vec![];
    let res = db.query_iter(format!(r"SELECT id FROM users")).unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        let id = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        if id > ADMIN {
            results.push(id);
        }
    }
    assert_eq!(results.len(), (USER_ITERS as usize - 1) * NSTORIES as usize);

    drop(db);
}

#[test]
fn test_remove_all_shared() {
    init_logger();
    let dbname = "testRemoveAllSharedDisguise".to_string();
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
    db.query_drop(format!(
        r"INSERT INTO users (id, username) VALUES ({}, 'hello{}');",
        ADMIN, ADMIN
    ))
    .unwrap();
    // register user in Edna
    let admin_user_share = edna.register_principal(&ADMIN.to_string(), String::from("password"));
    for u in 1..USER_ITERS + 1 {
        // insert user into DB
        db.query_drop(format!(
            r"INSERT INTO users (id, username) VALUES ({}, 'hello{}');",
            u, u
        ))
        .unwrap();

        // insert a bunch of data for each user
        for s in 0..NSTORIES {
            db.query_drop(format!(
                r"INSERT INTO stories (id, user_id) VALUES ({}, {});",
                u * NSTORIES + s,
                u
            ))
            .unwrap();
            db.query_drop(format!(r"INSERT INTO moderations (moderator_user_id, story_id, user_id, action) VALUES ({}, {}, {}, 'bad story!');", ADMIN, NSTORIES*u + s, u)).unwrap();
        }

        // register user in Edna
        let user_share = edna.register_principal(&u.to_string(), String::from("password"));
        user_shares.push(user_share.clone());
    }

    // APPLY REMOVE ALL DISGUISE
    let did = edna
        .apply_disguise(
            "NULL".to_string(),
            REMOVE_ALL_JSON,
            TABLEINFO_JSON,
            GUISEGEN_JSON,
            None,
            None,
            false,
        )
        .unwrap();

    // CHECK DISGUISE RESULTS
    // moderations are all gone
    let mut results = vec![];
    let res = db
        .query_iter(format!(r"SELECT id FROM moderations"))
        .unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 1);
        let id = helpers::mysql_val_to_string(&vals[0]);
        results.push(id);
    }
    assert_eq!(results.len(), 0);

    // admin restore
    edna.reveal_disguise(
        ADMIN.to_string(),
        did,
        TABLEINFO_JSON,
        GUISEGEN_JSON,
        None,
        Some(admin_user_share),
        false,
    )
    .unwrap();
    let mut results = vec![];
    let res = db
        .query_iter(format!(
            r"SELECT moderator_user_id, user_id FROM moderations"
        ))
        .unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 2);
        let mod_id = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        let id = helpers::mysql_val_to_u64(&vals[1]).unwrap();
        assert_eq!(mod_id, ADMIN);
        warn!("Got user ID {}", id);
        assert!(id >= USER_ITERS + 1);
        results.push(id.to_string());
    }
    assert_eq!(results.len(), USER_ITERS as usize * NSTORIES as usize);

    // all users restore
    for u in 1..USER_ITERS + 1 {
        edna.reveal_disguise(
            u.to_string(),
            did,
            TABLEINFO_JSON,
            GUISEGEN_JSON,
            None,
            Some(user_shares[u as usize - 1].clone()),
            false,
        )
        .unwrap();
        let mut results = vec![];
        let res = db
            .query_iter(format!(
                r"SELECT moderator_user_id, user_id FROM moderations WHERE user_id = {}",
                u
            ))
            .unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 2);
            let mod_id = helpers::mysql_val_to_u64(&vals[0]).unwrap();
            let id = helpers::mysql_val_to_u64(&vals[1]).unwrap();
            assert_eq!(mod_id, ADMIN);
            assert!(id == u);
            results.push(id.to_string());
        }
        assert_eq!(results.len(), NSTORIES as usize);
    }
    drop(db);
}
