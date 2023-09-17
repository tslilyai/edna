extern crate log;
extern crate mysql;

use edna::helpers;
use log::warn;
use mysql::prelude::*;
use mysql::Opts;
use std::collections::HashSet;
use std::*;

const SCHEMA: &'static str = include_str!("./schema.sql");
const PPGEN_JSON: &'static str = include_str!("./disguises/pp_gen.json");
const ANON_JSON: &'static str = include_str!("./disguises/universal_anon_disguise.json");
const HOBBY_JSON: &'static str = include_str!("./disguises/hobby_decorrelation.json");
const ANON_GROUPED_JSON: &'static str =
    include_str!("./disguises/universal_anon_grouped_disguise.json");
const GDPR_JSON: &'static str = include_str!("./disguises/gdpr_disguise.json");
const TABLEINFO_JSON: &'static str = include_str!("./disguises/table_info.json");
const USER_ITERS: u64 = 3;
const NSTORIES: u64 = 4;

fn init_logger() {
    let _ = env_logger::builder()
        // Include all events in tests
        .filter_level(log::LevelFilter::Debug)
        // Ensure events are captured by `cargo test`
        .is_test(true)
        // Ignore errors initializing the logger if tests race to configure it
        .try_init();
}

#[test]
fn test_app_join_test() {
    init_logger();
    let dbname = "testAppJoin".to_string();
    helpers::init_db(true, "tester", "pass", "127.0.0.1", &dbname, SCHEMA);
    let mut edna =
        edna::EdnaClient::new("tester", "pass", "127.0.0.1", &dbname, true, false, false);

    let mut db = mysql::Conn::new(
        Opts::from_url(&format!("mysql://tester:pass@127.0.0.1/{}", dbname)).unwrap(),
    )
    .unwrap();
    assert_eq!(db.ping(), true);

    // INITIALIZATION
    // insert a bunch of tags
    db.query_drop(format!(
        r"INSERT INTO tags (id, tag) VALUES (1, 'star wars');"
    ))
    .unwrap();
    db.query_drop(format!(
        r"INSERT INTO tags (id, tag) VALUES (2, 'neutral');"
    ))
    .unwrap();
    for u in 1..USER_ITERS + 1 {
        // insert user into DB
        db.query_drop(format!(
            r"INSERT INTO users (id, username) VALUES ({}, 'hello{}');",
            u, u
        ))
        .unwrap();

        // insert a bunch of data for each user
        // make sure to add tags
        for s in 0..NSTORIES {
            let story_id = (u * NSTORIES) + s;
            db.query_drop(format!(
                r"INSERT INTO stories (id, user_id, url) VALUES ({}, {}, '{}.com');",
                story_id, u, u
            ))
            .unwrap();
            if s % 2 == 0 {
                warn!("Tagging story {} with tag star wars", story_id);
                db.query_drop(format!(
                    r"INSERT INTO taggings (tag_id, story_id) VALUES ({}, {});",
                    1, story_id
                ))
                .unwrap();
            } else {
                warn!("Tagging story {} with tag neutral", story_id);
                db.query_drop(format!(
                    r"INSERT INTO taggings (tag_id, story_id) VALUES ({}, {});",
                    2, story_id
                ))
                .unwrap();
            }
            db.query_drop(format!(r"INSERT INTO moderations (moderator_user_id, story_id, user_id, action) VALUES ({}, {}, {}, 'bad story!');", u, story_id, u)).unwrap();
        }

        // register user in Edna
        edna.register_principal(&u.to_string(), String::from("password"));
    }

    // APPLY DISGUISE FOR JOINED TAGS
    let u = 1;
    edna.apply_disguise(
        u.to_string(),
        HOBBY_JSON,
        TABLEINFO_JSON,
        PPGEN_JSON,
        None,
        None,
        false,
    )
    .unwrap();

    // CHECK DISGUISE RESULTS
    // no correlated stories with tag 'star wars'
    let mut results = vec![];
    let res = db
        .query_iter(format!(
            r"SELECT stories.id FROM stories 
                INNER JOIN taggings ON stories.id = taggings.story_id 
                INNER JOIN tags ON taggings.tag_id = tags.id 
                WHERE stories.user_id={} AND tags.tag = 'star wars'",
            u
        ))
        .unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 1);
        let id = helpers::mysql_val_to_string(&vals[0]);
        results.push(id);
    }
    assert_eq!(results.len(), 0);

    // no other user has decorrelated stories with tag star wars
    let mut results = vec![];
    let res = db
        .query_iter(format!(
            r"SELECT stories.id FROM stories 
                INNER JOIN taggings ON stories.id = taggings.story_id 
                INNER JOIN tags ON taggings.tag_id = tags.id 
                WHERE stories.user_id={} AND tags.tag = 'neutral'",
            u + 1
        ))
        .unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 1);
        let id = helpers::mysql_val_to_string(&vals[0]);
        results.push(id);
    }
    // note that this requires there to be >1 user in the test
    assert_eq!(results.len(), (NSTORIES / 2) as usize);

    // no correlated stories with tag 'star wars'
    let mut results = vec![];
    let res = db
        .query_iter(format!(
            r"SELECT stories.id FROM stories 
                INNER JOIN taggings ON stories.id = taggings.story_id 
                INNER JOIN tags ON taggings.tag_id = tags.id 
                WHERE stories.user_id={} AND tags.tag = 'neutral'",
            u
        ))
        .unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 1);
        let id = helpers::mysql_val_to_string(&vals[0]);
        results.push(id);
    }
    assert_eq!(results.len(), (NSTORIES / 2) as usize);
    drop(db);
}

#[test]
fn test_app_decor_grouped_disguise() {
    init_logger();
    let dbname = "testAppDecorGroupedDisguise".to_string();
    helpers::init_db(true, "tester", "pass", "127.0.0.1", &dbname, SCHEMA);
    let mut edna =
        edna::EdnaClient::new("tester", "pass", "127.0.0.1", &dbname, true, false, false);

    let mut db = mysql::Conn::new(
        Opts::from_url(&format!("mysql://tester:pass@127.0.0.1/{}", dbname)).unwrap(),
    )
    .unwrap();
    assert_eq!(db.ping(), true);

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
                r"INSERT INTO stories (id, user_id, url) VALUES ({}, {}, '{}.com');",
                story_id, u, u
            ))
            .unwrap();
            db.query_drop(format!(r"INSERT INTO moderations (moderator_user_id, story_id, user_id, action) VALUES ({}, {}, {}, 'bad story!');", u, s*u + s, u)).unwrap();
        }

        // register user in Edna
        edna.register_principal(&u.to_string(), String::from("password"));
    }

    // APPLY GROUPED DECOR DISGUISE
    edna.apply_disguise(
        "NULL".to_string(),
        ANON_GROUPED_JSON,
        TABLEINFO_JSON,
        PPGEN_JSON,
        None,
        None,
        false,
    )
    .unwrap();

    // CHECK DISGUISE RESULTS
    // no correlated stories
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

    // stories have pseudoprincipals as owners
    let res = db
        .query_iter(format!(r"SELECT id, user_id, url FROM stories"))
        .unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 3);
        let id = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        let userid = helpers::mysql_val_to_u64(&vals[1]).unwrap();
        let url = helpers::mysql_val_to_string(&vals[2]);
        warn!("userid for id {} url {} is {}", id, url, userid);
        assert!(userid >= USER_ITERS);
    }

    // stories with the SAME url have the same owner
    let res = db
        .query_iter(format!(r"SELECT COUNT(id) FROM stories GROUP BY user_id"))
        .unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 1);
        let nstories = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        assert_eq!(nstories, NSTORIES);
    }
    drop(db);
}
#[test]
fn test_app_anon_disguise() {
    init_logger();
    let dbname = "testAppAnonDisguise".to_string();
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
    edna.apply_disguise(
        "NULL".to_string(),
        ANON_JSON,
        TABLEINFO_JSON,
        PPGEN_JSON,
        None,
        None,
        false,
    )
    .unwrap();

    // CHECK DISGUISE RESULTS
    // users exist
    for u in 1..USER_ITERS + 1 {
        let mut results = vec![];
        let res = db
            .query_iter(format!(
                r"SELECT * FROM users WHERE users.username='hello{}'",
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
        assert_eq!(results.len(), 1);
    }

    // no correlated stories
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
        assert!(guises.insert(user_id));
        assert!(user_id >= USER_ITERS);
        stories_results.push(user_id);
    }
    assert_eq!(stories_results.len() as u64, USER_ITERS * NSTORIES);

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
        assert!(guises.insert(user_id));
        assert!(guises.insert(moderator_user_id));
        assert!(user_id >= USER_ITERS);
        assert!(moderator_user_id >= USER_ITERS);
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
    drop(db);
}

#[test]
fn test_app_gdpr_disguise() {
    init_logger();
    let dbname = "testAppGDPR".to_string();
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

    // APPLY GDPR DISGUISES
    for u in 1..USER_ITERS + 1 {
        edna.apply_disguise(
            u.to_string(),
            GDPR_JSON,
            TABLEINFO_JSON,
            PPGEN_JSON,
            None,
            None,
            false,
        )
        .unwrap();
    }

    // CHECK DISGUISE RESULTS
    // users removed
    for u in 1..USER_ITERS + 1 {
        let mut results = vec![];
        let res = db
            .query_iter(format!(
                r"SELECT * FROM users WHERE users.username='hello{}'",
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
        assert!(guises.insert(user_id));
        assert!(guises.insert(moderator_user_id));
        assert!(user_id >= USER_ITERS);
        assert!(moderator_user_id >= USER_ITERS);
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
    drop(db);
}

#[test]
fn test_compose_anon_gdpr_disguises() {
    init_logger();
    let dbname = "testAppComposeDisguise".to_string();
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
    let _anon_did = edna
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
    for u in 1..USER_ITERS + 1 {
        edna.apply_disguise(
            u.to_string(),
            GDPR_JSON,
            TABLEINFO_JSON,
            PPGEN_JSON,
            None,
            Some(user_shares[u as usize - 1].clone()),
            false,
        )
        .unwrap();
    }

    // CHECK DISGUISE RESULTS
    // users removed
    for u in 1..USER_ITERS + 1 {
        let mut results = vec![];
        let res = db
            .query_iter(format!(
                r"SELECT * FROM users WHERE users.username='hello{}'",
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
        assert!(guises.insert(user_id));
        assert!(user_id >= USER_ITERS);
        stories_results.push(user_id);
    }
    assert_eq!(stories_results.len(), 0);

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
        assert!(guises.insert(user_id));
        assert!(guises.insert(moderator_user_id));
        assert!(user_id >= USER_ITERS);
        assert!(moderator_user_id >= USER_ITERS);
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
    drop(db);
}
