extern crate log;
extern crate mysql;

use edna_cryptdb::helpers;
use mysql::prelude::*;
use mysql::Opts;
use std::*;

const SCHEMA: &'static str = include_str!("./schema.sql");
const DBNAME: &'static str = "test_normal";

fn init_logger() {
    let _ = env_logger::builder()
        // Include all events in tests
        .filter_level(log::LevelFilter::Warn)
        // Ensure events are captured by `cargo test`
        .is_test(true)
        // Ignore errors initializing the logger if tests race to configure it
        .try_init();
}

#[test]
fn test_normal_execution() {
    init_logger();

    // init schema, etc.
    helpers::init_db(true, "tester", "pass", "127.0.0.1", DBNAME, SCHEMA);
    edna_cryptdb::EdnaClient::new(&format!(
        "mysql://tester:pass@127.0.0.1/{}",
        DBNAME), true, false
    );
    let mut db = mysql::Conn::new(
        Opts::from_url(&format!("mysql://tester:pass@127.0.0.1/{}", DBNAME)).unwrap(),
    )
    .unwrap();
    assert_eq!(db.ping(), true);

    /*
     * NOTE: the column types are all right, but the mysql value returned is always Bytes,
     * so it always parses as a String
     */

    /*
     * TEST 1: all tables successfully created
     */
    let mut results = vec![];
    let res = db.query_iter("SHOW tables;").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        let name = helpers::mysql_val_to_string(&vals[0]);
        results.push(name);
    }
    let tables = vec![
        "stories",
        "users",
        "moderations",
        "tags",
        "taggings",
        "EdnaPrincipals",
        "EdnaBags",
        "EdnaRemovedSharedObjects",
        "EdnaShares",
        "EdnaEncLocs",
    ];
    assert_eq!(results.len(), tables.len());
    for tab in results {
        assert!(tables.iter().any(|tt| &tab == *tt));
    }

    /*
     * TEST 2: insert users works properly
     */
    let mut results = vec![];
    db.query_drop(r"INSERT INTO users (username) VALUES ('hello_1'), ('hello_2');")
        .unwrap();
    let res = db.query_iter(r"SELECT * FROM users WHERE users.username='hello_1' OR users.username='hello_2' ORDER BY users.id;").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 3);
        let id = helpers::mysql_val_to_string(&vals[0]);
        let username = helpers::mysql_val_to_string(&vals[1]);
        let karma = helpers::mysql_val_to_string(&vals[2]);
        results.push((id, username, karma));
    }
    assert_eq!(results.len(), 2);
    assert_eq!(
        results[0],
        ("1".to_string(), "hello_1".to_string(), "0".to_string())
    );
    assert_eq!(
        results[1],
        ("2".to_string(), "hello_2".to_string(), "0".to_string())
    );

    /*
     * TEST 3: insert into datatables works properly
     */
    let mut results = vec![];
    db.query_drop(r"INSERT INTO moderations (moderator_user_id, story_id, user_id, action) VALUES (1, 0, 2, 'bad story!');").unwrap();
    let res = db
        .query_iter(r"SELECT * FROM moderations ORDER BY moderations.id;")
        .unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 5);
        let id = helpers::mysql_val_to_string(&vals[0]);
        let mod_id = helpers::mysql_val_to_string(&vals[1]);
        let story_id = helpers::mysql_val_to_string(&vals[2]);
        let user_id = helpers::mysql_val_to_string(&vals[3]);
        let action = helpers::mysql_val_to_string(&vals[4]);
        results.push((id, mod_id, story_id, user_id, action));
    }
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0],
        (
            "1".to_string(),
            "1".to_string(),
            "0".to_string(),
            "2".to_string(),
            "bad story!".to_string()
        )
    );

    /*
     * TEST 4: complex insert into datatables works properly
     */
    let mut results = vec![];
    db.query_drop(r"INSERT INTO moderations (moderator_user_id, story_id, user_id, action) VALUES (2, 0, 1, 'worst story!');").unwrap();
    //((SELECT id FROM users WHERE username='hello_2'), '0', '1', 'worst story!');").unwrap();
    let res = db
        .query_iter(r"SELECT * FROM moderations ORDER BY moderations.id;")
        .unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 5);
        let id = helpers::mysql_val_to_string(&vals[0]);
        let mod_id = helpers::mysql_val_to_string(&vals[1]);
        let story_id = helpers::mysql_val_to_string(&vals[2]);
        let user_id = helpers::mysql_val_to_string(&vals[3]);
        let action = helpers::mysql_val_to_string(&vals[4]);
        results.push((id, mod_id, story_id, user_id, action));
    }
    assert_eq!(results.len(), 2);
    assert_eq!(
        results[0],
        (
            "1".to_string(),
            "1".to_string(),
            "0".to_string(),
            "2".to_string(),
            "bad story!".to_string()
        )
    );
    assert_eq!(
        results[1],
        (
            "2".to_string(),
            "2".to_string(),
            "0".to_string(),
            "1".to_string(),
            "worst story!".to_string()
        )
    );

    /*
     * TEST 5: complex joins
     */
    let mut results = vec![];
    let res = db
        .query_iter(
            r"SELECT moderations.moderator_user_id, users.username 
                            FROM users JOIN moderations ON users.id = moderations.user_id 
                            ORDER BY moderations.user_id ASC 
                            LIMIT 2;",
        )
        .unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 2);
        let mod_id = helpers::mysql_val_to_string(&vals[0]);
        let username = helpers::mysql_val_to_string(&vals[1]);
        results.push((mod_id, username));
    }
    assert_eq!(results.len(), 2);
    assert_eq!(results[0], ("2".to_string(), "hello_1".to_string()));
    assert_eq!(results[1], ("1".to_string(), "hello_2".to_string()));

    /*
     * TEST 6: update correctly changes values to point to new UIDs (correctly handling
     * deletion upon updates to NULL)
     */
    let mut results = vec![];
    db.query_drop(r"UPDATE moderations SET user_id = NULL, story_id = 1, moderator_user_id = 3 WHERE moderations.user_id=1;").unwrap();
    let res = db
        .query_iter(r"SELECT * FROM moderations WHERE moderations.moderator_user_id =3;")
        .unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 5);
        let id = helpers::mysql_val_to_string(&vals[0]);
        let mod_id = helpers::mysql_val_to_string(&vals[1]);
        let story_id = helpers::mysql_val_to_string(&vals[2]);
        let user_id = helpers::mysql_val_to_string(&vals[3]);
        let action = helpers::mysql_val_to_string(&vals[4]);
        results.push((id, mod_id, story_id, user_id, action));
    }
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0],
        (
            "2".to_string(),
            "3".to_string(),
            "1".to_string(),
            "NULL".to_string(),
            "worst story!".to_string()
        )
    );

    /*
     * TEST 7: deletions correctly remove
     */
    let mut results = vec![];
    db.query_drop(r"DELETE FROM moderations WHERE moderator_user_id = 1")
        .unwrap();
    //(SELECT id FROM users WHERE username='hello_1');").unwrap();
    let res = db.query_iter(r"SELECT * FROM moderations;").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 5);
        let id = helpers::mysql_val_to_string(&vals[0]);
        let mod_id = helpers::mysql_val_to_string(&vals[1]);
        let story_id = helpers::mysql_val_to_string(&vals[2]);
        let user_id = helpers::mysql_val_to_string(&vals[3]);
        let action = helpers::mysql_val_to_string(&vals[4]);
        results.push((id, mod_id, story_id, user_id, action));
    }
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0],
        (
            "2".to_string(),
            "3".to_string(),
            "1".to_string(),
            "NULL".to_string(),
            "worst story!".to_string()
        )
    );

    drop(db);
}
