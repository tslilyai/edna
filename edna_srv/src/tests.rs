use super::rocket;
use crate::apiproxy::*;
use log::warn;
use mysql::prelude::*;
use mysql::{Opts, Value};
use rocket::http::ContentType;
use rocket::local::asynchronous::Client;
use serde_json::json;

const LOBSTERS_TABLEINFO_JSON: &'static str =
    include_str!("../../applications/lobsters/src/disguises/table_info.json");
const LOBSTERS_PPGEN_JSON: &'static str =
    include_str!("../../applications/lobsters/src/disguises/pp_gen.json");
const LOBSTERS_DECAY_JSON: &'static str =
    include_str!("../../applications/lobsters/src/disguises/data_decay.json");
const LOBSTERS_GDPR_JSON: &'static str =
    include_str!("../../applications/lobsters/src/disguises/gdpr_disguise.json");

const HOTCRP_TABLEINFO_JSON: &'static str =
    include_str!("../../applications/hotcrp/src/disguises/table_info.json");
//const HOTCRP_ANON_JSON: &'static str =
//    include_str!("../../applications/hotcrp/src/disguises/universal_anon_disguise.json");
const HOTCRP_GDPR_JSON: &'static str =
    include_str!("../../applications/hotcrp/src/disguises/gdpr_disguise.json");
const HOTCRP_PPGEN_JSON: &'static str =
    include_str!("../../applications/hotcrp/src/disguises/pp_gen.json");

pub async fn test_lobsters_disguise() {
    let client = Client::tracked(rocket(
        true,
        "tslilyai",
        "pass",
        "127.0.0.1",
        "testdb",
        "../applications/lobsters/schema.sql",
        true,
    ))
    .await
    .unwrap();
    let mut db = mysql::Conn::new(
        Opts::from_url(&format!(
            "mysql://tslilyai:pass@{}/{}",
            "127.0.0.1", "testdb"
        ))
        .unwrap(),
    )
    .unwrap();
    assert_eq!(db.ping(), true);

    let nusers: u64 = 5;
    let nstories: u64 = 5;
    let ncomments: u64 = 5;
    let mut short_id = 1;

    // create all users
    for u in 1..nusers {
        let postdata = json!({
            "uid": u.to_string(),
            "pw": u.to_string(),
        });
        let response = client
            .post("/register_principal")
            .body(postdata.to_string())
            .header(ContentType::JSON)
            .dispatch()
            .await;
        let strbody = response.into_string().await.unwrap();
        let _body: RegisterPrincipalResponse = serde_json::from_str(&strbody).unwrap();
        db.query_drop(format!("INSERT INTO users (id, username, last_login) VALUES ({}, '{}name', '2019-08-15 00:00:00.000');", u,u)).unwrap();

        // create some number of comments and stories for each user
        for s in 0..nstories {
            short_id = short_id + 1;
            db.query_drop(format!("INSERT INTO stories (short_id, user_id, url, title, created_at) VALUES ({}, {}, '{}url', '{}title', '2019-08-15 00:00:00.000');", 
                        short_id, u, s, s)).unwrap();
            for _ in 0..ncomments {
                short_id = short_id + 1;
                db.query_drop(format!("INSERT INTO comments (short_id, story_id, user_id, comment, created_at) VALUES ({}, {}, {}, '{}comment', '2019-08-15 00:00:00.000');", 
                        short_id, s, u, short_id)).unwrap();
            }
        }
    }
    warn!("Created {} users", nusers);

    /***********************************
     * gdpr deletion (no composition)
     ***********************************/
    let mut dids = vec![];
    for u in 1..nusers {
        let postdata = json!({
            "disguise_json": LOBSTERS_GDPR_JSON,
            "tableinfo_json": LOBSTERS_TABLEINFO_JSON,
            "ppgen_json": LOBSTERS_PPGEN_JSON,
            "password": u.to_string(),
            "user": u.to_string(),
        });

        let endpoint = format!("/apply_disguise");
        let response = client
            .post(&endpoint)
            .body(postdata.to_string())
            .header(ContentType::JSON)
            .dispatch()
            .await;
        let strbody = response.into_string().await.unwrap();
        warn!("Delete strbody response: {}", strbody);
        let body: ApplyDisguiseResponse = serde_json::from_str(&strbody).unwrap();
        warn!("Deleted account of {}", u);
        dids.push(body.did);
    }
    // check results of delete: user has no comments
    for u in 1..nusers {
        let res = db
            .query_iter(format!("SELECT * FROM comments WHERE user_id = {};", u))
            .unwrap();
        let mut rows = vec![];
        for row in res {
            let rowvals = row.unwrap().unwrap();
            let vals: Vec<Value> = rowvals.iter().map(|v| v.clone().into()).collect();
            rows.push(vals);
        }
        assert_eq!(rows.len(), 0);
        let res = db
            .query_iter(format!("SELECT * FROM stories WHERE user_id = {};", u))
            .unwrap();
        let mut rows = vec![];
        for row in res {
            let rowvals = row.unwrap().unwrap();
            let vals: Vec<Value> = rowvals.iter().map(|v| v.clone().into()).collect();
            rows.push(vals);
        }
        assert_eq!(rows.len(), 0);
        let res = db
            .query_iter(format!("SELECT * FROM users WHERE id = {};", u))
            .unwrap();
        let mut rows = vec![];
        for row in res {
            let rowvals = row.unwrap().unwrap();
            let vals: Vec<Value> = rowvals.iter().map(|v| v.clone().into()).collect();
            rows.push(vals);
        }
        assert_eq!(rows.len(), 0);
    }
    // TODO more checks for anon

    /***********************************
     * gdpr restore (no composition)
     ***********************************/
    for u in 1..nusers {
        let postdata = json!({
            "tableinfo_json": LOBSTERS_TABLEINFO_JSON,
            "ppgen_json": LOBSTERS_PPGEN_JSON,
            "password": u.to_string(),
        });

        client
            .post(&format!("/reveal_disguise/{}/{}", u, dids[u as usize - 1]))
            .body(postdata.to_string())
            .header(ContentType::JSON)
            .dispatch()
            .await;
        warn!("Restored account of {}", u);
    }
    // check
    for u in 1..nusers {
        let res = db
            .query_iter(format!("SELECT * FROM comments WHERE user_id = {};", u))
            .unwrap();
        let mut rows = vec![];
        for row in res {
            let rowvals = row.unwrap().unwrap();
            let vals: Vec<Value> = rowvals.iter().map(|v| v.clone().into()).collect();
            rows.push(vals);
        }
        assert_eq!(rows.len(), ncomments as usize * nstories as usize);
        let res = db
            .query_iter(format!("SELECT * FROM stories WHERE user_id = {};", u))
            .unwrap();
        let mut rows = vec![];
        for row in res {
            let rowvals = row.unwrap().unwrap();
            let vals: Vec<Value> = rowvals.iter().map(|v| v.clone().into()).collect();
            rows.push(vals);
        }
        assert_eq!(rows.len(), nstories as usize);
        let res = db
            .query_iter(format!("SELECT * FROM users WHERE id = {};", u))
            .unwrap();
        let mut rows = vec![];
        for row in res {
            let rowvals = row.unwrap().unwrap();
            let vals: Vec<Value> = rowvals.iter().map(|v| v.clone().into()).collect();
            rows.push(vals);
        }
        assert_eq!(rows.len(), 1);
    }

    /**********************************
     * decay
     ***********************************/
    // decay
    let mut dids = vec![];
    for u in 1..nusers {
        let postdata = json!({
            "disguise_json": LOBSTERS_DECAY_JSON,
            "tableinfo_json": LOBSTERS_TABLEINFO_JSON,
            "ppgen_json": LOBSTERS_PPGEN_JSON,
            "password": u.to_string(),
            "user": u.to_string(),
        });

        let endpoint = format!("/apply_disguise");
        let response = client
            .post(&endpoint)
            .body(postdata.to_string())
            .header(ContentType::JSON)
            .dispatch()
            .await;
        let strbody = response.into_string().await.unwrap();
        warn!("Decay strbody response: {}", strbody);
        let body: ApplyDisguiseResponse = serde_json::from_str(&strbody).unwrap();
        warn!("Decayed account of {}", u);
        dids.push(body.did);
    }

    // check results of decay: user has no associated data
    for u in 1..nusers {
        let res = db
            .query_iter(format!("SELECT * FROM comments WHERE user_id = {};", u))
            .unwrap();
        let mut rows = vec![];
        for row in res {
            let rowvals = row.unwrap().unwrap();
            let vals: Vec<Value> = rowvals.iter().map(|v| v.clone().into()).collect();
            rows.push(vals);
        }
        assert_eq!(rows.len(), 0);
        let res = db
            .query_iter(format!("SELECT * FROM stories WHERE user_id = {};", u))
            .unwrap();
        let mut rows = vec![];
        for row in res {
            let rowvals = row.unwrap().unwrap();
            let vals: Vec<Value> = rowvals.iter().map(|v| v.clone().into()).collect();
            rows.push(vals);
        }
        assert_eq!(rows.len(), 0);
        let res = db
            .query_iter(format!("SELECT * FROM users WHERE id = {};", u))
            .unwrap();
        let mut rows = vec![];
        for row in res {
            let rowvals = row.unwrap().unwrap();
            let vals: Vec<Value> = rowvals.iter().map(|v| v.clone().into()).collect();
            rows.push(vals);
        }
        assert_eq!(rows.len(), 0);
    }
    /***********************************
     * gdpr deletion (composition)
     ***********************************/

    /***********************************
     * gdpr restore (composition)
     ***********************************/
    // check
}

pub async fn test_hotcrp_disguise() {
    let client = Client::tracked(rocket(
        true,
        "tslilyai",
        "pass",
        "127.0.0.1",
        "testdb",
        "../applications/hotcrp/src/schema.sql",
        true,
    ))
    .await
    .unwrap();
    let mut db = mysql::Conn::new(
        Opts::from_url(&format!("mysql://tslilyai:pass@127.0.0.1/{}", "testdb")).unwrap(),
    )
    .unwrap();
    assert_eq!(db.ping(), true);

    let nusers: u64 = 5;

    // create all users
    for u in 1..nusers {
        let postdata = json!({
            "uid": u.to_string(),
            "pw": u.to_string(),
        });
        let response = client
            .post("/register_principal")
            .body(postdata.to_string())
            .header(ContentType::JSON)
            .dispatch()
            .await;
        let strbody = response.into_string().await.unwrap();
        let _body: RegisterPrincipalResponse = serde_json::from_str(&strbody).unwrap();
        // TODO datagen
    }
    warn!("Created {} users", nusers);

    /***********************************
     * gdpr deletion (no composition)
     ***********************************/
    let mut dids = vec![];
    for u in 1..nusers {
        let postdata = json!({
            "disguise_json": HOTCRP_GDPR_JSON,
            "tableinfo_json": HOTCRP_TABLEINFO_JSON,
            "ppgen_json": HOTCRP_PPGEN_JSON,
            "password": u.to_string(),
            "user": u.to_string(),
        });

        let endpoint = format!("/apply_disguise");
        let response = client
            .post(&endpoint)
            .body(postdata.to_string())
            .header(ContentType::JSON)
            .dispatch()
            .await;
        let strbody = response.into_string().await.unwrap();
        warn!("Delete strbody response: {}", strbody);
        let body: ApplyDisguiseResponse = serde_json::from_str(&strbody).unwrap();
        dids.push(body.did);
        warn!("Deleted account of {}", u);
    }
    // check results of delete: user has no comments
    for u in 1..nusers {
        let res = db
            .query_iter(format!(
                "SELECT * FROM ContactInfo WHERE contactId = {};",
                u
            ))
            .unwrap();
        let mut rows = vec![];
        for row in res {
            let rowvals = row.unwrap().unwrap();
            let vals: Vec<Value> = rowvals.iter().map(|v| v.clone().into()).collect();
            rows.push(vals);
        }
        assert_eq!(rows.len(), 0);
    }

    /***********************************
     * gdpr restore (no composition)
     ***********************************/
    /*for u in 1..nusers {
        let postdata = json!({
            "tableinfo_json": HOTCRP_TABLEINFO_JSON,
            "ppgen_json": HOTCRP_PPGEN_JSON,
            "password": u.to_string(),
        });
        client
            .post(&format!("/reveal_disguise/{}/{}", u, dids[u as usize - 1]))
            .body(postdata.to_string())
            .header(ContentType::JSON)
            .dispatch()
            .await;
        warn!("Restored account of {}", u);
    }
    // check
    for u in 1..nusers {
        let res = db
            .query_iter(format!(
                "SELECT * FROM ContactInfo WHERE contactId = {};",
                u
            ))
            .unwrap();
        let mut rows = vec![];
        for row in res {
            let rowvals = row.unwrap().unwrap();
            let vals: Vec<Value> = rowvals.iter().map(|v| v.clone().into()).collect();
            rows.push(vals);
        }
        assert_eq!(rows.len(), 1);
    }

    /**********************************
     * anon
     ***********************************/
    // anon
    for u in 1..nusers {
        let postdata = json!({
            "user": u.to_string(),
            "disguise_json": HOTCRP_ANON_JSON,
            "tableinfo_json": HOTCRP_TABLEINFO_JSON,
            "ppgen_json": HOTCRP_PPGEN_JSON,
            "password": u.to_string(),
        });

        let endpoint = format!("/apply_disguise");
        let response = client
            .post(&endpoint)
            .body(postdata.to_string())
            .header(ContentType::JSON)
            .dispatch()
            .await;
        let strbody = response.into_string().await.unwrap();
        warn!("Anon strbody response: {}", strbody);
        let body: ApplyDisguiseResponse = serde_json::from_str(&strbody).unwrap();
        warn!("Anon account of {}", u);
    }

    // check results of anon: user account exists?
    for u in 1..nusers {
        let res = db
            .query_iter(format!(
                "SELECT * FROM ContactInfo WHERE contactId = {};",
                u
            ))
            .unwrap();
        let mut rows = vec![];
        for row in res {
            let rowvals = row.unwrap().unwrap();
            let vals: Vec<Value> = rowvals.iter().map(|v| v.clone().into()).collect();
            rows.push(vals);
        }
        assert_eq!(rows.len(), 1);
    }*/
    /***********************************
     * gdpr deletion (composition)
     ***********************************/

    /***********************************
     * gdpr restore (composition)
     ***********************************/
    // check
}
