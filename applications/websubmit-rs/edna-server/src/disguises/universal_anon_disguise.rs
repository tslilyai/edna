use crate::backend::MySqlBackend;
use edna::*;
use mysql::from_value;
use mysql::params;
use rand::prelude::*;
use sql_parser::ast::Expr;
use std::collections::HashMap;
use std::time;

const PPGEN_JSON: &'static str = include_str!("./pp_gen.json");

struct UpdateFK {
    new_uid: String,
    user: String,
    lec: u64,
    q: u64,
}

const TABLEINFO_JSON: &'static str = include_str!("./table_info.json");
const ANON_JSON: &'static str = include_str!("./universal_anon_disguise.json");

pub fn apply(bg: &mut MySqlBackend, is_baseline: bool) -> Result<DID, mysql::Error> {
    if is_baseline {
        return apply_ll(bg, true);
    }
    bg.edna.apply_disguise(
        "NULL".to_string(),
        ANON_JSON,
        TABLEINFO_JSON,
        PPGEN_JSON,
        None,
        None,
        false,
    )
}

pub fn apply_ll(bg: &mut MySqlBackend, is_baseline: bool) -> Result<DID, mysql::Error> {
    // DECOR ANSWERS

    // TODO prevent new users from joining

    let mut users = vec![];
    let res = bg.query_iter("SELECT email FROM users WHERE is_anon = 0;");
    for r in res {
        let uid: String = from_value(r[0].clone());
        users.push(uid);
    }
    let did: u64 = 0;

    for u in users {
        // XXX transaction
        //let mut txn = db.start_transaction(TxOpts::default()).unwrap();
        let beginning_start = time::Instant::now();
        // TODO lock user account

        let start = time::Instant::now();
        if !is_baseline {
            bg.edna.start_disguise(Some(u.clone()));
        }

        // get all answers sorted by user and lecture
        let mut user_lec_answers: HashMap<u64, Vec<u64>> = HashMap::new();
        let res = bg.query_iter(&format!(
            "SELECT lec, q FROM answers WHERE `email` = '{}';",
            u
        ));
        //let res = txn.query_iter(&format!("SELECT lec, q FROM answers WHERE `email` = '{}';", u))?;
        for r in res {
            let key: u64 = from_value(r[0].clone());
            let val: u64 = from_value(r[1].clone());
            match user_lec_answers.get_mut(&key) {
                Some(qs) => qs.push(val),
                None => {
                    user_lec_answers.insert(key, vec![val]);
                }
            };
        }
        debug!(
            bg.log,
            "WSAnon: get answers: {}",
            start.elapsed().as_micros()
        );

        let mut updates = vec![];
        let mut pps = vec![];
        for (lecture, qs) in user_lec_answers {
            let new_uid: String;
            if !is_baseline {
                // insert a new pseudoprincipal
                let start = time::Instant::now();
                let rowvals = get_insert_guise_rowvals();
                new_uid = rowvals[0].value();
                debug!(
                    bg.log,
                    "WSAnon: create pseudoprincipal: {}",
                    start.elapsed().as_micros()
                );

                // XXX issue where using bg adds quotes everywhere...
                pps.push(format!(
                    "({}, {}, {}, {})",
                    rowvals[0].value(),
                    rowvals[1].value(),
                    rowvals[2].value(),
                    rowvals[3].value(),
                ));

                // register new ownershiprecord for pseudoprincipal
                let start = time::Instant::now();
                bg.edna
                    .register_and_save_pseudoprincipal_record(did, &u, &new_uid, &vec![]);
                debug!(
                    bg.log,
                    "WSAnon: save pseudoprincipals: {}",
                    start.elapsed().as_micros()
                );
            } else {
                let rowvals = get_insert_guise_vals();
                pps.push(format!(
                    "({}, {}, {}, {})",
                    rowvals[0], rowvals[1], rowvals[2], rowvals[3],
                ));
                new_uid = rowvals[0].to_string();
            }

            // rewrite answers for all qs to point from user to new pseudoprincipal
            for q in qs {
                updates.push(UpdateFK {
                    new_uid: new_uid.trim_matches('\'').to_string(),
                    user: u.trim_matches('\'').to_string(),
                    lec: lecture,
                    q: q,
                });
            }
        }

        if !pps.is_empty() {
            let start = time::Instant::now();
            //txn.query_drop(&format!(r"INSERT INTO `users` VALUES {};", pps.join(",")))?;
            bg.query_drop(&format!(r"INSERT INTO `users` VALUES {};", pps.join(",")));
            debug!(
                bg.log,
                "WSAnon: INSERT INTO `users` VALUES {};: {}",
                pps.join(","),
                start.elapsed().as_micros()
            );
            let start = time::Instant::now();
            //txn.exec_batch(
            bg.exec_batch(
                r"UPDATE answers SET `email` = :newuid WHERE `email` = :email AND lec = :lec AND q = :q;",
                updates.iter().map(|u| {
                    params! {
                        "newuid" => &u.new_uid,
                        "email" => &u.user,
                        "lec" => u.lec,
                        "q" => u.q,
                    }
                }).collect(),
            );
            debug!(
                bg.log,
                "WSAnon: update {} fks: {}",
                updates.len(),
                start.elapsed().as_micros()
            );
        }

        if !is_baseline {
            let _res = bg.edna.end_disguise();
        }
        debug!(
            bg.log,
            "WSAnon: total: {}",
            beginning_start.elapsed().as_micros()
        );
        //db.query_drop(&format!("DELETE FROM users WHERE `email` = '{}';", u))?;
        //txn.commit().unwrap();
    }
    Ok(did)
}

// we don't need to reveal

fn get_insert_guise_rowvals() -> Vec<RowVal> {
    let mut rng = rand::thread_rng();
    let gid: u64 = rng.gen();
    let email: u32 = rng.gen();
    vec![
        RowVal::new("email".to_string(), format!("{}@anon.com", email)),
        RowVal::new("apikey".to_string(), format!("{}", gid)),
        RowVal::new("is_admin".to_string(), format!("{}", 0)),
        RowVal::new("is_anon".to_string(), format!("{}", 1)),
    ]
}

fn get_insert_guise_vals() -> Vec<Expr> {
    use sql_parser::ast::Value;
    let mut rng = rand::thread_rng();
    let gid: u64 = rng.gen();
    let email: u32 = rng.gen();
    vec![
        Expr::Value(Value::String(format!("{}@anon.com", email.to_string()))),
        Expr::Value(Value::String(gid.to_string())),
        Expr::Value(Value::Number(0.to_string())),
        Expr::Value(Value::Number(1.to_string())),
    ]
}
