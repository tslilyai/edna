use crate::backend::MySqlBackend;
use crate::ADMIN_EMAIL;
use mysql::from_value;
use mysql::params;
use rand::prelude::*;
use sql_parser::ast::Expr;
use std::collections::HashMap;
use std::time;

struct UpdateFK {
    new_uid: String,
    user: String,
    lec: u64,
    q: u64,
}

pub fn apply(bg: &mut MySqlBackend) -> Result<(), mysql::Error> {
    // DECOR ANSWERS

    // TODO prevent new users from joining

    let beginning_start = time::Instant::now();
    let mut users = vec![];
    let res = bg.query(ADMIN_EMAIL, "SELECT email FROM users WHERE is_anon = 0;");
    for r in res {
        let uid: String = from_value(r[0].clone());
        users.push(uid);
    }

    for u in users {
        // TODO lock user account

        // get all answers sorted by user and lecture
        let mut user_lec_answers: HashMap<u64, Vec<u64>> = HashMap::new();
        let res = bg.query(
            ADMIN_EMAIL,
            &format!("SELECT lec, q FROM answers WHERE email = '{}';", u),
        );
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

        let mut updates = vec![];
        let mut pps = vec![];
        for (lecture, qs) in user_lec_answers {
            let new_uid: String;
            let rowvals = get_insert_guise_vals(u.trim_matches('\''));
            pps.push(format!(
                "({}, {}, {}, {}, {}, {})",
                rowvals[0], rowvals[1], rowvals[2], rowvals[3], rowvals[4], rowvals[5]
            ));
            new_uid = rowvals[0].to_string();

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
            //let start = time::Instant::now();
            bg.query(
                "",
                &format!(r"INSERT INTO `users` VALUES {};", pps.join(",")),
            );
            //debug!(
            //   bg.log,
            //  "WSAnon: INSERT INTO `users` VALUES {};: {}",
            // pps.join(","),
            //start.elapsed().as_micros()
            //);
            //let start = time::Instant::now();
            bg.exec_batch(
                r"UPDATE answers SET `email` = :newuid WHERE `email` = :user AND lec = :lec AND q = :q;",
                updates.iter().map(|u| {
                    params! {
                        "newuid" => &u.new_uid,
                        "user" => &u.user,
                        "lec" => u.lec,
                        "q" => u.q,
                    }
                }),
            );
            //debug!(
            //    bg.log,
            //    "WSAnon: update {} fks: {}",
            //   updates.len(),
            //  start.elapsed().as_micros()
            //);
        }
    }
    debug!(
        bg.log,
        "WSAnon: total: {}",
        beginning_start.elapsed().as_micros()
    );
    //db.query_drop(&format!("DELETE FROM users WHERE `email` = '{}';", u))?;
    Ok(())
}

// we don't need to reveal

fn get_insert_guise_vals(owner: &str) -> Vec<Expr> {
    use sql_parser::ast::Value;
    let mut rng = rand::thread_rng();
    let gid: u64 = rng.gen();
    let email: u32 = rng.gen();
    vec![
        Expr::Value(Value::String(format!("{}@anon.com", email.to_string()))),
        Expr::Value(Value::String(gid.to_string())),
        Expr::Value(Value::Number(0.to_string())),
        Expr::Value(Value::Number(1.to_string())),
        Expr::Value(Value::Number(0.to_string())),
        Expr::Value(Value::String(owner.to_string())),
    ]
}
