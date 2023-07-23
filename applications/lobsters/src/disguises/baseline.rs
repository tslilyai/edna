use crate::*;
use mysql::from_value;
use mysql::*;
use std::time;

fn get_insert_guise_cols() -> Vec<String> {
    vec![
        "id".to_string(),
        "username".to_string(),
        "karma".to_string(),
        "last_login".to_string(),
        "password_reset_token".to_string(),
        "rss_token".to_string(),
        "session_token".to_string(),
        "email".to_string(),
        "created_at".to_string(),
    ]
}

fn get_insert_guise_vals() -> Vec<sql_parser::ast::Expr> {
    use sql_parser::ast::*;
    let mut rng = rand::thread_rng();
    let gid: u64 = rng.gen_range(20000, i64::MAX as u64);
    let username: String = format!("anon{}", gid);
    vec![
        Expr::Value(Value::Number(gid.to_string())),
        Expr::Value(Value::String(username.clone())),
        Expr::Value(Value::Number(0.to_string())),
        Expr::Value(Value::String(Local::now().naive_local().to_string())),
        Expr::Value(Value::String(Local::now().naive_local().to_string())),
        Expr::Value(Value::String(Local::now().naive_local().to_string())),
        Expr::Value(Value::String(Local::now().naive_local().to_string())),
        Expr::Value(Value::String(format!("{}@mail.com", username))),
        Expr::Value(Value::String(Local::now().naive_local().to_string())),
    ]
}

fn insert_new_users(nusers: usize, db: &mut mysql::PooledConn) -> Vec<String> {
    let start = time::Instant::now();
    let mut users = vec![];
    if nusers == 0 {
        return users;
    }
    let mut uids = vec![];
    let cols = get_insert_guise_cols();
    for _ in 0..nusers {
        let vals: Vec<String> = get_insert_guise_vals()
            .iter()
            .map(|v| v.to_string())
            .collect();
        users.push(format!("({})", vals.join(",")));
        uids.push(vals[0].clone());
    }
    let q = format!(
        "INSERT INTO {} ({}) VALUES {};",
        "users",
        cols.join(","),
        users.join(",")
    );
    db.query_drop(&q).unwrap();
    warn!("{}: {}mus", q, start.elapsed().as_micros());
    uids
}

fn update_ids_to_new_users(table: &str, fkcol: &str, uid: &str, db: &mut mysql::PooledConn) {
    let res = db
        .query_iter(&format!(
            "SELECT id FROM {} WHERE {} = {}",
            table, fkcol, uid
        ))
        .unwrap();
    let mut ids = vec![];
    for row in res {
        let id: u64 = from_value(row.unwrap().unwrap()[0].clone());
        ids.push(id);
    }
    let new_users = insert_new_users(ids.len(), db);
    for (i, id) in ids.iter().enumerate() {
        db.query_drop(&format!(
            "UPDATE {} SET {}={} WHERE id={}",
            table, fkcol, new_users[i], id
        ))
        .unwrap();
    }
}

pub fn apply_hobby_anon(uid: u64, db: &mut mysql::PooledConn) -> Result<()> {
    let from = "INNER JOIN taggings ON taggings.story_id = stories.id \
                INNER JOIN tags ON taggings.tag_id = tags.id";

    // remove votes
    let mut q = format!(
        "DELETE X FROM votes X \
                                     INNER JOIN stories ON X.story_id = stories.id {} \
                                     WHERE X.user_id = {} AND tags.tag = 'starwars'",
        from, uid
    );
    let start = time::Instant::now();
    db.query_drop(&q)?;
    warn!("{}: {}mus", q, start.elapsed().as_micros());

    let start = time::Instant::now();
    q = format!(
        "DELETE X FROM votes X \
                                     INNER JOIN comments ON comments.id = X.comment_id \
                                     INNER JOIN stories ON comments.story_id = stories.id {} \
                                     WHERE X.user_id = {} AND tags.tag = 'starwars'",
        from, uid
    );
    db.query_drop(&q)?;
    warn!("{}: {}mus", q, start.elapsed().as_micros());

    let start = time::Instant::now();
    q = format!(
        "SELECT stories.id FROM stories {} WHERE stories.user_id = {} AND tags.tag = 'starwars'",
        from, uid
    );
    let res = db.query_iter(&q)?;
    let mut ids = vec![];
    for row in res {
        let id: u64 = from_value(row.unwrap().unwrap()[0].clone());
        ids.push(id);
    }
    warn!("{}: {}mus", q, start.elapsed().as_micros());

    let new_users = insert_new_users(ids.len(), db);
    let start = time::Instant::now();
    for (i, id) in ids.iter().enumerate() {
        q = format!(
            "UPDATE stories SET user_id={} WHERE id={}",
            new_users[i], id
        );
        db.query_drop(&q)?;
    }
    warn!("{}: {}mus", q, start.elapsed().as_micros());

    let start = time::Instant::now();
    q = format!(
        "SELECT comments.id FROM comments \
                                     INNER JOIN stories ON comments.story_id = stories.id {} \
                                     WHERE comments.user_id = {} AND tags.tag = 'starwars'",
        from, uid
    );
    let res = db.query_iter(&q)?;
    let mut ids = vec![];
    for row in res {
        let id: u64 = from_value(row.unwrap().unwrap()[0].clone());
        ids.push(id);
    }
    warn!("{}: {}mus", q, start.elapsed().as_micros());

    let start = time::Instant::now();
    let new_users = insert_new_users(ids.len(), db);
    for (i, id) in ids.iter().enumerate() {
        q = format!(
            "UPDATE comments SET user_id={} WHERE id={}",
            new_users[i], id
        );
        db.query_drop(&q)?;
        warn!("{}: {}mus", q, start.elapsed().as_micros());
    }
    Ok(())
}

pub fn apply_decay(uid: u64, db: &mut mysql::PooledConn) -> Result<()> {
    db.query_drop(&format!("DELETE FROM users WHERE id={}", uid))?;
    db.query_drop(&format!("DELETE FROM hat_requests WHERE user_id={}", uid))?;
    db.query_drop(&format!("DELETE FROM hats WHERE user_id={}", uid))?;
    db.query_drop(&format!("DELETE FROM hidden_stories WHERE user_id={}", uid))?;
    db.query_drop(&format!("DELETE FROM invitations WHERE user_id={}", uid))?;
    db.query_drop(&format!("DELETE FROM read_ribbons WHERE user_id={}", uid))?;
    db.query_drop(&format!("DELETE FROM saved_stories WHERE user_id={}", uid))?;
    db.query_drop(&format!(
        "DELETE FROM suggested_taggings WHERE user_id={}",
        uid
    ))?;
    db.query_drop(&format!(
        "DELETE FROM suggested_titles WHERE user_id={}",
        uid
    ))?;
    db.query_drop(&format!("DELETE FROM tag_filters WHERE user_id={}", uid))?;

    update_ids_to_new_users("comments", "user_id", &uid.to_string(), db);
    update_ids_to_new_users("stories", "user_id", &uid.to_string(), db);
    update_ids_to_new_users("messages", "author_user_id", &uid.to_string(), db);
    update_ids_to_new_users("messages", "recipient_user_id", &uid.to_string(), db);
    update_ids_to_new_users("mod_notes", "moderator_user_id", &uid.to_string(), db);
    update_ids_to_new_users("mod_notes", "user_id", &uid.to_string(), db);
    update_ids_to_new_users(
        "moderations",
        "moderator_user_id",
        &uid.to_string(),
        db,
    );
    update_ids_to_new_users("moderations", "user_id", &uid.to_string(), db);
    update_ids_to_new_users("votes", "user_id", &uid.to_string(), db);
    Ok(())
}

pub fn apply_delete(uid: u64, db: &mut mysql::PooledConn) -> Result<()> {
    db.query_drop(&format!("DELETE FROM users WHERE id={}", uid))?;
    db.query_drop(&format!("DELETE FROM hat_requests WHERE user_id={}", uid))?;
    db.query_drop(&format!("DELETE FROM hats WHERE user_id={}", uid))?;
    db.query_drop(&format!("DELETE FROM hidden_stories WHERE user_id={}", uid))?;
    db.query_drop(&format!("DELETE FROM invitations WHERE user_id={}", uid))?;
    db.query_drop(&format!("DELETE FROM read_ribbons WHERE user_id={}", uid))?;
    db.query_drop(&format!("DELETE FROM saved_stories WHERE user_id={}", uid))?;
    db.query_drop(&format!(
        "DELETE FROM suggested_taggings WHERE user_id={}",
        uid
    ))?;
    db.query_drop(&format!(
        "DELETE FROM suggested_titles WHERE user_id={}",
        uid
    ))?;
    db.query_drop(&format!("DELETE FROM tag_filters WHERE user_id={}", uid))?;

    db.query_drop(&format!(
        "UPDATE comments SET comment='dummy text' WHERE user_id={}",
        uid
    ))?;
    db.query_drop(&format!(
        "UPDATE comments SET markeddown_comment='dummy text' WHERE user_id={}",
        uid
    ))?;
    update_ids_to_new_users("comments", "user_id", &uid.to_string(), db);

    db.query_drop(&format!(
        "UPDATE stories SET url='dummy url' WHERE user_id={}",
        uid
    ))?;
    db.query_drop(&format!(
        "UPDATE stories SET title='dummy title' WHERE user_id={}",
        uid
    ))?;
    update_ids_to_new_users("stories", "user_id", &uid.to_string(), db);

    update_ids_to_new_users("messages", "author_user_id", &uid.to_string(), db);
    update_ids_to_new_users("messages", "recipient_user_id", &uid.to_string(), db);
    update_ids_to_new_users("mod_notes", "moderator_user_id", &uid.to_string(), db);
    update_ids_to_new_users("mod_notes", "user_id", &uid.to_string(), db);
    update_ids_to_new_users(
        "moderations",
        "moderator_user_id",
        &uid.to_string(),
        db,
    );
    update_ids_to_new_users("moderations", "user_id", &uid.to_string(), db);
    update_ids_to_new_users("votes", "user_id", &uid.to_string(), db);
    Ok(())
}
