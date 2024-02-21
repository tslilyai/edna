//https://github.com/lobsters/lobsters/blob/ab604bcb1aa594458b6769469cd3289a9aa7e1f3/db/migrate/20231023155620_add_user_setting_show_email.rb

use edna::{RowVal, TableRow};
//use log::warn;
use mysql::prelude::*;

pub fn apply(db: &mut mysql::PooledConn) {
    db.query_drop("ALTER TABLE users ADD COLUMN show_email INT DEFAULT 0")
        .unwrap();
}

pub fn update(rows: Vec<TableRow>) -> Vec<TableRow> {
    let mut new_rows = vec![];
    for row in rows {
        if row.table == "users" {
            let mut user_row = row.clone();
            user_row
                .row
                .push(RowVal::new("show_email".to_string(), "0".to_string()));
            new_rows.push(user_row);
        } else {
            new_rows.push(row.clone());
        }
    }
    new_rows
}
