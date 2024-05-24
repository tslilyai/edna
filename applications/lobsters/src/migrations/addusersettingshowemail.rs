//https://github.com/lobsters/lobsters/blob/ab604bcb1aa594458b6769469cd3289a9aa7e1f3/db/migrate/20231023155620_add_user_setting_show_email.rb

use edna::{helpers, RowVal, TableRow};
use log::warn;
use std::time;

pub fn apply(db: &mut mysql::PooledConn) {
    let start = time::Instant::now();
    helpers::query_drop("ALTER TABLE users ADD COLUMN show_email INT DEFAULT 0", db).unwrap();
    warn!("addusersetting apply: {}mus", start.elapsed().as_micros());
}

pub fn update(rows: Vec<TableRow>) -> Vec<TableRow> {
    let start = time::Instant::now();
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
    warn!("addusersetting update: {}mus", start.elapsed().as_micros());
    new_rows
}
