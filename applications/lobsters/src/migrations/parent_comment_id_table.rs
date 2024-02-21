use edna::{helpers, RowVal, TableRow};
//use log::warn;
use mysql::prelude::*;
use std::str::FromStr;

pub fn apply(db: &mut mysql::PooledConn) {
    let mut table_rows =
        helpers::get_query_tablerows_str("stories", "SELECT * FROM stories", db).unwrap();
    if table_rows.len() == 0 {
        return;
    }
    db.query_drop("DELETE FROM comments WHERE TRUE").unwrap();
    db.query_drop("ALTER TABLE comments DROP COLUMN parent_comment_id")
        .unwrap();
    db.query_drop(format!(
        r"CREATE TABLE `parentcomments` (`parent_comment_id` int, `comment_id` int NOT NULL);"
    ))
    .unwrap();

    // insert new rows into database
    table_rows = update(table_rows);
    for table_row in &table_rows {
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

pub fn update(rows: Vec<TableRow>) -> Vec<TableRow> {
    let mut new_rows = vec![];
    for row in rows {
        if row.table == "comments" {
            // Update 2: Update all comments to have parent comment id table
            let parent = helpers::get_value_of_col(&row.row, "parent_comment_id").unwrap();
            let id = helpers::get_value_of_col(&row.row, "id").unwrap();

            if parent != "NULL" {
                // note that this assumes usernames are still unique
                new_rows.push(TableRow {
                    table: "parentcomments".to_string(),
                    row: vec![
                        RowVal::new("parent_comment_id".to_string(), parent),
                        RowVal::new("comment_id".to_string(), id),
                    ],
                });
            }

            // remove parent comment id from comment row, also remove any null columns
            let comment_row = row
                .row
                .clone()
                .into_iter()
                .filter(|rv| &rv.column() != "parent_comment_id" && rv.value() != "NULL")
                .collect::<Vec<RowVal>>();
            new_rows.push(TableRow {
                table: "comments".to_string(),
                row: comment_row,
            });
        } else {
            new_rows.push(row.clone());
        }
    }
    new_rows
}
