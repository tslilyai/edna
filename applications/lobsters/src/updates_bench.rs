use edna::{helpers, EdnaClient, RowVal, TableRow};
use log::warn;
use mysql::prelude::*;
use std::fs::{OpenOptions};
use std::io::Write;
use std::str::FromStr;
use std::time;
use std::time::Duration;

const TABLEINFO_JSON: &'static str = include_str!("./disguises/table_info_updated.json");
const PPGEN_JSON: &'static str = include_str!("./disguises/pp_gen_updated.json");
const GDPR_JSON: &'static str = include_str!("./disguises/gdpr_disguise.json");

pub fn run_updates_test(
    edna: &mut EdnaClient,
    db: &mut mysql::PooledConn,
    num_updates: usize,
    use_txn: bool,
) {
    let overall_start = time::Instant::now();
    let mut delete_durations = vec![];
    let mut restore_durations = vec![];

    let uid = 1; // TODO

    // UNSUB
    let start = time::Instant::now();
    let did = edna
        .apply_disguise(
            uid.to_string(),
            GDPR_JSON,
            TABLEINFO_JSON,
            PPGEN_JSON,
            None, //Some(uid.to_string()),
            None,
            use_txn,
        )
        .unwrap();
    delete_durations.push((overall_start.elapsed(), start.elapsed()));
    warn!("Ran unsub: {}", start.elapsed().as_micros());

    // apply schema updates!
    apply_updates(db, 0, num_updates);
    
    // record one-by-one, so they count as separate updates in Edna
    edna.record_update(update_0);
    edna.record_update(update_1);
    edna.record_update(update_2);
    edna.record_update(update_3);

    // RESUB
    let start = time::Instant::now();
    edna.reveal_disguise(
        uid.to_string(),
        did,
        TABLEINFO_JSON,
        PPGEN_JSON,
        Some(edna::RevealPPType::Restore),
        true, // allow partial row reveals
        Some(uid.to_string()),
        None,
        use_txn,
    )
    .unwrap();
    restore_durations.push((overall_start.elapsed(), start.elapsed()));
    warn!("Ran resub: {}", start.elapsed().as_micros());

    print_update_stats(&delete_durations, &restore_durations);
}

// Update 0: Update all comments to remove all as
fn update_0(rows: Vec<TableRow>) -> Vec<TableRow> {
    let mut new_rows = vec![];
    for row in rows {
        if row.table == "comments" {
            let mut new_row = vec![];
            for rv in &row.row {
                if &rv.column() == "comment" {
                    let v = rv.value();
                    new_row.push(RowVal::new(rv.column(), v.replace("a", "")));
                } else {
                    new_row.push(rv.clone());
                }
            }
            new_rows.push(TableRow {
                table: row.table.to_string(),
                row: new_row,
            });
        } else {
            new_rows.push(row.clone());
        }
    }

    new_rows
}
// Update 1: Update all comments to remove all rs
fn update_1(rows: Vec<TableRow>) -> Vec<TableRow> {
    let mut new_rows = vec![];
    for row in rows {
        if row.table == "comments" {
            let mut new_row = vec![];
            for rv in &row.row {
                if &rv.column() == "comment" {
                    let v = rv.value();
                    new_row.push(RowVal::new(rv.column(), v.replace("r", "")));
                } else {
                    new_row.push(rv.clone());
                }
            }
            new_rows.push(TableRow {
                table: row.table.to_string(),
                row: new_row,
            });
        } else {
            new_rows.push(row.clone());
        }
    }
    new_rows
}
fn update_2(rows: Vec<TableRow>) -> Vec<TableRow> {
    let mut new_rows = vec![];
    for row in rows {
        if row.table == "comments" {
            // Update 2: Update all comments to have parent comment id table
            let parent = helpers::get_value_of_col(&row.row, "parent_comment_id").unwrap();
            let id = helpers::get_value_of_col(&row.row, "id").unwrap();

            // note that this assumes usernames are still unique
            new_rows.push(TableRow {
                table: "parentcomments".to_string(),
                row: vec![
                    RowVal::new("parent_comment_id".to_string(), parent),
                    RowVal::new("comment_id".to_string(), id),
                ],
            });

            // remove parent comment id from comment row
            let comment_row = row
                .row
                .clone()
                .into_iter()
                .filter(|rv| &rv.column() == "parent_comment_id")
                .collect::<Vec<RowVal>>();
            new_rows.push(TableRow {
                table: "comment".to_string(),
                row: comment_row,
            });
        } else {
            new_rows.push(row.clone());
        }
    }
    new_rows
}
// Update 3: comment is blob text put into separate table, has new foreign key
fn update_3(rows: Vec<TableRow>) -> Vec<TableRow> {
    let mut new_rows = vec![];
    for row in rows {
        if row.table == "comments" {
            let comment = helpers::get_value_of_col(&row.row, "comment").unwrap();
            let id = helpers::get_value_of_col(&row.row, "id").unwrap();

            // note that this assumes usernames are still unique
            new_rows.push(TableRow {
                table: "commentblobs".to_string(),
                row: vec![
                    RowVal::new("id".to_string(), id.clone()),
                    RowVal::new("comment".to_string(), comment),
                ],
            });

            // remove comment blob from comment row
            let comment_row = row
                .row
                .clone()
                .iter()
                .map(|rv| {
                    if &rv.column() == "comment" {
                        RowVal::new("comment".to_string(), id.clone())
                    } else {
                        rv.clone()
                    }
                })
                .collect();
            new_rows.push(TableRow {
                table: "comment".to_string(),
                row: comment_row,
            });
        } else {
            new_rows.push(row.clone());
        }
    }
    new_rows
}

fn apply_updates(db: &mut mysql::PooledConn, ubegin: usize, uend: usize) {
    // get initial table rows
    let mut table_rows = vec![];
    let res = helpers::get_query_rows_str_q(r"SELECT * FROM comments", db).unwrap();
    for row in res {
        table_rows.push(TableRow {
            table: "comments".to_string(),
            row: row.clone(),
        });
    }

    if ubegin <= 0 && uend > 0 {
        // Update all comments to remove all as
        table_rows = update_0(table_rows);
        for r in &table_rows {
            let comment = helpers::get_value_of_col(&r.row, "comment").unwrap();
            let id = helpers::get_value_of_col(&r.row, "id").unwrap();
            db.query_drop(format!(
                "UPDATE comments SET comment = {} WHERE id = {};",
                comment, id
            ))
            .unwrap();
        }
    }
    if ubegin <= 1 && uend > 1 {
        // Update all comments to remove all rs
        table_rows = update_1(table_rows);
        for r in &table_rows {
            let comment = helpers::get_value_of_col(&r.row, "comment").unwrap();
            let id = helpers::get_value_of_col(&r.row, "id").unwrap();
            db.query_drop(format!(
                "UPDATE comments SET comment = {} WHERE id = {};",
                comment, id
            ))
            .unwrap();
        }
    }
    if ubegin <= 2 && uend > 2 {
        // Update 2: Update all comments to have parent comment id table
        db.query_drop("DELETE FROM comments WHERE TRUE").unwrap();
        db.query_drop("ALTER TABLE comments DROP COLUMN parent_comment_id")
            .unwrap();
        db.query_drop(format!(
            r"CREATE TABLE `parentcomments` (`parent_comment_id` int, `comment_id` int NOT NULL);"
        ))
        .unwrap();

        // insert new rows into database
        table_rows = update_2(table_rows);
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
    if 3 >= ubegin && 3 < uend {
        // Update 3: Update all comments to have parent comment id table
        db.query_drop("DELETE FROM comments WHERE TRUE").unwrap();
        db.query_drop("ALTER TABLE comments ALTER COLUMN comments int unsigned")
            .unwrap();
        db.query_drop(format!(
            r"CREATE TABLE `commentblobs` (`comment` int, `id` int NOT NULL);"
        ))
        .unwrap();

        table_rows = update_3(table_rows);
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
}

fn print_update_stats(
    delete_durations: &Vec<(Duration, Duration)>,
    restore_durations: &Vec<(Duration, Duration)>,
) {
    let filename = format!(
        "../../results/lobsters_results/update_stats.csv",
    );

    // print out stats
    let mut f = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&filename)
        .unwrap();

    writeln!(
        f,
        "{}",
        delete_durations
            .iter()
            .map(|d| format!(
                "{}:{}",
                d.0.as_millis().to_string(),
                d.1.as_micros().to_string()
            ))
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
    writeln!(
        f,
        "{}",
        restore_durations
            .iter()
            .map(|d| format!(
                "{}:{}",
                d.0.as_millis().to_string(),
                d.1.as_micros().to_string()
            ))
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
}