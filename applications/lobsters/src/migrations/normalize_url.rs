//https://github.com/lobsters/lobsters/blob/ab604bcb1aa594458b6769469cd3289a9aa7e1f3/db/migrate/20230828195756_normalize_url.rb

use edna::{helpers, RowVal, TableRow};
use log::warn;
use mysql::prelude::*;
use url::Url;
use urlnorm::UrlNormalizer;

pub fn apply(db: &mut mysql::PooledConn) {
    db.query_drop("ALTER TABLE stories ADD COLUMN normalized_url varchar(250) AFTER url")
        .unwrap();
    let stories = helpers::get_query_tablerows_str("stories", "SELECT * FROM stories", db).unwrap();
    if stories.len() == 0 {
        return;
    }
    let new_rows = update(stories);
    db.query_drop("DELETE FROM stories WHERE true").unwrap();
    let cols: Vec<String> = new_rows[0]
        .row
        .iter()
        .map(|rv| rv.column().clone())
        .collect();
    let colstr = cols.join(",");
    let mut all_stories = vec![];
    for s in new_rows {
        let vals: Vec<String> = s
            .row
            .iter()
            .map(|rv| {
                if rv.value().is_empty() {
                    "\"\"".to_string()
                } else if rv.value() == "NULL" {
                    "NULL".to_string()
                } else {
                    for c in rv.value().chars() {
                        if !c.is_numeric() {
                            return format!("\"{}\"", rv.value().clone());
                        }
                    }
                    rv.value().clone()
                }
            })
            .collect();
        all_stories.push(format!("({})", vals.join(",")));
    }

    db.query_drop(format!(
        "INSERT INTO stories ({}) VALUES {}",
        colstr,
        all_stories.join(","),
    ))
    .unwrap();
}

pub fn update(rows: Vec<TableRow>) -> Vec<TableRow> {
    let norm = UrlNormalizer::default();
    let mut new_rows = vec![];
    for row in rows {
        if row.table == "stories" {
            let url = helpers::get_value_of_col(&row.row, "url").unwrap();
            if url == "" || url == "NULL" {
                new_rows.push(row.clone());
                continue;
            }
            warn!("Url is {}", url);
            let url = match Url::parse(&url) {
                Ok(u) => u,
                Err(_) => Url::parse(&format!("http://www.{}.com", url)).unwrap(),
            };
            let normalized_url = norm.compute_normalization_string(&url);
            let story_row: Vec<_> = row
                .row
                .clone()
                .iter()
                .map(|rv| {
                    if &rv.column() == "normalized_url" {
                        RowVal::new("normalized_url".to_string(), normalized_url.clone())
                    } else {
                        rv.clone()
                    }
                })
                .collect();
            new_rows.push(TableRow {
                table: "stories".to_string(),
                row: story_row,
            })
        } else {
            new_rows.push(row.clone());
        }
    }
    new_rows
}
