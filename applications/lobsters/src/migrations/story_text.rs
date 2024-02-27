use edna::{helpers, RowVal, TableRow};
use log::warn;
use std::time;

pub fn apply(db: &mut mysql::PooledConn) {
    let start = time::Instant::now();
    helpers::query_drop(
        "create table story_texts (`id` int, `title` varchar(150), `description` mediumtext, `body` mediumtext) AS (SELECT id, description, title, `story_cache` FROM stories)",
        db,
    )
    .unwrap();
    helpers::query_drop("create index `index_id` on story_texts (`id`)", db).unwrap();

    //(`id` int, `title` varchar(150), `description` mediumtext, `body` mediumtext, `created_at` datetime, INDEX `index_id` (`id`))", db).unwrap();
    let stories = helpers::get_query_tablerows_str("stories", "SELECT * FROM stories", db).unwrap();
    if stories.len() == 0 {
        return;
    }
    /*let new_rows = update(stories);
    let cols: Vec<String> = new_rows[0]
        .row
        .iter()
        .map(|rv| rv.column().clone())
        .collect();
    let colstr = cols.join(",");
    let mut all_stories = vec![];
    for s in new_rows {
        if s.table == "story_texts" {
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
    }

    helpers::query_drop(
        &format!(
            "INSERT INTO story_texts ({}) VALUES {}",
            colstr,
            all_stories.join(","),
        ),
        db,
    )
    .unwrap();*/
    helpers::query_drop("ALTER TABLE stories DROP COLUMN story_cache", db).unwrap();
    //helpers::query_drop("OPTIMIZE TABLE stories", db).unwrap();
    //helpers::query_drop("OPTIMIZE TABLE story_texts", db).unwrap();

    warn!("story_text apply: {}mus", start.elapsed().as_micros());
}

pub fn update(rows: Vec<TableRow>) -> Vec<TableRow> {
    let start = time::Instant::now();
    let mut new_rows = vec![];
    for row in rows {
        if row.table == "stories" {
            let id = helpers::get_value_of_col(&row.row, "id").unwrap();
            let description = helpers::get_value_of_col(&row.row, "description").unwrap();
            let title = helpers::get_value_of_col(&row.row, "title").unwrap();
            let body = helpers::get_value_of_col(&row.row, "story_cache").unwrap();

            new_rows.push(TableRow {
                table: "story_texts".to_string(),
                row: vec![
                    RowVal::new("id".to_string(), id),
                    RowVal::new("title".to_string(), title),
                    RowVal::new("description".to_string(), description),
                    RowVal::new("body".to_string(), body),
                ],
            });
            new_rows.push(TableRow {
                table: "stories".to_string(),
                row: row
                    .row
                    .iter()
                    .cloned()
                    .filter(|x| x.column() != "story_cache")
                    .collect(),
            });
        } else {
            new_rows.push(row.clone());
        }
    }
    warn!("story_text update: {}mus", start.elapsed().as_micros());
    new_rows
}
