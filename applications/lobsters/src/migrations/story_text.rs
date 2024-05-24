use edna::{helpers, RowVal, TableRow};
use log::warn;
use std::time;

pub fn apply(db: &mut mysql::PooledConn) {
    let start = time::Instant::now();
    helpers::query_drop(
        "create table story_texts AS SELECT `id`, `title`, `description`, `story_cache` FROM stories", db)
    .unwrap();
    helpers::query_drop(
        "alter table story_texts rename column `story_cache` to `body`",
        db,
    )
    .unwrap();
    helpers::query_drop("ALTER TABLE story_texts ADD PRIMARY KEY (id)", db).unwrap();

    helpers::query_drop("create table storiesNew like stories", db).unwrap();
    helpers::query_drop("alter table storiesNew drop column `story_cache`", db).unwrap();
    helpers::query_drop("insert into storiesNew (`id`,`created_at`,`user_id`,`url`,`title`,`description`,`short_id`,`is_expired`,`upvotes`,`downvotes`,`is_moderated`,`hotness`,`markeddown_description`,`comments_count`,`merged_story_id`,`unavailable_at`,`twitter_id`,`user_is_author`) SELECT
`id`,`created_at`,`user_id`,`url`,`title`,`description`,`short_id`,`is_expired`,`upvotes`,`downvotes`,`is_moderated`,`hotness`,`markeddown_description`,`comments_count`,`merged_story_id`,`unavailable_at`,`twitter_id`,`user_is_author` from stories;", db).unwrap();
    helpers::query_drop("DROP TABLE stories", db).unwrap();
    helpers::query_drop("RENAME TABLE storiesNew to stories", db).unwrap();
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
