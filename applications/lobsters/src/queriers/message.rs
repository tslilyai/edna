extern crate log;
extern crate mysql;
use chrono;

use mysql::prelude::*;
use std::*;
//use log::{warn, debug};
use log::error;

pub fn post_message_about_story_from(
    db: &mut mysql::PooledConn,
    id: u32,
    from: Option<u64>,
    story: u32,
) -> mysql::Result<()> {
    let from = from.unwrap();
    match db.query_first::<u64, String>(format!(
        "SELECT `stories`.`user_id` \
             FROM `stories` \
             WHERE `stories`.`short_id` = '{}'",
        story
    ))? {
        Some(author) => {
            let now = format!("\'{}\'", chrono::Local::now().naive_local());
            db.query_drop(format!(
                    "INSERT INTO `messages` \
                    (`created_at`, `author_user_id`, `recipient_user_id`, `subject`, `body`, `short_id`) \
                    VALUES \
                    ({}, {}, {}, {}, {}, {})",
                    now,
                    from,
                    author,
                    "\'messagesubject\'", 
                    "\'messagebody\'", 
                    id, 
                ))?;
            Ok(())
        }
        _ => {
            error!("Could not find story {} for message", story);
            Ok(())
        }
    }
}
