extern crate log;
extern crate mysql;

use chrono;
use log::warn;
use mysql::prelude::*;
use std::collections::HashSet;
use std::time;
use std::*;

pub fn read_story(
    db: &mut mysql::PooledConn,
    acting_as: Option<u64>,
    id: u64,
) -> Result<Vec<String>, mysql::Error> {
    let mut result = vec![];
    let start = time::Instant::now();
    let (author, story): (u64, u64) = db
        .query_first(format!(
            "SELECT `stories`.`id`, `stories`.`user_id` \
             FROM `stories` \
             WHERE `stories`.`short_id` = '{}'",
            id
        ))?
        .unwrap();
    result.push(format!("({}, {})", story, author));
    warn!("\t select stories {}", start.elapsed().as_micros());

    let start = time::Instant::now();
    db.query_drop(format!(
        "SELECT `users`.* FROM `users` WHERE `users`.`id` = {}",
        author
    ))?;
    //result.push(format!("({}, {})", story, author));
    warn!("\t select users {}", start.elapsed().as_micros());

    // NOTE: technically this happens before the select from user...
    if let Some(uid) = acting_as {
        // keep track of when the user last saw this story
        // NOTE: *technically* the update only happens at the end...
        let mut rr = None;
        let now = format!("\'{}\'", chrono::Local::now().naive_local());
        let start = time::Instant::now();
        db.query_map(
            format!(
                "SELECT  `read_ribbons`.`id` \
                 FROM `read_ribbons` \
                 WHERE `read_ribbons`.`user_id` = {} \
                 AND `read_ribbons`.`story_id` = {}",
                uid, story
            ),
            |id: u32| rr = Some(id),
        )
        .unwrap();
        warn!("\t select rr {}", start.elapsed().as_micros());

        let start = time::Instant::now();
        match rr {
            None => {
                db.query_drop(format!(
                    "INSERT INTO `read_ribbons` \
                         (`created_at`, `updated_at`, `user_id`, `story_id`) \
                         VALUES ({}, {}, {}, {})",
                    now, now, uid, story
                ))?;
            }
            Some(rr) => {
                db.query_drop(format!(
                    "UPDATE `read_ribbons` \
                         SET `updated_at` = {} \
                         WHERE `read_ribbons`.`id` = {}",
                    now, rr
                ))?;
            }
        };
        warn!("\t update rr {}", start.elapsed().as_micros());
    }

    // XXX: probably not drop here, but we know we have no merged stories
    let start = time::Instant::now();
    db.query_drop(format!(
        "SELECT `stories`.`id` \
             FROM `stories` \
             WHERE `stories`.`merged_story_id` = {}",
        story
    ))?;
    warn!("\t select stories2 {}", start.elapsed().as_micros());

    let mut users = HashSet::new();
    let mut comments = HashSet::new();
    let start = time::Instant::now();
    db.query_map(
        format!(
            "SELECT `comments`.`user_id`, `comments`.`id`, \
         `comments`.`upvotes` - `comments`.`downvotes` AS saldo \
         FROM `comments` \
         WHERE `comments`.`story_id` = {} \
         ORDER BY \
         saldo ASC, \
         confidence DESC",
            story
        ),
        |(user_id, id, _saldo): (u64, u32, i32)| {
            users.insert(user_id);
            comments.insert(id);
        },
    )?;
    warn!("\t select comments {}", start.elapsed().as_micros());

    if !users.is_empty() {
        let start = time::Instant::now();
        // get user info for all commenters
        let users = users
            .into_iter()
            .map(|id| format!("{}", id))
            .collect::<Vec<_>>()
            .join(", ");
        db.query_drop(&format!(
            "SELECT `users`.* FROM `users` WHERE `users`.`id` IN ({})",
            users
        ))?;
        warn!("\t select users {}", start.elapsed().as_micros());
    }

    // get comment votes
    // XXX: why?!
    if !comments.is_empty() {
        let start = time::Instant::now();
        let comments = comments
            .into_iter()
            .map(|id| format!("{}", id))
            .collect::<Vec<_>>()
            .join(", ");
        db.query_drop(&format!(
            "SELECT `votes`.* FROM `votes` WHERE `votes`.`comment_id` IN ({})",
            comments
        ))?;
        warn!("\t select commentvotes {}", start.elapsed().as_micros());
    }

    // OTE: lobste.rs here fetches the user list again. unclear why?
    if let Some(uid) = acting_as {
        let start = time::Instant::now();
        db.query_drop(format!(
            "SELECT `votes`.* \
                 FROM `votes` \
                 WHERE `votes`.`user_id` = {} \
                 AND `votes`.`story_id` = {} \
                 AND `votes`.`comment_id` IS NULL",
            uid, story
        ))?;
        warn!("\t select votes {}", start.elapsed().as_micros());
        let start = time::Instant::now();
        db.query_drop(format!(
            "SELECT `hidden_stories`.* \
                 FROM `hidden_stories` \
                 WHERE `hidden_stories`.`user_id` = {} \
                 AND `hidden_stories`.`story_id` = {}",
            uid, story
        ))?;
        warn!("\t select hiddenstories {}", start.elapsed().as_micros());
        let start = time::Instant::now();
        db.query_drop(format!(
            "SELECT `saved_stories`.* \
                 FROM `saved_stories` \
                 WHERE `saved_stories`.`user_id` = {} \
                 AND `saved_stories`.`story_id` = {}",
            uid, story
        ))?;
        warn!("\t select savedstories {}", start.elapsed().as_micros());
    }

    let mut tags = HashSet::new();
    let start = time::Instant::now();
    db.query_map(
        format!(
            "SELECT `taggings`.`tag_id` \
             FROM `taggings` \
             WHERE `taggings`.`story_id` = {}",
            story
        ),
        |tag_id: u32| {
            tags.insert(tag_id);
        },
    )?;
    warn!("\t select taggings {}", start.elapsed().as_micros());

    if !tags.is_empty() {
        let start = time::Instant::now();
        let tags = tags
            .into_iter()
            .map(|id| format!("{}", id))
            .collect::<Vec<_>>()
            .join(", ");
        db.query_drop(&format!(
            "SELECT `tags`.* FROM `tags` WHERE `tags`.`id` IN ({})",
            tags
        ))?;
        warn!("\t select tags {}", start.elapsed().as_micros());
    }
    Ok(result)
}

pub fn post_story(
    db: &mut mysql::PooledConn,
    acting_as: Option<u64>,
    id: u64,
    title: String,
) -> Result<(), mysql::Error> {
    let user = acting_as.unwrap();

    // check that tags are active
    let mut res = db
        .query_iter(
            "SELECT  `tags`.* FROM `tags` \
             WHERE `tags`.`inactive` = 0 AND `tags`.`tag` IN ('test')",
        )
        .unwrap();
    let tag = res.next().unwrap().unwrap().get::<u32, _>("id").unwrap() as u32;
    drop(res);

    // get the category tag for 20% of stories
    let mut res = db
        .query_iter(
            "SELECT  `tags`.* FROM `tags` \
             WHERE `tags`.`inactive` = 0 AND `tags`.`tag` IN ('starwars')",
        )
        .unwrap();
    let starwars_tag = res.next().unwrap().unwrap().get::<u32, _>("id").unwrap() as u32;
    drop(res);

    db.query_drop(format!(
        "SELECT  1 AS one FROM `stories` \
         WHERE `stories`.`short_id` = '{}'",
        id,
    ))?;

    // TODO: check for similar stories if there's a url
    // SELECT  `stories`.*
    // FROM `stories`
    // WHERE `stories`.`url` IN (
    //  'https://google.com/test',
    //  'http://google.com/test',
    //  'https://google.com/test/',
    //  'http://google.com/test/',
    //  ... etc
    // )
    // AND (is_expired = 0 OR is_moderated = 1)

    // TODO
    // real impl queries `tags` and `users` again here..?

    // TODO: real impl checks *new* short_id and duplicate urls *again*
    // TODO: sometimes submit url

    // NOTE: MySQL technically does everything inside this and_then in a transaction,
    // but let's be nice to it
    let q = db.query_iter(format!(
        "INSERT INTO `stories` \
             (`created_at`, `user_id`, `title`, \
             `description`, `short_id`, `upvotes`, `hotness`, \
             `markeddown_description`) \
             VALUES (\'{}\', {}, \'{}\', \'{}\', \'{}\', {}, {}, \'{}\')",
        chrono::Local::now().naive_local(),
        user,
        title,
        "to infinity", // lorem ipsum?
        id,
        1,
        -19216.2884921 - id as f64,
        "<p>to infinity</p>\\n",
    ))?;
    // TODO this returned none?
    let story = id + 1; //q.last_insert_id().unwrap();
    drop(q);

    db.query_drop(format!(
        "INSERT INTO `taggings` (`story_id`, `tag_id`) \
         VALUES ({}, {})",
        story, tag
    ))?;

    // 20% of stories have the category tag
    if story % 5 == 0 {
        db.query_drop(format!(
            "INSERT INTO `taggings` (`story_id`, `tag_id`) \
             VALUES ({}, {})",
            story, starwars_tag
        ))?;
    }

    /*let key = format!("\'user:{}:stories_submitted\'", user);
    db.query_drop(format!(
        "INSERT INTO keystores (`key`, `value`) \
         VALUES ({}, {})",
         // not supported by parser ON DUPLICATE KEY UPDATE `keystores`.`value` = `keystores`.`value` + 1",
        key, 1),
    )?;*/

    // "priming"
    /*let key = format!("user:{}:stories_submitted", user);
    db.query_drop(format!(
        "SELECT  `keystores`.* \
         FROM `keystores` \
         WHERE `keystores`.`key` = {}",
        key,),
    )?;*/

    db.query_drop(format!(
        "SELECT  `votes`.* FROM `votes` \
         WHERE `votes`.`user_id` = {} \
         AND `votes`.`story_id` = {} \
         AND `votes`.`comment_id` IS NULL",
        user, story
    ))?;

    db.query_drop(format!(
        "INSERT INTO `votes` (`user_id`, `story_id`, `vote`) \
         VALUES ({}, {}, {})",
        user, story, 1
    ))?;

    db.query_drop(format!(
        "SELECT \
         `comments`.`upvotes`, \
         `comments`.`downvotes` \
         FROM `comments` \
         JOIN `stories` ON (`stories`.`id` = `comments`.`story_id`) \
         WHERE `comments`.`story_id` = {} \
         AND `comments`.`user_id` <> `stories`.`user_id`",
        story,
    ))?;

    db.query_drop(format!(
        "UPDATE `stories` \
         SET `hotness` = {} \
         WHERE `stories`.`id` = {}",
        -19216.5479744, story
    ))?;

    Ok(())
}
