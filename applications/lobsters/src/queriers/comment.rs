extern crate log;
extern crate mysql;

use log::warn;
use mysql::prelude::*;
use std::collections::HashSet;
use std::time;
use std::*;

pub fn get_comments(
    db: &mut mysql::PooledConn,
    acting_as: Option<u64>,
) -> Result<(), mysql::Error> {
    let mut comments = HashSet::new();
    let mut users = HashSet::new();
    let mut stories = HashSet::new();
    let start = time::Instant::now();
    db.query_map(
        &format!(
            "SELECT  `comments`.`id`, `comments`.`user_id`, `comments`.`story_id` \
             FROM `comments` \
             WHERE `comments`.`is_deleted` = 0 \
             AND `comments`.`is_moderated` = 0 \
             ORDER BY id DESC \
             LIMIT 40", // OFFSET 0",
        ),
        |(id, user_id, story_id): (u32, u64, u32)| {
            comments.insert(id);
            users.insert(user_id);
            stories.insert(story_id);
        },
    )?;
    warn!("\t select comments {}", start.elapsed().as_micros());

    let stories = stories
        .into_iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(",");

    if let Some(uid) = acting_as {
        let start = time::Instant::now();
        db.query_drop(&format!(
            "SELECT 1 FROM hidden_stories \
                     WHERE user_id = {} \
                     AND hidden_stories.story_id IN ({})",
            uid, stories
        ))?;
        warn!("\t select hidden {}", start.elapsed().as_micros());
    }

    assert!(!users.is_empty());
    let users = users
        .into_iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(",");
    let start = time::Instant::now();
    db.query_drop(&format!(
        "SELECT `users`.* FROM `users` \
             WHERE `users`.`id` IN ({})",
        users
    ))?;
    warn!("\t select users {}", start.elapsed().as_micros());

    let mut authors = HashSet::new();
    let start = time::Instant::now();
    db.query_map(
        &format!(
            "SELECT  `stories`.`user_id` FROM `stories` \
             WHERE `stories`.`id` IN ({})",
            stories
        ),
        |user_id: u64| authors.insert(user_id),
    )?;
    warn!("\t select authors {}", start.elapsed().as_micros());

    assert!(!authors.is_empty());
    if let Some(uid) = acting_as {
        let comments = comments
            .iter()
            .map(|id| format!("{}", id))
            .collect::<Vec<_>>()
            .join(",");
        let start = time::Instant::now();
        db.query_drop(&format!(
            "SELECT `votes`.* FROM `votes` \
                 WHERE `votes`.`user_id` = {} \
                 AND `votes`.`comment_id` IN ({})",
            uid, comments
        ))?;
        warn!("\t select votes {}", start.elapsed().as_micros());
    }

    // NOTE: the real website issues all of these one by one...
    let authors = authors
        .into_iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(",");

    let start = time::Instant::now();
    db.query_drop(&format!(
        "SELECT  `users`.* FROM `users` \
             WHERE `users`.`id` IN ({})",
        authors
    ))?;
    warn!("\t select users again {}", start.elapsed().as_micros());
    Ok(())
}

pub fn post_comment(
    db: &mut mysql::PooledConn,
    acting_as: Option<u64>,
    id: u64,
    story: u64,
    parent: Option<u64>,
) -> Result<(), mysql::Error> {
    let user = acting_as.unwrap();
    let (author, hotness, story): (u64, f64, u64) = db
        .query_first(format!(
            "SELECT `stories`.`user_id`, `stories`.`hotness`, stories.id \
             FROM `stories` \
             WHERE `stories`.`short_id` = '{}'",
            story
        ))?
        .unwrap();

    db.query_drop(format!(
        "SELECT `users`.* FROM `users` WHERE `users`.`id` = {}",
        author
    ))?;

    let parent: Option<(u64, u64)> = if let Some(parent) = parent {
        // check that parent exists
        if let Some(p) = db.query_first(format!(
            "SELECT  `comments`.id, comments.thread_id FROM `comments` \
                 WHERE `comments`.`story_id` = {} \
                 AND `comments`.`short_id` = '{}'",
            story, parent
        ))? {
            Some(p)
        } else {
            eprintln!(
                "failed to find parent comment {} in story {}",
                parent, story
            );
            None
        }
    } else {
        None
    };

    // TODO: real site checks for recent comments by same author with same
    // parent to ensure we don't double-post accidentally

    // check that short id is available
    db.query_drop(format!(
        "SELECT  1 AS one FROM `comments` \
         WHERE `comments`.`short_id` = '{}'",
        id
    ))?;

    // TODO: real impl checks *new* short_id *again*

    // NOTE: MySQL technically does everything inside this and_then in a transaction,
    // but let's be nice to it
    let now = chrono::Local::now().naive_local();
    let q = if let Some((parent, thread)) = parent {
        db.query_iter(format!(
            "INSERT INTO `comments` \
             (`created_at`, `updated_at`, `short_id`, `story_id`, \
             `user_id`, `parent_comment_id`, `thread_id`, \
             `comment`, `upvotes`, `confidence`, \
             `markeddown_comment`) \
             VALUES (\'{}\', \'{}\', \'{}\', {}, {}, {}, {}, {}, {}, {}, {})",
            now,
            now,
            id,
            story,
            user,
            parent,
            thread,
            "\'moar benchmarking\'", // lorem ipsum?
            1,
            0.1828847834138887,
            "\'<p>moar benchmarking</p>\\n\'",
        ))?
    } else {
        db.query_iter(format!(
            "INSERT INTO `comments` \
             (`created_at`, `updated_at`, `short_id`, `story_id`, \
             `user_id`, `thread_id`, `comment`, `upvotes`, `confidence`, \
             `markeddown_comment`) \
             VALUES (\'{}\', \'{}\', \'{}\', {}, {}, {}, {}, {}, {}, {})",
            now,
            now,
            id,
            story,
            user,
            id,
            "\'moar benchmarking\'", // lorem ipsum?
            1,
            0.1828847834138887,
            "\'<p>moar benchmarking</p>\\n\'",
        ))?
    };
    // TODO last insert ID not working?
    //q.last_insert_id().unwrap();
    drop(q);
    let comment: u64 = db
        .query_first(format!(
            "SELECT comments.id FROM comments WHERE comments.short_id='{}'",
            id
        ))?
        .unwrap();

    db.query_drop(format!(
        "SELECT  `votes`.* FROM `votes` \
         WHERE `votes`.`user_id` = {} \
         AND `votes`.`story_id` = {} \
         AND `votes`.`comment_id` = {}",
        user, story, comment,
    ))?;

    db.query_drop(format!(
        "INSERT INTO `votes` \
         (`user_id`, `story_id`, `comment_id`, `vote`) \
         VALUES ({}, {}, {}, {})",
        user, story, comment, 1,
    ))?;

    db.query_drop(format!(
        "SELECT `stories`.`id` \
         FROM `stories` \
         WHERE `stories`.`merged_story_id` = {}",
        story,
    ))?;

    // why are these ordered?
    let res: Vec<(u64, u64)> = db.query(format!(
        "SELECT `comments`.`id`, \
         `comments`.`upvotes` + `comments`.`downvotes` AS saldo \
         FROM `comments` \
         WHERE `comments`.`story_id` = {} \
         ORDER BY \
         saldo ASC, \
         confidence DESC",
        story
    ))?;
    let count = res.len() + 1;

    db.query_drop(format!(
        "UPDATE `stories` \
        SET `comments_count` = {} 
        WHERE `stories`.`id` = {}",
        count, story
    ))?;

    db.query_drop(format!(
        "SELECT `tags`.* \
         FROM `tags` \
         INNER JOIN `taggings` \
         ON `tags`.`id` = `taggings`.`tag_id` \
         WHERE `taggings`.`story_id` = {}",
        story,
    ))?;

    db.query_drop(format!(
        "SELECT \
         `comments`.`upvotes`, \
         `comments`.`downvotes` \
         FROM `comments` \
         JOIN `stories` ON (`stories`.`id` = `comments`.`story_id`) \
         WHERE `comments`.`story_id` = {} \
         AND `comments`.`user_id` <> `stories`.`user_id`",
        story
    ))?;

    db.query_drop(format!(
        "SELECT `stories`.`id` \
         FROM `stories` \
         WHERE `stories`.`merged_story_id` = {}",
        story
    ))?;

    // why oh why is story hotness *updated* here?!
    db.query_drop(format!(
        "UPDATE `stories` \
         SET `hotness` = {} \
         WHERE `stories`.`id` = {}",
        hotness - 1.0,
        story,
    ))?;

    /*let key = format!("\'user:{}:comments_posted\'", user);
    db.query_drop(format!(
        "INSERT INTO keystores (`key`, `value`) \
         VALUES ({}, {})",
         //ON DUPLICATE KEY UPDATE `keystores`.`value` = `keystores`.`value` + 1",
        key, 1,
    ))?;*/
    Ok(())
}
