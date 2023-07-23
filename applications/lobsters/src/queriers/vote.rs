extern crate log;
extern crate mysql;

use mysql::prelude::*;
use std::*;
//use log::{warn, debug};
use log::error;

pub fn vote_on_comment(
    db: &mut mysql::PooledConn,
    acting_as: Option<u64>,
    comment: u64,
    pos: bool,
) -> Result<(), mysql::Error> {
    let user = acting_as.unwrap();

    match db.query_first::<(u64, u32, u32, u32, u32), String>(format!(
        "SELECT `comments`.`user_id`, `comments`.`story_id`, \
                `comments`.`upvotes`, `comments`.`downvotes`, \
                `comments`.`id` \
             FROM `comments` \
             WHERE `comments`.`short_id` = '{}'",
        comment
    ))? {
        Some((author, sid, upvotes, downvotes, comment)) => {
            db.query_drop(format!(
                "SELECT  `votes`.* \
                     FROM `votes` \
                     WHERE `votes`.`user_id` = {} \
                     AND `votes`.`story_id` = {} \
                     AND `votes`.`comment_id` = {}",
                user, sid, comment,
            ))?;

            // TODO: do something else if user has already voted
            // TODO: technically need to re-load comment under transaction

            // NOTE: MySQL technically does everything inside this and_then in a transaction,
            // but let's be nice to it
            db.query_drop(format!(
                "INSERT INTO `votes` \
                     (`user_id`, `story_id`, `comment_id`, `vote`) \
                     VALUES \
                     ({}, {}, {}, {})",
                user,
                sid,
                comment,
                match pos {
                    true => 1,
                    false => 0,
                },
            ))?;

            db.query_drop(format!(
                "UPDATE `users` \
                        SET `karma` = `karma` {} \
                        WHERE `users`.`id` = {}",
                match pos {
                    true => "+ 1",
                    false => "- 1",
                },
                author
            ))?;

            // approximate Comment::calculate_hotness
            let confidence = upvotes as f64 / (upvotes as f64 + downvotes as f64);
            db.query_drop(format!(
                "UPDATE `comments` \
                         SET \
                         `upvotes` = `upvotes` {}, \
                         `downvotes` = `downvotes` {}, \
                         `confidence` = {} \
                         WHERE `id` = {}",
                match pos {
                    true => "+ 1",
                    false => "+ 0",
                },
                match pos {
                    true => "+ 0",
                    false => "+ 1",
                },
                confidence,
                comment
            ))?;

            // get all the stuff needed to compute updated hotness
            let score: Option<f64> = db.query_first(format!(
                "SELECT `stories`.`hotness` \
                     FROM `stories` \
                     WHERE `stories`.`id` = {}",
                sid,
            ))?;
            let score = score.unwrap();

            db.query_drop(format!(
                "SELECT `tags`.* \
                     FROM `tags` \
                     INNER JOIN `taggings` ON `tags`.`id` = `taggings`.`tag_id` \
                     WHERE `taggings`.`story_id` = {}",
                sid
            ))?;

            db.query_drop(format!(
                "SELECT \
                     `comments`.`upvotes`, \
                     `comments`.`downvotes` \
                     FROM `comments` \
                     JOIN `stories` ON (`stories`.`id` = `comments`.`story_id`) \
                     WHERE `comments`.`story_id` = {} \
                     AND `comments`.`user_id` <> `stories`.`user_id`",
                sid
            ))?;

            db.query_drop(format!(
                "SELECT `stories`.`id` \
                     FROM `stories` \
                     WHERE `stories`.`merged_story_id` = {}",
                sid
            ))?;

            // the *actual* algorithm for computing hotness isn't all
            // that interesting to us. it does affect what's on the
            // frontpage, but we're okay with using a more basic
            // upvote/downvote ratio thingy. See Story::calculated_hotness
            // in the lobsters source for details.
            db.query_drop(&format!(
                "UPDATE stories SET \
                         upvotes = upvotes {}, \
                         downvotes = downvotes {}, \
                         hotness = {} \
                         WHERE id = {}",
                match pos {
                    true => "+ 1",
                    false => "+ 0",
                },
                match pos {
                    true => "+ 0",
                    false => "+ 1",
                },
                score
                    - match pos {
                        true => 1.0,
                        false => -1.0,
                    },
                sid,
            ))?;

            Ok(())
        }
        _ => {
            error!("Could not find comment for vote with id {}", comment);
            Ok(())
        }
    }
}

pub fn vote_on_story(
    db: &mut mysql::PooledConn,
    acting_as: Option<u64>,
    story_id: u64,
    pos: bool,
) -> Result<(), mysql::Error> {
    let user = acting_as.unwrap();
    let (author, score, story): (u64, f64, u64) = db.query(format!(
        "SELECT `stories`.user_id, stories.hotness, stories.id \
                 FROM `stories` \
                 WHERE `stories`.`short_id` = '{}'",
        story_id
    ))?[0];
    db.query_drop(format!(
        "SELECT  `votes`.* \
         FROM `votes` \
         WHERE `votes`.`user_id` = {} \
         AND `votes`.`story_id` = {} \
         AND `votes`.`comment_id` IS NULL",
        user, story
    ))?;

    // TODO: do something else if user has already voted
    // TODO: technically need to re-load story under transaction

    // NOTE: MySQL technically does everything inside this and_then in a transaction,
    // but let's be nice to it
    db.query_drop(format!(
        "INSERT INTO `votes` \
         (`user_id`, `story_id`, `vote`) \
         VALUES \
         ({}, {}, {})",
        user,
        story,
        match pos {
            true => 1,
            false => 0,
        },
    ))?;

    db.query_drop(format!(
        "UPDATE `users` \
         SET `karma` = `karma` {} \
         WHERE `users`.`id` = {}",
        match pos {
            true => "+ 1",
            false => "- 1",
        },
        author
    ))?;

    // get all the stuff needed to compute updated hotness
    db.query_drop(format!(
        "SELECT `tags`.* \
         FROM `tags` \
         INNER JOIN `taggings` ON `tags`.`id` = `taggings`.`tag_id` \
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
        story,
    ))?;

    db.query_drop(format!(
        "SELECT `stories`.`id` \
         FROM `stories` \
         WHERE `stories`.`merged_story_id` = {}",
        story,
    ))?;

    // the *actual* algorithm for computing hotness isn't all
    // that interesting to us. it does affect what's on the
    // frontpage, but we're okay with using a more basic
    // upvote/downvote ratio thingy. See Story::calculated_hotness
    // in the lobsters source for details.
    db.query_drop(format!(
        "UPDATE stories SET \
         upvotes = stories.upvotes {}, \
         downvotes = stories.downvotes {}, \
         hotness = {} \
         WHERE stories.id = {}",
        match pos {
            true => "+ 1",
            false => "+ 0",
        },
        match pos {
            true => "+ 0",
            false => "+ 1",
        },
        score
            - match pos {
                true => 1.0,
                false => -1.0,
            },
        story_id,
    ))?;
    Ok(())
}
