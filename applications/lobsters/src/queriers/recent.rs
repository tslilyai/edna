extern crate log;
extern crate mysql;

use log::warn;
use mysql::prelude::*;
use std::collections::HashSet;
use std::time;
use std::*;

pub fn recent(db: &mut mysql::PooledConn, acting_as: Option<u64>) -> Result<(), mysql::Error> {
    // /recent is a little weird:
    // https://github.com/lobsters/lobsters/blob/50b4687aeeec2b2d60598f63e06565af226f93e3/app/models/story_repository.rb#L41
    // but it *basically* just looks for stories in the past few days
    // because all our stories are for the same day, we add a LIMIT
    // also note the `NOW()` hack to support dbs primed a while ago
    let mut users = HashSet::new();
    let mut stories = HashSet::new();
    // XXX removed cast
    let start = time::Instant::now();
    // add USE INDEX to avoid index_merge_intersection
    // same as running set session optimizer_switch="index_merge_intersection=off";
    db.query_map(
        "SELECT `stories`.`user_id`, `stories`.`id` \
             FROM `stories` \
             USE INDEX (index_stories_on_is_expired) \
             WHERE `stories`.`merged_story_id` IS NULL \
             AND `stories`.`is_expired` = 0 \
             AND CAST(upvotes as signed) - CAST(downvotes as signed) <= 5 \
             ORDER BY `id` DESC LIMIT 51",
        //AND saldo <= 5 \
        |(uid, id): (u64, u32)| {
            users.insert(uid);
            stories.insert(id);
        },
    )?;
    warn!("\t select stories {}", start.elapsed().as_micros());

    assert!(!stories.is_empty(), "got no stories from /recent");

    assert!(!stories.is_empty());
    let stories_in = stories
        .iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(",");
    if let Some(uid) = acting_as {
        let start = time::Instant::now();
        db.query_drop(format!(
            "SELECT `hidden_stories`.`story_id` \
                 FROM `hidden_stories` \
                 WHERE `hidden_stories`.`user_id` = {}",
            uid,
        ))?;
        warn!("\t select hiddenstories {}", start.elapsed().as_micros());
        let start = time::Instant::now();
        db.query_drop(format!(
            "SELECT `tag_filters`.* FROM `tag_filters` \
                 WHERE `tag_filters`.`user_id` = {}",
            uid
        ))?;
        warn!("\t select tag filters {}", start.elapsed().as_micros());
        let start = time::Instant::now();
        db.query_drop(format!(
            "SELECT `taggings`.`story_id` \
                 FROM `taggings` \
                 WHERE `taggings`.`story_id` IN ({})",
            //AND `taggings`.`tag_id` IN ({})",
            stories_in,
            //tags
        ))?;
        warn!("\t select taggings {}", start.elapsed().as_micros());
    }

    assert!(!users.is_empty());
    let users = users
        .into_iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(",");
    let start = time::Instant::now();
    db.query_drop(format!(
        "SELECT `users`.* FROM `users` WHERE `users`.`id` IN ({})",
        users,
    ))?;
    warn!("\t select users {}", start.elapsed().as_micros());

    let start = time::Instant::now();
    db.query_drop(format!(
        "SELECT `suggested_titles`.* \
             FROM `suggested_titles` \
             WHERE `suggested_titles`.`story_id` IN ({})",
        stories_in
    ))?;
    warn!("\t select titles {}", start.elapsed().as_micros());

    let start = time::Instant::now();
    db.query_drop(format!(
        "SELECT `suggested_taggings`.* \
             FROM `suggested_taggings` \
             WHERE `suggested_taggings`.`story_id` IN ({})",
        stories_in
    ))?;
    warn!(
        "\t select suggestedtaggings {}",
        start.elapsed().as_micros()
    );

    let mut tags = HashSet::new();
    if !tags.is_empty() {
        let start = time::Instant::now();
        db.query_map(
            &format!(
                "SELECT `taggings`.`tag_id` FROM `taggings` \
                 WHERE `taggings`.`story_id` IN ({})",
                stories_in
            ),
            |tag_id: u32| tags.insert(tag_id),
        )?;
        warn!("\t select taggings {}", start.elapsed().as_micros());

        let start = time::Instant::now();
        let tags = tags
            .into_iter()
            .map(|id| format!("{}", id))
            .collect::<Vec<_>>()
            .join(",");
        db.query_drop(&format!(
            "SELECT `tags`.* FROM `tags` WHERE `tags`.`id` IN ({})",
            tags
        ))?;
        warn!("\t select tags {}", start.elapsed().as_micros());
    }

    // also load things that we need to highlight
    if let Some(uid) = acting_as {
        let stories = stories
            .into_iter()
            .map(|id| format!("{}", id))
            .collect::<Vec<_>>()
            .join(",");

        let start = time::Instant::now();
        db.query_drop(&format!(
            "SELECT `votes`.* FROM `votes` \
                     WHERE `votes`.`user_id` = {} \
                     AND `votes`.`story_id` IN ({}) \
                     AND `votes`.`comment_id` IS NULL",
            uid, stories
        ))?;
        warn!("\t select votes {}", start.elapsed().as_micros());

        let start = time::Instant::now();
        db.query_drop(&format!(
            "SELECT `hidden_stories`.* \
                     FROM `hidden_stories` \
                     WHERE `hidden_stories`.`user_id` = {} \
                     AND `hidden_stories`.`story_id` IN ({})",
            uid, stories
        ))?;
        warn!("\t select hiddenstories2 {}", start.elapsed().as_micros());

        let start = time::Instant::now();
        db.query_drop(&format!(
            "SELECT `saved_stories`.* \
                     FROM `saved_stories` \
                     WHERE `saved_stories`.`user_id` = {} \
                     AND `saved_stories`.`story_id` IN ({})",
            uid, stories
        ))?;
        warn!("\t select savedstories {}", start.elapsed().as_micros());
    }

    Ok(())
}
