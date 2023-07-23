extern crate log;
extern crate mysql;

use mysql::prelude::*;
use std::collections::HashSet;
use std::*;
//use log::{warn, debug};

pub fn query_frontpage(
    db: &mut mysql::PooledConn,
    acting_as: Option<u64>,
) -> Result<Vec<String>, mysql::Error> {
    let mut result = vec![];
    let mut users: HashSet<u64> = HashSet::new();
    let mut stories: HashSet<u64> = HashSet::new();
    db.query_map(
        "SELECT  `stories`.`user_id`, `stories`.`id` FROM `stories` \
         WHERE `stories`.`merged_story_id` IS NULL \
         AND `stories`.`is_expired` = 0 \
         AND `stories`.`upvotes` - `stories`.`downvotes` >= 0 \
         ORDER BY hotness LIMIT 51",
        // OFFSET 0" parser can't handle this for some reason,
        |(user_id, id)| {
            users.insert(user_id);
            stories.insert(id);
            result.push(format!("(user+storyids: {},{})", user_id, id));
        },
    )?;
    assert!(!stories.is_empty(), "got no stories from /frontpage");

    let stories_in = stories
        .iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(",");

    if let Some(uid) = acting_as {
        db.query_map(
            format!(
                "SELECT `hidden_stories`.`story_id` \
             FROM `hidden_stories` \
             WHERE `hidden_stories`.`user_id` = {}",
                uid
            ),
            |hiddenid: u64| result.push(format!("(hidden_id: {})", hiddenid)),
        )?;

        db.query_map(
            format!(
                "SELECT `tag_filters`.`id` FROM `tag_filters` \
             WHERE `tag_filters`.`user_id` = {}",
                uid
            ),
            |tag_filters: u64| result.push(format!("(tagfilers: {:?})", tag_filters)),
        )?;

        db.query_map(
            format!(
                "SELECT `taggings`.`story_id` \
             FROM `taggings` \
             WHERE `taggings`.`story_id` IN ({})",
                // AND `taggings`.`tag_id` IN ({})",
                stories_in,
                //tags
            ),
            |tagging_story_ids: u64| {
                result.push(format!("(tagging_story_id: {})", tagging_story_ids))
            },
        )?;
    }

    let users = users
        .into_iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(",");
    db.query_map(
        &format!(
            "SELECT `users`.`username` FROM `users` WHERE `users`.`id` IN ({})",
            users
        ),
        |users: String| result.push(format!("(user: {:?})", users)),
    )?;

    db.query_map(
        &format!(
            "SELECT `suggested_titles`.`id` \
         FROM `suggested_titles` \
         WHERE `suggested_titles`.`story_id` IN ({})",
            stories_in
        ),
        |suggestion: u64| result.push(format!("(suggestion: {:?})", suggestion)),
    )?;

    db.query_map(
        &format!(
            "SELECT `suggested_taggings`.`id` \
         FROM `suggested_taggings` \
         WHERE `suggested_taggings`.`story_id` IN ({})",
            stories_in
        ),
        |suggestion: u64| result.push(format!("(suggested tag: {:?})", suggestion)),
    )?;

    let mut tags: HashSet<u64> = HashSet::new();
    db.query_map(
        &format!(
            "SELECT `taggings`.`tag_id` FROM `taggings` \
             WHERE `taggings`.`story_id` IN ({})",
            stories_in
        ),
        |tag_id: u64| {
            tags.insert(tag_id);
            result.push(format!("(tagid: {})", tag_id));
        },
    )?;

    let tags = tags
        .into_iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(",");
    if tags.len() > 0 {
        db.query_map(
            &format!(
                "SELECT `tags`.`id` FROM `tags` WHERE `tags`.`id` IN ({})",
                tags
            ),
            |tag: u64| result.push(format!("(tag: {:?})", tag)),
        )?;
    }

    // also load things that we need to highlight
    if let Some(uid) = acting_as {
        let story_params = stories
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .join(",");
        db.query_map(
            &format!(
                "SELECT `votes`.`id` FROM `votes` \
                     WHERE `votes`.`user_id` = {} \
                     AND `votes`.`story_id` IN ({}) \
                     AND `votes`.`comment_id` IS NULL",
                uid, story_params
            ),
            |vote: u64| result.push(format!("(vote: {:?})", vote)),
        )?;

        db.query_map(
            &format!(
                "SELECT `hidden_stories`.`id` \
                     FROM `hidden_stories` \
                     WHERE `hidden_stories`.`user_id` = {} \
                     AND `hidden_stories`.`story_id` IN ({})",
                uid, story_params
            ),
            |hidden: u64| result.push(format!("(hidden: {:?})", hidden)),
        )?;

        db.query_map(
            &format!(
                "SELECT `saved_stories`.`id` \
                     FROM `saved_stories` \
                     WHERE `saved_stories`.`user_id` = {} \
                     AND `saved_stories`.`story_id` IN ({})",
                uid, story_params
            ),
            |saved: u64| result.push(format!("(saved: {:?})", saved)),
        )?;
    }
    Ok(result)
}
