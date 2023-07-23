extern crate log;
extern crate mysql;

use mysql::prelude::*;
use std::*;

pub fn login(db: &mut mysql::PooledConn, uid: u64) -> Result<(), mysql::Error> {
    let _user: Option<u64> = db.query_first(format!(
        "SELECT 1 as one FROM `users` WHERE `users`.`username` = 'user{}'",
        uid - 1
    ))?;
    /*if user.is_none() {
        db.query_drop(format!("INSERT INTO `users` (`username`) VALUES ('user{}')",uid-1))?;
    }*/
    Ok(())
}

pub fn get_profile(db: &mut mysql::PooledConn, uid: u64) -> Result<(), mysql::Error> {
    let uids: Vec<u64> = db.query(format!(
        "SELECT `users`.id FROM `users` \
             WHERE `users`.`username` = {}",
        (format!("\'user{}\'", uid - 1))
    ))?;
    if uids.is_empty() {
        return Ok(());
    }
    let uid = uids[0];

    let rows: Vec<(u64, u64)> = db.query(format!(
        "SELECT  `tags`.`id`, COUNT(*) AS `count` FROM `taggings` \
             INNER JOIN `tags` ON `taggings`.`tag_id` = `tags`.`id` \
             INNER JOIN `stories` ON `stories`.`id` = `taggings`.`story_id` \
             WHERE `tags`.`inactive` = 0 \
             AND `stories`.`user_id` = {} \
             GROUP BY `tags`.`id` \
             ORDER BY `count` desc LIMIT 1",
        uid
    ))?;

    if !rows.is_empty() {
        let tag: u64 = rows[0].0;
        db.query_drop(format!(
            "SELECT  `tags`.* \
             FROM `tags` \
             WHERE `tags`.`id` = {}",
            tag,
        ))?;
    }
    db.query_drop(format!(
        "SELECT  `keystores`.* \
         FROM `keystores` \
         WHERE `keystores`.`key` = {}",
        (format!("\'user:{}:stories_submitted\'", uid))
    ))?;
    db.query_drop(format!(
        "SELECT  `keystores`.* \
         FROM `keystores` \
         WHERE `keystores`.`key` = {}",
        (format!("\'user:{}:comments_posted\'", uid))
    ))?;
    db.query_drop(format!(
        "SELECT  1 AS one FROM `hats` \
         WHERE `hats`.`user_id` = {} LIMIT 1",
        uid
    ))?;
    Ok(())
}
