use crate::{queriers, COMMENTS_PER_STORY, VOTES_PER_COMMENT, VOTES_PER_STORY, VOTES_PER_USER};
use chrono::Local;
use histogram_sampler;
use log::{error, warn};
use mysql::prelude::*;
use rand::prelude::*;
use rand_distr::Distribution;

// taken from jonhoo.trawler

#[derive(Clone, Debug)]
pub struct Sampler {
    pub votes_per_user: histogram_sampler::Sampler,
    pub votes_per_story: histogram_sampler::Sampler,
    pub votes_per_comment: histogram_sampler::Sampler,
    pub comments_per_story: histogram_sampler::Sampler,
}

use rand;
impl Sampler {
    pub fn new(scale: f64) -> Self {
        fn adjust<'a, F>(
            hist: &'static [(usize, usize)],
            f: F,
        ) -> impl Iterator<Item = (usize, usize)>
        where
            F: Fn(f64) -> f64,
        {
            hist.into_iter()
                .map(move |&(bin, n)| (bin, f(n as f64).round() as usize))
        }
        let votes_per_user = adjust(VOTES_PER_USER, |n| n * scale);

        let votes_per_story = adjust(VOTES_PER_STORY, |n| n * scale);

        let votes_per_comment = adjust(VOTES_PER_COMMENT, |n| n * scale);

        let comments_per_story = adjust(COMMENTS_PER_STORY, |n| n * scale);

        Sampler {
            votes_per_user: histogram_sampler::Sampler::from_bins(votes_per_user, 100),
            votes_per_story: histogram_sampler::Sampler::from_bins(votes_per_story, 10),
            votes_per_comment: histogram_sampler::Sampler::from_bins(votes_per_comment, 10),
            comments_per_story: histogram_sampler::Sampler::from_bins(comments_per_story, 10),
        }
    }

    pub fn user<R: rand::Rng>(&self, rng: &mut R) -> u32 {
        self.votes_per_user.sample(rng) as u32 + 1
    }

    pub fn nusers(&self) -> u32 {
        self.votes_per_user.nvalues() as u32
    }

    pub fn comment_for_vote<R: rand::Rng>(&self, rng: &mut R) -> u32 {
        self.votes_per_comment.sample(rng) as u32
    }

    pub fn story_for_vote<R: rand::Rng>(&self, rng: &mut R) -> u32 {
        self.votes_per_story.sample(rng) as u32
    }

    pub fn nstories(&self) -> u32 {
        std::cmp::max(
            self.votes_per_story.nvalues(),
            self.comments_per_story.nvalues(),
        ) as u32
    }

    pub fn story_for_comment<R: rand::Rng>(&self, rng: &mut R) -> u32 {
        self.comments_per_story.sample(rng) as u32
    }

    pub fn ncomments(&self) -> u32 {
        self.votes_per_comment.nvalues() as u32
    }
}

/*pub fn tag_stories(sampler: &Sampler, db: &mut mysql::PooledConn) {
    let id: u64 = db
        .query_first(r"SELECT `id` FROM `tags` WHERE `tag` = 'starwars'")
        .unwrap()
        .unwrap();
    for s in 1..sampler.nstories() {
        if s % 5 == 0 {
            db.query_drop(format!(
                r"INSERT INTO taggings (tag_id, story_id) VALUES ({}, {});",
                id, s
            ))
            .unwrap();
        }
    }
}*/

pub fn gen_data(sampler: &Sampler, db: &mut mysql::PooledConn) -> (u32, u32) {
    let nstories = sampler.nstories();
    let mut rng = rand::thread_rng();
    println!(
        "Generating {} stories, {} comments, {} users",
        nstories,
        sampler.ncomments(),
        sampler.nusers()
    );
    db.query_drop("INSERT INTO `tags` (`tag`) VALUES ('test');")
        .unwrap();
    db.query_drop("INSERT INTO `tags` (`tag`) VALUES ('starwars');")
        .unwrap();
    let mut users = vec![];
    for uid in 0..sampler.nusers() {
        warn!("Generating user {}", uid);
        users.push(format!(
            "('user{}', '{}')",
            uid,
            Local::now().naive_local().to_string()
        ));
    }
    db.query_drop(format!(
        "INSERT INTO `users` (`username`, `last_login`) VALUES {}",
        users.join(", ")
    ))
    .unwrap();
    error!("Created {} users", sampler.nusers());

    for id in 0..nstories {
        // NOTE: we're assuming that users who vote much also submit many stories
        let user_id = Some(sampler.user(&mut rng) as u64);
        warn!("Generating story {} for user {:?}", id, user_id);
        queriers::stories::post_story(db, user_id, id.into(), format!("Base article {}", id))
            .unwrap();
    }
    error!("Created {} stories", nstories);
    for id in 0..sampler.ncomments() {
        // NOTE: assume that users who vote much also submit many stories
        let story_shortid = id % nstories; // TODO: distribution
        let user_id = Some(sampler.user(&mut rng) as u64);
        let parent = if rng.gen_bool(0.5) {
            // half the time, send a message to the story author
            warn!(
                "Generating message {} for user {:?} for story {}",
                id, user_id, story_shortid
            );
            queriers::message::post_message_about_story_from(db, id, user_id, story_shortid)
                .unwrap();

            // we need to pick a parent in the same story
            let generated_comments = id - story_shortid;
            // how many stories do we know there are per story?
            let generated_comments_per_story = generated_comments / nstories;
            // pick the nth comment to chosen story
            if generated_comments_per_story != 0 {
                let story_comment = rng.gen_range(0, generated_comments_per_story);
                Some((story_shortid + nstories * story_comment) as u64)
            } else {
                None
            }
        } else {
            None
        };
        warn!(
            "Generating comment {} from user {:?} and story{}, parent {:?}",
            id, user_id, story_shortid, parent
        );
        queriers::comment::post_comment(db, user_id, id.into(), story_shortid.into(), parent)
            .unwrap();
        if id % 1000 == 0 {
            error!("Created {}/{} comments", id, sampler.ncomments());
        }
    }
    let nstories = sampler.nstories();
    let ncomments = sampler.ncomments();
    (nstories, ncomments)
}
