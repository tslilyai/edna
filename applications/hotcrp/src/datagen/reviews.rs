use crate::datagen::*;
use edna::helpers::*;
use mysql::prelude::*;
use mysql::Value::Bytes;
use rand::seq::SliceRandom;
use sql_parser::ast::*;

fn get_review_cols() -> Vec<&'static str> {
    vec!["paperId", "contactId", "requestedBy"]
}

fn get_review_vals(paper_id: usize, reviewer: usize) -> Vec<Expr> {
    vec![
        Expr::Value(Value::Number(paper_id.to_string())),
        Expr::Value(Value::Number(reviewer.to_string())),
        Expr::Value(Value::Number(reviewer.to_string())),
    ]
}

pub fn insert_reviews(
    users_pc: &Vec<usize>,
    npapers: usize,
    nreviews: usize,
    ncomments: usize,
    nconflicts: usize,
    db: &mut mysql::PooledConn,
) -> Result<(), mysql::Error> {
    let mut new_reviews = vec![];
    let paper_ids: Vec<usize> = (1..npapers).collect();
    for pid in paper_ids {
        let pc_mems: Vec<&usize> = users_pc
            .choose_multiple(&mut rand::thread_rng(), nconflicts + nreviews)
            .collect();
        let reviewers = &pc_mems[nconflicts..];

        // insert some number of conflicts
        for conflict in 0..nconflicts {
            insert_paper_conflict(pid, *pc_mems[conflict], CONFLICT_PCMASK, db)?;
        }
        // insert reviewer reviews and paper watches
        // assume reviewers preferred to review this paper
        for rid in 1..nreviews + 1 {
            let reviewer = *reviewers[rid - 1];
            new_reviews.push(get_review_vals(pid, reviewer));
            insert_paper_watch(pid, reviewer, db)?;
            insert_review_preference(pid, reviewer, db)?;
        }

        // insert comments by reviewers
        for i in 1..ncomments + 1 {
            insert_paper_comment(pid, *reviewers[i % nreviews], db)?;
        }
    }
    let review_cols = get_review_cols();
    get_query_rows_prime(
        &Statement::Insert(InsertStatement {
            table_name: string_to_objname("PaperReview"),
            columns: review_cols
                .iter()
                .map(|c| Ident::new(c.to_string()))
                .collect(),
            source: InsertSource::Query(Box::new(values_query(new_reviews))),
        }),
        db,
    )?;
    Ok(())
}

pub fn get_reviews(uid: u64, db: &mut mysql::PooledConn) -> Result<Vec<u64>, mysql::Error> {
    let res = db.query_iter(&format!(
        "SELECT ReviewId FROM PaperReview WHERE ContactId = {}",
        uid
    ))?;
    let mut ids = vec![];
    for row in res {
        let rowvals = row.unwrap().unwrap();
        ids.push(u64::from_value(rowvals[0].clone()));
    }
    Ok(ids)
}

pub fn update_review(rid: u64, db: &mut mysql::PooledConn) -> Result<(), mysql::Error> {
    let res = db.query_iter(&format!(
        "SELECT data FROM PaperReview WHERE ReviewId = {}",
        rid
    ))?;
    let mut newdata = "hello world".to_string();
    for row in res {
        let data = &row.unwrap().unwrap()[0];
        match data {
            Bytes(vs) => {
                let mut vs = vs.clone();
                vs.append(&mut newdata.as_bytes().to_vec());
                newdata = String::from_utf8(vs.to_vec()).unwrap()
            }
            _ => (),
        }
        break;
    }
    db.query_drop(&format!(
        "UPDATE PaperReview SET data = \'{}\' WHERE ReviewId = {}",
        newdata, rid
    ))?;
    Ok(())
}
