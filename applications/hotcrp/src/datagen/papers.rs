use crate::datagen::*;
use edna::helpers::*;
use log::debug;
use rand::seq::SliceRandom;
use sql_parser::ast::*;

fn get_paper_cols() -> Vec<&'static str> {
    vec![
        "paperId",
        "outcome",
        "leadContactId",
        "shepherdContactId",
        "managerContactId",
    ]
}

fn get_paper_vals(
    paper_id: usize,
    lead_author: usize,
    manager: usize,
    accepted: u64,
    shepherd: Option<usize>,
) -> Vec<Expr> {
    let shepherd_val = match shepherd {
        None => 0,
        Some(s) => s,
    };
    debug!("Creating paper with shepherd {}", shepherd_val);
    vec![
        Expr::Value(Value::Number(paper_id.to_string())),
        Expr::Value(Value::Number(accepted.to_string())),
        Expr::Value(Value::Number(lead_author.to_string())),
        Expr::Value(Value::Number(shepherd_val.to_string())),
        Expr::Value(Value::Number(manager.to_string())),
    ]
}

pub fn insert_papers(
    users_nonpc: &Vec<usize>,
    users_pc: &Vec<usize>,
    papers_rej: &Vec<usize>,
    papers_acc: &Vec<usize>,
    ncomments: usize,
    nconflicts: usize,
    db: &mut mysql::PooledConn,
) -> Result<(), mysql::Error> {
    let mut new_papers = vec![];
    for pid in papers_rej {
        let authors: Vec<&usize> = users_nonpc
            .choose_multiple(&mut rand::thread_rng(), nconflicts)
            .collect();

        // insert conflict and paper watch for every author
        for a in &authors {
            insert_paper_conflict(*pid, **a, CONFLICT_AUTHOR, db)?;
            insert_paper_watch(*pid, **a, db)?;
        }

        // insert paper comments by authors
        for i in 1..ncomments + 1 {
            insert_paper_comment(*pid, *authors[i % nconflicts], db)?;
        }

        let manager = users_pc.choose(&mut rand::thread_rng()).unwrap();
        new_papers.push(get_paper_vals(*pid, *authors[0], *manager, 0, None));
    }
    for pid in papers_acc {
        let authors: Vec<&usize> = users_nonpc
            .choose_multiple(&mut rand::thread_rng(), nconflicts)
            .collect();

        // insert authors conflicts
        for a in &authors {
            insert_paper_conflict(*pid, **a, CONFLICT_AUTHOR, db)?;
            insert_paper_watch(*pid, **a, db)?;
        }

        // insert paper comments by authors
        for i in 1..ncomments + 1 {
            insert_paper_comment(*pid, *authors[i % nconflicts], db)?;
        }

        let manager = users_pc.choose(&mut rand::thread_rng()).unwrap();
        // for now, shepherd is just a random pc member (not necessarily a reviewer)
        let shepherd = users_pc.choose(&mut rand::thread_rng()).unwrap();
        debug!("Creating new accepted paper with shepherd {}", shepherd);
        new_papers.push(get_paper_vals(
            *pid,
            *authors[0],
            *manager,
            1,
            Some(*shepherd),
        ));
    }

    let paper_cols = get_paper_cols();
    get_query_rows_prime(
        &Statement::Insert(InsertStatement {
            table_name: string_to_objname("Paper"),
            columns: paper_cols
                .iter()
                .map(|c| Ident::new(c.to_string()))
                .collect(),
            source: InsertSource::Query(Box::new(values_query(new_papers))),
        }),
        db,
    )?;
    Ok(())
}
