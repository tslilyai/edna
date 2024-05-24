use crate::datagen::*;
use crate::*;
use rand::{distributions::Alphanumeric, Rng};

/*shepherd_val
 * Paper metadata:
 * - Each paper is assigned 1 leadContactId
 * - Each paper is assigned 1 managerContactId
 * - Accepted papers are assigned a shepherdId that is one of the reviewers
 * - Reviews and paper conflicts per paper
 */
const NREVIEWS: usize = 8;
const NCONFLICT_REVIEWER: usize = 2; // from pc
const NCONFLICT_AUTHOR: usize = 2; // from pool of other users
const NCOMMENTS: usize = 3; // made by reviewers, users w/authorship conflicts

pub fn get_random_string() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(12)
        .map(char::from)
        .collect()
}

/*pub fn get_schema_tables() -> Vec<disguise::TableColumns> {
    let mut table_cols = vec![];
    let stmts = helpers::get_create_schema_statements(SCHEMA, true);
    for stmt in stmts {
        match stmt {
            Statement::CreateTable(CreateTableStatement { name, columns, .. }) => {
                table_cols.push(disguise::TableColumns {
                    name: name.to_string(),
                    cols: columns.iter().map(|c| c.name.to_string()).collect(),
                    colformats: columns.iter().map(|c| helpers::get_parser_colformat(&c.data_type)).collect(),
                });
            }
            _ => unimplemented!("Not a create table statement?"),
        }
    }
    table_cols
}*/

pub fn populate_database(db: &mut mysql::PooledConn, args: &Cli) -> Result<(), mysql::Error> {
    let total_users = args.nusers_nonpc + args.nusers_pc;
    let other_uids: Vec<usize> = (1..args.nusers_nonpc + 1).collect();
    let pc_uids: Vec<usize> = (args.nusers_nonpc + 1..total_users + 1).collect();
    let papers_rej: Vec<usize> = (1..args.npapers_rej + 1).collect();
    let papers_acc: Vec<usize> =
        (args.npapers_rej + 1..(args.npapers_rej + args.npapers_accept + 1)).collect();

    // insert users
    warn!("INSERTING USERS");
    users::insert_users(args.nusers_nonpc + args.nusers_pc, db)?;

    // insert papers, author comments on papers, coauthorship conflicts
    warn!("INSERTING PAPERS");
    papers::insert_papers(
        &other_uids,
        &pc_uids,
        &papers_rej,
        &papers_acc,
        NCOMMENTS,
        NCONFLICT_AUTHOR,
        db,
    )?;

    // insert reviews, reviewer comments on papers, reviewer conflicts
    warn!("INSERTING REVIEWS");
    reviews::insert_reviews(
        &pc_uids,
        args.npapers_rej + args.npapers_accept,
        NREVIEWS,
        NCOMMENTS,
        NCONFLICT_REVIEWER,
        db,
    )?;

    Ok(())
}