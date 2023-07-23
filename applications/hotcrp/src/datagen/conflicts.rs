use edna::helpers::*;
use sql_parser::ast::*;

pub const CONFLICT_AUTHOR: usize = 32;
pub const CONFLICT_PCMASK: usize = 31;

pub fn insert_paper_conflict(
    paper_id: usize,
    user_id: usize,
    conflict_type: usize,
    db: &mut mysql::PooledConn,
) -> Result<(), mysql::Error> {
    let conflict_cols = vec!["paperId", "contactId", "conflictType"];
    let conflict_vals = vec![vec![
        Expr::Value(Value::Number(paper_id.to_string())),
        Expr::Value(Value::Number(user_id.to_string())),
        Expr::Value(Value::Number(conflict_type.to_string())),
    ]];
    get_query_rows_prime(
        &Statement::Insert(InsertStatement {
            table_name: string_to_objname("PaperConflict"),
            columns: conflict_cols
                .iter()
                .map(|c| Ident::new(c.to_string()))
                .collect(),
            source: InsertSource::Query(Box::new(values_query(conflict_vals))),
        }),
        db,
    )?;
    Ok(())
}
