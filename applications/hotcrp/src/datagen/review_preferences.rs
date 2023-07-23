use edna::helpers::*;
use sql_parser::ast::*;

pub fn insert_review_preference(
    paper_id: usize,
    author_id: usize,
    db: &mut mysql::PooledConn,
) -> Result<(), mysql::Error> {
    let review_preferences_cols = vec!["paperId", "contactId"];
    let review_preferences_vals = vec![vec![
        Expr::Value(Value::Number(paper_id.to_string())),
        Expr::Value(Value::Number(author_id.to_string())),
    ]];
    get_query_rows_prime(
        &Statement::Insert(InsertStatement {
            table_name: string_to_objname("PaperReviewPreference"),
            columns: review_preferences_cols
                .iter()
                .map(|c| Ident::new(c.to_string()))
                .collect(),
            source: InsertSource::Query(Box::new(values_query(review_preferences_vals))),
        }),
        db,
    )?;
    Ok(())
}
