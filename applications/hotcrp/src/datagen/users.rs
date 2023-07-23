use crate::datagen::*;
use edna::helpers::*;
use mysql::prelude::*;
use sql_parser::ast::*;

pub fn get_random_email() -> String {
    format!("anonymous{}@secret.mail", get_random_string())
}

pub fn get_contact_info_cols() -> Vec<&'static str> {
    vec![
        "contactId",
        "firstName",
        "lastName",
        "unaccentedName",
        "email",
        "preferredEmail",
        "affiliation",
        "phone",
        "country",
        "password",
        "passwordTime",
        "passwordUseTime",
        "collaborators",
        "updateTime",
        "lastLogin",
        "defaultWatch",
        "roles",
        "disabled",
        "contactTags",
        "data",
    ]
}

pub fn get_contact_info_vals(uid: usize) -> Vec<Expr> {
    vec![
        Expr::Value(Value::Number(uid.to_string())),
        Expr::Value(Value::String(get_random_string())),
        Expr::Value(Value::String(get_random_string())),
        Expr::Value(Value::String(get_random_string())),
        Expr::Value(Value::String(get_random_email())),
        Expr::Value(Value::Null),
        Expr::Value(Value::String(String::new())),
        Expr::Value(Value::Null),
        Expr::Value(Value::Null),
        Expr::Value(Value::String("password".to_string())),
        Expr::Value(Value::Number(0.to_string())),
        Expr::Value(Value::Number(0.to_string())),
        Expr::Value(Value::Null),
        Expr::Value(Value::Number(0.to_string())),
        Expr::Value(Value::Number(0.to_string())),
        Expr::Value(Value::Number(2.to_string())),
        Expr::Value(Value::Number(0.to_string())),
        Expr::Value(Value::Number(0.to_string())),
        Expr::Value(Value::Null),
        Expr::Value(Value::Null),
    ]
}

pub fn insert_users(nusers: usize, db: &mut mysql::PooledConn) -> Result<(), mysql::Error> {
    // insert users
    let mut new_ci = vec![];
    for uid in 1..nusers + 1 {
        new_ci.push(get_contact_info_vals(uid));
    }
    let fk_cols = get_contact_info_cols();
    db.query_drop(
        &Statement::Insert(InsertStatement {
            table_name: string_to_objname("ContactInfo"),
            columns: fk_cols.iter().map(|c| Ident::new(c.to_string())).collect(),
            source: InsertSource::Query(Box::new(values_query(new_ci))),
        })
        .to_string(),
    )?;
    Ok(())
}

pub fn insert_users_anon(nusers: usize, db: &mut mysql::PooledConn) -> Result<(), mysql::Error> {
    if nusers == 0 {
        return Ok(());
    }
    // insert users
    let mut new_ci = vec![];
    for _ in 0..nusers {
        let mut vals = get_contact_info_vals(1);
        vals.remove(0);
        new_ci.push(vals);
    }
    let mut fk_cols = get_contact_info_cols();
    fk_cols.remove(0);
    let q = Statement::Insert(InsertStatement {
            table_name: string_to_objname("ContactInfo"),
            columns: fk_cols.iter().map(|c| Ident::new(c.to_string())).collect(),
            source: InsertSource::Query(Box::new(values_query(new_ci))),
        })
        .to_string();
    db.query_drop(&q)?;
    Ok(())
}

pub fn insert_single_user(db: &mut mysql::PooledConn) -> Result<(), mysql::Error> {
    // insert users
    let mut fk_cols = get_contact_info_cols();
    let mut vals = get_contact_info_vals(1);
    vals.remove(0);
    fk_cols.remove(0);
    let mut new_ci = vec![];
    new_ci.push(vals);
    db.query_drop(
        &Statement::Insert(InsertStatement {
            table_name: string_to_objname("ContactInfo"),
            columns: fk_cols.iter().map(|c| Ident::new(c.to_string())).collect(),
            source: InsertSource::Query(Box::new(values_query(new_ci))),
        })
        .to_string(),
    )?;
    Ok(())
}
