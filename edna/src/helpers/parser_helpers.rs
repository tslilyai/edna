use crate::*;
use log::{debug, warn};
use rand;
use regex::*;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::str::FromStr;

/*****************************************
 * OTHER
 ****************************************/
pub fn vec_to_expr<T: Serialize>(vs: &Vec<T>) -> Expr {
    if vs.is_empty() {
        Expr::Value(Value::Null)
    } else {
        let serialized = serde_json::to_string(&vs).unwrap();
        Expr::Value(Value::String(serialized))
    }
}

pub fn pred_expr_to_string(pred: &Option<Expr>) -> String {
    match pred {
        Some(p) => p.to_string(),
        None => "true".to_string(),
    }
}

// get which columns have updated values set
pub fn get_updated_cols(setexpr: &SetExpr) -> HashSet<String> {
    let mut updated = HashSet::new();
    match setexpr {
        SetExpr::Select(s) => {
            for si in &s.projection {
                match si {
                    SelectItem::Expr { expr, alias } => match expr {
                        Expr::Value(_) => {
                            assert!(alias.is_some());
                            let a = alias.as_ref().unwrap();
                            updated.insert(a.to_string());
                        }
                        _ => warn!(
                            "get_updated_cols: Found expr non-value {}, {:?}",
                            expr, alias
                        ),
                    },
                    SelectItem::Wildcard => (),
                }
            }
        }
        SetExpr::SetOperation {
            op, left, right, ..
        } => {
            if op == &SetOperator::Union {
                updated.extend(get_updated_cols(left));
                updated.extend(get_updated_cols(right));
            }
        }
        _ => unimplemented!("{:?} Not a select query!", setexpr),
    }
    updated
}

// get which columns are predicated on
pub fn get_conditional_cols(setexpr: &SetExpr) -> HashSet<String> {
    let mut ids = HashSet::new();
    match setexpr {
        SetExpr::Select(s) => {
            if let Some(select) = &s.selection {
                ids = get_expr_idents(&select).into_iter().collect();
            }
        }
        SetExpr::SetOperation {
            op, left, right, ..
        } => {
            if op == &SetOperator::Union {
                ids.extend(get_conditional_cols(left));
                ids.extend(get_conditional_cols(right));
            }
        }
        _ => unimplemented!("{:?} Not a select query!", setexpr),
    }
    ids
}

pub fn update_select_from(setexpr: &mut SetExpr, to_name: &Option<String>) {
    match setexpr {
        SetExpr::Select(ref mut s) => {
            // select from the last created table
            if let Some(name) = to_name {
                s.from = vec![TableWithJoins {
                    relation: TableFactor::Table {
                        name: string_to_objname(&name),
                        alias: None,
                    },
                    joins: vec![],
                }];
            }
        }
        SetExpr::SetOperation {
            op,
            ref mut left,
            ref mut right,
            ..
        } => {
            if op == &SetOperator::Union {
                update_select_from(left, to_name);
                update_select_from(right, to_name);
            }
        }
        _ => unimplemented!("{:?} Not a select query!", setexpr),
    }
}

pub fn select_ordered_statement(table: &str, selection: Option<Expr>, order_by: &str) -> Statement {
    Statement::Select(SelectStatement {
        query: Box::new(Query {
            ctes: vec![],
            body: SetExpr::Select(Box::new(Select {
                distinct: false,
                projection: vec![SelectItem::Wildcard],
                from: str_to_tablewithjoins(&table),
                selection: selection.clone(),
                group_by: vec![],
                having: None,
            })),
            order_by: vec![OrderByExpr {
                expr: Expr::Identifier(vec![Ident::new(order_by.to_string())]),
                asc: Some(true),
            }],
            limit: None,
            offset: None,
            fetch: None,
        }),
        as_of: None,
    })
}

pub fn select_1_statement(table: &str, selection: Option<Expr>) -> Statement {
    Statement::Select(SelectStatement {
        query: Box::new(Query::select(Select {
            distinct: false,
            projection: vec![SelectItem::Expr {
                expr: Expr::Value(Value::Number(1.to_string())),
                alias: None,
            }],
            from: str_to_tablewithjoins(&table),
            selection: selection.clone(),
            group_by: vec![],
            having: None,
        })),
        as_of: None,
    })
}

pub fn select_statement(table: &str, selection: &Option<Expr>) -> Statement {
    Statement::Select(SelectStatement {
        query: Box::new(Query::select(Select {
            distinct: true,
            projection: vec![SelectItem::Wildcard],
            from: str_to_tablewithjoins(&table),
            selection: selection.clone(),
            group_by: vec![],
            having: None,
        })),
        as_of: None,
    })
}

pub fn values_query(vals: Vec<Vec<Expr>>) -> Query {
    Query {
        ctes: vec![],
        body: SetExpr::Values(Values(vals)),
        order_by: vec![],
        limit: None,
        offset: None,
        fetch: None,
    }
}
pub fn str_to_tablewithjoins(name: &str) -> Vec<TableWithJoins> {
    vec![TableWithJoins {
        relation: TableFactor::Table {
            name: string_to_objname(name),
            alias: None,
        },
        joins: vec![],
    }]
}

pub fn process_schema_stmt(stmt: &str, in_memory: bool) -> String {
    // get rid of unsupported types
    debug!("helpers:{}", stmt);
    let mut new = stmt.replace(r"int unsigned", "int");

    // remove semicolon
    new = new.trim_matches(';').to_string();

    // get rid of DEFAULT/etc. commands after query
    let mut end_index = new.len();
    if let Some(i) = new.find("DEFAULT CHARSET") {
        end_index = i;
    } else if let Some(i) = new.find("default charset") {
        end_index = i;
    }
    new.truncate(end_index);

    if in_memory {
        new = new.replace(r"mediumtext", "varchar(255)");
        new = new.replace(r"tinytext", "varchar(255)");
        new = new.replace(r" text ", " varchar(255) ");
        new = new.replace(r" text,", " varchar(255),");
        new = new.replace(r" text)", " varchar(255))");
        new = new.replace(r"FULLTEXT", "");
        new = new.replace(r"fulltext", "");
        new = new.replace(r"InnoDB", "MEMORY");
        if !new.contains("MEMORY") {
            new.push_str(" ENGINE = MEMORY");
        }
    } else if !new.contains("ENGINE") && !new.contains("engine") {
        new.push_str(" ENGINE = InnoDB");
    }
    new.push_str(";");
    warn!("helpers new:{}", new);
    new
}

pub fn get_create_schema_statements(schema: &str, in_memory: bool) -> Vec<Statement> {
    let mut stmts = vec![];
    let mut stmt = String::new();
    let int_re = Regex::new(r"int\(\d+\)").unwrap();
    let varbinary_re = Regex::new(r"varbinary\(\d+\)").unwrap();
    let binary_re = Regex::new(r"binary\(\d+\)").unwrap();
    let space_re = Regex::new(r",\x20+\)").unwrap();
    for line in schema.lines() {
        if line.starts_with("--") || line.is_empty() {
            continue;
        }
        if !stmt.is_empty() {
            stmt.push_str(" ");
        }
        // XXX hack
        if !line.contains("UNIQUE KEY") && !line.contains("KEY") {
            stmt.push_str(line);
        }
        if stmt.ends_with(';') {
            // only save create table statements for now
            if stmt.contains("CREATE") {
                let res1 = int_re.replace_all(&stmt, "int");
                let res2 = varbinary_re.replace_all(&res1, "int");
                let res3 = binary_re.replace_all(&res2, "int");
                let res4 = space_re.replace_all(&res3, ")");
                let stmt = process_schema_stmt(&res4, in_memory);
                stmts.push(get_single_parsed_stmt(&stmt).unwrap());
            }
            stmt = String::new();
        }
    }
    stmts
}

pub fn get_single_parsed_stmt(stmt: &String) -> Result<Statement, mysql::Error> {
    warn!("Parsing stmt {}", stmt);
    let asts = sql_parser::parser::parse_statements(stmt.to_string());
    match asts {
        Err(e) => Err(mysql::Error::IoError(io::Error::new(
            io::ErrorKind::InvalidInput,
            e,
        ))),
        Ok(asts) => {
            if asts.len() != 1 {
                return Err(mysql::Error::IoError(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("More than one stmt {:?}", asts),
                )));
            }
            Ok(asts[0].clone())
        }
    }
}

// returns if the first value is larger than the second
pub fn string_vals_cmp(v1: &str, v2: &str) -> cmp::Ordering {
    let res = match (f64::from_str(v1), f64::from_str(v2)) {
        (Ok(v1), Ok(v2)) => v1.partial_cmp(&v2).unwrap(),
        (Ok(v1), Err(_)) => v1.partial_cmp(&f64::from_str(v2).unwrap()).unwrap(),
        (Err(_), Ok(v2)) => f64::from_str(v1).unwrap().partial_cmp(&v2).unwrap(),
        (Err(_), Err(_)) => v1.cmp(v2),
    };
    /*(Value::Null, Value::Null) => res = Ordering::Equal,
    (_, Value::Null) => res = Ordering::Greater,
    (Value::Null, _) => res = Ordering::Less,
    _ => unimplemented!("value not comparable! {:?} and {:?}", v1, v2),*/
    debug!("comparing {:?} =? {:?} : {:?}", v1, v2, res);
    res
}

// returns if the first value is larger than the second
pub fn parser_vals_cmp(v1: &sql_parser::ast::Value, v2: &sql_parser::ast::Value) -> cmp::Ordering {
    let res: cmp::Ordering;
    match (v1, v2) {
        (Value::Number(i1), Value::Number(i2)) => {
            res = f64::from_str(i1)
                .unwrap()
                .partial_cmp(&f64::from_str(i2).unwrap())
                .unwrap()
        }
        (Value::String(i1), Value::Number(i2)) => {
            res = f64::from_str(i1)
                .unwrap()
                .partial_cmp(&f64::from_str(i2).unwrap())
                .unwrap()
        }
        (Value::Number(i1), Value::String(i2)) => {
            res = f64::from_str(i1)
                .unwrap()
                .partial_cmp(&f64::from_str(i2).unwrap())
                .unwrap()
        }
        (Value::String(i1), Value::String(i2)) => res = i1.cmp(i2),
        (Value::Null, Value::Null) => res = Ordering::Equal,
        (_, Value::Null) => res = Ordering::Greater,
        (Value::Null, _) => res = Ordering::Less,
        _ => unimplemented!("value not comparable! {:?} and {:?}", v1, v2),
    }
    debug!("comparing {:?} =? {:?} : {:?}", v1, v2, res);
    res
}

pub fn plus_parser_vals(
    v1: &sql_parser::ast::Value,
    v2: &sql_parser::ast::Value,
) -> sql_parser::ast::Value {
    Value::Number((parser_val_to_f64(v1) + parser_val_to_f64(v2)).to_string())
}

pub fn minus_parser_vals(
    v1: &sql_parser::ast::Value,
    v2: &sql_parser::ast::Value,
) -> sql_parser::ast::Value {
    Value::Number((parser_val_to_f64(v1) - parser_val_to_f64(v2)).to_string())
}

pub fn get_computed_parser_val_with(base_val: &Value, f: &Box<dyn Fn(&str) -> String>) -> Value {
    match base_val {
        Value::Number(i) => Value::Number(f(i)),
        Value::String(i) => Value::String(f(i)),
        _ => unimplemented!("value not supported ! {}", base_val),
    }
}

pub fn get_default_parser_val_with(base_val: &Value, val: &str) -> Value {
    match base_val {
        Value::Number(_i) => Value::Number(val.to_string()),
        Value::String(_i) => Value::String(val.to_string()),
        Value::Null => Value::String(val.to_string()),
        _ => unimplemented!("value not supported ! {}", base_val),
    }
}

pub fn get_random_parser_val_from(val: &Value) -> Value {
    match val {
        Value::Number(_i) => Value::Number(rand::random::<u32>().to_string()),
        Value::String(_i) => Value::String(rand::random::<u64>().to_string()),
        Value::Null => Value::Number(rand::random::<u64>().to_string()),
        _ => unimplemented!("value not supported ! {}", val),
    }
}

pub fn parser_val_to_f64(val: &sql_parser::ast::Value) -> f64 {
    match val {
        Value::Number(i) => f64::from_str(i).unwrap(),
        Value::String(i) => f64::from_str(i).unwrap(),
        _ => unimplemented!("value not a number! {}", val),
    }
}

pub fn parser_val_to_u64(val: &sql_parser::ast::Value) -> u64 {
    match val {
        Value::Number(i) => u64::from_str(i).unwrap(),
        Value::String(i) => u64::from_str(i).unwrap(),
        _ => unimplemented!("value not a number! {}", val),
    }
}

pub fn parser_val_to_u64_opt(val: &sql_parser::ast::Value) -> Option<u64> {
    match val {
        Value::Number(i) => Some(u64::from_str(i).unwrap()),
        Value::String(i) => Some(u64::from_str(i).unwrap()),
        _ => None,
    }
}

pub fn parser_expr_to_u64(val: &Expr) -> Result<u64, mysql::Error> {
    match val {
        Expr::Value(Value::Number(i)) => Ok(u64::from_str(i).unwrap()),
        Expr::Value(Value::String(i)) => match u64::from_str(i) {
            Ok(v) => Ok(v),
            Err(_e) => Err(mysql::Error::IoError(io::Error::new(
                io::ErrorKind::Other,
                format!("expr {:?} is not an int", val),
            ))),
        },
        _ => Err(mysql::Error::IoError(io::Error::new(
            io::ErrorKind::Other,
            format!("expr {:?} is not an int", val),
        ))),
    }
}

pub fn parser_val_to_common_val(val: &sql_parser::ast::Value) -> mysql_common::value::Value {
    match val {
        Value::Null => mysql_common::value::Value::NULL,
        Value::String(s) => mysql_common::value::Value::Bytes(s.as_bytes().to_vec()),
        Value::HexString(s) => mysql_common::value::Value::Bytes(hex::decode(s).unwrap()),
        Value::Number(i) => {
            if !i.contains('.') {
                mysql_common::value::Value::Int(i64::from_str(i).unwrap())
            } else {
                mysql_common::value::Value::Double(f64::from_str(i).unwrap())
            }
        }
        Value::Boolean(b) => {
            let bit = match b {
                true => 1,
                false => 0,
            };
            mysql_common::value::Value::Int(bit)
        }
        _ => unimplemented!("Value not supported: {}", val),
    }
}

/***************************
 * IDENT STUFF
 ***************************/

pub fn get_expr_idents(e: &Expr) -> Vec<String> {
    let mut ids = vec![];
    match e {
        Expr::Identifier(_) => ids.push(e.to_string()),
        Expr::Value(_) => (),
        Expr::IsNull { expr, .. } => ids.append(&mut get_expr_idents(&expr)),
        Expr::InList { expr, list, .. } => {
            ids.append(&mut get_expr_idents(&expr));
            for e in list {
                ids.append(&mut get_expr_idents(&e));
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            ids.append(&mut get_expr_idents(&expr));
            ids.append(&mut get_expr_idents(&low));
            ids.append(&mut get_expr_idents(&high));
        }
        Expr::BinaryOp { left, right, .. } => {
            ids.append(&mut get_expr_idents(&left));
            ids.append(&mut get_expr_idents(&right));
        }
        Expr::UnaryOp { expr, .. } => {
            ids.append(&mut get_expr_idents(&expr));
        }
        Expr::Cast { expr, .. } => {
            ids.append(&mut get_expr_idents(&expr));
        }
        Expr::Collate { expr, .. } => {
            ids.append(&mut get_expr_idents(&expr));
        }
        Expr::Nested(expr) => {
            ids.append(&mut get_expr_idents(&expr));
        }
        Expr::Row { exprs } => {
            for e in exprs {
                ids.append(&mut get_expr_idents(&e));
            }
        }
        Expr::Case {
            conditions,
            results,
            ..
        } => {
            for e in conditions {
                ids.append(&mut get_expr_idents(&e));
            }
            for e in results {
                ids.append(&mut get_expr_idents(&e));
            }
        }
        Expr::List(exprs) => {
            for e in exprs {
                ids.append(&mut get_expr_idents(&e));
            }
        }
        _ => unimplemented!("No identifier tracking for expr {:?}", e),
    }
    ids
}

pub fn trim_quotes(s: &str) -> &str {
    let mut s = s.trim_matches('\'');
    s = s.trim_matches('\"');
    s = s.trim_matches('`');
    s
}

pub fn string_to_idents(s: &str) -> Vec<Ident> {
    s.split(".")
        .into_iter()
        .map(|i| Ident::new(trim_quotes(i)))
        .collect()
}

pub fn string_to_objname(s: &str) -> ObjectName {
    let idents = string_to_idents(s);
    ObjectName(idents)
}

pub fn str_subset_of_idents(dt: &str, ids: &Vec<Ident>) -> Option<(usize, usize)> {
    let dt_split: Vec<Ident> = dt.split(".").map(|i| Ident::new(i)).collect();
    idents_subset_of_idents(&dt_split, ids)
}

// end exclusive
pub fn str_ident_match(shorts: &str, longs: &str) -> bool {
    let mut i = 0;
    let mut j = 0;
    let shortvs: Vec<&str> = shorts.split(".").collect();
    let longvs: Vec<&str> = longs.split(".").collect();
    while j < longvs.len() {
        if i < shortvs.len() {
            if shortvs[i] == longvs[j] {
                i += 1;
            } else {
                // reset comparison from beginning of dt
                i = 0;
            }
            j += 1;
        } else {
            break;
        }
    }
    if i == shortvs.len() {
        return true;
    }
    false
}

// end exclusive
pub fn idents_subset_of_idents(id1: &Vec<Ident>, id2: &Vec<Ident>) -> Option<(usize, usize)> {
    let mut i = 0;
    let mut j = 0;
    while j < id2.len() {
        if i < id1.len() {
            if id1[i] == id2[j] {
                i += 1;
            } else {
                // reset comparison from beginning of dt
                i = 0;
            }
            j += 1;
        } else {
            break;
        }
    }
    if i == id1.len() {
        return Some((j - i, j));
    }
    None
}

/***************************
 * EXPR STUFF
 ***************************/
// return table name and optionally column if not wildcard
pub fn expr_to_col(e: &Expr) -> (String, String) {
    //debug!("expr_to_col: {:?}", e);
    match e {
        // only support form column or table.column
        Expr::Identifier(ids) => {
            if ids.len() > 2 || ids.len() < 1 {
                unimplemented!("expr needs to be of form table.column {}", e);
            }
            if ids.len() == 2 {
                return (ids[0].to_string(), ids[1].to_string());
            }
            return ("".to_string(), ids[0].to_string());
        }
        _ => unimplemented!("expr_to_col {} not supported", e),
    }
}

pub fn expr_to_guise_parent_key(
    expr: &Expr,
    guiseed_cols: &Vec<(String, String)>,
) -> Option<(String, String)> {
    match expr {
        Expr::Identifier(ids) => {
            let col = ids[ids.len() - 1].to_string();
            if let Some(i) = guiseed_cols.iter().position(|(gc, _pc)| *gc == col) {
                Some(guiseed_cols[i].clone())
            } else {
                None
            }
        }
        _ => unimplemented!("Expr is not a col {}", expr),
    }
}

pub fn expr_is_col(expr: &Expr) -> bool {
    match expr {
        Expr::Identifier(_) | Expr::QualifiedWildcard(_) => true,
        _ => false,
    }
}

pub fn expr_is_value(expr: &Expr) -> bool {
    match expr {
        Expr::Value(_) => true,
        _ => false,
    }
}

pub fn lhs_expr_to_name(left: &Expr) -> String {
    match left {
        Expr::Identifier(_) => {
            let (tab, mut col) = expr_to_col(&left);
            if !tab.is_empty() {
                col = format!("{}.{}", tab, col);
            }
            col
        }
        _ => unimplemented!("Bad lhs {}", left),
    }
}

pub fn rhs_expr_to_name_or_value(right: &Expr) -> (Option<String>, Option<Value>) {
    let mut rval = None;
    let mut rname = None;
    match right {
        Expr::Identifier(_) => {
            let (tab, mut col) = expr_to_col(&right);
            if !tab.is_empty() {
                col = format!("{}.{}", tab, col);
            }
            rname = Some(col);
        }
        Expr::Value(val) => {
            rval = Some(val.clone());
        }
        Expr::UnaryOp { op, expr } => {
            if let Expr::Value(ref val) = **expr {
                match op {
                    UnaryOperator::Minus => {
                        let n = -1.0 * parser_val_to_f64(&val);
                        rval = Some(Value::Number(n.to_string()));
                    }
                    _ => unimplemented!("Unary op not supported! {:?}", expr),
                }
            } else {
                unimplemented!("Unary op not supported! {:?}", expr);
            }
        }
        _ => unimplemented!("Bad rhs? {}", right),
    }
    (rname, rval)
}
