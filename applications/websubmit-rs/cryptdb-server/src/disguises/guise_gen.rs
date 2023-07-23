use edna_cryptdb::GuiseGen;
use rand::prelude::*;
use sql_parser::ast::*;
use std::sync::{Arc, RwLock};

fn get_insert_guise_cols() -> Vec<String> {
    vec![
        "email".to_string(),
        "apikey".to_string(),
        "is_admin".to_string(),
        "is_anon".to_string(),
    ]
}

pub fn get_insert_guise_vals() -> Vec<Expr> {
    let mut rng = rand::thread_rng();
    let gid: u64 = rng.gen();
    let email: u32 = rng.gen();
    vec![
        Expr::Value(Value::String(format!("{}@anon.com", email.to_string()))),
        Expr::Value(Value::String(gid.to_string())),
        Expr::Value(Value::Number(0.to_string())),
        Expr::Value(Value::Number(1.to_string())),
    ]
}

pub fn get_guise_gen() -> Arc<RwLock<GuiseGen>> {
    Arc::new(RwLock::new(GuiseGen {
        guise_name: "users".to_string(),
        guise_id_col: "email".to_string(),
        col_generation: Box::new(get_insert_guise_cols),
        val_generation: Box::new(get_insert_guise_vals),
    }))
}
