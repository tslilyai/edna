use crate::backend::MySqlBackend;
use edna::*;
use mysql::from_value;
use mysql::params;
use mysql::prelude::*;
use rand::prelude::*;
use sql_parser::ast::Expr;
use std::collections::HashMap;
use std::time;

const GUISEGEN_JSON: &'static str = include_str!("./guise_gen.json");
const TABLEINFO_JSON: &'static str = include_str!("./table_info.json");
const ANON_JSON: &'static str = include_str!("./universal_anon_disguise.json");

pub fn apply(
    bg: &MySqlBackend,
    is_baseline: bool,
) -> Result<DID, mysql::Error> {
    bg.edna.lock().unwrap().apply_disguise(
        "NULL".to_string(),
        ANON_JSON,
        TABLEINFO_JSON,
        GUISEGEN_JSON,
        None,
        None,
        false,
    )
}
