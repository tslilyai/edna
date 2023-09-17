use crate::backend::MySqlBackend;
use edna_cryptdb::*;

const PPGEN_JSON: &'static str = include_str!("./pp_gen.json");
const TABLEINFO_JSON: &'static str = include_str!("./table_info.json");
const ANON_JSON: &'static str = include_str!("./universal_anon_disguise.json");

pub fn apply(bg: &mut MySqlBackend) -> Result<DID, mysql::Error> {
    bg.edna.apply_disguise(
        "NULL".to_string(),
        ANON_JSON,
        TABLEINFO_JSON,
        PPGEN_JSON,
        None,
        None,
        false,
    )
}
