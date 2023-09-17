use crate::*;
use edna::*;

const TABLEINFO_JSON: &'static str = include_str!("./table_info.json");
const ANON_JSON: &'static str = include_str!("./universal_anon_disguise.json");
const PPGEN_JSON: &'static str = include_str!("./pp_gen.json");

pub fn apply(edna: &mut EdnaClient) -> Result<DID, mysql::Error> {
    edna.apply_disguise(
        "NULL".to_string(),
        &ANON_JSON,
        TABLEINFO_JSON,
        PPGEN_JSON,
        None,
        None,
        false,
    )
}

// no reveal
