use crate::*;
use edna::*;

const TABLEINFO_JSON: &'static str = include_str!("./table_info.json");
const GDPR_JSON: &'static str = include_str!("./gdpr_disguise.json");
const GUISEGEN_JSON: &'static str = include_str!("./guise_gen.json");

pub fn apply(
    edna: &mut EdnaClient,
    uid: u64,
    password: String,
    compose: bool,
) -> Result<DID, mysql::Error> {
    let gdpr_json = str::replace(GDPR_JSON, "UID", &uid.to_string());
    let pw = if !compose { None } else { Some(password) };
    edna.apply_disguise(
        uid.to_string(),
        &gdpr_json,
        TABLEINFO_JSON,
        GUISEGEN_JSON,
        pw,
        None,
        false,
    )
}

pub fn reveal(
    uid: u64,
    did: DID,
    edna: &mut EdnaClient,
    password: String,
) -> Result<(), mysql::Error> {
    edna.reveal_disguise(
        uid.to_string(),
        did,
        TABLEINFO_JSON,
        GUISEGEN_JSON,
        Some(password),
        None,
        false,
    )
}
