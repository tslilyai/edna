use crate::backend::MySqlBackend;
use edna_cryptdb::{DID, UID};

const TABLEINFO_JSON: &'static str = include_str!("./table_info.json");
const GDPR_JSON: &'static str = include_str!("./gdpr_disguise.json");
const GUISEGEN_JSON: &'static str = include_str!("./guise_gen.json");

pub fn apply(
    bg: &mut MySqlBackend,
    user_email: UID,
    user_pw: String,
    compose: bool,
) -> Result<DID, mysql::Error> {
    // we don't need private keys if there are no locs for composition
    let pw = if !compose { None } else { Some(user_pw) };
    bg.edna.apply_disguise(
        user_email,
        GDPR_JSON,
        TABLEINFO_JSON,
        GUISEGEN_JSON,
        pw,
        None,
        false,
    )
}

pub fn reveal(
    did: DID,
    bg: &mut MySqlBackend,
    user_email: UID,
    user_pw: String,
) -> Result<(), mysql::Error> {
    bg.edna.reveal_disguise(
        user_email,
        did,
        TABLEINFO_JSON,
        GUISEGEN_JSON,
        Some(user_pw),
        None,
        false,
    )
}
