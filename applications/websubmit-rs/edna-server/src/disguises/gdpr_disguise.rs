use crate::backend::MySqlBackend;
use edna::{DID, UID};

const TABLEINFO_JSON: &'static str = include_str!("./table_info.json");
const GDPR_JSON: &'static str = include_str!("./gdpr_disguise.json");
const PPGEN_JSON: &'static str = include_str!("./pp_gen.json");

pub fn apply(
    bg: &mut MySqlBackend,
    user_email: UID,
    user_pw: String,
    compose: bool,
    is_baseline: bool,
) -> Result<DID, mysql::Error> {
    if is_baseline {
        bg.query_drop(&format!(
            "DELETE FROM answers WHERE `email` = '{}'",
            user_email
        ));
        bg.query_drop(&format!("DELETE FROM users WHERE email = '{}'", user_email));
        return Ok(rand::random());
    }
    // we don't need private keys if there are no locs for composition
    let pw = if !compose { None } else { Some(user_pw) };
    bg.edna.apply_disguise(
        user_email,
        GDPR_JSON,
        TABLEINFO_JSON,
        PPGEN_JSON,
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
    is_baseline: bool,
) -> Result<(), mysql::Error> {
    if is_baseline {
        return Ok(());
    }
    bg.edna.reveal_disguise(
        user_email,
        did,
        TABLEINFO_JSON,
        PPGEN_JSON,
        Some(edna::RevealPPType::Restore),
        true, // allow partial row reveals
        Some(user_pw),
        None,
        false,
    )
}
