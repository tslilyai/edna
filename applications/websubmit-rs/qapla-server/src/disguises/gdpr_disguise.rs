use crate::backend::MySqlBackend;

pub fn apply(
    bg: &mut MySqlBackend,
    user_email: String,
    user_pw: String,
) -> Result<(), mysql::Error> {
    bg.query(
        "",
        &format!(
            "UPDATE users SET is_deleted=1 WHERE (email = '{}' AND apikey = '{}') OR owner = '{}'",
            user_email, user_pw, user_email
        ),
    );
    Ok(())
}

pub fn reveal(
    bg: &mut MySqlBackend,
    user_email: String,
    user_pw: String,
) -> Result<(), mysql::Error> {
    bg.query(
        "",
        &format!(
            "UPDATE users SET is_deleted=0 WHERE (email = '{}' AND apikey = '{}') OR owner = '{}'",
            user_email, user_pw, user_email
        ),
    );
    return Ok(());
}
