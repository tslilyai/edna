use std::fs;
use std::io::{Error, ErrorKind, Read};
use toml;

pub const ADMIN: (&'static str, &'static str) = (
    "malte@cs.brown.edu",
    "b4bc3cef020eb6dd20defa1a7a8340dee889bc2164612e310766e69e45a1d5a7",
);

#[derive(Debug, Clone)]
pub struct Config {
    /// Textual identifier for class
    pub class: String,
    /// System admin addresses
    pub admins: Vec<String>,
    /// Staff email addresses
    pub staff: Vec<String>,
    /// Web template directory
    pub template_dir: String,
    /// Web resource root directory
    pub resource_dir: String,
    /// Secret (for API key generation)
    pub secret: String,
    /// Whether to send emails
    pub send_emails: bool,
    /// mysql settings
    pub mysql_user: String,
    pub mysql_pass: String,
}

pub(crate) fn parse(path: &str) -> Result<Config, Error> {
    let mut f = fs::File::open(path)?;
    let mut buf = String::new();
    f.read_to_string(&mut buf)?;

    let value = match toml::Parser::new(&buf).parse() {
        None => {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "failed to parse config!",
            ))
        }
        Some(v) => v,
    };

    Ok(Config {
        class: value.get("class").unwrap().as_str().unwrap().into(),
        admins: value
            .get("admins")
            .unwrap()
            .as_slice()
            .unwrap()
            .into_iter()
            .map(|v| v.as_str().unwrap().into())
            .collect(),
        staff: value
            .get("staff")
            .unwrap()
            .as_slice()
            .unwrap()
            .into_iter()
            .map(|v| v.as_str().unwrap().into())
            .collect(),
        template_dir: value.get("template_dir").unwrap().as_str().unwrap().into(),
        resource_dir: value.get("resource_dir").unwrap().as_str().unwrap().into(),
        secret: value.get("secret").unwrap().as_str().unwrap().into(),
        send_emails: value.get("send_emails").unwrap().as_bool().unwrap().into(),
        mysql_pass: value.get("mysql_pass").unwrap().as_str().unwrap().into(),
        mysql_user: value.get("mysql_user").unwrap().as_str().unwrap().into(),
    })
}
