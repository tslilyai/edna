use crate::backend::MySqlBackend;
use crate::config::Config;
use crate::email;
use crypto::digest::Digest;
use crypto::sha2::Sha256;
use mysql::from_value;
use rocket::form::Form;
use rocket::http::Status;
use rocket::http::{CookieJar};
use rocket::outcome::IntoOutcome;
use rocket::request::{self, FromRequest, Request};
use rocket::response::Redirect;
use rocket::State;
use rocket_dyn_templates::Template;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// (username, apikey)
pub(crate) struct ApiKey {
    pub user: String,
    pub key: String,
}

#[derive(Debug, FromForm)]
pub(crate) struct ApiKeyRequest {
    email: String,
}

#[derive(Debug, FromForm)]
pub(crate) struct ApiKeySubmit {
    email: String,
    key: String,
}

#[derive(Debug)]
pub(crate) enum ApiKeyError {
    Ambiguous,
    Missing,
    BackendFailure,
}

#[rocket::async_trait]
impl<'r> FromRequest<'r> for ApiKey {
    type Error = ApiKeyError;

    async fn from_request(request: &'r Request<'_>) -> request::Outcome<Self, Self::Error> {
        let be = request
            .guard::<&State<Arc<Mutex<MySqlBackend>>>>()
            .await
            .unwrap();
        request
            .cookies()
            .get("apikey")
            .and_then(|cookie| cookie.value().parse().ok())
            .and_then(|key: String| {
                let anonkey = match request.cookies().get("anonkey") {
                    Some(ak) => ak.value(),
                    None => "",
                };
                match check_api_key(&be, &key, &anonkey) {
                    Ok(user) => Some(ApiKey {
                        user: user,
                        key: key,
                    }),
                    Err(_) => None,
                }
            })
            .or_error((Status::Unauthorized, ApiKeyError::Missing))
    }
}

#[post("/", data = "<data>")]
pub(crate) fn generate(
    data: Form<ApiKeyRequest>,
    backend: &State<Arc<Mutex<MySqlBackend>>>,
    config: &State<Config>,
) -> Template {
    // generate an API key from email address
    let mut hasher = Sha256::new();
    hasher.input_str(&data.email);
    // add a secret to make API keys unforgeable without access to the server
    hasher.input_str(&config.secret);
    let hash = hasher.result_str();

    // register with edna
    // api key acts as the password
    let mut bg = backend.lock().unwrap();
    let share_str = if !bg.is_baseline {
        let share = bg
            .edna
            .register_principal(&data.email, hash.as_str().into());
        serde_json::to_string(&share).unwrap()
    } else {
        String::new()
    };

    let is_admin = if config.admins.contains(&data.email) {
        1.into()
    } else {
        0.into()
    };

    // insert into MySql if not exists
    bg.insert(
        "users",
        vec![
            data.email.as_str().into(),
            hash.as_str().into(),
            is_admin,
            false.into(), // is anon
        ],
    );

    if config.send_emails {
        email::send(
            bg.log.clone(),
            "no-reply@csci2390-submit.cs.brown.edu".into(),
            vec![data.email.clone()],
            format!("{} API key", config.class),
            format!("APIKEY#{}\nSHARE#{}", hash.as_str(), share_str),
        )
        .expect("failed to send API key email");
    }
    drop(bg);

    // return to user
    let mut ctx = HashMap::new();
    ctx.insert("apikey_email", data.email.clone());
    ctx.insert("parent", "layout".into());
    Template::render("apikey/generate", &ctx)
}

pub(crate) fn check_api_key(
    backend: &Arc<Mutex<MySqlBackend>>,
    key: &str,
    anonkey: &str,
) -> Result<String, ApiKeyError> {
    let mut bg = backend.lock().unwrap();
    warn!(bg.log, "Login key is {}", key);

    // login this principal
    let rs = if !anonkey.is_empty() {
        bg.query_iter(&format!("SELECT * FROM users WHERE apikey = '{}'", anonkey))
    } else {
        bg.query_iter(&format!("SELECT * FROM users WHERE apikey = '{}'", key))
    };
    drop(bg);
    if rs.len() < 1 {
        Err(ApiKeyError::Missing)
    } else if rs.len() > 1 {
        Err(ApiKeyError::Ambiguous)
    } else if rs.len() >= 1 {
        Ok(from_value::<String>(rs[0][0].clone()))
    } else {
        Err(ApiKeyError::BackendFailure)
    }
}

#[post("/", data = "<data>")]
pub(crate) fn login(
    data: Form<ApiKeySubmit>,
    cookies: &CookieJar<'_>,
    backend: &State<Arc<Mutex<MySqlBackend>>>,
) -> Redirect {
    // check that the API key exists and set cookie
    let res = check_api_key(&*backend, &data.key, &String::new());
    match res {
        Err(ApiKeyError::BackendFailure) => {
            eprintln!("Problem communicating with MySql backend");
        }
        Err(ApiKeyError::Missing) => {
            eprintln!("No such API key: {}", data.key);
        }
        Err(ApiKeyError::Ambiguous) => {
            eprintln!("Ambiguous API key: {}", data.key);
        }
        Ok(_) => (),
    }

    if res.is_err() {
        Redirect::to("/")
    } else {
        cookies.add(("apikey", data.key.clone()));
        cookies.add(("email", data.email.clone()));
        cookies.add(("anonkey", "".to_string()));
        Redirect::to("/leclist")
    }
}

#[post("/")]
pub(crate) fn logout(cookies: &CookieJar<'_>) -> Redirect {
    // check that the API key exists and set cookie
    if let Some(cookie) = cookies.get("anonkey") {
        cookies.remove(cookie.clone());
    }
    if let Some(cookie) = cookies.get("apikey") {
        cookies.remove(cookie.clone());
    }
    if let Some(cookie) = cookies.get("email") {
        cookies.remove(cookie.clone());
    }

    Redirect::to("/")
}
