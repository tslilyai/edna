use crate::admin::Admin;
use crate::apikey::ApiKey;
use crate::backend::MySqlBackend;
use crate::disguises;
use crate::email;
use mysql::from_value;
use rocket::form::{Form, FromForm};
use rocket::http::{CookieJar};
use rocket::response::Redirect;
use rocket::State;
use rocket_dyn_templates::Template;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time;

#[derive(Serialize)]
pub(crate) struct LectureQuestion {
    pub id: u64,
    pub prompt: String,
    pub answer: Option<String>,
}

#[derive(Serialize)]
pub(crate) struct LectureQuestionsContext {
    pub lec_id: u8,
    pub questions: Vec<LectureQuestion>,
    pub parent: &'static str,
}

#[derive(Serialize)]
pub struct LectureListEntry {
    id: u64,
    label: String,
}

#[derive(Serialize)]
pub struct LectureListContext {
    lectures: Vec<LectureListEntry>,
    parent: &'static str,
}

#[derive(Debug, FromForm)]
pub(crate) struct RestoreRequest {
    email: String,
    did: u64,
    apikey: String,
}

#[derive(Debug, FromForm)]
pub(crate) struct EditCapabilitiesRequest {
    email: String,
    apikey: String,
    lec_id: u8,
}

/*
 * ANONYMIZATION
 */
#[post("/")]
pub(crate) fn anonymize_answers(
    _adm: Admin,
    backend: &State<Arc<Mutex<MySqlBackend>>>,
) -> Redirect {
    let mut bg = backend.lock().unwrap();
    let did = disguises::universal_anon_disguise::apply(&mut *bg).unwrap();

    // send emails to all users
    let res = bg.query_iter("SELECT * FROM users");
    let emails = res.into_iter().map(|r| from_value(r[0].clone())).collect();
    email::send(
        bg.log.clone(),
        "no-reply@csci2390-submit.cs.brown.edu".into(),
        emails,
        "Your Websubmit Answers Have Been Anonymized".into(),
        format!("DID#{}", serde_json::to_string(&did).unwrap()),
    )
    .expect("failed to send email");

    Redirect::to(format!("/leclist"))
}

#[get("/")]
pub(crate) fn anonymize(_adm: Admin) -> Template {
    let mut ctx = HashMap::new();
    ctx.insert("parent", String::from("layout"));
    Template::render("admin/anonymize", &ctx)
}

#[get("/")]
pub(crate) fn edit_as_pseudoprincipal_auth_request() -> Template {
    let mut ctx = HashMap::new();
    ctx.insert("parent", String::from("layout"));
    Template::render("edit_as_pseudoprincipal/get_credentials", &ctx)
}

#[post("/", data = "<data>")]
pub(crate) fn edit_as_pseudoprincipal(
    cookies: &CookieJar<'_>,
    data: Form<EditCapabilitiesRequest>,
    backend: &State<Arc<Mutex<MySqlBackend>>>,
) -> Template {
    // get all the UIDs that this user can access
    // logs in all the PPs
    let mut bg = backend.lock().unwrap();
    let start = time::Instant::now();
    let pps = bg.edna.get_pseudoprincipals(
        data.email.to_string(),
        Some(data.apikey.to_string()),
        None,
    );
    info!(
        bg.log,
        "Get pseudoprincipals: {}mus",
        start.elapsed().as_micros()
    );

    // get all answers for lectures
    let mut answers = HashMap::new();
    let mut apikey = String::new();

    'pploop: for pp in pps {
        let answers_res = bg.query_iter(&format!(
            "SELECT email, q, answer FROM answers WHERE lec = {} AND email = '{}';",
            data.lec_id, pp
        ));
        if answers_res.is_empty() {
            info!(
                bg.log,
                "No answers for lec {} for pp {}", data.lec_id, pp
                );
        } else {
            for r in answers_res {
                let email: String = from_value(r[0].clone());
                let qid: u64 = from_value(r[1].clone());
                let atext: String = from_value(r[2].clone());
                answers.insert(qid, atext);
                let apikey_res = bg.query_iter(&format!(
                    "SELECT apikey FROM users WHERE email = '{}'",
                    email
                ));
                apikey = from_value(apikey_res[0][0].clone());
                debug!(
                    bg.log,
                    "Editing as as pp {}: {}mus",
                    apikey,
                    start.elapsed().as_micros()
                );
                break;
            }
            break 'pploop;
        }
    }

    let res = bg.query_iter(&format!(
        "SELECT * FROM questions WHERE lec = {}",
        data.lec_id
    ));
    drop(bg);
    let mut qs: Vec<_> = res
        .into_iter()
        .map(|r| {
            let id: u64 = from_value(r[1].clone());
            let answer = answers.get(&id).map(|s| s.to_owned());
            LectureQuestion {
                id: id,
                prompt: from_value(r[2].clone()),
                answer: answer,
            }
        })
        .collect();
    qs.sort_by(|a, b| a.id.cmp(&b.id));

    let ctx = LectureQuestionsContext {
        lec_id: data.lec_id,
        questions: qs,
        parent: "layout",
    };

    // this just lets the user act as the latest pseudoprincipal
    // but it won't reset afterward.... so the user won't be able to do anything else
    cookies.add(("anonkey", apikey.clone()));
    cookies.add(("email", data.email.to_string()));
    cookies.add(("apikey", data.apikey.clone()));
    Template::render("questions", &ctx)
}

/*
 * GDPR deletion
 */
#[post("/")]
pub(crate) fn delete_submit(apikey: ApiKey, backend: &State<Arc<Mutex<MySqlBackend>>>) -> Redirect {
    let mut bg = backend.lock().unwrap();
    let did =
        disguises::gdpr_disguise::apply(&mut *bg, apikey.user.clone(), apikey.key.clone(), true)
            .unwrap();

    email::send(
        bg.log.clone(),
        "no-reply@csci2390-submit.cs.brown.edu".into(),
        vec![apikey.user.clone()],
        "You Have Deleted Your Websubmit Account".into(),
        format!("DID#{}", did),
        //"You have successfully deleted your account! To restore your account, please click http://localhost:8000/restore/{}",
    )
    .expect("failed to send email");
    drop(bg);

    Redirect::to(format!("/login"))
}

#[get("/")]
pub(crate) fn restore() -> Template {
    let mut ctx = HashMap::new();
    ctx.insert("parent", String::from("layout"));
    Template::render("restore", &ctx)
}

#[post("/", data = "<data>")]
pub(crate) fn restore_account(
    data: Form<RestoreRequest>,
    backend: &State<Arc<Mutex<MySqlBackend>>>,
) -> Redirect {
    let mut bg = backend.lock().unwrap();
    disguises::gdpr_disguise::reveal(data.did, &mut *bg, data.email.clone(), data.apikey.clone())
        .expect("Failed to reverse GDPR deletion disguise");
    drop(bg);

    Redirect::to(format!("/login"))
}
