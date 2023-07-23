use crate::admin::Admin;
use crate::apikey::ApiKey;
use crate::backend::MySqlBackend;
use crate::disguises;
use crate::email;
use crate::ADMIN_EMAIL;
use mysql::from_value;
use rocket::form::{Form, FromForm};
use rocket::http::{Cookie, CookieJar};
use rocket::response::Redirect;
use rocket::State;
use rocket_dyn_templates::Template;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Debug, FromForm)]
pub(crate) struct RestoreRequest {
    email: String,
    apikey: String,
}

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
    disguises::universal_anon_disguise::apply(&mut *bg).expect("Failed to apply anon disguise");

    // send emails to all users
    let mut emails = vec![];
    let res = bg.query(ADMIN_EMAIL, "SELECT email FROM users WHERE is_anon = 0;");
    for r in res {
        if r[0].as_sql(true) == "NULL" {
            continue;
        }
        let uid: String = from_value(r[0].clone());
        emails.push(uid);
    }
    email::send(
        "no-reply@csci2390-submit.cs.brown.edu".into(),
        emails,
        "Your Websubmit Answers Have Been Anonymized, edit them at /anon/edit/<lecture number>"
            .into(),
        String::new(),
    )
    .expect("failed to send email");
    drop(bg);

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
    // get all answers for lectures for all pps
    let mut answers = HashMap::new();
    let mut ppapikey = String::new();
    let mut bg = backend.lock().unwrap();

    let pps_res = bg.query(
        &data.email,
        &format!(
            "SELECT email, apikey \
            FROM users \
            WHERE users.owner = '{}'",
            data.email,
        ),
    );

    let mut pps = vec![];
    let mut apikeys = vec![];
    for r in pps_res {
        pps.push(r[0].as_sql(true));
        apikeys.push(r[1].as_sql(true));
    }

    let answers_res = bg.query(
        &data.email,
        &format!(
            "SELECT q, answer, email \
            FROM answers \
            WHERE answers.lec = {} AND answers.email IN ({})",
            data.lec_id, pps.join(","),
        ),
    );
    for r in answers_res {
        if r[1].as_sql(true) == "NULL" {
            continue;
        }
        let qid: u64 = from_value(r[0].clone());
        let atext: String = from_value(r[1].clone());
        let email: String = from_value(r[2].clone());
        if ppapikey.is_empty() {
            for (i, pp) in pps.iter().enumerate() {
                if pp == &email {
                    ppapikey = apikeys[i].clone();
                    break;
                }
            }
        }
        answers.insert(qid, atext);
    }

    let res = bg.query(
        &data.email,
        &format!(
            "SELECT q, question FROM questions WHERE lec = {}",
            data.lec_id
        ),
    );
    let mut qs: Vec<LectureQuestion> = vec![];
    for r in res {
        let qid: u64 = from_value(r[0].clone());
        let answer = answers.get(&qid).map(|s| s.to_owned());
        if answer == None {
            continue;
        }
        qs.push(LectureQuestion {
            id: qid,
            prompt: from_value(r[1].clone()),
            answer: answer,
        });
    }
    qs.sort_by(|a, b| a.id.cmp(&b.id));
    drop(bg);

    let ctx = LectureQuestionsContext {
        lec_id: data.lec_id,
        questions: qs,
        parent: "layout",
    };
    // this just lets the user act as the latest pseudoprincipal
    // but it won't reset afterward.... so the user won't be able to do anything else
    let cookie = Cookie::build("anonkey", ppapikey.clone()).path("/").finish();
    cookies.add(cookie);
    let cookie = Cookie::build("email", data.email.to_string())
        .path("/")
        .finish();
    cookies.add(cookie);
    let cookie = Cookie::build("apikey", data.apikey.clone())
        .path("/")
        .finish();
    cookies.add(cookie);
    Template::render("questions", &ctx)
}

/*
 * GDPR deletion
 */
#[post("/")]
pub(crate) fn gdpr_delete(apikey: ApiKey, backend: &State<Arc<Mutex<MySqlBackend>>>) -> Redirect {
    let mut bg = backend.lock().unwrap();
    disguises::gdpr_disguise::apply(&mut *bg, apikey.user.clone(), apikey.key.clone())
        .expect("Failed to apply GDPR deletion disguise");

    email::send(
        "no-reply@csci2390-submit.cs.brown.edu".into(),
        vec![apikey.user.clone()],
        "You Have Deleted Your Websubmit Account".into(),
        format!("You have successfully deleted your account! To restore your account, please click http://localhost:8000/restore"),
    )
    .expect("failed to send email");

    Redirect::to(format!("/login"))
}

#[get("/")]
pub(crate) fn restore() -> Template {
    let mut ctx = HashMap::new();
    ctx.insert("parent", String::from("layout"));
    Template::render("restore", &ctx)
}

#[post("/", data = "<data>")]
pub(crate) fn gdpr_restore(
    data: Form<RestoreRequest>,
    backend: &State<Arc<Mutex<MySqlBackend>>>,
) -> Redirect {
    let mut bg = backend.lock().unwrap();
    disguises::gdpr_disguise::reveal(&mut *bg, data.email.clone(), data.apikey.clone())
        .expect("Failed to reverse GDPR deletion disguise");

    Redirect::to(format!("/login"))
}
