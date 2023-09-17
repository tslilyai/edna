use crate::*;
use edna::EdnaClient;
use rocket::serde::{json::Json, Deserialize};
use rocket::State;
use rocket_okapi::{openapi, JsonSchema};
use std::sync::{Arc, Mutex};

/************************
 * OpenAPI JSON Types
 ************************/

#[derive(Serialize, JsonSchema)]
pub struct APIRowVal {
    pub column: String,
    pub value: String,
}

/************************
 * High-Level API
 ************************/
#[derive(Deserialize, JsonSchema)]
pub struct ApplyDisguise {
    user: String,
    disguise_json: String,
    tableinfo_json: String,
    password: String,
    pseudoprincipalgen_json: String,
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct ApplyDisguiseResponse {
    pub did: edna::DID,
}

#[openapi]
#[post("/apply_disguise", format = "json", data = "<data>")]
pub(crate) fn apply_disguise(
    data: Json<ApplyDisguise>,
    edna: &State<Arc<Mutex<EdnaClient>>>,
) -> Json<ApplyDisguiseResponse> {
    let mut e = edna.lock().unwrap();
    let password = if data.password == "" {
        None
    } else {
        Some(data.password.clone())
    };
    let did = e
        .apply_disguise(
            data.user.clone(),
            &data.disguise_json,
            &data.tableinfo_json,
            &data.ppgen_json,
            password,
            None,
            false, // use_txn
        )
        .unwrap();
    let json = Json(ApplyDisguiseResponse { did: did });
    return json;
}

#[derive(Deserialize, JsonSchema)]
pub struct RevealDisguise {
    tableinfo_json: String,
    pseudoprincipalgen_json: String,
    password: String,
}

#[openapi]
#[post("/reveal_disguise/<uid>/<did>", format = "json", data = "<data>")]
pub(crate) fn reveal_disguise(
    uid: edna::UID,
    did: edna::DID,
    data: Json<RevealDisguise>,
    edna: &State<Arc<Mutex<EdnaClient>>>,
) {
    let mut e = edna.lock().unwrap();
    let password = if data.password == "" {
        None
    } else {
        Some(data.password.clone())
    };

    // XXX clones
    e.reveal_disguise(
        uid,
        did,
        &data.tableinfo_json,
        &data.ppgen_json,
        Some(edna::RevealPPType::Restore),
        password,
        None,
        false, // use_txn
    )
    .unwrap();
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct RegisterPrincipal {
    pub uid: String,
    pub pw: String,
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct RegisterPrincipalResponse {
    // base64-encoded user share
    pub share: String,
}

#[openapi]
#[post("/register_principal", data = "<data>")]
pub(crate) fn register_principal(
    data: Json<RegisterPrincipal>,
    edna: &State<Arc<Mutex<EdnaClient>>>,
) -> Json<RegisterPrincipalResponse> {
    let mut e = edna.lock().unwrap();
    let share = e.register_principal(&data.uid, data.pw.clone());
    let share_str = serde_json::to_string(&share).unwrap();
    return Json(RegisterPrincipalResponse { share: share_str });
}

#[derive(Deserialize, JsonSchema)]
pub struct GetPseudoprincipals {
    password: String,
}

#[openapi]
#[post("/get_pps_of/<uid>", format = "json", data = "<data>")]
pub(crate) fn get_pseudoprincipals_of(
    uid: String,
    data: Json<GetPseudoprincipals>,
    edna: &State<Arc<Mutex<EdnaClient>>>,
) -> Json<Vec<edna::UID>> {
    let e = edna.lock().unwrap();
    let password = if data.password == "" {
        None
    } else {
        Some(data.password.clone())
    };

    let pps: Vec<String> = e
        .get_pseudoprincipals(uid, password, None)
        .into_iter()
        .collect();
    return Json(pps);
}

/************************
 * Low-Level API
 ************************/

#[derive(Serialize, JsonSchema)]
pub struct StartDisguiseResponse {
    did: edna::DID,
}

#[openapi]
#[get("/start_disguise/<acting_uid>")]
pub(crate) fn start_disguise(
    acting_uid: edna::UID,
    edna: &State<Arc<Mutex<EdnaClient>>>,
) -> Json<StartDisguiseResponse> {
    let e = edna.lock().unwrap();
    let acting_uid = if acting_uid == "NULL " {
        None
    } else {
        Some(acting_uid.clone())
    };
    return Json(StartDisguiseResponse {
        did: e.start_disguise(acting_uid),
    });
}

#[openapi]
#[get("/end_disguise/<did>")]
pub(crate) fn end_disguise(did: edna::DID, edna: &State<Arc<Mutex<EdnaClient>>>) {
    let _ = did;
    let e = edna.lock().unwrap();
    e.end_disguise();
}

#[openapi]
#[get("/start_reveal/<did>")]
pub(crate) fn start_reveal(did: edna::DID, edna: &State<Arc<Mutex<EdnaClient>>>) {
    let e = edna.lock().unwrap();
    e.start_reveal(did)
}

#[openapi]
#[get("/end_reveal/<did>")]
pub(crate) fn end_reveal(did: edna::DID, edna: &State<Arc<Mutex<EdnaClient>>>) {
    let mut e = edna.lock().unwrap();
    e.end_reveal(did);
}

#[derive(Deserialize, JsonSchema)]
pub struct GetRecordsOfDisguise {
    did: edna::DID,
    decrypt_cap: edna::records::DecryptCap,
}

#[derive(Serialize, JsonSchema)]
pub struct GetRecordsOfDisguiseResponse {
    diff_records: Vec<Vec<u8>>,
    ownership_records: Vec<Vec<u8>>,
}

#[openapi]
#[post("/get_records_of_disguise", format = "json", data = "<data>")]
pub(crate) fn get_records_of_disguise(
    data: Json<GetRecordsOfDisguise>,
    edna: &State<Arc<Mutex<EdnaClient>>>,
) -> Json<GetRecordsOfDisguiseResponse> {
    let e = edna.lock().unwrap();
    let records = e.get_records_of_disguise(data.did, &data.decrypt_cap);
    return Json(GetRecordsOfDisguiseResponse {
        diff_records: records.0,
        ownership_records: records.1,
    });
}

#[derive(Deserialize, JsonSchema)]
pub struct CleanupRecordsOfDisguise {
    did: edna::DID,
    decrypt_cap: edna::records::DecryptCap,
}
#[openapi]
#[post("/cleanup_records_of_disguise", format = "json", data = "<data>")]
pub(crate) fn cleanup_records_of_disguise(
    data: Json<CleanupRecordsOfDisguise>,
    edna: &State<Arc<Mutex<EdnaClient>>>,
) {
    let e = edna.lock().unwrap();
    e.cleanup_records_of_disguise(data.did, &data.decrypt_cap);
}

#[derive(Deserialize, JsonSchema)]
pub struct SavePseudoprincipalRecord {
    did: edna::DID,
    old_uid: edna::UID,
    new_uid: edna::UID,
    record_bytes: Vec<u8>,
}

#[openapi]
#[post("/save_pp_record", format = "json", data = "<data>")]
pub(crate) fn save_pseudoprincipal_record(
    data: Json<SavePseudoprincipalRecord>,
    edna: &State<Arc<Mutex<EdnaClient>>>,
) {
    let e = edna.lock().unwrap();
    e.register_and_save_pseudoprincipal_record(
        data.did,
        &data.old_uid,
        &data.new_uid,
        &data.record_bytes,
    );
}

#[derive(Deserialize, JsonSchema)]
pub struct SaveDiffRecord {
    uid: edna::UID,
    did: edna::DID,
    data: Vec<u8>,
}

#[openapi]
#[post("/save_diff_record", format = "json", data = "<data>")]
pub(crate) fn save_diff_record(data: Json<SaveDiffRecord>, edna: &State<Arc<Mutex<EdnaClient>>>) {
    let e = edna.lock().unwrap();
    e.save_diff_record(data.uid.clone(), data.did, data.data.clone());
}
