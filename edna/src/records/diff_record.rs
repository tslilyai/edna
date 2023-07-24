use crate::crypto::*;
use crate::helpers::*;
use crate::lowlevel_api::*;
use crate::records::*;
use crate::{RowVal, TableInfo, DID, UID};
use crypto_box::PublicKey;
use log::warn;
use mysql::prelude::*;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time;
//use std::mem::size_of_val;

pub const REMOVE_GUISE: u8 = 0;
pub const DECOR_GUISE: u8 = 1;
pub const MODIFY_GUISE: u8 = 2;
pub const REMOVE_PRINCIPAL: u8 = 5;

#[derive(Default, Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct DiffRecordWrapper {
    pub did: DID,
    pub uid: UID,
    pub record_data: Vec<u8>,

    // FOR SECURITY DESIGN
    // for randomness
    pub nonce: u64,
}

#[derive(Default, Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct EdnaDiffRecord {
    // metadata set by Edna
    pub typ: u8,

    // guise information
    pub table: String,
    pub tabids: Vec<String>,

    // MODIFY/REMOVE : store old blobs
    pub old_value: Vec<RowVal>,

    // MODIFY: store new blobs
    pub col: String,
    pub old_val: String,
    pub new_val: String,

    // REMOVE PRINCIPAL
    pub pubkey: Vec<u8>,
    pub enc_locators_index: Index,
}

pub fn diff_records_from_bytes(bytes: &Vec<u8>) -> Vec<DiffRecordWrapper> {
    bincode::deserialize(bytes).unwrap()
}
pub fn diff_record_from_bytes(bytes: &Vec<u8>) -> DiffRecordWrapper {
    bincode::deserialize(bytes).unwrap()
}
pub fn diff_record_to_bytes(record: &DiffRecordWrapper) -> Vec<u8> {
    bincode::serialize(record).unwrap()
}
pub fn edna_diff_record_from_bytes(bytes: &Vec<u8>) -> EdnaDiffRecord {
    bincode::deserialize(bytes).unwrap()
}
pub fn edna_diff_record_to_bytes(record: &EdnaDiffRecord) -> Vec<u8> {
    bincode::serialize(record).unwrap()
}

// create diff record for generic data
pub fn new_generic_diff_record_wrapper(uid: &UID, did: DID, data: Vec<u8>) -> DiffRecordWrapper {
    let mut record: DiffRecordWrapper = Default::default();
    record.nonce = thread_rng().gen();
    record.uid = uid.to_string();
    record.did = did;
    record.record_data = data;
    record
}

// create diff record for removing a principal
pub fn new_remove_principal_record(uid: &UID, did: DID, pdata: &PrincipalData) -> EdnaDiffRecord {
    let mut record: DiffRecordWrapper = Default::default();
    record.nonce = thread_rng().gen();
    record.uid = uid.to_string();
    record.did = did;

    let mut edna_record: EdnaDiffRecord = Default::default();
    edna_record.pubkey = match &pdata.pubkey {
        Some(pk) => pk.as_bytes().to_vec(),
        None => vec![],
    };
    edna_record.enc_locators_index = pdata.enc_locators_index;
    edna_record.typ = REMOVE_PRINCIPAL;
    edna_record
}

// create diff record for removing a principal
pub fn new_remove_principal_record_wrapper(
    uid: &UID,
    did: DID,
    pdata: &PrincipalData,
) -> DiffRecordWrapper {
    let mut record: DiffRecordWrapper = Default::default();
    record.nonce = thread_rng().gen();
    record.uid = uid.to_string();
    record.did = did;

    let mut edna_record: EdnaDiffRecord = Default::default();
    edna_record.pubkey = match &pdata.pubkey {
        Some(pk) => pk.as_bytes().to_vec(),
        None => vec![],
    };
    edna_record.enc_locators_index = pdata.enc_locators_index;
    edna_record.typ = REMOVE_PRINCIPAL;
    record.record_data = edna_diff_record_to_bytes(&edna_record);
    /*error!("REMOVE PRINC: nonce {}, uid {}, did {}, pubkey {}, tp {}, all: {}",
        size_of_val(&record.nonce),
        size_of_val(&*record.uid),
        size_of_val(&record.did),
        size_of_val(&*edna_record.pubkey),
        size_of_val(&edna_record.typ),
        size_of_val(&record),
    );*/
    record
}

// create diff records about db objects
pub fn new_delete_record(
    table: String,
    tabids: Vec<RowVal>,
    old_value: Vec<RowVal>,
) -> EdnaDiffRecord {
    let mut edna_record: EdnaDiffRecord = Default::default();
    edna_record.typ = REMOVE_GUISE;
    edna_record.table = table;
    edna_record.tabids = tabids.iter().map(|rv| rv.value().clone()).collect();
    edna_record.old_value = old_value;

    // XXX Remove
    /*let mut old_val_rvs = 0;
    for v in &edna_record.old_value {
        old_val_rvs += size_of_val(&*v);
    }
    error!("REMOVE DATA: nonce {}, did {}, table {}, tableid {}, tp {}, oldvalblob {}, all: {}",
        size_of_val(&record.nonce),
        size_of_val(&record.did),
        size_of_val(&*edna_record.table),
        size_of_val(&*edna_record.tabids),
        size_of_val(&edna_record.typ),
        size_of_val(&*edna_record.old_value) + old_val_rvs,
        size_of_val(&record),
    );*/
    edna_record
}

// create diff records about db objects
pub fn new_delete_record_wrapper(
    did: DID,
    table: String,
    tabids: Vec<RowVal>,
    old_value: Vec<RowVal>,
) -> DiffRecordWrapper {
    let mut record: DiffRecordWrapper = Default::default();
    record.nonce = thread_rng().gen();
    record.did = did;

    let mut edna_record: EdnaDiffRecord = Default::default();
    edna_record.typ = REMOVE_GUISE;
    edna_record.table = table;
    edna_record.tabids = tabids.iter().map(|rv| rv.value().clone()).collect();
    edna_record.old_value = old_value;
    record.record_data = edna_diff_record_to_bytes(&edna_record);
    record
}

pub fn new_modify_record(
    table: String,
    tabids: Vec<RowVal>,
    old_value: String,
    new_value: String,
    col: String,
) -> EdnaDiffRecord {
    let mut edna_record: EdnaDiffRecord = Default::default();
    edna_record.typ = MODIFY_GUISE;
    edna_record.table = table;
    edna_record.tabids = tabids.iter().map(|rv| rv.value().clone()).collect();
    edna_record.col = col;
    edna_record.old_val = old_value;
    edna_record.new_val = new_value;
    edna_record

    /*error!("MODIFY DATA: nonce {}, did {}, table {}, tableids {}, tp {}, col {}, old_col_val {}, newval {}, all: {}",
        size_of_val(&record.nonce),
        size_of_val(&record.did),
        size_of_val(&*edna_record.table),
        size_of_val(&*edna_record.tabids),
        size_of_val(&edna_record.typ),
        size_of_val(&*edna_record.col),
        size_of_val(&*edna_record.old_val),
        size_of_val(&*edna_record.new_val),
        size_of_val(&record),
    );*/
}

pub fn new_modify_record_wrapper(
    did: DID,
    table: String,
    tabids: Vec<RowVal>,
    old_value: String,
    new_value: String,
    col: String,
) -> DiffRecordWrapper {
    let mut record: DiffRecordWrapper = Default::default();
    record.nonce = thread_rng().gen();
    record.did = did;

    let mut edna_record: EdnaDiffRecord = Default::default();
    edna_record.typ = MODIFY_GUISE;
    edna_record.table = table;
    edna_record.tabids = tabids.iter().map(|rv| rv.value().clone()).collect();
    edna_record.col = col;
    edna_record.old_val = old_value;
    edna_record.new_val = new_value;
    record.record_data = edna_diff_record_to_bytes(&edna_record);

    /*error!("MODIFY DATA: nonce {}, did {}, table {}, tableids {}, tp {}, col {}, old_col_val {}, newval {}, all: {}",
        size_of_val(&record.nonce),
        size_of_val(&record.did),
        size_of_val(&*edna_record.table),
        size_of_val(&*edna_record.tabids),
        size_of_val(&edna_record.typ),
        size_of_val(&*edna_record.col),
        size_of_val(&*edna_record.old_val),
        size_of_val(&*edna_record.new_val),
        size_of_val(&record),
    );*/
    record
}

impl EdnaDiffRecord {
    pub fn reveal<Q: Queryable>(
        &self,
        timap: &HashMap<String, TableInfo>,
        uid: &str,
        llapi: &mut LowLevelAPI,
        db: &mut Q,
    ) -> Result<bool, mysql::Error> {
        match self.typ {
            // only ever called for a real principal
            REMOVE_PRINCIPAL => {
                let start = time::Instant::now();
                let pubkey = if self.pubkey.is_empty() {
                    None
                } else {
                    Some(PublicKey::from(get_pk_bytes(self.pubkey.clone())))
                };
                let pdata = PrincipalData {
                    pubkey: pubkey,
                    is_anon: false,
                    enc_locators_index: self.enc_locators_index,
                };
                warn!("Going to reveal principal {}", uid);
                llapi.register_saved_principal(
                    &uid.to_string(),
                    false,
                    &pdata.pubkey,
                    self.enc_locators_index,
                    true,
                );
                warn!("Reveal removed principal: {}", start.elapsed().as_micros());
            }

            REMOVE_GUISE => {
                let start = time::Instant::now();
                // get current guise in db
                let table_info = timap.get(&self.table).unwrap();
                let record_guise_selection = get_select_of_ids_str(&table_info, &self.tabids);
                let selected = get_query_rows_str_q::<Q>(
                    &str_select_statement(
                        &self.table,
                        &self.table,
                        &record_guise_selection.to_string(),
                    ),
                    db,
                )?;

                // Note: data can be revealed even if it should've been disguised in the interim

                // item has been re-inserted, ignore
                if !selected.is_empty() {
                    // true here because it's technically revealed
                    return Ok(true);
                }

                // check that all fk columns exist
                // if one of them that doesn't exist is a UID, rewrite to the latest revealed
                // UID (if exists)
                let table_info = timap.get(&self.table).unwrap();
                for (reftable, refcol, fk_col) in &table_info.other_fk_cols {
                    // if original entity does not exist, do not reveal
                    let curval = get_value_of_col(&self.old_value, fk_col).unwrap();
                    if curval.to_lowercase() == "null" {
                        continue;
                    }
                    // xxx this might have problems with quotes?
                    let selection = format!(
                        "SELECT * FROM {} WHERE {}.{} = {}",
                        reftable, reftable, refcol, curval
                    );
                    warn!("selection: {}", selection.to_string());
                    let selected = get_query_rows_str_q::<Q>(&selection, db)?;
                    if selected.is_empty() {
                        warn!(
                            "No original entity exists for fk col {} val {}",
                            fk_col, curval
                        );

                        // TODO handle case where fk_col is pointing to the users table, in which
                        // we could reveal as long as there's some speaks-for path to the stored
                        // UID in the diff, and we rewrite this col to the correct restored UID
                        return Ok(false);
                    }
                }

                // otherwise insert it
                let cols: Vec<String> = self
                    .old_value
                    .iter()
                    .map(|rv| rv.column().clone())
                    .collect();
                let colstr = cols.join(",");
                let vals: Vec<String> = self
                    .old_value
                    .iter()
                    .map(|rv| {
                        if rv.value().is_empty() {
                            "\"\"".to_string()
                        } else if rv.value() == "NULL" {
                            "NULL".to_string()
                        } else {
                            for c in rv.value().chars() {
                                if !c.is_numeric() {
                                    return format!("\"{}\"", rv.value().clone());
                                }
                            }
                            rv.value().clone()
                        }
                    })
                    .collect();
                let valstr = vals.join(",");
                db.query_drop(format!(
                    "INSERT INTO {} ({}) VALUES ({})",
                    self.table, colstr, valstr
                ))?;
                warn!(
                    "Reveal removed data for {}: {}",
                    self.table,
                    start.elapsed().as_micros()
                );
            }
            MODIFY_GUISE => {
                // get current guise in db
                let table_info = timap.get(&self.table).unwrap();
                let record_guise_selection = get_select_of_ids_str(&table_info, &self.tabids);
                let selected = get_query_rows_str_q(
                    &str_select_statement(
                        &self.table,
                        &self.table,
                        &record_guise_selection.to_string(),
                    ),
                    db,
                )?;

                // if field hasn't been modified, return it to original
                if selected.is_empty() {
                    warn!("DiffRecordWrapper Reveal: Modified value no longer exists\n",);
                }
                for rv in &selected[0] {
                    if rv.column() == self.col {
                        if rv.value() != self.new_val {
                            warn!(
                                "DiffRecordWrapper Reveal: Modified value {:?} not equal to new value {:?}\n",
                                rv.value(), self.new_val
                            );
                            return Ok(false);
                        }
                    }
                }

                // If we're revealing an fk cols, check for existence of
                // referenced obj
                // Note that this shouldn't run into issues with user foreign keys if the developer
                // isn't being dumb and using modify instead of decorrelate
                let table_info = timap.get(&self.table).unwrap();
                for (reftable, refcol, fk_col) in &table_info.other_fk_cols {
                    if &self.col == fk_col {
                        // if original entity does not exist, do not reveal
                        let curval = get_value_of_col(&self.old_value, fk_col).unwrap();
                        if curval.to_lowercase() == "null" {
                            continue;
                        }
                        // xxx this might have problems with quotes?
                        let selection = format!(
                            "SELECT * FROM {} WHERE {}.{} = {}",
                            reftable, reftable, refcol, curval
                        );
                        warn!("selection: {}", selection.to_string());
                        let selected = get_query_rows_str_q::<Q>(&selection, db)?;
                        if selected.is_empty() {
                            warn!(
                                "No original entity exists for fk col {} val {}",
                                fk_col, curval
                            );
                            return Ok(false);
                        }
                    }
                }

                // ok, we can actually update this!
                db.query_drop(format!(
                    "UPDATE {} SET {} = '{}' WHERE {}",
                    self.table, self.col, self.old_val, record_guise_selection,
                ))?;
            }
            _ => unimplemented!("Bad diff record update type?"), // do nothing for PRIV_KEY
        }
        Ok(true)
    }
}
