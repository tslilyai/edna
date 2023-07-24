use crate::helpers::*;
use crate::lowlevel_api::*;
use crate::{RowVal, DID, UID};
use log::debug;
use mysql::prelude::*;
//use log::error;
use crate::*;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
//use std::mem::size_of_val;

#[derive(Default, Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct SpeaksForRecordWrapper {
    pub old_uid: UID,
    pub new_uid: UID,
    pub did: DID,
    pub nonce: u64,
    pub record_data: Vec<u8>,
}

#[derive(Default, Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct EdnaSpeaksForRecord {
    pub cname: String,
    pub cids: Vec<String>,
    pub fk_col: String,
}

pub fn edna_sf_record_from_bytes(bytes: &Vec<u8>) -> Result<EdnaSpeaksForRecord, bincode::Error> {
    bincode::deserialize(bytes)
}

pub fn edna_sf_record_to_bytes(record: &EdnaSpeaksForRecord) -> Vec<u8> {
    bincode::serialize(record).unwrap()
}

pub fn speaksfor_record_from_bytes(bytes: &Vec<u8>) -> SpeaksForRecordWrapper {
    bincode::deserialize(bytes).unwrap()
}
pub fn speaksfor_records_from_bytes(bytes: &Vec<u8>) -> Vec<SpeaksForRecordWrapper> {
    bincode::deserialize(bytes).unwrap()
}
pub fn new_generic_speaksfor_record_wrapper(
    old_uid: UID,
    new_uid: UID,
    did: DID,
    data: Vec<u8>,
) -> SpeaksForRecordWrapper {
    let mut record: SpeaksForRecordWrapper = Default::default();
    record.new_uid = new_uid;
    record.old_uid = old_uid;
    record.did = did;
    record.nonce = thread_rng().gen();
    record.record_data = data;

    /*error!("OTW DATA: nonce {}, old_uid {}, new_uid {}, did {}, all: {}",
        size_of_val(&record.nonce),
        size_of_val(&record.did),
        size_of_val(&*record.new_uid),
        size_of_val(&*record.old_uid),
        size_of_val(&record),
    );*/

    record
}

pub fn new_edna_speaksfor_record(
    cname: String,
    cids: Vec<RowVal>,
    fk_col: String,
    new_uid: &str,
) -> EdnaSpeaksForRecord {
    let mut edna_record: EdnaSpeaksForRecord = Default::default();
    edna_record.cname = cname;
    // ensure that if this is predicated on the original user ID, that we will check for the pseudoprincipal UID
    edna_record.cids = cids
        .iter()
        .map(|rv| {
            if fk_col == rv.column() {
                new_uid.to_string()
            } else {
                rv.value().clone()
            }
        })
        .collect();
    edna_record.fk_col = fk_col;

    /*error!("EDNA OT: cn {}, cids {}, fkcol {}, total {}",
        size_of_val(&*edna_record.cname),
        size_of_val(&*edna_record.cids),
        size_of_val(&*edna_record.fk_col),
        size_of_val(&edna_record),
    );*/
    edna_record
}

impl EdnaSpeaksForRecord {
    pub fn reveal<Q: Queryable>(
        &self,
        timap: &HashMap<String, TableInfo>,
        otw: &SpeaksForRecordWrapper,
        guise_gen: &GuiseGen,
        llapi: &mut LowLevelAPI,
        db: &mut Q,
    ) -> Result<bool, mysql::Error> {
        // NOTE we could also transfer the principal's records to the original principal's bag, and
        // reencrypt

        // if original entity does not exist, do not recorrelate
        let selection = format!("{} = '{}'", guise_gen.id_col, otw.old_uid.to_string());
        debug!("selection: {}", selection.to_string());
        let selected = get_query_rows_str_q::<Q>(
            &str_select_statement(&guise_gen.name, &guise_gen.name, &selection.to_string()),
            db,
        )?;
        debug!("selected : {:?}", selected);
        if selected.is_empty() {
            debug!(
                "SFRecord Reveal: Original entity col {} = {}, original otw {:?} does not exist\n",
                guise_gen.id_col, otw.old_uid, otw,
            );
            return Ok(false);
        }

        let table_info = timap.get(&self.cname).unwrap();
        let record_guise_selection = get_select_of_ids_str(&table_info, &self.cids);
        let mut failed = false;

        // if foreign key is rewritten, don't reverse anything
        let selected = get_query_rows_str_q::<Q>(
            &str_select_statement(&self.cname, &self.cname, &record_guise_selection),
            db,
        )?;
        debug!("selected: {:?}", selected);

        // if row doesn't exist, don't do anything
        // NOTE: this will fail if we're doing the recorrelation mess and one of the CIDs turns
        // out to be the fk_col...
        if selected.len() == 0 {
            debug!(
                "Did not find any records to recorrelate with selection {}",
                record_guise_selection
            );
            failed = true;
        } else {
            assert_eq!(selected.len(), 1);
            let curval = get_value_of_col(&selected[0], &self.fk_col).unwrap();
            if curval != otw.new_uid {
                debug!(
                    "SFRecord Cannot Reveal: Foreign key col {} rewritten from {} to {}\n",
                    self.fk_col, otw.new_uid, curval
                );
                failed = true;
            } else {
                // ok, we can actually update this to point to the original entity!
                let q = format!(
                    "UPDATE {} SET {} = \'{}\' WHERE {}",
                    self.cname, self.fk_col, otw.old_uid, record_guise_selection
                );
                debug!("updating pp to original owner: {}", q);
                db.query_drop(q.clone())?;
            }
        }

        // remove the pseudoprincipal in all cases since it shouldn't exist
        // NOTE: the pseudoprincipal may already not exist because there may be more than one
        // object decorrelated to the same pp
        // TODO: handle referential integrity of any objects that may now refer to this one
        db.query_drop(format!(
            "DELETE FROM {} WHERE {} = \'{}\'",
            guise_gen.name,
            guise_gen.id_col,
            otw.new_uid.clone()
        ))?;
        debug!("Delete user {} from table {}", otw.new_uid, guise_gen.name);
        // remove PP metadata from the record ctrler (when all locators are gone)
        llapi.forget_principal(&otw.new_uid, otw.did);
        Ok(!failed)
    }
}
