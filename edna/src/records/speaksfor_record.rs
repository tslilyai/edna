use crate::helpers::*;
use crate::lowlevel_api::*;
use crate::*;
use crate::{RevealPPType::*, RowVal, DID, UID};
use log::debug;
use mysql::from_value;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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
    pub cids: Vec<String>, /* ids of the obj being decorrelated */
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

    /*error!("SFR DATA: nonce {}, old_uid {}, new_uid {}, did {}, all: {}",
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
        timap: &HashMap<String, TableInfo>,
        sfrws: &Vec<SpeaksForRecordWrapper>,
        pp_gen: &PseudoprincipalGenerator,
        reveal_pps: Option<RevealPPType>,
        llapi: &mut LowLevelAPI,
        db: &mut Q,
    ) -> Result<bool, mysql::Error> {
        if sfrws.is_empty() {
            return Ok(true);
        }
        let mut old_to_new: HashMap<UID, HashMap<TableName, Vec<UID>>> = HashMap::new();
        let mut obj_ids: HashMap<(UID, TableName), Vec<Vec<String>>> = HashMap::new();
        let pps_to_delete: Vec<UID> = sfrws
            .iter()
            .map(|sfrw| format!("'{}'", sfrw.new_uid))
            .collect();
        for sfrw in sfrws {
            let sfr = edna_sf_record_from_bytes(&sfrw.record_data).unwrap();
            let olduid = format!("'{}'", sfrw.old_uid);
            let newuid = format!("'{}'", sfrw.new_uid);
            debug!("Adding new sfr: {}, {}, {}", sfr.cname, olduid, newuid);
            match old_to_new.get_mut(&olduid) {
                Some(news) => match news.get_mut(&sfr.cname) {
                    Some(ids) => ids.push(newuid),
                    None => {
                        news.insert(sfr.cname.clone(), vec![newuid]);
                    }
                },
                None => {
                    let mut hm = HashMap::new();
                    hm.insert(sfr.cname.clone(), vec![newuid]);
                    old_to_new.insert(olduid.clone(), hm);
                }
            }
            match obj_ids.get_mut(&(olduid.clone(), sfr.cname.clone())) {
                Some(rows) => rows.push(sfr.cids),
                None => {
                    obj_ids.insert((olduid.clone(), sfr.cname), vec![sfr.cids]);
                }
            }
        }

        // CHECK: if original entity does not exist, do not recorrelate
        let selection = format!(
            "{} IN ({})",
            pp_gen.id_col,
            old_to_new.keys().cloned().collect::<Vec<_>>().join(",")
        );
        debug!("selection: {}", selection.to_string());
        let selected = get_query_rows_str_q::<Q>(
            &str_select_statement(&pp_gen.name, &pp_gen.name, &selection.to_string()),
            db,
        )?;
        debug!("selected : {:?}", selected);
        if selected.len() != old_to_new.keys().len() {
            debug!(
                "SFRecord Reveal: {} col selection {} != {}\n",
                pp_gen.id_col,
                selected.len(),
                old_to_new.keys().len(),
            );
            return Ok(false);
        }

        // DO RECORRELATION FOR TABLE IN SFR
        // Note: only works for speaksfor records from edna's HLAPI
        // NOTE we could also transfer the principal's records to the original principal's bag, and
        // reencrypt
        // TODO: this will fail if we're doing the recorrelation chain mess and one of the CIDs turns
        // out to be the fk_col...
        for (old_uid, table_newuids) in &old_to_new {
            for (table, newuids) in table_newuids {
                debug!("Updating SFR: {}, {}, {}", table, old_uid, newuids.len());
                let tinfo = timap.get(table).unwrap();
                EdnaSpeaksForRecord::update_or_delete_table_rows(
                    old_uid,
                    newuids,
                    tinfo,
                    obj_ids.get(&(old_uid.clone(), table.clone())),
                    false,
                    db,
                )?;
            }
        }

        if reveal_pps == Some(Delete) || reveal_pps == Some(Restore) {
            // Check for references from *any other* table to the pp; if so, do the slow path and rewrite to point to original.
            // batch for all pseudoprincipals (do an IN select)
            let mut all_pps = vec![];
            for (_old_uid, table_newuids) in &old_to_new {
                for (_table, new_uids) in table_newuids {
                    all_pps.append(&mut new_uids.clone());
                }
            }
            for (table, tinfo) in timap.into_iter() {
                if table == &pp_gen.name {
                    continue;
                }
                // note that we already restored links for the actual SPRs so we don't need to
                // worry about accidentally catching them here
                let all_selection = if all_pps.len() == 1 {
                    tinfo
                        .owner_fk_cols
                        .iter()
                        .map(|c| format!("`{}` = {}", c, all_pps[0]))
                        .collect::<Vec<String>>()
                        .join(" OR ")
                } else {
                    tinfo
                        .owner_fk_cols
                        .iter()
                        .map(|c| format!("`{}` IN ({})", c, all_pps.join(", ")))
                        .collect::<Vec<String>>()
                        .join(" OR ")
                };

                let checkstmt = format!("SELECT COUNT(*) FROM {} WHERE {}", table, all_selection);
                let start = time::Instant::now();
                let res = db.query_iter(checkstmt.clone()).unwrap();
                let mut count: u64 = 0;
                for row in res {
                    count = from_value(row.unwrap().unwrap()[0].clone());
                    break;
                }
                warn!(
                    "{}:\tcount {}:\t{}mus",
                    checkstmt,
                    count,
                    start.elapsed().as_micros()
                );
                if count == 0 {
                    continue;
                }

                // SLOW PATH: WE NEED TO UPDATE STUFF HERE
                for (old_uid, table_new_uids) in &old_to_new {
                    let mut new_uids = vec![];
                    for (_, ids) in table_new_uids {
                        new_uids.append(&mut ids.clone());
                    }
                    EdnaSpeaksForRecord::update_or_delete_table_rows(
                        old_uid,
                        &new_uids,
                        tinfo,
                        None,
                        reveal_pps == Some(Delete),
                        db,
                    )?;
                }
            }
        }

        // FOR ALL OTHER THAN RETAIN: remove the pps
        if reveal_pps != Some(Retain) {
            db.query_drop(format!(
                "DELETE FROM {} WHERE {} IN ({})",
                pp_gen.name,
                pp_gen.id_col,
                pps_to_delete.join(",")
            ))?;
        }
        debug!(
            "Delete users {:?} from table {}",
            pps_to_delete, pp_gen.name
        );
        // remove PP metadata from the record ctrler (when all locators are gone)
        // do per new uid because did might differ
        // NOTE: pps kept for "restore" can never be reclaimed now
        for sfrw in sfrws {
            llapi.forget_principal(&sfrw.new_uid, sfrw.did);
        }
        return Ok(false);
    }

    fn update_or_delete_table_rows<Q: Queryable>(
        old_uid: &UID,
        newuids: &Vec<UID>,
        tinfo: &TableInfo,
        object_ids: Option<&Vec<Vec<String>>>,
        delete: bool,
        db: &mut Q,
    ) -> Result<(), mysql::Error> {
        // NOTE: we batch here, which assumes that pseudoprincipal only owns the items of the
        // original sfr table that was decorrelated (otherwise we'd have to check the specific
        // table row)
        let mut selection = if newuids.len() == 1 {
            tinfo
                .owner_fk_cols
                .iter()
                .map(|c| format!("`{}` = {}", c, newuids[0]))
                .collect::<Vec<String>>()
                .join(" OR ")
        } else {
            tinfo
                .owner_fk_cols
                .iter()
                .map(|c| format!("`{}` IN ({})", c, newuids.join(", ")))
                .collect::<Vec<String>>()
                .join(" OR ")
        };

        match object_ids {
            None => (),
            Some(ids) => {
                let mut obj_select = vec![];
                for row in ids {
                    let mut checks = vec![];
                    for (i, c) in tinfo.id_cols.iter().enumerate() {
                        checks.push(format!("{} = {}", c, row[i]));
                    }
                    obj_select.push(format!("({})", checks.join(" AND ")));
                }
                selection = format!("{} AND ({})", selection, obj_select.join(" OR "));
            }
        };

        let update_stmt = if delete {
            format!("DELETE FROM {} WHERE {}", tinfo.name, selection)
        } else {
            // if only one owner col, skip the case
            let updates = if tinfo.owner_fk_cols.len() == 1 {
                format!("{} = {}", tinfo.owner_fk_cols[0], old_uid)
            } else {
                let newuids_select = if newuids.len() == 1 {
                    format!("= {}", newuids[0])
                } else {
                    format!("IN ({})", newuids.join(","))
                };
                tinfo
                    .owner_fk_cols
                    .iter()
                    .map(|c| {
                        format!(
                            "{} = (SELECT CASE WHEN `{}` {} THEN {} ELSE `{}` END)",
                            c, c, newuids_select, old_uid, c
                        )
                    })
                    .collect::<Vec<String>>()
                    .join(", ")
            };
            format!("UPDATE {} SET {} WHERE {}", tinfo.name, updates, selection)
        };
        debug!("updating pp to original owner: {}", &update_stmt);
        db.query_drop(update_stmt).unwrap();
        return Ok(());
    }
}
