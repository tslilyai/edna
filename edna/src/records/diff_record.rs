use crate::crypto::*;
use crate::lowlevel_api::*;
use crate::records::*;
use crate::*;
use crate::{PseudoprincipalGenerator, RevealPPType, TableInfo, TableName, TableRow, DID, UID};
use crypto_box::PublicKey;
use log::{debug, warn};
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::time;
use mysql::from_value;
//use std::mem::size_of_val;

pub const REMOVE: u8 = 0;
pub const DECOR: u8 = 1;
pub const NEW_PP: u8 = 2;
pub const MODIFY: u8 = 3;
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

    // old and new rows
    pub old_values: Vec<TableRow>,
    pub new_values: Vec<TableRow>,

    // REMOVE PRINCIPAL
    pub pubkey: Vec<u8>,
    pub enc_locators_index: Index,

    // NEW_PSEUDOPRINCIPAL
    pub old_uid: UID,
    pub new_uid: UID,
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

// create diff records about db objects
pub fn new_delete_record(old_value: TableRow) -> EdnaDiffRecord {
    let mut edna_record: EdnaDiffRecord = Default::default();
    edna_record.typ = REMOVE;
    edna_record.old_values = vec![old_value];
    edna_record
}

pub fn new_modify_record(old_value: TableRow, new_value: TableRow) -> EdnaDiffRecord {
    let mut edna_record: EdnaDiffRecord = Default::default();
    edna_record.typ = MODIFY;
    edna_record.old_values = vec![old_value];
    edna_record.new_values = vec![new_value];
    edna_record
}

pub fn new_pseudoprincipal_record(
    pp: TableRow,
    old_uid: UID,
    new_uid: UID,
) -> EdnaDiffRecord {
    let mut edna_record: EdnaDiffRecord = Default::default();
    edna_record.typ = NEW_PP;
    edna_record.old_uid = old_uid;
    edna_record.new_uid = new_uid;
    edna_record.old_values = vec![];
    edna_record.new_values = vec![pp];
    edna_record
}
pub fn new_decor_record(
    old_child: TableRow,
    new_child: TableRow,
) -> EdnaDiffRecord {
    let mut edna_record: EdnaDiffRecord = Default::default();
    edna_record.typ = DECOR;
    edna_record.old_values = vec![old_child];
    edna_record.new_values = vec![new_child];
    edna_record
}

impl EdnaDiffRecord {
    fn reveal_natural_principal(
        &self,
        uid: &UID,
        llapi: &mut LowLevelAPI,
    ) -> Result<bool, mysql::Error> {
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
        warn!("Reveal natural principal: {}", start.elapsed().as_micros());
        return Ok(true);
    }

    pub fn reveal<Q: Queryable>(
        &self,
        timap: &HashMap<String, TableInfo>,
        pp_gen: &PseudoprincipalGenerator,
        recorrelated_pps: &HashSet<UID>,
        edges: &HashMap<UID, Vec<SFChainRecord>>,
        uid: &UID,
        did: DID,
        reveal_pps: RevealPPType,
        llapi: &mut LowLevelAPI,
        db: &mut Q,
    ) -> Result<bool, mysql::Error> {
        // all diff records should be restoring something
        match self.typ {
            // only ever called for a natural principal
            REMOVE_PRINCIPAL => self.reveal_natural_principal(uid, llapi),
            _ => {
                // remove rows before we reinsert old ones to avoid false
                // duplicates
                let mut failed = EdnaDiffRecord::remove_new_values(
                    &self.new_values,
                    &self.new_uid,
                    timap,
                    pp_gen,
                    reveal_pps,
                    llapi,
                    did,
                    db,
                )?;
                failed &= EdnaDiffRecord::restore_old_values(
                    &self.old_values,
                    timap,
                    pp_gen,
                    recorrelated_pps,
                    edges,
                    db,
                )?;
                Ok(failed)
            }
        }
    }

    /// get_count_of_children checks for the number of children of this table referring to this fk.
    fn get_count_of_children<Q: Queryable>(
        new_fk_value: &UID,
        tinfo: &TableInfo,
        db: &mut Q,
    ) -> Result<u64, mysql::Error> {
        let selection = tinfo
            .owner_fks
            .iter()
            .map(|c| format!("`{}` = {}", c.to_col, new_fk_value))
            .collect::<Vec<String>>()
            .join(" OR ");

        let checkstmt = format!("SELECT COUNT(*) FROM {} WHERE {}", tinfo.name, selection);
        let res = db.query_iter(checkstmt.clone()).unwrap();
        let mut count: u64 = 0;
        for row in res {
            count = from_value(row.unwrap().unwrap()[0].clone());
            break;
        }
        return Ok(count);
    }

    /// decor_update_or_delete_children will either delete the children rows
    /// belonging to pseudoprincipals, or update them to point to the new user
    fn decor_update_or_delete_children<Q: Queryable>(
        old_uid: &UID,
        pp_uid: &UID,
        tinfo: &TableInfo,
        delete: bool,
        db: &mut Q,
    ) -> Result<(), mysql::Error> {
        let count = EdnaDiffRecord::get_count_of_children(pp_uid, tinfo, db)?;
        if count == 0 {
            return Ok(());
        } else {
            let selection = tinfo
                .owner_fks
                .iter()
                .map(|c| format!("`{}` = {}", c.to_col, pp_uid))
                .collect::<Vec<String>>()
                .join(" OR ");

            let update_stmt = if delete {
                format!("DELETE FROM {} WHERE {}", tinfo.name, selection)
            } else {
                // if only one owner col, skip the case
                let updates = if tinfo.owner_fks.len() == 1 {
                    format!("{} = {}", tinfo.owner_fks[0].to_col, old_uid)
                } else {
                    tinfo
                        .owner_fks
                        .iter()
                        .map(|fk| {
                            format!(
                                "{} = (SELECT CASE WHEN `{}` = {} THEN {} ELSE `{}` END)",
                                fk.to_col, fk.to_col, pp_uid, old_uid, fk.to_col
                            )
                        })
                        .collect::<Vec<String>>()
                        .join(", ")
                };
                format!("UPDATE {} SET {} WHERE {}", tinfo.name, updates, selection)
            };
            debug!(
                "updating pp {} to original owner {}: {}",
                pp_uid, old_uid, &update_stmt
            );
            db.query_drop(update_stmt).unwrap();
            return Ok(());
        }
    }

    pub fn restore_old_values<Q: Queryable>(
        old_values: &Vec<TableRow>,
        timap: &HashMap<TableName, TableInfo>,
        pp_gen: &PseudoprincipalGenerator,
        recorrelated_pps: &HashSet<UID>,
        edges: &HashMap<UID, Vec<SFChainRecord>>,
        db: &mut Q,
    ) -> Result<bool, mysql::Error> {
        let start = time::Instant::now();
        if old_values.len() == 0 {
            return Ok(true);
        }
        let mut failed = false;
        for old_value in old_values {
            let table = &old_value.table;
            let mut old_value_row = old_value.row.clone();

            // get current obj in db
            let table_info = timap.get(table).unwrap();
            let ids = helpers::get_ids(&table_info.id_cols, &old_value_row);
            let selection = helpers::get_select_of_ids_str(&ids);
            let selected = helpers::get_query_rows_str_q::<Q>(
                &helpers::str_select_statement(table, table, &selection.to_string()),
                db,
            )?;

            // CHECK 1: Uniqueness, don't reinsert if the item already exists
            if !selected.is_empty() {
                failed = true;
                continue;
            }

            // CHECK 2: Referential integrity to non-owners
            for fk in &table_info.other_fks {
                // if original entity does not exist, do not reveal
                let curval = helpers::get_value_of_col(&old_value_row, &fk.from_col).unwrap();
                if curval.to_lowercase() == "null" {
                    continue;
                }
                // xxx this might have problems with quotes?
                let selection = format!(
                    "SELECT * FROM {} WHERE {}.{} = {}",
                    fk.from_table, fk.from_table, fk.from_col, curval
                );
                warn!("selection: {}", selection.to_string());
                let selected = helpers::get_query_rows_str_q::<Q>(&selection, db)?;
                if selected.is_empty() {
                    warn!(
                        "No original entity exists for fk col {} val {}",
                        fk.from_col, curval
                    );
                    failed = true;
                }
            }
            // CHECK 3: Referential integrity to owners
            // if the original UID doesn't exist, rewrite the object to point to
            // the latest revealed UID (if exists).  We can reveal as long as
            // there's some speaks-for path to the stored UID in the diff, and
            // we rewrite this col to the correct restored UID.
            if table != &pp_gen.table {
                for fk in &table_info.owner_fks {
                    let mut curval =
                        helpers::get_value_of_col(&old_value_row, &fk.from_col).unwrap();
                    debug!("Diff record corresponds to table referring to user");
                    if recorrelated_pps.contains(&curval) {
                        warn!("Recorrelated pps contained the old_uid {}", curval);

                        // find the most recent UID in the path up to this one
                        // that should exist in the DB
                        let old_uid =
                            sfchain_record::find_old_uid(edges, &curval, recorrelated_pps);
                        curval = old_uid.unwrap_or(curval);
                        helpers::set_value_of_col(&mut old_value_row, &fk.from_col, &curval);
                    }
                    // select here too
                    let selection = format!(
                        "SELECT * FROM {} WHERE {}.{} = {}",
                        pp_gen.table, pp_gen.table, pp_gen.id_col, curval
                    );
                    warn!("selection: {}", selection.to_string());
                    let selected = helpers::get_query_rows_str_q::<Q>(&selection, db)?;
                    if selected.is_empty() {
                        warn!(
                            "No original entity exists for fk col {} val {}",
                            fk.from_col, curval
                        );
                        return Ok(false);
                    }
                }
            }

            // We've check for uniqueness and referential integrity; now insert
            // the old row!
            let cols: Vec<String> = old_value_row.iter().map(|rv| rv.column().clone()).collect();
            let colstr = cols.join(",");
            let vals: Vec<String> = old_value
                .row
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
                table, colstr, valstr
            ))?;
        }
        warn!(
            "Restore {} removed rows for: {}",
            old_values.len(),
            start.elapsed().as_micros()
        );
        if !failed {
            return Ok(true);
        } else {
            return Ok(false);
        }
    }

    pub fn remove_new_values<Q: Queryable>(
        new_values: &Vec<TableRow>,
        new_uid: &UID,
        timap: &HashMap<TableName, TableInfo>,
        pp_gen: &PseudoprincipalGenerator,
        reveal_pps: RevealPPType,
        llapi: &mut LowLevelAPI,
        did: DID,
        db: &mut Q,
    ) -> Result<bool, mysql::Error> {
        let start = time::Instant::now();
        let mut failed = false;
        let mut remove_last = vec![];
        let mut remove_first = vec![];
        for nv in new_values {
            if nv.table == pp_gen.table {
                remove_last.push(nv);
            } else {
                remove_first.push(nv)
            }
        }
        fn helper<Q: Queryable>(
            new_values: &Vec<&TableRow>,
            new_uid: &UID,
            timap: &HashMap<String, TableInfo>,
            pp_gen: &PseudoprincipalGenerator,
            reveal_pps: RevealPPType,
            llapi: &mut LowLevelAPI,
            did: DID,
            db: &mut Q,
        ) -> Result<bool, mysql::Error> {
            let mut failed = false;
            for new_value in new_values {
                let table = &new_value.table;

                // get current obj in db
                let table_info = timap.get(table).unwrap();
                let ids = helpers::get_ids(&table_info.id_cols, &new_value.row);
                let selection = helpers::get_select_of_ids_str(&ids);
                let selected = helpers::get_query_rows_str_q::<Q>(
                    &helpers::str_select_statement(table, table, &selection.to_string()),
                    db,
                )?;

                // CHECK 1: Is the item already removed?
                if selected.is_empty() {
                    continue;
                }

                /*
                * CHECK 2: Referential integrity, we need to do something to
                * rows that refer to this one

                * Note: only need to worry about referential integrity of new
                * pseudoprincipal rows, because we assume that modified rows
                * (the only other new row type) keep the same unique
                * identifiers, and thus any object pointing to the modified row
                * will also point to the old unmodified row.  We will have to
                * run new row removal and old row restoration in a transaction
                * to avoid any period of broken referential integrity.

                * if an object exists that refers to the pseudoprincipal, then
                * this is because we haven't rewritten it yet to an old object.
                * we can get rid of this referential loop by removing all new
                * non-pseudoprincipal objects first (which we do by filtering
                * out the values prior to calling this helper function.)

                * Note: we could also transfer the principal's records to the
                * original principal's bag, and reencrypt

                * Note: this will fail if we're one of the modified
                * pseudoprincipals happens to be in the speaks-for chain, and
                * is already recorrelated
                */
                if table == &pp_gen.table {
                    if reveal_pps == RevealPPType::Delete
                        || reveal_pps == RevealPPType::Restore
                    {
                        // NOTE: Assume only one id used as a foreign key
                        let pp_uid = ids[0].value();

                        // either delete or rewrite pp children to point to
                        // original principal.
                        for (child_table, tinfo) in timap.into_iter() {
                            if child_table == &pp_gen.table {
                                continue;
                            }
                            // we already removed the new value representing the
                            // rewritten child, so we don't have to worry about
                            // rewriting it here
                            let old_uid = ids[0].value();
                            EdnaDiffRecord::decor_update_or_delete_children(
                                &old_uid,
                                &new_uid,
                                tinfo,
                                reveal_pps == RevealPPType::Delete,
                                db,
                            )?;
                        }

                        // now remove the pseudoprincipal
                        db.query_drop(format!("DELETE FROM {} WHERE {}", &table, selection))?;

                        // remove PP metadata from the record ctrler (when all
                        // locators are gone) do per new uid because did might
                        // differ NOTE: pps kept for "restore" can never be
                        // reclaimed now
                        llapi.forget_principal(&pp_uid, did);
                    }
                } else {
                    // retain the pseudoprincipal or a non-pseudoprincipal object
                    // because things refer to it.
                    for (_, tinfo) in timap.into_iter() {
                        if EdnaDiffRecord::get_count_of_children(&ids[0].value(), tinfo, db)? == 0 {
                            // remove the object if no references
                            db.query_drop(format!("DELETE FROM {} WHERE {}", &table, selection))?;
                        } else {
                            failed = true;
                        }
                    }
                }
            }
            Ok(!failed)
        }
        failed &= helper(&remove_first, new_uid, timap, pp_gen, reveal_pps, llapi, did, db)?;
        failed &= helper(&remove_last, new_uid, timap, pp_gen, reveal_pps, llapi, did, db)?;
        warn!(
            "Removed {} new rows: {}",
            new_values.len(),
            start.elapsed().as_micros()
        );
        return Ok(!failed);
    }
}
