use crate::crypto::*;
use crate::lowlevel_api::*;
use crate::records::*;
use crate::revealer::RevealArgs;
use crate::*;
use crate::{RevealPPType, TableInfo, TableRow, DID, UID};
use crypto_box::PublicKey;
use log::{debug, warn};
use mysql::from_value;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use std::time;
//use std::mem::size_of_val;

pub const REMOVE: u8 = 0;
pub const DECOR: u8 = 1;
pub const NEW_PP: u8 = 2;
pub const MODIFY: u8 = 3;
pub const REMOVE_PRINCIPAL: u8 = 5;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum RestoreOrUpdate {
    RESTORE = 1,
    UPDATE = 2,
}

#[derive(Default, Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub struct EdnaDiffRecordWrapper {
    pub did: DID,
    pub uid: UID,
    pub record: EdnaDiffRecord,
}

#[derive(Default, Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct DiffRecordWrapper {
    pub did: DID,
    pub uid: UID,
    pub record_data: Vec<u8>,

    // FOR SECURITY DESIGN
    // for randomness
    pub nonce: u64,

    // time record was created
    pub t: u64,
}

#[derive(Default, Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
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

    pub t: u64,
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
pub fn edna_diff_record_from_bytes(bytes: &Vec<u8>, t: u64) -> EdnaDiffRecord {
    let mut rec: EdnaDiffRecord = bincode::deserialize(bytes).unwrap();
    rec.t = t;
    rec
}
pub fn edna_diff_record_to_bytes(record: &EdnaDiffRecord) -> Vec<u8> {
    bincode::serialize(record).unwrap()
}

// create diff record for generic data
pub fn new_generic_diff_record_wrapper(
    start_time: time::Instant,
    uid: &UID,
    did: DID,
    data: Vec<u8>,
) -> DiffRecordWrapper {
    let mut record: DiffRecordWrapper = Default::default();
    record.nonce = thread_rng().gen();
    record.t = start_time.elapsed().as_secs();
    record.uid = uid.to_string();
    record.did = did;
    record.record_data = data;
    record
}

// create diff record for removing a principal
pub fn new_remove_principal_record(pdata: &PrincipalData) -> EdnaDiffRecord {
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

pub fn new_pseudoprincipal_record(pp: TableRow, old_uid: UID, new_uid: UID) -> EdnaDiffRecord {
    let mut edna_record: EdnaDiffRecord = Default::default();
    edna_record.typ = NEW_PP;
    edna_record.old_uid = old_uid;
    edna_record.new_uid = new_uid;
    edna_record.old_values = vec![];
    edna_record.new_values = vec![pp];
    edna_record
}
pub fn new_decor_record(old_child: TableRow, new_child: TableRow) -> EdnaDiffRecord {
    let mut edna_record: EdnaDiffRecord = Default::default();
    edna_record.typ = DECOR;
    edna_record.old_values = vec![old_child];
    edna_record.new_values = vec![new_child];
    edna_record
}

impl EdnaDiffRecord {
    fn reveal_removed_principal(
        &self,
        uid: &UID,
        llapi: &mut LowLevelAPI,
    ) -> Result<bool, mysql::Error> {
        let start = time::Instant::now();
        let pubkey = if self.pubkey.is_empty() {
            None
        } else {
            Some(PublicKey::from(get_pk_bytes(&self.pubkey.clone())))
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
        warn!(
            "Reveal removed principal: {}mus",
            start.elapsed().as_micros()
        );
        return Ok(true);
    }

    // reveals new pps by removing them in batch
    pub fn reveal_new_pps<Q: Queryable>(
        records: &Vec<&EdnaDiffRecord>,
        args: &mut RevealArgs<Q>,
    ) -> Result<bool, mysql::Error> {
        /* Note: only need to worry about referential integrity to new
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
        * out the values prior to calling this function in the revealer.)

        * Note: we could also transfer the principal's records to the
        * original principal's bag, and reencrypt

        * Note: this will fail if one of the modified
        * pseudoprincipals happens to be in the speaks-for chain, and
        * is already recorrelated
        */
        let fnstart = time::Instant::now();
        if records.len() == 0 || args.reveal_pps == RevealPPType::Retain {
            return Ok(true);
        }

        let mut old_to_new: HashMap<UID, Vec<UID>> = HashMap::new();
        let mut pps_to_delete: Vec<UID> = vec![];
        for r in records {
            // all diff records should be removing a new pp
            assert_eq!(r.new_values.len(), 1);
            let table = &r.new_values[0].table;
            assert_eq!(&args.pp_gen.table, table);

            // find the most recent UID in the path up to this one
            // that should exist in the DB
            let old_uid =
                sfchain_record::find_old_uid(args.edges, &r.new_uid, args.recorrelated_pps);
            let old_uid = old_uid.unwrap_or(r.old_uid.clone());
            warn!(
                "reveal_new_pps: Going to check old {} -> pp {}",
                old_uid, r.new_uid
            );
            match old_to_new.get_mut(&old_uid) {
                Some(uids) => uids.push(r.new_uid.clone()),
                None => {
                    old_to_new.insert(old_uid.clone(), vec![r.new_uid.clone()]);
                }
            }
            pps_to_delete.push(r.new_uid.clone());
        }
        if pps_to_delete.len() == 0 {
            return Ok(true);
        }
        let all_pp_select = if pps_to_delete.len() == 1 {
            format!("= {}", pps_to_delete[0])
        } else {
            format!("IN ({})", pps_to_delete.join(","))
        };

        if args.reveal_pps == RevealPPType::Restore {
            // CHECK: if original entities do not exist, do not recorrelate
            let selection = if old_to_new.len() == 1 {
                format!(
                    "{} = {}",
                    args.pp_gen.id_col,
                    old_to_new.keys().collect::<Vec<_>>()[0]
                )
            } else {
                format!(
                    "{} IN ({})",
                    args.pp_gen.id_col,
                    old_to_new.keys().cloned().collect::<Vec<_>>().join(",")
                )
            };
            debug!("reveal pps selection: {}", selection);
            let selected = helpers::get_query_rows_str_q::<Q>(
                &helpers::str_select_statement(&args.pp_gen.table, &args.pp_gen.table, &selection),
                args.db,
            )?;
            if selected.len() != old_to_new.keys().len() {
                debug!(
                    "NEW_PP Reveal: {} col selection {} != {}\n",
                    args.pp_gen.id_col,
                    selected.len(),
                    old_to_new.keys().len(),
                );
                return Ok(false);
            }
        }

        for (child_table, tinfo) in &args.timap {
            if child_table == &args.pp_gen.table {
                continue;
            }
            if tinfo.owner_fks.len() == 0 {
                continue;
            }
            // get count of children SO WE DON'T UPDATE if we don't need
            // to (select is cheaper!)
            let start = time::Instant::now();
            let all_select = if tinfo.owner_fks.len() == 1 {
                format!("{} {}", tinfo.owner_fks[0].from_col, all_pp_select)
            } else {
                tinfo
                    .owner_fks
                    .iter()
                    .map(|fk| format!("{} {}", fk.from_col, all_pp_select))
                    .collect::<Vec<String>>()
                    .join(" OR ")
            };

            let checkstmt = format!("SELECT COUNT(*) FROM {} WHERE {}", tinfo.table, all_select);
            //warn!("Check count of pseudoprincipals {}", checkstmt);
            let res = args.db.query_iter(checkstmt.clone()).unwrap();
            let mut count: u64 = 0;
            for row in res {
                count = from_value(row.unwrap().unwrap()[0].clone());
                break;
            }
            warn!(
                "Check count of pseudoprincipals: {}mus",
                start.elapsed().as_micros()
            );
            if count == 0 {
                warn!("Don't update, found 0 children");
                continue;
            }
            if args.reveal_pps == RevealPPType::Delete {
                helpers::query_drop(
                    &format!("DELETE FROM {} WHERE {}", tinfo.table, all_select),
                    args.db,
                )?;
            } else {
                // Actually update pp children, if any exist!
                for (old_uid, new_uids) in old_to_new.iter() {
                    // NOTE: Assume only one id used as a foreign key.
                    let new_uids_str = new_uids.join(",");
                    let pp_select = if new_uids.len() == 1 {
                        format!("= {}", new_uids[0])
                    } else {
                        format!("IN ({})", new_uids_str)
                    };

                    let selection = if tinfo.owner_fks.len() == 1 {
                        format!("{} {}", tinfo.owner_fks[0].from_col, pp_select)
                    } else {
                        tinfo
                            .owner_fks
                            .iter()
                            .map(|fk| format!("{} {}", fk.from_col, pp_select))
                            .collect::<Vec<String>>()
                            .join(" OR ")
                    };

                    // if only one owner col, skip the case
                    let updates = if tinfo.owner_fks.len() == 1 {
                        format!("{} = {}", tinfo.owner_fks[0].from_col, old_uid)
                    } else {
                        tinfo
                            .owner_fks
                            .iter()
                            .map(|fk| {
                                format!(
                                    "{} = (SELECT CASE WHEN `{}` {} THEN {} ELSE `{}` END)",
                                    fk.from_col, fk.from_col, pp_select, old_uid, fk.from_col
                                )
                            })
                            .collect::<Vec<String>>()
                            .join(", ")
                    };
                    helpers::query_drop(
                        &format!("UPDATE {} SET {} WHERE {}", tinfo.table, updates, selection),
                        args.db,
                    )?;
                }
            }
        }
        for new_uid in pps_to_delete {
            // remove PP metadata from the record ctrler (when all locators
            // are gone) do per new uid because did might differ NOTE: pps
            // kept for "restore" can never be reclaimed now
            args.llapi_locked.forget_principal(&new_uid, args.did);
        }

        // now remove the pseudoprincipals
        let delete_q = format!(
            "DELETE FROM {} WHERE {} {}",
            args.pp_gen.table, args.pp_gen.id_col, all_pp_select
        );
        helpers::query_drop(&delete_q, args.db)?;
        warn!(
            "Reveal {} new pps: {}mus",
            records.len(),
            fnstart.elapsed().as_micros()
        );
        Ok(true)
    }

    pub fn reveal<Q: Queryable>(&self, args: &mut RevealArgs<Q>) -> Result<bool, mysql::Error> {
        let fnstart = time::Instant::now();

        // get updates to apply to old and new values
        let mut new_values = self.new_values.clone();
        let mut old_values = self.old_values.clone();
        for up_lock in &args.updates {
            if up_lock.t < self.t {
                debug!(
                    "Skipping update with time {} before disguise time {}",
                    up_lock.t, self.t
                );
                continue;
            }
            let start = time::Instant::now();
            let up = up_lock.upfn.lock().unwrap();
            new_values = (up)(new_values);
            old_values = (up)(old_values);
            warn!(
                "reveal diff record apply updates to vals: {}mus",
                start.elapsed().as_micros()
            );
            drop(up);
        }
        debug!("Updated old values are {:?}", old_values);
        debug!("Updated new values are {:?}", new_values);

        // all diff records should be restoring something
        let res = match self.typ {
            // only ever called for a natural principal
            REMOVE_PRINCIPAL => self.reveal_removed_principal(&args.uid, args.llapi_locked),
            NEW_PP => unimplemented!("no single record reveal of pseudoprincipals!"),
            _ => {
                let mut rows_for_table: HashMap<String, (Vec<TableRow>, Vec<TableRow>)> =
                    HashMap::new();
                for ov in &old_values {
                    match rows_for_table.get_mut(&ov.table) {
                        Some(ds) => ds.1.push(ov.clone()),
                        None => {
                            rows_for_table.insert(ov.table.clone(), (vec![], vec![ov.clone()]));
                        }
                    }
                }
                for nv in &new_values {
                    match rows_for_table.get_mut(&nv.table) {
                        Some(ds) => ds.0.push(nv.clone()),
                        None => {
                            rows_for_table.insert(nv.table.clone(), (vec![nv.clone()], vec![]));
                        }
                    }
                }

                // restore users first
                let mut success = true;
                if let Some((nvs, ovs)) = rows_for_table.get(&args.pp_gen.table) {
                    success &= self.reveal_table_rows(&args.pp_gen.table, nvs, ovs, args)?;
                }
                for (table, (nvs, ovs)) in &rows_for_table {
                    if table != &args.pp_gen.table {
                        success &= self.reveal_table_rows(table, nvs, ovs, args)?;
                    }
                }
                Ok(success)
            }
        };
        warn!("Reveal diff record: {}mus", fnstart.elapsed().as_micros());
        return res;
    }

    fn reveal_table_rows<Q: Queryable>(
        &self,
        table: &str,
        new_values: &Vec<TableRow>,
        old_values: &Vec<TableRow>,
        args: &mut RevealArgs<Q>,
    ) -> Result<bool, mysql::Error> {
        let mut old_rows_updated = vec![];
        let mut new_rows_to_remove: HashSet<TableRow> = new_values.clone().into_iter().collect();
        let table_info = args.timap.get(table).unwrap().clone();

        warn!("Revealing table rows for {}", table);

        // TODO if any old values update the primary key of a new value, we can't do an
        // INSERT-UPDATE.
        // gotta do this when we look at individual diff records instead of the batch, so in the
        // revealer

        // (1) if match between new and old row, update old row (need to check existing
        // item) and mark new row as not needed to remove.
        for ov in &old_values.clone() {
            let old_ids = helpers::get_ids(&table_info.id_cols, &ov.row);
            let mut orig = ov.clone();
            for nv in new_values {
                let new_ids = helpers::get_ids(&table_info.id_cols, &nv.row);
                // note that the tables should always be the same...
                assert_eq!(nv.table, ov.table);
                if new_ids == old_ids {
                    // we found a match, update the old value here!
                    orig = self.update_old_value(nv, ov, args)?;

                    // we can delete the item only if there's no matching old value
                    // that we've already tried to rewrite the new value to.
                    new_rows_to_remove.remove(nv);
                    // note: we should have only one unique new row and old row for each id
                    // since we filter this in the revealer
                    break;
                }
            }
            old_rows_updated.push(orig);
        }

        // (2) remove all new rows that didn't match (subject to checks)
        let start = time::Instant::now();
        let mut success = self.remove_new_rows_of_table(&table_info, &new_rows_to_remove, args)?;
        if success {
            warn!(
                "Removed {} new rows: {}mus",
                new_rows_to_remove.len(),
                start.elapsed().as_micros()
            );
        } else {
            warn!(
                "Failed to remove or update {} non-pp rows: {}mus",
                new_rows_to_remove.len(),
                start.elapsed().as_micros()
            );
        }

        // (3) reveal each old row table by performing insert updates
        success &= self.restore_old_values(table, &old_rows_updated, args)?;
        if success {
            warn!(
                "Restored {} old rows: {}mus",
                old_rows_updated.len(),
                start.elapsed().as_micros()
            );
        } else {
            warn!(
                "Failed to restore {} old_values: {}mus",
                old_rows_updated.len(),
                start.elapsed().as_micros()
            );
        }
        Ok(success)
    }

    /// get_count_of_children checks for the number of children of this table referring to this fk.
    fn get_count_of_children<Q: Queryable>(
        parent_table: &str,
        ids: &Vec<RowVal>,
        child_tinfo: &TableInfo,
        args: &mut RevealArgs<Q>,
    ) -> Result<u64, mysql::Error> {
        let fnstart = time::Instant::now();
        let mut wheres = vec![];
        if parent_table == args.pp_gen.table {
            for fk in &child_tinfo.owner_fks {
                let id = helpers::get_value_of_col(&ids, &fk.to_col).unwrap();
                wheres.push(format!("`{}` = {}", fk.from_col, id));
            }
        } else {
            for fk in &child_tinfo.other_fks {
                if fk.to_table == parent_table {
                    let id = helpers::get_value_of_col(&ids, &fk.to_col).unwrap();
                    wheres.push(format!("`{}` = {}", fk.from_col, id))
                }
            }
        }
        if wheres.len() == 0 {
            return Ok(0);
        }
        let checkstmt = format!(
            "SELECT COUNT(*) FROM {} WHERE {}",
            child_tinfo.table,
            wheres.join(" OR ")
        );
        let res = args.db.query_iter(checkstmt.clone()).unwrap();
        let mut count: u64 = 0;
        for row in res {
            count = from_value(row.unwrap().unwrap()[0].clone());
            break;
        }
        warn!(
            "get_count_of_children: {} children of table {} point to table {} id {}: {}mus",
            count,
            child_tinfo.table,
            parent_table,
            ids[0].value(),
            fnstart.elapsed().as_micros()
        );

        return Ok(count);
    }

    pub fn restore_old_values<Q: Queryable>(
        &self,
        table: &str,
        old_values: &Vec<TableRow>,
        args: &mut RevealArgs<Q>,
    ) -> Result<bool, mysql::Error> {
        let fnstart = time::Instant::now();
        if old_values.len() == 0 {
            return Ok(true);
        }
        let table_info = args.timap.get(table).unwrap();
        let mut old_values = old_values.clone();

        // CHECK: Referential integrity to non-owners
        for tr in &mut old_values {
            for fk in &table_info.other_fks {
                // if original entity does not exist, do not reveal
                let curval = helpers::get_value_of_col(&tr.row, &fk.from_col).unwrap();
                if curval.to_lowercase() == "null" {
                    // can actually legit be null! e.g., votes in lobsters only point to one of
                    // stories or votes
                    continue;
                }
                let selection = format!(
                    "SELECT COUNT(*) FROM {} WHERE {}.{} = {}",
                    fk.to_table,
                    fk.to_table,
                    fk.to_col,
                    helpers::to_mysql_valstr(&curval)
                );
                let start = time::Instant::now();
                let res = args.db.query_iter(selection.clone()).unwrap();
                warn!(
                    "other fk selection {}: {}mus",
                    selection.to_string(),
                    start.elapsed().as_micros()
                );
                let mut count: u64 = 0;
                for row in res {
                    count = from_value(row.unwrap().unwrap()[0].clone());
                    break;
                }
                if count == 0 {
                    warn!(
                        "restore old objs: No entity exists for fk col {} val {}: {}mus",
                        fk.from_col,
                        curval,
                        fnstart.elapsed().as_micros()
                    );
                    // could be too over-the-top to automatically return here
                    return Ok(false);
                }
            }
            // CHECK 3: Referential integrity to owners
            // if the original UID doesn't exist, rewrite the object to point to
            // the latest revealed UID (if exists).  We can reveal as long as
            // there's some speaks-for path to the stored UID in the diff, and
            // we rewrite this col to the correct restored UID.
            if table != &args.pp_gen.table {
                for fk in &table_info.owner_fks {
                    let mut curval = helpers::get_value_of_col(&tr.row, &fk.from_col).unwrap();
                    if args.recorrelated_pps.contains(&curval) {
                        warn!("Recorrelated pps contained the old_uid {}", curval);

                        // find the most recent UID in the path up to this one
                        // that should exist in the DB
                        let old_uid = sfchain_record::find_old_uid(
                            args.edges,
                            &curval,
                            args.recorrelated_pps,
                        );
                        curval = old_uid.unwrap_or(curval);
                        helpers::set_value_of_col(&mut tr.row, &fk.from_col, &curval);
                    }

                    // select here too
                    let selection = format!(
                        "SELECT * FROM {} WHERE {}.{} = {}",
                        args.pp_gen.table,
                        args.pp_gen.table,
                        args.pp_gen.id_col,
                        helpers::to_mysql_valstr(&curval)
                    );
                    warn!("owner fk selection: {}", selection.to_string());
                    let selected = helpers::get_query_rows_str_q::<Q>(&selection, args.db)?;

                    if selected.is_empty() {
                        warn!(
                            "restore old objs: no original entity exists for fk col {} val {}: {}mus",
                            fk.from_col,
                            curval,
                            fnstart.elapsed().as_micros()
                        );
                        return Ok(false);
                    }
                }
                warn!("rewritten old_value_row = {:?}", tr.row);
            }
        }

        // We've check for uniqueness and referential integrity; now either insert or update
        // the old rows!
        let cols: Vec<String> = old_values[0]
            .row
            .iter()
            .map(|rv| rv.column().clone())
            .collect();
        let colstr = cols.join(",");
        let mut vals = vec![];

        for tr in old_values {
            let row: Vec<String> = tr
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
            vals.push(format!("({})", row.join(",")));
        }
        let valstr = vals.join(",");
        let updates: Vec<String> = cols
            .iter()
            .map(|c| format!("{} = VALUES({})", c, c))
            .collect();
        let insert_q = format!(
            "INSERT INTO {} ({}) VALUES {} ON DUPLICATE KEY UPDATE {}",
            table,
            colstr,
            valstr,
            updates.join(",")
        );
        helpers::query_drop(&insert_q, args.db)?;
        warn!(
            "restore old objs: {}: {}mus",
            insert_q,
            fnstart.elapsed().as_micros()
        );
        Ok(true)
    }

    /// To restore old values (after we've already updated the relevant ones
    /// using new values). Typically only rows that have been removed, instead
    /// of decorrelated or modified
    pub fn update_old_value<Q: Queryable>(
        &self,
        new_value: &TableRow,
        old_value: &TableRow,
        args: &mut RevealArgs<Q>,
    ) -> Result<TableRow, mysql::Error> {
        let fnstart = time::Instant::now();
        warn!("Try to update old value! {:?}", old_value);
        let table = &old_value.table;
        let mut old_value_row = old_value.row.clone();

        // get current obj in db
        let table_info = args.timap.get(table).unwrap();
        let ids = helpers::get_ids(&table_info.id_cols, &old_value_row);
        let item_selection = helpers::get_select_of_ids_str(&ids);
        let is = helpers::get_query_rows_str_q::<Q>(
            &helpers::str_select_statement(table, table, &item_selection.to_string()),
            args.db,
        )?;
        let item: Vec<RowVal>;
        // if no item existed, then we're just going to insert the old value.
        if !is.is_empty() {
            item = is[0].clone()
        } else {
            return Ok(old_value.clone());
        };

        // update! we either get here because we're updating a new value, or we had an old value
        // with an existing item in the database that we want to see if we can update
        for rv in &old_value.row {
            let col = rv.column();
            let iv_orig = helpers::get_value_of_col(&item, &col).unwrap();
            let nv_orig = helpers::get_value_of_col(&new_value.row, &col).unwrap();
            // compare the stripped versions
            let ov = rv.value().replace("\'", "");
            let iv = iv_orig.replace("\'", "");
            let nv = nv_orig.replace("\'", "");

            // update the old value to the current value only if it doesn't match
            if nv != iv && iv != ov {
                helpers::set_value_of_col(&mut old_value_row, &col, &iv);
                /* update the new value to the old value only if it matched the current
                if nv == iv && ov != nv {
                   updates.push(format!("`{}` = '{}'", rv.column(), rv.value()));
                }*/
            } else {
                // don't update if the item column is different from the
                // value that we changed it to!
                debug!(
                    "Don't update column {}, nv {}, item v {}, ov {}",
                    rv.column(),
                    nv,
                    iv,
                    ov
                );
                if !args.allow_singlecolumn_reveals {
                    warn!(
                        "restore old objs, don't restore partial rows: {}mus",
                        fnstart.elapsed().as_micros()
                    );
                    unimplemented!("Don't restore partial rows");
                }
            }
        }
        warn!(
            "update old obj {:?} to {:?}: {}mus",
            old_value.row,
            old_value_row,
            fnstart.elapsed().as_micros()
        );
        Ok(TableRow {
            table: table.clone(),
            row: old_value_row,
        })
    }

    /// To remove new value rows, or update new value rows to the old value (so
    /// we don't do redundant work)
    fn remove_new_rows_of_table<Q: Queryable>(
        &self,
        table_info: &TableInfo,
        new_values: &HashSet<TableRow>,
        args: &mut RevealArgs<Q>,
    ) -> Result<bool, mysql::Error> {
        let mut delete_select = vec![];

        for new_value in new_values {
            warn!("Going to try to remove new row! {:?}", new_value);

            /*
             * CHECK 2: Referential integrity, we need to do something to rows
             * that refer to this one.
             */
            let new_ids = helpers::get_ids(&table_info.id_cols, &new_value.row);
            // don't delete if things refer to it
            let mut nchildren = 0;
            for (_, tinfo) in &args.timap.clone() {
                nchildren += EdnaDiffRecord::get_count_of_children(
                    &table_info.table,
                    &new_ids,
                    &tinfo,
                    args,
                )?;
            }
            if nchildren == 0 {
                delete_select.push(format!("({})", helpers::get_select_of_ids_str(&new_ids)));
            }
        }

        if delete_select.len() == 0 {
            return Ok(true);
        }
        helpers::query_drop(
            &format!(
                "DELETE FROM {} WHERE {}",
                table_info.table,
                delete_select.join(" OR ")
            ),
            args.db,
        )?;
        Ok(true)
    }
}
