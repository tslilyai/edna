use crate::crypto::*;
use crate::lowlevel_api::*;
use crate::records::*;
use crate::revealer::RevealArgs;
use crate::*;
use crate::{RevealPPType, TableInfo, TableRow, DID, UID};
use crypto_box::PublicKey;
use log::{debug, info, warn};
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
            Some(PublicKey::from(get_pk_bytes(self.pubkey.clone())))
        };
        let pdata = PrincipalData {
            pubkey: pubkey,
            is_anon: false,
            enc_locators_index: self.enc_locators_index,
        };
        info!("Going to reveal principal {}", uid);
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
            info!(
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
                info!("Don't update, found 0 children");
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
        let mut old_values: HashSet<_> = old_values.iter().cloned().collect();
        debug!("Updated old values are {:?}", old_values);
        debug!("Updated new values are {:?}", new_values);

        // all diff records should be restoring something
        let res = match self.typ {
            // only ever called for a natural principal
            REMOVE_PRINCIPAL => self.reveal_removed_principal(&args.uid, args.llapi_locked),
            NEW_PP => unimplemented!("no single record reveal of pseudoprincipals!"),
            _ => {
                // remove rows before we reinsert old ones to avoid false
                // duplicates
                let mut success = true;
                let mut start = time::Instant::now();
                success &= self.remove_or_update_rows(&new_values, &mut old_values, args)?;
                if success {
                    info!(
                        "Removed {} new non-pp rows: {}mus",
                        new_values.len(),
                        start.elapsed().as_micros()
                    );
                } else {
                    info!(
                        "Failed to remove or update non-pp rows: {}mus",
                        start.elapsed().as_micros()
                    );
                }
                start = time::Instant::now();
                for ov in &old_values {
                    // restore users first
                    if ov.table == "users" {
                        success &= self.restore_old_value(
                            None,
                            &ov,
                            RestoreOrUpdate::RESTORE,
                            None,
                            args,
                        )?;
                        if success {
                            info!("Restored {:?}: {}mus", ov, start.elapsed().as_micros());
                        } else {
                            info!(
                                "Failed to restore old_value {:?}: {}mus",
                                ov,
                                start.elapsed().as_micros()
                            );
                        }
                    }
                }
                for ov in &old_values {
                    if ov.table != "users" {
                        success &= self.restore_old_value(
                            None,
                            &ov,
                            RestoreOrUpdate::RESTORE,
                            None,
                            args,
                        )?;
                        if success {
                            info!("Restored {:?}: {}mus", ov, start.elapsed().as_micros());
                        } else {
                            info!(
                                "Failed to restore old_value {:?}: {}mus",
                                ov,
                                start.elapsed().as_micros()
                            );
                        }
                    }
                }
                Ok(success)
            }
        };
        warn!("Reveal diff record: {}mus", fnstart.elapsed().as_micros());
        return res;
    }

    /// get_count_of_children checks for the number of children of this table referring to this fk.
    fn get_count_of_children<Q: Queryable>(
        new_fk_value: &UID,
        tinfo: &TableInfo,
        db: &mut Q,
    ) -> Result<u64, mysql::Error> {
        if tinfo.owner_fks.len() == 0 {
            return Ok(0);
        }
        let fnstart = time::Instant::now();
        let selection = tinfo
            .owner_fks
            .iter()
            .map(|c| format!("`{}` = {}", c.from_col, new_fk_value))
            .collect::<Vec<String>>()
            .join(" OR ");

        let checkstmt = format!("SELECT COUNT(*) FROM {} WHERE {}", tinfo.table, selection);
        let res = db.query_iter(checkstmt.clone()).unwrap();
        let mut count: u64 = 0;
        for row in res {
            count = from_value(row.unwrap().unwrap()[0].clone());
            break;
        }
        warn!(
            "get_count_of_children: {} children of table {} point to id {}: {}mus",
            count,
            tinfo.table,
            new_fk_value,
            fnstart.elapsed().as_micros()
        );

        return Ok(count);
    }

    /// To restore old values (after we've already updated the relevant ones
    /// using new values). Typically only rows that have been removed, instead
    /// of decorrelated or modified
    pub fn restore_old_value<Q: Queryable>(
        &self,
        new_value: Option<&TableRow>,
        old_value: &TableRow,
        restore_or_update: RestoreOrUpdate,
        item_selected: Option<Vec<RowVal>>,
        args: &mut RevealArgs<Q>,
    ) -> Result<bool, mysql::Error> {
        let fnstart = time::Instant::now();
        info!("Try to {:?} old value! {:?}", restore_or_update, old_value);
        let table = &old_value.table;
        let mut old_value_row = old_value.row.clone();
        let mut restore_or_update = restore_or_update;

        // get current obj in db
        let table_info = args.timap.get(table).unwrap();
        let ids = helpers::get_ids(&table_info.id_cols, &old_value_row);
        let item_selection = helpers::get_select_of_ids_str(&ids);

        let is = match item_selected {
            None => {
                let is = helpers::get_query_rows_str_q::<Q>(
                    &helpers::str_select_statement(table, table, &item_selection.to_string()),
                    args.db,
                )?;
                info!("restore old objs: going to restore {:?}", old_value_row);

                // CHECK 1: If we're going to restore, don't reinsert if the item already exists
                if restore_or_update == RestoreOrUpdate::RESTORE && !is.is_empty() {
                    info!(
                        "restore old objs: found objects for {}, going to update instead!",
                        item_selection
                    );
                    restore_or_update = RestoreOrUpdate::UPDATE;
                }
                is[0].clone()
            }
            Some(is) => is,
        };

        // CHECK 2: Referential integrity to non-owners
        for fk in &table_info.other_fks {
            // if original entity does not exist, do not reveal
            let curval = helpers::get_value_of_col(&old_value_row, &fk.from_col).unwrap();
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
            info!("other fk selection: {}", selection.to_string());
            let res = args.db.query_iter(selection.clone()).unwrap();
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
                let mut curval = helpers::get_value_of_col(&old_value_row, &fk.from_col).unwrap();
                if args.recorrelated_pps.contains(&curval) {
                    info!("Recorrelated pps contained the old_uid {}", curval);

                    // find the most recent UID in the path up to this one
                    // that should exist in the DB
                    let old_uid =
                        sfchain_record::find_old_uid(args.edges, &curval, args.recorrelated_pps);
                    curval = old_uid.unwrap_or(curval);
                    helpers::set_value_of_col(&mut old_value_row, &fk.from_col, &curval);
                }

                // select here too
                let selection = format!(
                    "SELECT * FROM {} WHERE {}.{} = {}",
                    args.pp_gen.table,
                    args.pp_gen.table,
                    args.pp_gen.id_col,
                    helpers::to_mysql_valstr(&curval)
                );
                info!("owner fk selection: {}", selection.to_string());
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
            info!("rewritten old_value_row = {:?}", old_value_row);
        }

        // We've check for uniqueness and referential integrity; now either insert or update
        // the old row!
        if restore_or_update == RestoreOrUpdate::RESTORE {
            let cols: Vec<String> = old_value_row.iter().map(|rv| rv.column().clone()).collect();
            let colstr = cols.join(",");
            let vals: Vec<String> = old_value_row
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
            let insert_q = format!("INSERT INTO {} ({}) VALUES ({})", table, colstr, valstr);
            helpers::query_drop(&insert_q, args.db)?;
            warn!(
                "restore old objs: {}: {}mus",
                insert_q,
                fnstart.elapsed().as_micros()
            );
            Ok(true)
        } else {
            // update! we either get here because we're updating a new value,
            // or we had an old value with an existing item in the database that
            // we want to see if we can update
            let new_value = new_value.unwrap_or(old_value);
            if is.len() < 1 {
                warn!(
                    "restore old objs: no item to update {}mus",
                    fnstart.elapsed().as_micros()
                );
                return Ok(false);
            }
            let item = &is;
            let mut updates = vec![];
            for (ix, rv) in old_value_row.iter().enumerate() {
                assert_eq!(rv.column(), item[ix].column());
                assert_eq!(rv.column(), new_value.row[ix].column());
                // compare the stripped versions
                let ov = rv.value().replace("\'", "");
                let iv = item[ix].value().replace("\'", "");
                let nv = new_value.row[ix].value().replace("\'", "");

                if nv == iv && ov != nv {
                    updates.push(format!("`{}` = '{}'", rv.column(), rv.value()));
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
                        return Ok(false);
                    }
                }
            }
            // somehow this row exactly matches, we can just keep this row
            if updates.len() > 0 {
                // update the relevant columns
                let update_q = format!(
                    "UPDATE {} SET {} WHERE {}",
                    &table,
                    updates.join(", "),
                    item_selection
                );
                helpers::query_drop(&update_q, args.db)?;
            }
            warn!("restore old objs: {}mus", fnstart.elapsed().as_micros());
            Ok(true)
        }
    }

    /// To remove new value rows, or update new value rows to the old value (so
    /// we don't do redundant work)
    fn remove_or_update_rows<Q: Queryable>(
        &self,
        new_values: &Vec<TableRow>,
        old_values: &mut HashSet<TableRow>,
        args: &mut RevealArgs<Q>,
    ) -> Result<bool, mysql::Error> {
        let fnstart = time::Instant::now();
        let mut success = true;
        for new_value in new_values {
            let table = &new_value.table;
            info!("Going to try to remove new row! {:?}", new_value);

            // get current obj in db
            let table_info = args.timap.get(table).unwrap();
            let ids = helpers::get_ids(&table_info.id_cols, &new_value.row);
            let selection = helpers::get_select_of_ids_str(&ids);
            let selected = helpers::get_query_rows_str_q::<Q>(
                &helpers::str_select_statement(table, table, &selection.to_string()),
                args.db,
            )?;

            // CHECK 1: Is the item already removed?
            if selected.is_empty() {
                info!(
                    "remove_or_update_rows: No new item to remove! {}: {}mus",
                    selection,
                    fnstart.elapsed().as_micros()
                );
                // this can still succeed!
                // success = false;
                continue;
            }

            /*
             * CHECK 2: Referential integrity, we need to do something to rows
             * that refer to this one. Only check for pseudoprincipals (e.g.,
             * records of type "NEW_PP")
             */
            // Update the new value to the old value in place, if we find a matching old
            // value
            let mut should_delete = true;
            for ov in &old_values.clone() {
                let old_ids = helpers::get_ids(&table_info.id_cols, &ov.row);
                if ids == old_ids {
                    // we found a match, make sure not to delete this item even if we fail to restore it since we're updating the old value here!
                    should_delete = false;
                    if self.restore_old_value(
                        Some(new_value),
                        &ov,
                        RestoreOrUpdate::UPDATE,
                        Some(selected[0].clone()),
                        args,
                    )? {
                        old_values.remove(&ov);
                    }
                    break;
                }
            }
            // we can delete the item only if there's no matching old value
            // that we've already tried to rewrite the new value to.
            if should_delete {
                // retain if things refer to it
                for (_, tinfo) in &args.timap {
                    let nchildren =
                        EdnaDiffRecord::get_count_of_children(&ids[0].value(), &tinfo, args.db)?;
                    if nchildren == 0 {
                        let delete_q = format!("DELETE FROM {} WHERE {}", &table, selection);
                        helpers::query_drop(&delete_q, args.db)?;
                    } else {
                        success = false;
                    }
                }
            }
        }
        warn!(
            "remove_or_update_rows: {}mus",
            fnstart.elapsed().as_micros()
        );
        Ok(success)
    }
}
