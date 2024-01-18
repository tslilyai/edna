use crate::helpers::*;
use crate::lowlevel_api::*;
use crate::records::*;
use crate::*;
use log::{info, warn};
use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;
use std::mem::size_of_val;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

const REMOVED_SHARED_TABLE: &'static str = "EdnaRemovedSharedObjects";

#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct Object {
    table: TableName,
    row: Vec<RowVal>,
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SharedDiffRecord {
    // stores the diff record with everyone else rewritten to pps
    diff: EdnaDiffRecord,
    // stores the map from natural p to (ppuid, pprow, decorrelated child row)
    np_to_pp: HashMap<UID, (UID, TableRow, TableRow)>,
}

pub struct Disguiser {
    pub llapi: Arc<Mutex<LowLevelAPI>>,
    pub pool: mysql::Pool,
    // keeps set of shared objects that have been removed,
    // tracking how the UIDs persisted in the DB map to pseudoprincipals stored in diffs
    pub removed_shared_objs: HashMap<Object, SharedDiffRecord>,
    // keep track of seen PPs, cleaned up in the RecordCtrler. TODO persist
    seen_pps: HashSet<UID>,
}

impl Disguiser {
    pub fn new(
        llapi: Arc<Mutex<LowLevelAPI>>,
        pool: mysql::Pool,
        in_memory: bool,
        reset: bool,
    ) -> Disguiser {
        let mut disguiser = Disguiser {
            llapi: llapi,
            pool: pool,
            removed_shared_objs: HashMap::new(),
            seen_pps: HashSet::new(),
        };
        disguiser.init(in_memory, reset);
        disguiser
    }

    fn init(&mut self, in_memory: bool, reset: bool) {
        let mut db = self.pool.get_conn().unwrap();
        db.query_drop("SET max_heap_table_size = 4294967295;")
            .unwrap();
        let engine = if in_memory { "MEMORY" } else { "InnoDB" };

        if reset {
            db.query_drop(format!("DROP TABLE IF EXISTS {}", REMOVED_SHARED_TABLE))
                .unwrap();
        }
        // create table
        db.query_drop(format!(
            "CREATE TABLE IF NOT EXISTS {} (object varchar(2048), data varchar(2048)) ENGINE = {};",
            REMOVED_SHARED_TABLE, engine
        ))
        .unwrap();

        let obj_rows =
            get_query_rows_str(&format!("SELECT * FROM {}", REMOVED_SHARED_TABLE), &mut db)
                .unwrap();
        for row in obj_rows {
            let rv_bytes = base64::decode(&row[0].value()).unwrap();
            let rv: Object = bincode::deserialize(&rv_bytes).unwrap();
            let data_bytes = base64::decode(&row[1].value()).unwrap();
            let data: SharedDiffRecord = bincode::deserialize(&data_bytes).unwrap();
            self.removed_shared_objs.insert(rv, data);
        }
    }

    pub fn get_sizes(&self, dbname: &str) -> (usize, usize) {
        let mut db = self.pool.get_conn().unwrap();
        let rows = get_query_rows_str(
            &format!(
                "SELECT \
                (DATA_LENGTH + INDEX_LENGTH) AS `Size (B)` \
                FROM information_schema.TABLES \
                WHERE TABLE_SCHEMA = \'{}\' AND TABLE_NAME = \'{}\'",
                dbname, REMOVED_SHARED_TABLE
            ),
            &mut db,
        )
        .unwrap();
        let pbytes = usize::from_str(&rows[0][0].value()).unwrap();

        let mut rsobytes = 0;
        rsobytes += size_of_val(&self.removed_shared_objs);
        for (obj, rso) in self.removed_shared_objs.iter() {
            rsobytes += size_of_val(&obj);
            for rv in &obj.row {
                rsobytes += size_of_val(rv);
            }
            rsobytes += size_of_val(rso);
            //TODO
        }
        (rsobytes, pbytes)
    }

    pub fn apply<Q: Queryable>(
        &mut self,
        disguise: &Disguise,
        table_info: &HashMap<String, TableInfo>,
        pp_gen: &PseudoprincipalGenerator,
        conn: &mut Q,
        password: Option<String>,
        user_share: Option<(Share, Loc)>,
    ) -> Result<DID, mysql::Error> {
        let mut decrypt_cap = vec![];

        if disguise.user != None {
            let llapi = self.llapi.lock().unwrap();

            let priv_key =
                llapi.get_priv_key(&(disguise.user.clone().unwrap()), password, user_share);
            if let Some(key) = priv_key {
                decrypt_cap = key;
            }

            drop(llapi);
        }

        warn!("Applying disguise for {:?}", disguise.user);
        self.apply_using_secretkey(disguise, table_info, pp_gen, decrypt_cap, conn)
    }

    pub fn apply_using_secretkey<Q: Queryable>(
        &mut self,
        disguise: &Disguise,
        table_info: &HashMap<String, TableInfo>,
        pp_gen: &PseudoprincipalGenerator,
        decrypt_cap: records::DecryptCap,
        conn: &mut Q,
    ) -> Result<DID, mysql::Error> {
        let start = time::Instant::now();
        let mut llapi = self.llapi.lock().unwrap();
        let disg_start = time::Instant::now();
        let did = llapi.start_disguise(disguise.user.clone());
        warn!(
            "Edna: start disguise: {}mus",
            disg_start.elapsed().as_micros()
        );

        let get_rec_start = time::Instant::now();
        let (_, sfchain) = llapi.get_recs_and_privkeys(&decrypt_cap);
        warn!(
            "Edna: get records: {}mus",
            get_rec_start.elapsed().as_micros()
        );
        drop(llapi);

        let sfchain_records: Vec<&SFChainRecord> = sfchain.iter().map(|(_k, v)| v).collect();

        // Handle REMOVE first
        // Any items that would've been modified or decorrelated would've been
        // revealed anyways
        let remove_start = time::Instant::now();
        let drop_me_later =
            self.execute_removes(did, disguise, table_info, pp_gen, &sfchain_records, conn);
        warn!(
            "Edna: Execute removes total: {}mus",
            remove_start.elapsed().as_micros()
        );

        /*
         * Decor and modify
         */
        let decor_start = time::Instant::now();
        let mut llapi = self.llapi.lock().unwrap();
        for (table, transforms) in &disguise.table_disguises {
            let curtable_info = table_info.get(table).unwrap();

            let owner_selection = predicate::owner_filter_pred(
                &disguise.user,
                &curtable_info.owner_fks, // assume only one fk
                &sfchain_records,
            )
            .to_string();

            // Modify next
            for t in transforms {
                let mut selection = owner_selection.clone();
                match t {
                    Transformation::Modify {
                        pred,
                        from,
                        col,
                        gen_value,
                    } => {
                        if pred != "True" {
                            selection = format!("{} AND {}", selection, pred);
                        }
                        let selected_rows = get_query_rows_str_q(
                            &str_select_statement(&table, &from, &selection),
                            conn,
                        )
                        .unwrap();
                        if selected_rows.is_empty() {
                            continue;
                        }
                        info!(
                            "ApplyPredMod: Got {} selected rows matching predicate {}\n",
                            selected_rows.len(),
                            selection
                        );
                        Self::execute_modify(
                            did,
                            &mut llapi,
                            &table,
                            curtable_info,
                            &col,
                            gen_value::gen_strval(&gen_value),
                            &selected_rows,
                            selection.to_string(),
                            conn,
                            &disguise.user,
                        );
                    }
                    _ => continue,
                }
            }

            // handle Decor last this means that items stored from remove/modify
            // might restore ownership to the original principal; this is fine,
            // because we'd be revealing this too
            for t in transforms {
                let mut selection = owner_selection.clone();
                match t {
                    Transformation::Decor {
                        pred,
                        from,
                        user_fk_cols,
                        group_by_cols,
                    } => {
                        // select objects
                        if pred != "True" {
                            selection = format!("{} AND {}", selection, pred);
                        }
                        let selected_rows = get_query_rows_str_q(
                            &str_select_statement(&table, &from, &selection),
                            conn,
                        )
                        .unwrap();
                        if selected_rows.is_empty() {
                            continue;
                        }
                        info!(
                            "ApplyPredDecor: Got {} selected rows matching predicate {}\n",
                            selected_rows.len(),
                            selection
                        );

                        Self::execute_decor(
                            disguise.user.clone(),
                            did,
                            &mut llapi,
                            curtable_info,
                            &selected_rows,
                            user_fk_cols,
                            group_by_cols,
                            pp_gen,
                            &sfchain_records,
                            &mut self.seen_pps,
                            conn,
                        );
                    }
                    _ => continue,
                }
            }
        }
        warn!(
            "Edna: Execute modify/decor total: {}mus",
            decor_start.elapsed().as_micros()
        );

        for delstmt in drop_me_later {
            let start = time::Instant::now();
            conn.query_drop(delstmt.to_string()).unwrap();
            warn!("{}: {}mus", delstmt, start.elapsed().as_micros());
        }

        drop(llapi);

        let end_start = time::Instant::now();
        let mut llapi = self.llapi.lock().unwrap();
        llapi.end_disguise();
        drop(llapi);
        warn!("Edna: end disguise: {}", end_start.elapsed().as_micros());
        warn!("Edna: apply disguise: {}", start.elapsed().as_micros());
        Ok(did)
    }

    fn execute_removes<Q: Queryable>(
        &mut self,
        did: DID,
        disguise: &Disguise,
        table_info: &HashMap<String, TableInfo>,
        pp_gen: &PseudoprincipalGenerator,
        sfc_records: &Vec<&SFChainRecord>,
        db: &mut Q,
    ) -> Vec<String> {
        info!(
            "ApplyRemoves: removing objs for disguise {} with {} sfc_records\n",
            did,
            sfc_records.len()
        );
        // saves user objects that should be removed last for ref integrity
        let mut drop_me_later = vec![];

        for (table, transforms) in disguise.table_disguises.clone() {
            let curtable_info = table_info.get(&table).unwrap().clone();
            let mut llapi = self.llapi.lock().unwrap();
            for t in transforms {
                if let Transformation::Remove { pred, from } = t {
                    // select objects to remove
                    let mut selection = predicate::owner_filter_pred(
                        &disguise.user,
                        &curtable_info.owner_fks, // assume only one fk
                        sfc_records,
                    )
                    .to_string();
                    if pred != "True" {
                        selection = format!("{} AND {}", selection, pred);
                    }
                    let start = time::Instant::now();
                    let selected_rows = get_query_rows_str_q::<Q>(
                        &str_select_statement(&table.clone(), &from, &selection),
                        db,
                    )
                    .unwrap();
                    let pred_items: HashSet<Vec<RowVal>> =
                        HashSet::from_iter(selected_rows.iter().cloned());
                    warn!(
                        "Edna: select items for remove {} {:?}: {}",
                        selection,
                        pred_items,
                        start.elapsed().as_micros()
                    );

                    let start = time::Instant::now();
                    // SHORTCUT: only one owner col, just remove
                    if curtable_info.owner_fks.len() == 1 {
                        for i in &pred_items {
                            for fk in &curtable_info.owner_fks {
                                let curuid = get_value_of_col(&i, &fk.from_col).unwrap();

                                // save the current object (we don't need to save the anonymized object)
                                let obj_diff_record = new_delete_record(TableRow {
                                    table: table.clone(),
                                    row: i.to_vec(),
                                });

                                // if this was predicated on belonging to the
                                // original principal, then we should insert it
                                // into a bag whose locator should be sent to
                                // the original principal (even though it's
                                // encrypted with the pseudoprincipal's pubkey).
                                // NOTE: this works because there's only one
                                // owner column
                                llapi.save_diff_record(
                                    curuid.clone(),
                                    did,
                                    edna_diff_record_to_bytes(&obj_diff_record),
                                );
                                // if we're working on the principal table
                                // (e.g., a users table), remove the user from
                                // Edna's metadata
                                if pp_gen.table == table {
                                    info!(
                                        "Found item to delete from table {} that is principal {}",
                                        table, curuid
                                    );
                                    llapi.forget_principal(&curuid, did);
                                }
                            }
                        }
                        // actually delete the object
                        // delete user last for referential integrity, etc.
                        let delete_from = if from.is_empty() {
                            format!("{} X", table)
                        } else {
                            format!("{} {}", table, from.replace(&table, "X"))
                        };
                        let delstmt = format!(
                            "DELETE X FROM {} WHERE {}",
                            delete_from,
                            selection.replace(&table, "X")
                        );
                        info!("delstmt all: {}", delstmt);
                        if pp_gen.table == table {
                            drop_me_later.push(delstmt);
                        } else {
                            let start = time::Instant::now();
                            db.query_drop(delstmt.to_string()).unwrap();
                            warn!("{}: {}", delstmt, start.elapsed().as_micros());
                        }
                    } else {
                        // assert that users are never shared...
                        assert!(pp_gen.table != table);
                        Self::remove_shared_data(
                            &mut self.removed_shared_objs,
                            disguise,
                            did,
                            &mut llapi,
                            &curtable_info,
                            pp_gen,
                            &mut self.seen_pps,
                            &pred_items,
                            db,
                        );
                    }
                    warn!(
                        "Edna: insert {} remove records total: {}",
                        pred_items.len(),
                        start.elapsed().as_micros()
                    );
                }
            }
        }
        return drop_me_later;
    }

    fn remove_shared_data<Q: Queryable>(
        removed_shared_objs: &mut HashMap<Object, SharedDiffRecord>,
        disguise: &Disguise,
        did: DID,
        llapi: &mut LowLevelAPI,
        curtable_info: &TableInfo,
        pp_gen: &PseudoprincipalGenerator,
        seen_pps: &mut HashSet<UID>,
        pred_items: &HashSet<Vec<RowVal>>,
        db: &mut Q,
    ) {
        // Go through each object one by one... this is slow
        for i in pred_items {
            // NOTE: IDs cannot include owner values, otherwise these may
            // change as they're replaced by pseudoprincipals
            // this means that objects identified by e.g., userID + paperID are not
            // supported with multiple users
            let ids = get_ids(&curtable_info.id_cols, &i);

            let mut i_updates = vec![];
            let mut should_remove = true;
            let mut updated_mapping = false;

            // if this object has been removed by another user before, we want to
            // (1) store the relevent diffs for the principal(s)
            //      removing the object
            // (2) remove those principals' mapping
            // (3) actually update the persistent database object
            let obj = Object {
                table: curtable_info.table.clone(),
                row: i.clone(),
            };
            if let Some(shared_diff_record) = removed_shared_objs.get_mut(&obj) {
                for fk in &curtable_info.owner_fks {
                    let mut remove_mapping = false;
                    let np_uid = get_value_of_col(&i, &fk.from_col).unwrap();
                    // if the user has an existing mapping... (e.g., is not a
                    // pp that has been removed before)
                    if let Some(pp) = shared_diff_record.np_to_pp.get(&np_uid) {
                        let ppuid = pp.0.clone();
                        let pprow = pp.1.clone();
                        let i_with_pps = pp.2.clone();
                        // and if we should remove the object for this user...
                        if disguise.user.is_none() || disguise.user.as_ref().unwrap() == &np_uid {
                            info!(
                                "Registering pp {} for user {} for shared item",
                                ppuid, np_uid
                            );

                            let ix = i.iter().position(|r| &r.column() == &fk.to_col).unwrap();

                            // (1) store diff token for data to remove
                            llapi.save_diff_record(
                                np_uid.clone(),
                                did,
                                edna_diff_record_to_bytes(&shared_diff_record.diff),
                            );
                            // (2) register new pseudoprincipal
                            //      (storing a sfchain record and pp record)
                            llapi.register_pseudoprincipal(did, &np_uid, &ppuid, pprow);

                            // (3) insert a diff record for the decor state change
                            llapi.save_decor_record(
                                np_uid.clone(),
                                TableRow {
                                    table: curtable_info.table.clone(),
                                    row: i.clone(),
                                },
                                i_with_pps,
                                did,
                            );

                            // (2) remove mapping in edna metadata
                            remove_mapping = true;

                            // (3) how to persist
                            i_updates.push((i[ix].column(), ppuid.clone()));
                            info!(
                                "Updating mapping of column {} to {} of shared obj",
                                i[ix].column(),
                                ppuid
                            );
                        } else {
                            // there is a mapping for this user, but we aren't
                            // supposed to remove it for this user yet!
                            info!("Do not remove mapping of curuid {} of shared obj", np_uid);
                            should_remove = false;
                        }
                    }
                    if remove_mapping {
                        shared_diff_record.np_to_pp.remove(&np_uid);
                        updated_mapping = true;
                    }
                }
            } else {
                // IF ITEM HAS NOT YET BEEN REMOVED BEFORE (not in
                // removed_shared_objs) so that we don't create pseudoprincipals twice

                // (0) First, create pseudoprincipals for non-pp owners
                //     and save items to rewrite in diffs and database
                info!("Found shared obj to remove, hasn't been removed yet");
                let mut i_with_pps = i.clone();
                let mut pps = vec![];
                for fk in &curtable_info.owner_fks {
                    let curuid = get_value_of_col(&i, &fk.from_col).unwrap();

                    // skip pseudoprincipals
                    if llapi.principal_is_anon(&curuid) {
                        continue;
                    }

                    // create pseudoprincipal for this owner
                    let pp = Self::create_new_pseudoprincipal(seen_pps, pp_gen);
                    pps.push(pp.clone());

                    // we want to store the diff of an item with the pp as the new owner/fk
                    let ix = i.iter().position(|r| &r.column() == &fk.to_col).unwrap();
                    i_with_pps[ix] = RowVal::new(i[ix].column(), pp.0.clone());

                    if disguise.user.is_some() && disguise.user.as_ref().unwrap() != &curuid {
                        should_remove = false;
                    }
                }

                // (1) insert new pseudoprincipals into the DB
                // NOTE: this creates PPs even for users who might never remove
                // this item
                if !pps.is_empty() {
                    let cols = pps[0]
                        .1
                        .row
                        .iter()
                        .map(|rv| rv.column().clone())
                        .collect::<Vec<String>>()
                        .join(",");
                    let vals: Vec<String> = pps
                        .iter()
                        .map(|pp| {
                            format!(
                                "({})",
                                pp.1.row
                                    .iter()
                                    .map(|rv| rv.value().clone())
                                    .collect::<Vec<String>>()
                                    .join(",")
                            )
                        })
                        .collect();
                    let q = format!(
                        "INSERT INTO {} ({}) VALUES {};",
                        pp_gen.table,
                        cols,
                        vals.join(",")
                    );
                    let start = time::Instant::now();
                    db.query_drop(&q).unwrap();
                    warn!("{}: {}mus", q, start.elapsed().as_micros());
                }

                // (2) if we're not actually removing this object, put it into our
                // saved map for all principals that might remove it later
                // otherwise, store the diff for the user
                assert!(get_ids(&curtable_info.id_cols, &i_with_pps) == ids);
                let i_with_pps_diff_record = new_delete_record(TableRow {
                    table: curtable_info.table.to_string(),
                    row: i_with_pps.to_vec(),
                });

                for (ix, fk) in curtable_info.owner_fks.iter().enumerate() {
                    let curuid = get_value_of_col(&i, &fk.from_col).unwrap();
                    let newuid = pps[ix].0.clone();
                    let pp = pps[ix].1.clone();
                    // update with the pp value if we want to persist the
                    // item with the pp. this occurs if we are removing on behalf of
                    // either (1) all users, or (2) for one user who matches the
                    // invoking value
                    if disguise.user.is_none() || disguise.user.as_ref().unwrap() == &curuid {
                        info!("Saving diff record for shared obj to remove for {}", curuid);
                    } else {
                        info!(
                            "Inserting mapping from {} to {} for shared obj to remove",
                            curuid, newuid
                        );
                        match removed_shared_objs.get_mut(&Object {
                            table: curtable_info.table.clone(),
                            row: i.clone(),
                        }) {
                            Some(shared_diff_record) => {
                                shared_diff_record.np_to_pp.insert(
                                    curuid,
                                    (
                                        newuid,
                                        pp,
                                        TableRow {
                                            table: curtable_info.table.clone(),
                                            row: i_with_pps.clone(),
                                        },
                                    ),
                                );
                            }
                            None => {
                                let mut np_to_pp = HashMap::new();
                                np_to_pp.insert(
                                    curuid,
                                    (
                                        newuid,
                                        pp,
                                        TableRow {
                                            table: curtable_info.table.clone(),
                                            row: i_with_pps.clone(),
                                        },
                                    ),
                                );
                                removed_shared_objs.insert(
                                    Object {
                                        table: curtable_info.table.clone(),
                                        row: i.clone(),
                                    },
                                    SharedDiffRecord {
                                        diff: i_with_pps_diff_record.clone(),
                                        np_to_pp: np_to_pp,
                                    },
                                );
                            }
                        }
                        updated_mapping = true;
                    }
                }
            }

            // DONE WITH CREATING PSEUDOPRINCIPALS AND FINDING THINGS TO REWRITE.
            // ACTUALLY REMOVE THINGS OR PERFORM THE UPDATE

            // make sure to persist the metadata mapping update
            if updated_mapping {
                Self::persist_removed_shared_obj_update(
                    &Object {
                        table: curtable_info.table.clone(),
                        row: i.clone(),
                    },
                    removed_shared_objs,
                    db,
                );
            }

            // REGARDLESS OF WHETHER WE REMOVED THIS BEFORE
            // if all users are now persistently anonymous, remove the object
            //      otherwise, just store the newly updated object
            let selection = get_select_of_ids(&ids);
            if should_remove {
                let start = time::Instant::now();
                let delstmt = format!("DELETE FROM {} WHERE {}", curtable_info.table, selection);
                db.query_drop(delstmt.to_string()).unwrap();
                warn!("{}: {}", delstmt, start.elapsed().as_micros());
                // remove from Edna map too (and persist)
                removed_shared_objs.remove(&Object {
                    table: curtable_info.table.clone(),
                    row: i.clone(),
                });
                Self::persist_removed_shared_obj_delete(
                    &Object {
                        table: curtable_info.table.clone(),
                        row: ids.clone(),
                    },
                    db,
                );
            } else {
                if i_updates.is_empty() {
                    continue;
                }
                let updates = i_updates
                    .iter()
                    .map(|(c, v)| format!("{} = '{}'", c, v))
                    .collect::<Vec<String>>()
                    .join(", ");
                let updatestmt = format!(
                    "UPDATE {} SET {} WHERE {}",
                    curtable_info.table, updates, selection
                );
                let start = time::Instant::now();
                db.query_drop(updatestmt.to_string()).unwrap();
                warn!("{}: {}mus", updatestmt, start.elapsed().as_micros());
            }
        }
    }

    fn persist_removed_shared_obj_update<Q: Queryable>(
        rvs: &Object,
        removed_shared_objs: &HashMap<Object, SharedDiffRecord>,
        db: &mut Q,
    ) {
        // must exist because we only persist after inserting something
        let data = removed_shared_objs.get(rvs).unwrap();
        let insert_q = format!(
            "INSERT INTO {} (object, data) \
               VALUES (\'{}\', \'{}\') ON DUPLICATE KEY UPDATE data = VALUES(data);",
            REMOVED_SHARED_TABLE,
            base64::encode(&bincode::serialize(rvs).unwrap()),
            base64::encode(&bincode::serialize(data).unwrap()),
        );
        db.query_drop(&insert_q).unwrap();
    }

    fn persist_removed_shared_obj_delete<Q: Queryable>(rvs: &Object, db: &mut Q) {
        let bytes = base64::encode(&bincode::serialize(rvs).unwrap());
        db.query_drop(format!(
            "DELETE FROM {} WHERE object = '{}'",
            REMOVED_SHARED_TABLE, bytes
        ))
        .unwrap();
    }

    fn execute_decor<Q: Queryable>(
        uid: Option<UID>,
        did: DID,
        llapi: &mut LowLevelAPI,
        child_tableinfo: &TableInfo,
        items: &Vec<Vec<RowVal>>,
        user_fk_cols: &Vec<String>,
        group_by_cols: &Vec<String>,
        pp_gen: &PseudoprincipalGenerator,
        sfc_records: &Vec<&SFChainRecord>,
        seen_pps: &mut HashSet<UID>,
        db: &mut Q,
    ) {
        let start = time::Instant::now();
        // grouped by (1) owning users, and (2) column attributes
        let mut owner_groups: HashMap<Vec<String>, HashMap<Vec<String>, Vec<Vec<RowVal>>>> =
            HashMap::new();
        let mut pseudoprincipals = vec![];
        let mut valid_owners: Vec<String> = sfc_records
            .iter()
            .map(|ot| ot.new_uid.to_string())
            .collect();
        if uid.is_some() {
            valid_owners.push(uid.as_ref().unwrap().clone());
        }

        // group items and create a new pseudoprincipal for each one (for the fk column)
        // if there is a UID constraint, only create pseudoprincipals if the column matches the UID
        // otherwise, decorrelate all owner columns
        for i in items {
            let mut owner_attrs = vec![];
            for user_fk_col in user_fk_cols {
                owner_attrs.push(get_value_of_col(&i, &user_fk_col).unwrap());
            }

            // there were no group_by_cols, each item should be a separate group (grouped by
            // identifying columns)
            // otherwise, group by specified attributes
            let mut attrs = vec![];
            if group_by_cols.is_empty() {
                for col in &child_tableinfo.id_cols {
                    attrs.push(get_value_of_col(&i, &col).unwrap());
                }
            } else {
                for col in group_by_cols {
                    attrs.push(get_value_of_col(&i, &col).unwrap());
                }
            }
            match owner_groups.get_mut(&owner_attrs) {
                Some(groups) => match groups.get_mut(&attrs) {
                    Some(is) => is.push(i.clone()),
                    None => {
                        // we are only going to decorrelate columns of items in a group
                        // if we're decorrelating all columns, or they match the specified UID
                        for user_fk_col in user_fk_cols {
                            if uid.is_none()
                                || valid_owners
                                    .contains(&get_value_of_col(&i, &user_fk_col).unwrap())
                            {
                                pseudoprincipals
                                    .push(Self::create_new_pseudoprincipal(seen_pps, pp_gen));
                            }
                        }
                        groups.insert(attrs, vec![i.clone()]);
                    }
                },
                None => {
                    // remember to create pseudoprincipals!
                    for user_fk_col in user_fk_cols {
                        if uid.is_none()
                            || valid_owners.contains(&get_value_of_col(&i, &user_fk_col).unwrap())
                        {
                            pseudoprincipals
                                .push(Self::create_new_pseudoprincipal(seen_pps, pp_gen));
                        }
                    }
                    let mut groups = HashMap::new();
                    groups.insert(attrs, vec![i.clone()]);
                    owner_groups.insert(owner_attrs, groups);
                }
            }
        }
        // actually insert pseudoprincipals
        let cols = pseudoprincipals[0]
            .1
            .row
            .iter()
            .map(|rv| rv.column().clone())
            .collect::<Vec<String>>()
            .join(",");
        let vals: Vec<String> = pseudoprincipals
            .iter()
            .map(|pp| {
                format!(
                    "({})",
                    pp.1.row
                        .iter()
                        .map(|rv| rv.value().clone())
                        .collect::<Vec<String>>()
                        .join(",")
                )
            })
            .collect();
        let q = format!(
            "INSERT INTO {} ({}) VALUES {};",
            pp_gen.table,
            cols,
            vals.join(",")
        );
        info!("Decor insert pps query: {}", q);
        db.query_drop(&q).unwrap();
        warn!("{}: {}mus", q, start.elapsed().as_micros());

        let mut index = 0;
        for (_owners, groups) in &owner_groups {
            /*
             * DECOR OBJECT MODIFICATIONS
             * A) insert pseudoprincipals for parents
             * B) update child to point to new pseudoprincipal
             * */
            for (_group, items) in groups {
                for user_fk_col in user_fk_cols {
                    // the referenced UIDs are all the same for this group
                    // of items
                    let old_uid = get_value_of_col(&items[0], &user_fk_col).unwrap();
                    if uid.is_none() || valid_owners.contains(&old_uid) {
                        info!(
                            "decor_obj {}: Creating pseudoprincipal for old uid {} col {}",
                            child_tableinfo.table, old_uid, user_fk_col
                        );
                        let (new_uid, pp) = &pseudoprincipals[index];
                        let new_uid = new_uid.replace("\'", "");

                        index += 1;

                        // FIRST: Register the pseudoprincipal (install a sfchain record)
                        llapi.register_pseudoprincipal(did, &old_uid, &new_uid, pp.clone());
                        info!("Register anon principal {}", new_uid);

                        for i in items {
                            // A. DECOR DIFF
                            let mut i_with_pps = i.clone();
                            let ix = i.iter().position(|r| &r.column() == user_fk_col).unwrap();
                            i_with_pps[ix] = RowVal::new(i[ix].column(), new_uid.clone());
                            // note that we may have multiple decor records for
                            // each pp, since the pp may own multiple items
                            llapi.save_decor_record(
                                old_uid.clone(),
                                TableRow {
                                    table: child_tableinfo.table.clone(),
                                    row: i.clone(),
                                },
                                TableRow {
                                    table: child_tableinfo.table.to_string(),
                                    row: i_with_pps,
                                },
                                did,
                            );

                            // Note: if we add this here, then the rest of this disguise,
                            // we'll also predicate on anything owned by this
                            // pseudoprincipal XXX but this is incredibly expensive!
                            // so a disguise shouldn't be aware of its own
                            // pseudoprincipals
                            // sfc_records.push(new_sfchain_record)

                            // B. UPDATE CHILD FOREIGN KEY
                            let start = time::Instant::now();
                            let i_select = get_select_of_row(&child_tableinfo.id_cols, &i);
                            let q = format!(
                                "UPDATE {} SET {} = '{}' WHERE {}",
                                child_tableinfo.table, user_fk_col, new_uid, i_select
                            );
                            db.query_drop(&q).unwrap();
                            warn!("{}: {}mus", q, start.elapsed().as_micros());
                        }
                    }
                }
            }
        }
    }

    fn execute_modify<Q: Queryable>(
        did: DID,
        llapi: &mut LowLevelAPI,
        table: &str,
        table_info: &TableInfo,
        col: &str,
        new_val: String,
        items: &Vec<Vec<RowVal>>,
        selection: String,
        db: &mut Q,
        _original_uid: &Option<UID>,
    ) {
        let start = time::Instant::now();
        // update column for this item
        let q = format!(
            "UPDATE {} SET {} = {} WHERE {}",
            table, col, new_val, selection
        );
        db.query_drop(&q).unwrap();
        warn!("{}: {}mus", q, start.elapsed().as_micros());

        // RECORD INSERT
        let start = time::Instant::now();
        for i in items {
            let new_row: Vec<RowVal> = i
                .iter()
                .map(|rv| {
                    if rv.column() == col {
                        rv.clone()
                    } else {
                        RowVal::new(col.to_string(), new_val.clone())
                    }
                })
                .collect();
            let update_record = new_modify_record(
                TableRow {
                    table: table.to_string(),
                    row: i.clone(),
                },
                TableRow {
                    table: table.to_string(),
                    row: new_row,
                },
            );

            for fk in &table_info.owner_fks {
                let owner_uid = get_value_of_col(&i, &fk.from_col).unwrap();
                let bytes = edna_diff_record_to_bytes(&update_record);
                llapi.save_diff_record(owner_uid, did, bytes);
            }
        }
        warn!("Update record inserted: {}", start.elapsed().as_micros());
    }

    fn create_new_pseudoprincipal(
        seen_pps: &mut HashSet<UID>,
        pp_gen: &PseudoprincipalGenerator,
    ) -> (UID, TableRow) {
        let mut new = false;
        let mut new_uid = String::new();
        let mut rowvals = vec![];
        while !new {
            let new_parent_vals = pp_gen.get_vals();
            let new_parent_cols = pp_gen.cols.clone();
            let mut ix = 0;
            let mut uid_ix = 0;
            rowvals = new_parent_cols
                .iter()
                .map(|c| {
                    if c == &pp_gen.id_col {
                        uid_ix = ix;
                    }
                    let rv = RowVal::new(c.to_string(), new_parent_vals[ix].to_string());
                    ix += 1;
                    rv
                })
                .collect();
            new_uid = new_parent_vals[uid_ix].to_string();
            if seen_pps.insert(new_uid.clone()) {
                new = true;
            }
        }
        (
            new_uid,
            TableRow {
                table: pp_gen.table.clone(),
                row: rowvals,
            },
        )
    }
}
