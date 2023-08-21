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

pub struct HighLevelAPI {
    pub llapi: Arc<Mutex<LowLevelAPI>>,
    pub pool: mysql::Pool,
    // keeps set of shared objects that have been removed,
    // tracking how the UIDs persisted in the DB map to pseudoprincipals stored in diffs
    pub removed_shared_objs: HashMap<(TableName, Vec<RowVal>), (EdnaDiffRecord, HashMap<UID, UID>)>,
    // keep track of seen PPs, cleaned up in the RecordCtrler. TODO persist
    seen_pps: HashSet<UID>,
}

impl HighLevelAPI {
    pub fn new(
        llapi: Arc<Mutex<LowLevelAPI>>,
        pool: mysql::Pool,
        in_memory: bool,
        reset: bool,
    ) -> HighLevelAPI {
        let mut hlapi = HighLevelAPI {
            llapi: llapi,
            pool: pool,
            removed_shared_objs: HashMap::new(),
            seen_pps: HashSet::new(),
        };
        hlapi.init(in_memory, reset);
        hlapi
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
            let rv: (TableName, Vec<RowVal>) = bincode::deserialize(&rv_bytes).unwrap();
            let data_bytes = base64::decode(&row[1].value()).unwrap();
            let data: (EdnaDiffRecord, HashMap<UID, UID>) =
                bincode::deserialize(&data_bytes).unwrap();
            self.removed_shared_objs.insert(rv, data);
        }
    }

    fn persist_removed_shared_obj_update(&self, rvs: &(TableName, Vec<RowVal>)) {
        // must exist because we only persist after inserting something
        let mut db = self.pool.get_conn().unwrap();
        let data = self.removed_shared_objs.get(rvs).unwrap();
        let insert_q = format!(
            "INSERT INTO {} (object, data) \
               VALUES (\'{}\', \'{}\') ON DUPLICATE KEY UPDATE data = VALUES(data);",
            REMOVED_SHARED_TABLE,
            base64::encode(&bincode::serialize(rvs).unwrap()),
            base64::encode(&bincode::serialize(data).unwrap()),
        );
        db.query_drop(&insert_q).unwrap();
    }

    fn persist_removed_shared_obj_delete(&self, rvs: &(TableName, Vec<RowVal>)) {
        let mut db = self.pool.get_conn().unwrap();
        let bytes = base64::encode(&bincode::serialize(rvs).unwrap());
        db.query_drop(format!(
            "DELETE FROM {} WHERE object = '{}'",
            REMOVED_SHARED_TABLE, bytes
        ))
        .unwrap();
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
        for ((t, l), rso) in self.removed_shared_objs.iter() {
            rsobytes += size_of_val(&t);
            for rv in l {
                rsobytes += size_of_val(&rv);
            }
            rsobytes += size_of_val(&*l);
            rsobytes += size_of_val(&rso.0);
            rsobytes += size_of_val(&rso.1);
            for (uid1, uid2) in rso.1.iter() {
                rsobytes += size_of_val(&uid1);
                rsobytes += size_of_val(&uid2);
            }
        }
        (rsobytes, pbytes)
    }

    fn reveal_remove_diffs_of_table<Q: Queryable>(
        &self,
        table: &str,
        dsmap: &HashMap<String, Vec<(String, EdnaDiffRecord)>>,
        table_info: &HashMap<String, TableInfo>,
        llapi: &mut LowLevelAPI,
        conn: &mut Q,
    ) -> Result<bool, mysql::Error> {
        match dsmap.get(table) {
            Some(ds) => {
                for (uid, d) in ds {
                    info!("Reversing remove record {:?}\n", d);
                    let revealed = d.reveal(&table_info, &uid, llapi, conn)?;
                    if revealed {
                        info!("Remove Record revealed!\n");
                    } else {
                        info!("Failed to reveal remove record");
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            None => Ok(true),
        }
    }

    pub fn reveal<Q: Queryable>(
        &mut self,
        uid: Option<&UID>,
        did: DID,
        table_info: &HashMap<String, TableInfo>,
        guise_gen: &GuiseGen,
        conn: &mut Q,
        password: Option<String>,
        user_share: Option<(Share, Loc)>,
    ) -> Result<(), mysql::Error> {
        let mut decrypt_cap = vec![];

        if uid != None {
            let llapi = self.llapi.lock().unwrap();

            let priv_key = llapi.get_priv_key(&(uid.clone().unwrap()), password, user_share);
            if let Some(key) = priv_key {
                decrypt_cap = key;
            }
            drop(llapi);

            info!("got priv key");
        }

        warn!("Revealing disguise for {:?}", uid);
        self.reveal_using_secretkey(did, table_info, guise_gen, decrypt_cap, conn)
    }

    // Note: Decorrelations are not revealed if not using EdnaSpeaksForRecords
    // We reveal in reverse order of how we disguised
    pub fn reveal_using_secretkey<Q: Queryable>(
        &mut self,
        did: DID,
        table_info: &HashMap<String, TableInfo>,
        guise_gen: &GuiseGen,
        decrypt_cap: records::DecryptCap,
        conn: &mut Q,
    ) -> Result<(), mysql::Error> {
        // NOTE: We are currently not trying to reveal items with identifying columns that are fks
        // to the users table that have since been decorrelated (would need to check all possible
        // pps as fks)

        let start = time::Instant::now();
        let (dts, sfrs, pks) = self
            .llapi
            .lock()
            .unwrap()
            .get_recs_and_privkeys(&decrypt_cap);
        warn!(
            "Edna: Get records for reveal: {}",
            start.elapsed().as_micros()
        );

        // Invariant: any pps that have been decorrelated so far, but still have existing data
        // (meaning their disguises haven't yet been reverted, or they were further decorrelated)
        // will have a matching private key, but no direct speaks-for record pointing to them
        let mut recorrelated_pps: HashSet<UID> = pks.keys().cloned().collect();
        for sfr in &sfrs {
            //warn!("Got sfr uid: {}", sfr.new_uid);
            recorrelated_pps.remove(&sfr.new_uid);
        }
        // construct the graph of sf-relationships between principals
        let mut edges: HashMap<UID, Vec<PrivkeyRecord>> = HashMap::new();
        for (_, pk) in &pks {
            match edges.get_mut(&pk.new_uid) {
                Some(vs) => vs.push(pk.clone()),
                None => {
                    edges.insert(pk.new_uid.clone(), vec![pk.clone()]);
                }
            }
        }
        warn!("Recorrelated pps: {:?}", recorrelated_pps);

        let mut failed = false;
        let mut llapi = self.llapi.lock().unwrap();
        llapi.start_reveal(did);
        for dwrapper in &dts {
            let d = edna_diff_record_from_bytes(&dwrapper.record_data);
            if dwrapper.did == did && d.typ == REMOVE_PRINCIPAL {
                info!("Reversing principal remove record {:?}\n", d);
                if recorrelated_pps.contains(&dwrapper.uid) {
                    info!(
                        "Not reinserting removed and then recorrelated principal {}\n",
                        dwrapper.uid
                    );
                }
                let revealed = d.reveal(&table_info, &dwrapper.uid, &mut llapi, conn)?;
                if revealed {
                    info!("Principal Remove Record revealed!\n");
                } else {
                    failed = true;
                    info!("Failed to reveal remove record");
                }
            }
        }

        // reveal other row removals
        // this must be done in order to properly recorrelate sf_records later
        let start = time::Instant::now();

        // construct the graph of tables with removed items
        let mut remove_diffs_for_table: HashMap<String, Vec<(String, EdnaDiffRecord)>> =
            HashMap::new();
        for dwrapper in &dts {
            let d = edna_diff_record_from_bytes(&dwrapper.record_data);
            if dwrapper.did == did && d.typ == REMOVE_GUISE {
                match remove_diffs_for_table.get_mut(&d.table) {
                    Some(ds) => ds.push((dwrapper.uid.clone(), d)),
                    None => {
                        remove_diffs_for_table
                            .insert(d.table.clone(), vec![(dwrapper.uid.clone(), d.clone())]);
                    }
                }
            }
        }
        let mut removed_revealed = HashSet::new();

        // reveal user removed records first for referential integrity
        self.reveal_remove_diffs_of_table(
            &guise_gen.name,
            &remove_diffs_for_table,
            table_info,
            &mut llapi,
            conn,
        )?;
        removed_revealed.insert(guise_gen.name.clone());

        // insert all other removed records
        for table in remove_diffs_for_table.keys() {
            if removed_revealed.contains(table) {
                continue;
            }
            // reveal all referenced tables first
            let ti = table_info.get(table).unwrap();
            for (reftab, _, _) in &ti.other_fk_cols {
                if removed_revealed.contains(table) {
                    continue;
                }
                self.reveal_remove_diffs_of_table(
                    &reftab,
                    &remove_diffs_for_table,
                    table_info,
                    &mut llapi,
                    conn,
                )?;
                removed_revealed.insert(reftab.clone());
            }
            // then reveal this one
            self.reveal_remove_diffs_of_table(
                table,
                &remove_diffs_for_table,
                table_info,
                &mut llapi,
                conn,
            )?;
            removed_revealed.insert(table.clone());
        }

        // reveal all other (non-remove) diff records
        for dwrapper in &dts {
            let d = edna_diff_record_from_bytes(&dwrapper.record_data);
            if dwrapper.did == did && d.typ != REMOVE_GUISE && d.typ != REMOVE_PRINCIPAL {
                info!("Reversing record {:?}\n", d);
                let revealed = d.reveal(&table_info, &dwrapper.uid, &mut llapi, conn)?;
                if revealed {
                    info!("NonRemove Diff Record revealed!\n");
                } else {
                    failed = true;
                    info!("Failed to reveal non-remove record");
                }
            }
        }

        // reveal speaksfor records
        for sfr in &sfrs {
            // Note: only works for speaksfor records from edna's HLAPI
            match edna_sf_record_from_bytes(&sfr.record_data) {
                Err(_) => continue,
                Ok(d) => {
                    let mut rec_to_reveal = sfr.clone();
                    if sfr.did == did {
                        // if the original owner has since been recorrelated, then it won't exist
                        // and we can't actually reverse this SF record.
                        //
                        // change the sfr to recorrelate with the most recently restored principal
                        // in the path to sfr.new_uid
                        if recorrelated_pps.contains(&sfr.old_uid) {
                            warn!("Recorrelated pps contained the old_uid {}", sfr.old_uid);

                            // find the path to the uid we're trying to recorrelate
                            let path = find_path_to(&edges, &sfr.new_uid);

                            // there was no path?
                            if path.is_empty() {
                                failed = true;
                                warn!("No path to new_uid {}", sfr.new_uid);
                                continue;
                            }
                            // Fortunately, there's only ever one path to the new_uid
                            for node in path.iter().rev() {
                                // stop once we hit the first recorrelated pp
                                // this assumes that the first uid in the path has been restored already...
                                if recorrelated_pps.contains(&node.new_uid) {
                                    rec_to_reveal.old_uid = node.old_uid.clone();
                                    info!(
                                        "Changing UID to recorrelate to {} from {}",
                                        node.old_uid, sfr.old_uid
                                    );
                                    break;
                                }
                            }
                        }

                        info!("Reversing record {:?}\n", d);
                        let revealed =
                            d.reveal(&table_info, &rec_to_reveal, guise_gen, &mut llapi, conn)?;
                        if revealed {
                            info!(
                                "Reversed SpeaksFor Record {}->{}!\n",
                                sfr.old_uid, sfr.new_uid
                            );
                        } else {
                            failed = true;
                        }
                    }
                }
            }
        }

        llapi.cleanup_records_of_disguise(did, &decrypt_cap, &mut self.seen_pps);
        if failed {
            warn!("Reveal records failed, oh well clearing anyways");
        }
        llapi.end_reveal(did);
        warn!("Reveal records total: {}", start.elapsed().as_micros());
        Ok(())
    }

    pub fn apply<Q: Queryable>(
        &mut self,
        disguise: &Disguise,
        table_info: &HashMap<String, TableInfo>,
        guise_gen: &GuiseGen,
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
        self.apply_using_secretkey(disguise, table_info, guise_gen, decrypt_cap, conn)
    }

    pub fn apply_using_secretkey<Q: Queryable>(
        &mut self,
        disguise: &Disguise,
        table_info: &HashMap<String, TableInfo>,
        guise_gen: &GuiseGen,
        decrypt_cap: records::DecryptCap,
        conn: &mut Q,
    ) -> Result<DID, mysql::Error> {
        let start = time::Instant::now();
        let mut llapi = self.llapi.lock().unwrap();
        let disg_start = time::Instant::now();
        let did = llapi.start_disguise(disguise.user.clone());
        warn!("Edna: start disguise: {}", disg_start.elapsed().as_micros());

        let get_rec_start = time::Instant::now();
        let (_, speaksfor_records, _pks) = llapi.get_recs_and_privkeys(&decrypt_cap);
        warn!("Edna: get records: {}", get_rec_start.elapsed().as_micros());
        drop(llapi);

        // Handle REMOVE first
        // Any items that would've been modified or decorreltaed would've been revealed anyways
        let remove_start = time::Instant::now();
        let drop_me_later = self.execute_removes(
            did,
            disguise,
            table_info,
            guise_gen,
            &speaksfor_records,
            conn,
        );
        warn!(
            "Edna: Execute removes total: {}",
            remove_start.elapsed().as_micros()
        );

        /*
         * Decor and modify
         */
        let decor_start = time::Instant::now();
        let mut llapi = self.llapi.lock().unwrap();
        for (table, transforms) in &disguise.table_disguises {
            let curtable_info = table_info.get(table).unwrap();

            // Modify next
            for t in transforms {
                let mut selection = predicate::owner_filter_pred(
                    &table,
                    &disguise.user,
                    &curtable_info.owner_fk_cols, // assume only one fk
                    &speaksfor_records,
                )
                .to_string();

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
                        modify_items(
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

            // handle Decor last
            // this means that items stored from remove/modify might restore ownership to the
            // original principal? Which is fine, because we'd be revealing this too
            for t in transforms {
                let mut selection = predicate::owner_filter_pred(
                    &table,
                    &disguise.user,
                    &curtable_info.owner_fk_cols, // assume only one fk
                    &speaksfor_records,
                )
                .to_string();
                info!(
                    "Owner_filter_pred: {:?} with {} spr",
                    selection,
                    speaksfor_records.len()
                );

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

                        decor_items(
                            disguise.user.clone(),
                            did,
                            &mut llapi,
                            &table,
                            curtable_info,
                            &selected_rows,
                            user_fk_cols,
                            group_by_cols,
                            guise_gen,
                            &speaksfor_records,
                            &mut self.seen_pps,
                            conn,
                        );
                    }
                    _ => continue,
                }
            }
        }
        // drop all users now so we don't violate ref integrity from decorrelate
        for delstmt in drop_me_later {
            let start = time::Instant::now();
            db.query_drop(delstmt.to_string()).unwrap();
            warn!("{}: {}", delstmt, start.elapsed().as_micros());
        }

        warn!(
            "Edna: Execute modify/decor total: {}",
            decor_start.elapsed().as_micros()
        );
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
        guise_gen: &GuiseGen,
        sf_records: &Vec<SpeaksForRecordWrapper>,
        db: &mut Q,
    ) -> Vec<String> {
        info!(
            "ApplyRemoves: removing objs for disguise {} with {} sf_records\n",
            did,
            sf_records.len()
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
                        &table,
                        &disguise.user,
                        &curtable_info.owner_fk_cols, // assume only one fk
                        &sf_records,
                    )
                    .to_string();
                    if pred != "True" {
                        selection = format!("{} AND {}", selection, pred);
                    }
                    let start = time::Instant::now();
                    let selected_rows = get_query_rows_str_q::<Q>(
                        &str_select_statement(&table, &from, &selection),
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
                    if curtable_info.owner_fk_cols.len() == 1 {
                        for i in &pred_items {
                            let ids = get_ids(&curtable_info.id_cols, &i);
                            for owner_col in &curtable_info.owner_fk_cols {
                                let curuid = get_value_of_col(&i, &owner_col).unwrap();

                                // save the current object (we don't need to save the anonymized object)
                                let obj_diff_record =
                                    new_delete_record(table.to_string(), ids.clone(), i.to_vec());

                                // if this was predicated on belonging to the original principal,
                                // then we should insert it into a bag whose locator should be sent
                                // to the original principal (even though it's encrypted with the
                                // pseudoprincipal's pubkey).
                                // NOTE: this works because there's only one owner column
                                llapi.save_diff_record(
                                    curuid.clone(),
                                    did,
                                    edna_diff_record_to_bytes(&obj_diff_record),
                                );
                                // if we're working on the principal table (e.g., a users table),
                                // remove the user from Edna's metadata
                                if guise_gen.name == table {
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
                        if guise_gen.name == table {
                            drop_me_later.push(delstmt);
                        } else {
                            let start = time::Instant::now();
                            db.query_drop(delstmt.to_string()).unwrap();
                            warn!("{}: {}", delstmt, start.elapsed().as_micros());
                        }
                    } else {
                        // assert that users are never shared...
                        assert!(guise_gen.name != table);

                        // MULTIPLE OWNERS
                        // Go through each object one by one... this is slow
                        for i in &pred_items {
                            // NOTE: IDs cannot include owner values, otherwise these may
                            // change as they're replaced by pseudoprincipals
                            // this means that objects identified by e.g., userID + paperID are not
                            // supported with multiple users
                            let ids = get_ids(&curtable_info.id_cols, &i);

                            let mut i_updates = vec![];
                            let mut should_remove = true;
                            let mut updated_mapping = false;

                            // if this object has been removed, we want to
                            // (1) store the relevent speaks-for and diffs for the principal(s)
                            //      removing the object, and
                            // (2) remove those principals' mapping
                            // (3) also actually update the persistent database object
                            if let Some((diff, nps_to_pps)) = self
                                .removed_shared_objs
                                .get_mut(&(table.clone(), ids.clone()))
                            {
                                for owner_col in &curtable_info.owner_fk_cols {
                                    let mut remove_mapping = false;
                                    let curuid = get_value_of_col(&i, &owner_col).unwrap();
                                    // if the user has an existing mapping... (e.g., is not a
                                    // pp that has been removed before)
                                    if let Some(ppuid) = nps_to_pps.get(&curuid) {
                                        // and if we should remove the object for this user...
                                        if disguise.user.is_none()
                                            || disguise.user.as_ref().unwrap() == &curuid
                                        {
                                            let ix = i
                                                .iter()
                                                .position(|r| &r.column() == owner_col)
                                                .unwrap();

                                            // (1) store diff and SF tokens
                                            llapi.save_diff_record(
                                                curuid.clone(),
                                                did,
                                                edna_diff_record_to_bytes(&diff),
                                            );
                                            // save pp speaks-for records
                                            info!(
                                                "Registering pp {} for user {} for shared item",
                                                ppuid, curuid
                                            );
                                            let sf_record_bytes = edna_sf_record_to_bytes(
                                                &new_edna_speaksfor_record(
                                                    table.to_string(),
                                                    ids.clone(),
                                                    owner_col.to_string(),
                                                    &ppuid,
                                                ),
                                            );
                                            llapi.register_and_save_pseudoprincipal_record(
                                                did,
                                                &curuid,
                                                &ppuid,
                                                sf_record_bytes,
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
                                            info!(
                                                "Do not remove mapping of curuid {} of shared obj",
                                                curuid
                                            );
                                            should_remove = false;
                                        }
                                    }
                                    if remove_mapping {
                                        nps_to_pps.remove(&curuid);
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
                                for owner_col in &curtable_info.owner_fk_cols {
                                    let curuid = get_value_of_col(&i, &owner_col).unwrap();

                                    // skip pseudoprincipals
                                    if llapi.principal_is_anon(&curuid) {
                                        continue;
                                    }

                                    // create pseudoprincipal for this owner
                                    let pp =
                                        create_new_pseudoprincipal(&mut self.seen_pps, guise_gen);
                                    pps.push(pp.clone());

                                    // we want to store the diff of an item with the pp as the new owner/fk
                                    let ix =
                                        i.iter().position(|r| &r.column() == owner_col).unwrap();
                                    i_with_pps[ix] = RowVal::new(i[ix].column(), pp.0.clone());

                                    if disguise.user.is_some()
                                        && disguise.user.as_ref().unwrap() != &curuid
                                    {
                                        should_remove = false;
                                    }
                                }

                                // (1) insert new pseudoprincipals into the DB
                                // NOTE: this creates PPs even for users who might never remove
                                // this item
                                if !pps.is_empty() {
                                    let cols = pps[0]
                                        .1
                                        .iter()
                                        .map(|rv| rv.column().clone())
                                        .collect::<Vec<String>>()
                                        .join(",");
                                    let vals: Vec<String> = pps
                                        .iter()
                                        .map(|pp| {
                                            format!(
                                                "({})",
                                                pp.1.iter()
                                                    .map(|rv| rv.value().clone())
                                                    .collect::<Vec<String>>()
                                                    .join(",")
                                            )
                                        })
                                        .collect();
                                    let q = format!(
                                        "INSERT INTO {} ({}) VALUES {};",
                                        guise_gen.name,
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
                                let i_with_pps_diff_record = new_delete_record(
                                    table.to_string(),
                                    ids.clone(),
                                    i_with_pps.to_vec(),
                                );

                                for owner_col in &curtable_info.owner_fk_cols {
                                    let curuid = get_value_of_col(&i, &owner_col).unwrap();
                                    let newuid = get_value_of_col(&i_with_pps, &owner_col).unwrap();
                                    // update with the pp value if we want to persist the
                                    // item with the pp. this occurs if we are removing on behalf of
                                    // either (1) all users, or (2) for one user who matches the
                                    // invoking value
                                    if disguise.user.is_none()
                                        || disguise.user.as_ref().unwrap() == &curuid
                                    {
                                        info!(
                                            "Saving diff record for shared obj to remove for {}",
                                            curuid
                                        );
                                        llapi.save_diff_record(
                                            curuid.clone(),
                                            did,
                                            edna_diff_record_to_bytes(&i_with_pps_diff_record),
                                        );

                                        info!(
                                            "Updating {} to {} for shared obj to remove for {}",
                                            curuid, newuid, curuid
                                        );
                                        // save pp speaks-for records
                                        let sf_record_bytes =
                                            edna_sf_record_to_bytes(&new_edna_speaksfor_record(
                                                table.to_string(),
                                                ids.clone(),
                                                owner_col.to_string(),
                                                &newuid,
                                            ));
                                        llapi.register_and_save_pseudoprincipal_record(
                                            did,
                                            &curuid,
                                            &newuid,
                                            sf_record_bytes,
                                        );
                                        i_updates.push((owner_col.clone(), newuid));
                                    } else {
                                        info!("Inserting mapping from {} to {} for shared obj to remove", curuid, newuid);
                                        match self
                                            .removed_shared_objs
                                            .get_mut(&(table.clone(), ids.clone()))
                                        {
                                            Some((_diff, np_to_pps)) => {
                                                np_to_pps.insert(curuid, newuid);
                                            }
                                            None => {
                                                let mut np_to_pps = HashMap::new();
                                                np_to_pps.insert(curuid, newuid);
                                                self.removed_shared_objs.insert(
                                                    (table.clone(), ids.clone()),
                                                    (i_with_pps_diff_record.clone(), np_to_pps),
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
                                self.persist_removed_shared_obj_update(&(
                                    table.clone(),
                                    ids.clone(),
                                ));
                            }

                            // REGARDLESS OF WHETHER WE REMOVED THIS BEFORE
                            // if all users are now persistently anonymous, remove the object
                            //      otherwise, just store the newly updated object
                            let selection = get_select_of_ids(&ids);
                            if should_remove {
                                let delstmt = format!("DELETE FROM {} WHERE {}", table, selection);
                                db.query_drop(delstmt.to_string()).unwrap();
                                warn!("{}: {}", delstmt, start.elapsed().as_micros());
                                // remove from Edna map too (and persist)
                                self.removed_shared_objs
                                    .remove(&(table.clone(), ids.clone()));
                                self.persist_removed_shared_obj_delete(&(
                                    table.clone(),
                                    ids.clone(),
                                ));
                            } else {
                                if i_updates.is_empty() {
                                    continue;
                                }
                                let updates = i_updates
                                    .iter()
                                    .map(|(c, v)| format!("{} = '{}'", c, v))
                                    .collect::<Vec<String>>()
                                    .join(", ");
                                let updatestmt =
                                    format!("UPDATE {} SET {} WHERE {}", table, updates, selection);
                                let start = time::Instant::now();
                                db.query_drop(updatestmt.to_string()).unwrap();
                                warn!("{}: {}mus", updatestmt, start.elapsed().as_micros());
                            }
                        }
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
}

/*
 * Also only used by higher-level disguise specs
 */
fn decor_items<Q: Queryable>(
    uid: Option<UID>,
    did: DID,
    llapi: &mut LowLevelAPI,
    child_name: &str,
    child_tableinfo: &TableInfo,
    items: &Vec<Vec<RowVal>>,
    user_fk_cols: &Vec<String>,
    group_by_cols: &Vec<String>,
    guise_gen: &GuiseGen,
    sf_records: &Vec<SpeaksForRecordWrapper>,
    seen_pps: &mut HashSet<UID>,
    db: &mut Q,
) {
    let start = time::Instant::now();
    // grouped by (1) owning users, and (2) column attributes
    let mut owner_groups: HashMap<Vec<String>, HashMap<Vec<String>, Vec<Vec<RowVal>>>> =
        HashMap::new();
    let mut pseudoprincipals = vec![];
    let mut valid_owners: Vec<String> =
        sf_records.iter().map(|ot| ot.new_uid.to_string()).collect();
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
                            || valid_owners.contains(&get_value_of_col(&i, &user_fk_col).unwrap())
                        {
                            pseudoprincipals.push(create_new_pseudoprincipal(seen_pps, guise_gen));
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
                        pseudoprincipals.push(create_new_pseudoprincipal(seen_pps, guise_gen));
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
        .iter()
        .map(|rv| rv.column().clone())
        .collect::<Vec<String>>()
        .join(",");
    let vals: Vec<String> = pseudoprincipals
        .iter()
        .map(|pp| {
            format!(
                "({})",
                pp.1.iter()
                    .map(|rv| rv.value().clone())
                    .collect::<Vec<String>>()
                    .join(",")
            )
        })
        .collect();
    let q = format!(
        "INSERT INTO {} ({}) VALUES {};",
        guise_gen.name,
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
         * A) insert guises for parents
         * B) update child to point to new guise
         * */
        for (_group, items) in groups {
            for user_fk_col in user_fk_cols {
                // the referenced UIDs are all the same for this group
                // of items
                let old_uid = get_value_of_col(&items[0], &user_fk_col).unwrap();
                if uid.is_none() || valid_owners.contains(&old_uid) {
                    info!(
                        "decor_obj {}: Creating guise for old uid {} col {}",
                        child_name, old_uid, user_fk_col
                    );
                    let (new_uid, _) = &pseudoprincipals[index];
                    let new_uid = new_uid.replace("\'", "");

                    index += 1;

                    llapi.register_pseudoprincipal(did, &old_uid, &new_uid);
                    info!("Register anon principal {}", new_uid);

                    for i in items {
                        // A. REGISTER PSEUDOPRINCIPAL
                        // save a speaksfor record for the old uid
                        let child_ids = get_ids(&child_tableinfo.id_cols, &i);
                        info!("registering pp for: {:?}", child_ids);
                        let sf_record_bytes = edna_sf_record_to_bytes(&new_edna_speaksfor_record(
                            child_name.to_string(),
                            child_ids,
                            user_fk_col.to_string(),
                            &new_uid,
                        ));
                        llapi.insert_speaksfor_record(
                            did,
                            &old_uid,
                            &new_uid,
                            sf_record_bytes.clone(),
                        );

                        // add this here so for the rest of this disguise, we'll also predicate on
                        // anything owned by this pseudoprincipal
                        // XXX nooo this is so expensive
                        //sf_records.push(new_generic_speaksfor_record_wrapper(
                        //old_uid.to_string(),
                        //new_uid.to_string(),
                        //did,
                        //sf_record_bytes,
                        //));

                        // B. UPDATE CHILD FOREIGN KEY
                        let start = time::Instant::now();
                        let i_select = get_select_of_row(&child_tableinfo.id_cols, &i);
                        let q = format!(
                            "UPDATE {} SET {} = '{}' WHERE {}",
                            child_name, user_fk_col, new_uid, i_select
                        );
                        db.query_drop(&q).unwrap();
                        warn!("{}: {}mus", q, start.elapsed().as_micros());
                    }
                }
            }
        }
    }
}

fn modify_items<Q: Queryable>(
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

    // TOKEN INSERT
    let start = time::Instant::now();
    for i in items {
        let old_val: &str = &i.iter().find(|rv| rv.column() == col).unwrap().value();
        if old_val == new_val {
            continue;
        }
        let ids = get_ids(&table_info.id_cols, &i);
        let update_record = new_modify_record(
            table.to_string(),
            ids,
            old_val.to_string(),
            new_val.clone(),
            col.to_string(),
        );

        for owner_col in &table_info.owner_fk_cols {
            let owner_uid = get_value_of_col(&i, &owner_col).unwrap();
            let bytes = edna_diff_record_to_bytes(&update_record);
            llapi.save_diff_record(owner_uid, did, bytes);
        }
    }
    warn!("Update record inserted: {}", start.elapsed().as_micros());
}

fn create_new_pseudoprincipal(
    seen_pps: &mut HashSet<UID>,
    guise_gen: &GuiseGen,
) -> (UID, Vec<RowVal>) {
    let mut new = false;
    let mut new_uid = String::new();
    let mut rowvals = vec![];
    while !new {
        let new_parent_vals = guise_gen.get_vals();
        let new_parent_cols = guise_gen.cols.clone();
        let mut ix = 0;
        let mut uid_ix = 0;
        rowvals = new_parent_cols
            .iter()
            .map(|c| {
                if c == &guise_gen.id_col {
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
    (new_uid, rowvals)
}
