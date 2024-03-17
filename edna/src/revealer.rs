use crate::lowlevel_api::*;
use crate::records::*;
use crate::*;
use log::{info, warn};
use std::cmp::min;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

pub struct RevealArgs<'a, Q: Queryable> {
    pub timap: HashMap<String, TableInfo>,
    pub pp_gen: &'a PseudoprincipalGenerator,
    pub recorrelated_pps: &'a HashSet<UID>,
    pub edges: &'a HashMap<UID, Vec<SFChainRecord>>,
    pub uid: UID,
    pub did: DID,
    pub reveal_pps: RevealPPType,
    pub allow_singlecolumn_reveals: bool,
    pub llapi_locked: &'a mut LowLevelAPI,
    pub db: &'a mut Q,
    pub updates: Vec<Update>,
}
pub struct Revealer {
    pub llapi: Arc<Mutex<LowLevelAPI>>,
    pub pool: mysql::Pool,
}

impl Revealer {
    pub fn new(llapi: Arc<Mutex<LowLevelAPI>>, pool: mysql::Pool) -> Revealer {
        let revealer = Revealer {
            llapi: llapi,
            pool: pool,
        };
        revealer
    }

    fn reveal_diffs_of_table<Q: Queryable>(
        &self,
        table: &str,
        dsmap: &HashMap<String, Vec<(String, EdnaDiffRecord)>>,
        args: &mut RevealArgs<Q>,
        typ: u8,
    ) -> Result<bool, mysql::Error> {
        info!("Revealing remove diffs of table {}", table);
        match dsmap.get(table) {
            Some(ds) => {
                let mut bigdiff = EdnaDiffRecord {
                    typ: typ,
                    // old and new rows
                    old_values: vec![],
                    new_values: vec![],
                    pubkey: vec![],
                    enc_locators_index: 0,
                    old_uid: "".to_string(),
                    new_uid: "".to_string(),
                    t: 0,
                };
                for (uid, d) in ds {
                    // don't restore deleted pseudoprincipals that have been recorrelated!
                    if args.recorrelated_pps.contains(uid) && table == args.pp_gen.table {
                        info!(
                            "Skipping restoration of deleted recorrelated pp {}, table {}!",
                            uid, table
                        );
                        continue;
                    }
                    bigdiff.old_values.append(&mut d.old_values.clone());
                    bigdiff.new_values.append(&mut d.new_values.clone());
                    if bigdiff.t == 0 {
                        bigdiff.t = d.t;
                    } else {
                        bigdiff.t = min(d.t, bigdiff.t);
                    }
                    //info!("Reversing remove record {:?}\n", d);
                    //args.uid = uid.clone();
                    //let revealed = d.reveal(args)?;
                    //if revealed {
                    //    info!("Remove Record revealed!\n");
                    //} else {
                    //    info!("Failed to reveal remove record");
                    // }
                }
                bigdiff.reveal(args)
            }
            None => Ok(true),
        }
    }

    pub fn reveal<Q: Queryable>(
        &mut self,
        uid: Option<&UID>,
        did: DID,
        timap: &HashMap<String, TableInfo>,
        pp_gen: &PseudoprincipalGenerator,
        reveal_pps: Option<RevealPPType>,
        allow_singlecolumn_reveals: bool,
        db: &mut Q,
        password: Option<String>,
        user_share: Option<(Share, Loc)>,
    ) -> Result<(), mysql::Error> {
        let start = time::Instant::now();
        let mut privkey = vec![];

        if uid != None {
            let llapi_locked = self.llapi.lock().unwrap();

            let priv_key = llapi_locked.get_priv_key(&(uid.clone().unwrap()), password, user_share);
            if let Some(key) = priv_key {
                privkey = key;
            }
            drop(llapi_locked);

            info!("got priv key");
        }

        warn!(
            "Revealing disguise for {:?} after getting key: {}mus",
            uid,
            start.elapsed().as_micros()
        );
        let start = time::Instant::now();
        let res = self.reveal_using_secretkey(
            did,
            timap,
            pp_gen,
            reveal_pps,
            allow_singlecolumn_reveals,
            privkey,
            db,
        );
        warn!(
            "reveal using secretkey took {}mus",
            start.elapsed().as_micros()
        );
        res
    }

    // Note: Decorrelations are not revealed if not using EdnaSpeaksForRecords
    // We reveal in reverse order of how we disguised
    pub fn reveal_using_secretkey<Q: Queryable>(
        &mut self,
        did: DID,
        timap: &HashMap<String, TableInfo>,
        pp_gen: &PseudoprincipalGenerator,
        reveal_pps: Option<RevealPPType>,
        allow_singlecolumn_reveals: bool,
        privkey: records::PrivKey,
        db: &mut Q,
    ) -> Result<(), mysql::Error> {
        // NOTE: We are currently not trying to reveal items with identifying
        // columns that are fks to the users table that have since been
        // decorrelated (would need to check all possible pps as fks)
        let fnstart = time::Instant::now();
        let start = time::Instant::now();
        let mut llapi_locked = self.llapi.lock().unwrap();
        let (drws, pks) = llapi_locked.get_recs_and_privkeys(&privkey);
        warn!(
            "Edna: Get records for reveal: {}mus",
            start.elapsed().as_micros()
        );
        let reveal_pps = reveal_pps.unwrap_or(RevealPPType::Restore);
        let mut oldest_t = 0;
        let drs: HashSet<EdnaDiffRecordWrapper> = drws
            .iter()
            .map(|drw| {
                oldest_t = min(oldest_t, drw.t);
                EdnaDiffRecordWrapper {
                    did: drw.did,
                    uid: drw.uid.clone(),
                    record: edna_diff_record_from_bytes(&drw.record_data, drw.t),
                }
            })
            .collect();

        // Invariant: any pps that have been recorrelated so far, but still have
        // existing data (meaning their disguises haven't yet been reverted, or
        // they were further decorrelated) will have a matching private key, but
        // no direct diff record recording their data row
        let start = time::Instant::now();
        let mut recorrelated_pps: HashSet<UID> = pks.keys().cloned().collect();
        info!("Recorrelated pps pre-pruning: {:?}", recorrelated_pps);

        // remove pseudoprincipals that haven't been recorrelated yet
        for dr in drs.clone() {
            if dr.record.typ == NEW_PP {
                recorrelated_pps.remove(&dr.record.new_uid);
            }
        }
        // construct the graph of sf-relationships between principals
        let mut edges: HashMap<UID, Vec<SFChainRecord>> = HashMap::new();
        for (_, pk) in &pks {
            match edges.get_mut(&pk.new_uid) {
                Some(vs) => vs.push(pk.clone()),
                None => {
                    edges.insert(pk.new_uid.clone(), vec![pk.clone()]);
                }
            }
        }
        warn!(
            "Construct graph and get recorrelated pps: {:?}: {}mus",
            recorrelated_pps,
            start.elapsed().as_micros()
        );

        llapi_locked.start_reveal(did);
        let updates = llapi_locked.get_updates_since(oldest_t);
        let mut reveal_args = RevealArgs {
            timap: timap.clone(),
            pp_gen: &pp_gen,
            recorrelated_pps: &recorrelated_pps,
            edges: &edges,
            uid: String::new(),
            did: did,
            reveal_pps: reveal_pps,
            allow_singlecolumn_reveals: allow_singlecolumn_reveals,
            llapi_locked: &mut llapi_locked,
            db: db,
            updates: updates,
        };

        // first, reveal any removed principals
        let mut success = true;
        for dr in &drs {
            if dr.did == did && dr.record.typ == REMOVE_PRINCIPAL {
                info!("Reversing principal remove record {:?}\n", dr.record);
                if recorrelated_pps.contains(&dr.uid) {
                    info!(
                        "Not reinserting removed and then recorrelated principal {}\n",
                        dr.uid
                    );
                    continue;
                }
                reveal_args.uid = dr.uid.clone();
                let revealed = dr.record.reveal(&mut reveal_args)?;
                if revealed {
                    info!("Principal Remove Record revealed!\n");
                } else {
                    success = false;
                    info!("Failed to reveal principal remove record");
                }
            }
        }

        // reveal other row removals
        // first construct the graph of tables with removed items
        let mut remove_diffs_for_table: HashMap<String, Vec<(String, EdnaDiffRecord)>> =
            HashMap::new();
        let mut decor_diffs_for_table: HashMap<String, Vec<(String, EdnaDiffRecord)>> =
            HashMap::new();
        let mut modify_diffs_for_table: HashMap<String, Vec<(String, EdnaDiffRecord)>> =
            HashMap::new();
        for dr in &drs {
            if dr.did == did {
                let table = if dr.record.old_values.len() > 0 {
                    &dr.record.old_values[0].table
                } else {
                    &dr.record.new_values[0].table
                };
                match dr.record.typ {
                    REMOVE => match remove_diffs_for_table.get_mut(table) {
                        Some(ds) => ds.push((dr.uid.clone(), dr.record.clone())),
                        None => {
                            remove_diffs_for_table
                                .insert(table.clone(), vec![(dr.uid.clone(), dr.record.clone())]);
                        }
                    },
                    DECOR => match decor_diffs_for_table.get_mut(table) {
                        Some(ds) => ds.push((dr.uid.clone(), dr.record.clone())),
                        None => {
                            decor_diffs_for_table
                                .insert(table.clone(), vec![(dr.uid.clone(), dr.record.clone())]);
                        }
                    },
                    MODIFY => match modify_diffs_for_table.get_mut(table) {
                        Some(ds) => ds.push((dr.uid.clone(), dr.record.clone())),
                        None => {
                            modify_diffs_for_table
                                .insert(table.clone(), vec![(dr.uid.clone(), dr.record.clone())]);
                        }
                    },
                    _ => warn!("Skipping record typ {}", dr.record.typ),
                }
            }
        }

        // Note: we do restore removed records first because of referential
        // integrity, which can be violated if we restore decorrelations before
        // remove. This also means that we violate referential integrity when we
        // remove before decor.

        // reveal user removed records first for referential integrity
        self.reveal_diffs_of_table(
            &pp_gen.table,
            &remove_diffs_for_table,
            &mut reveal_args,
            REMOVE,
        )?;
        // insert all non-user removed records
        let mut all_tables = remove_diffs_for_table.keys().collect::<HashSet<_>>();
        all_tables.remove(&pp_gen.table);
        let mut all_tables_vec: Vec<_> = all_tables.clone().into_iter().collect();
        // make iteration deterministic
        all_tables_vec.sort();
        while all_tables.len() > 0 {
            for table in &all_tables_vec {
                // need to check that we haven't revealed this table yet
                if all_tables.contains(table) {
                    // reveal all referenced tables first
                    // note: we assume no circularity
                    let ti = timap.get(&table.to_string()).unwrap();
                    for fk in &ti.other_fks {
                        let reftab = &fk.to_table;
                        // if we haven't revealed the referenced table yet, reveal it!
                        if all_tables.contains(reftab) {
                            self.reveal_diffs_of_table(
                                reftab,
                                &remove_diffs_for_table,
                                &mut reveal_args,
                                REMOVE,
                            )?;
                            all_tables.remove(reftab);
                        }
                    }
                    // then reveal this one
                    self.reveal_diffs_of_table(
                        table,
                        &remove_diffs_for_table,
                        &mut reveal_args,
                        REMOVE,
                    )?;
                    all_tables.remove(table);
                }
            }
        }

        let mut all_tables = modify_diffs_for_table.keys().collect::<HashSet<_>>();
        all_tables.remove(&pp_gen.table);
        let mut all_tables_vec: Vec<_> = all_tables.clone().into_iter().collect();
        // make iteration deterministic
        all_tables_vec.sort();
        for table in &all_tables_vec {
            self.reveal_diffs_of_table(table, &modify_diffs_for_table, &mut reveal_args, MODIFY)?;
        }

        // revealing in order of disguising: undo all decor
        // but only if we aren't going to do so anyways
        // when revealing new PPs!
        if reveal_pps != RevealPPType::Restore {
            let mut all_tables = decor_diffs_for_table.keys().collect::<HashSet<_>>();
            all_tables.remove(&pp_gen.table);
            let mut all_tables_vec: Vec<_> = all_tables.clone().into_iter().collect();
            // make iteration deterministic
            all_tables_vec.sort();
            for table in &all_tables_vec {
                self.reveal_diffs_of_table(table, &decor_diffs_for_table, &mut reveal_args, DECOR)?;
            }
        }

        // reveal (by deleting) new pseudoprincipals
        // note that we do this after recorrelation in case a shared data item
        // introduced a record referring to a pseudoprincipal
        let mut pp_records = vec![];
        for dr in &drs {
            if dr.did == did && dr.record.typ == NEW_PP {
                pp_records.push(&dr.record);
            }
        }
        success &= EdnaDiffRecord::reveal_new_pps(&pp_records, &mut reveal_args)?;

        llapi_locked.cleanup_records_of_disguise(did, &privkey);
        if !success {
            info!(
                "Reveal records failed, clearing anyways: {}mus",
                fnstart.elapsed().as_micros()
            );
        }
        llapi_locked.end_reveal(did);
        drop(llapi_locked);
        warn!("Reveal records total: {}mus", fnstart.elapsed().as_micros());
        Ok(())
    }
}
