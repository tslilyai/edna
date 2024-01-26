use crate::lowlevel_api::*;
use crate::records::*;
use crate::*;
use log::{info, warn};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Instant;

pub type UpdateFn = Box<dyn Fn(Vec<TableRow>) -> Vec<TableRow> + Send + Sync>;

pub struct RevealArgs<'a, Q: Queryable> {
    pub timap: &'a HashMap<String, TableInfo>,
    pub pp_gen: &'a PseudoprincipalGenerator,
    pub recorrelated_pps: &'a HashSet<UID>,
    pub edges: &'a HashMap<UID, Vec<SFChainRecord>>,
    pub uid: UID,
    pub did: DID,
    pub reveal_pps: RevealPPType,
    pub llapi: &'a mut LowLevelAPI,
    pub db: &'a mut Q,
}
pub struct Revealer {
    pub llapi: Arc<Mutex<LowLevelAPI>>,
    pub pool: mysql::Pool,
    start: Instant,
    updates: Vec<(u64, UpdateFn)>,
    dryrun: bool,
}

impl Revealer {
    pub fn new(llapi: Arc<Mutex<LowLevelAPI>>, pool: mysql::Pool, dryrun: bool) -> Revealer {
        let revealer = Revealer {
            llapi: llapi,
            pool: pool,
            start: Instant::now(),
            updates: vec![],
            dryrun: dryrun,
        };
        revealer
    }

    fn reveal_remove_diffs_of_table<Q: Queryable>(
        &self,
        table: &str,
        dsmap: &HashMap<String, Vec<(String, EdnaDiffRecord)>>,
        args: &mut RevealArgs<Q>,
    ) -> Result<bool, mysql::Error> {
        match dsmap.get(table) {
            Some(ds) => {
                let mut success = true;
                for (uid, d) in ds {
                    // don't restore deleted pseudoprincipals that have been recorrelated!
                    if args.recorrelated_pps.contains(uid) && &d.new_uid == uid {
                        info!(
                            "Skipping restoration of deleted recorrelated pp {}, table {}!",
                            uid, table
                        );
                        continue;
                    }
                    info!("Reversing remove record {:?}\n", d);
                    args.uid = uid.clone();
                    let revealed = d.reveal(args)?;
                    if revealed {
                        info!("Remove Record revealed!\n");
                    } else {
                        info!("Failed to reveal remove record");
                    }
                    success &= revealed;
                }
                Ok(success)
            }
            None => Ok(true),
        }
    }

    pub fn reveal<Q: Queryable>(
        &mut self,
        uid: Option<&UID>,
        did: DID,
        table_info: &HashMap<String, TableInfo>,
        pp_gen: &PseudoprincipalGenerator,
        reveal_pps: Option<RevealPPType>,
        db: &mut Q,
        password: Option<String>,
        user_share: Option<(Share, Loc)>,
    ) -> Result<(), mysql::Error> {
        let mut privkey = vec![];

        if uid != None {
            let llapi = self.llapi.lock().unwrap();

            let priv_key = llapi.get_priv_key(&(uid.clone().unwrap()), password, user_share);
            if let Some(key) = priv_key {
                // PROXY: login user
                if !self.dryrun {
                    db.query_drop(format!("LOGIN {}", base64::encode(&key)))
                        .unwrap();
                }
                privkey = key;
            }
            drop(llapi);

            info!("got priv key");
        }

        warn!("Revealing disguise for {:?}", uid);
        self.reveal_using_secretkey(did, table_info, pp_gen, reveal_pps, privkey, db)
    }

    // Note: Decorrelations are not revealed if not using EdnaSpeaksForRecords
    // We reveal in reverse order of how we disguised
    pub fn reveal_using_secretkey<Q: Queryable>(
        &mut self,
        did: DID,
        table_info: &HashMap<String, TableInfo>,
        pp_gen: &PseudoprincipalGenerator,
        reveal_pps: Option<RevealPPType>,
        privkey: records::PrivKey,
        db: &mut Q,
    ) -> Result<(), mysql::Error> {
        // NOTE: We are currently not trying to reveal items with identifying
        // columns that are fks to the users table that have since been
        // decorrelated (would need to check all possible pps as fks)
        let fnstart = time::Instant::now();
        
        // PROXY: login user
        if !self.dryrun {
            if privkey.len() > 0 {
                db.query_drop(format!("LOGIN {}", base64::encode(&privkey)))
                    .unwrap();
            }
        }


        let start = time::Instant::now();
        let (drws, pks) = self
            .llapi
            .lock()
            .unwrap()
            .get_recs_and_privkeys(&privkey);
        warn!(
            "Edna: Get records for reveal: {}mus",
            start.elapsed().as_micros()
        );
        let reveal_pps = reveal_pps.unwrap_or(RevealPPType::Restore);
        let drs: HashSet<EdnaDiffRecordWrapper> = drws
            .iter()
            .map(|drw| EdnaDiffRecordWrapper {
                did: drw.did,
                uid: drw.uid.clone(),
                record: edna_diff_record_from_bytes(&drw.record_data),
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

        let mut llapi = self.llapi.lock().unwrap();
        llapi.start_reveal(did);
        let mut reveal_args = RevealArgs {
            timap: &table_info,
            pp_gen: &pp_gen,
            recorrelated_pps: &recorrelated_pps,
            edges: &edges,
            uid: String::new(),
            did: did,
            reveal_pps: reveal_pps,
            llapi: &mut llapi,
            db: db,
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
        for dr in &drs {
            if dr.did == did && dr.record.typ == REMOVE {
                let table = if dr.record.old_values.len() > 0 {
                    &dr.record.old_values[0].table
                } else {
                    &dr.record.new_values[0].table
                };
                match remove_diffs_for_table.get_mut(table) {
                    Some(ds) => ds.push((dr.uid.clone(), dr.record.clone())),
                    None => {
                        remove_diffs_for_table
                            .insert(table.clone(), vec![(dr.uid.clone(), dr.record.clone())]);
                    }
                }
            }
        }
        let mut removed_revealed = HashSet::new();

        // reveal user removed records first for referential integrity
        self.reveal_remove_diffs_of_table(
            &pp_gen.table,
            &remove_diffs_for_table,
            &mut reveal_args,
        )?;
        removed_revealed.insert(pp_gen.table.clone());

        // revealing in order of disguising: undo all decor
        // but only if we aren't going to do so anyways
        // when revealing new PPs!
        if reveal_pps != RevealPPType::Restore {
            for dr in &drs {
                if dr.did == did && dr.record.typ == DECOR {
                    info!("Reversing decor record {:?}\n", dr.record);
                    reveal_args.uid = dr.uid.clone();
                    let revealed = dr.record.reveal(&mut reveal_args)?;
                    if revealed {
                        info!("Decor Record revealed!\n");
                    } else {
                        success = false;
                        info!("Failed to reveal decor record");
                    }
                }
            }
        }

        // reveal all modify diff records
        for dr in &drs {
            if dr.did == did && dr.record.typ == MODIFY {
                info!("Reversing modify record {:?}\n", dr.record);
                reveal_args.uid = dr.uid.clone();
                let revealed = dr.record.reveal(&mut reveal_args)?;
                if revealed {
                    info!("Modify Diff Record revealed!\n");
                } else {
                    success = false;
                    info!("Failed to reveal modify record");
                }
            }
        }

        // insert all non-user removed records
        for table in remove_diffs_for_table.keys() {
            if removed_revealed.contains(table) {
                continue;
            }
            // reveal all referenced tables first
            let ti = table_info.get(table).unwrap();
            for fk in &ti.other_fks {
                let reftab = &fk.to_table;
                if removed_revealed.contains(reftab) {
                    continue;
                }
                self.reveal_remove_diffs_of_table(
                    reftab,
                    &remove_diffs_for_table,
                    &mut reveal_args,
                )?;
                removed_revealed.insert(reftab.clone());
            }
            // then reveal this one
            self.reveal_remove_diffs_of_table(table, &remove_diffs_for_table, &mut reveal_args)?;
            removed_revealed.insert(table.clone());
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

        llapi.cleanup_records_of_disguise(did, &privkey);
        if !success {
            info!(
                "Reveal records failed, clearing anyways: {}mus",
                fnstart.elapsed().as_micros()
            );
        }
        llapi.end_reveal(did);
        warn!("Reveal records total: {}mus", fnstart.elapsed().as_micros());
        Ok(())
    }

    pub fn record_update(&mut self, f: UpdateFn) {
        self.updates
            .push((self.start.elapsed().as_secs(), Box::new(f)));
    }
}
