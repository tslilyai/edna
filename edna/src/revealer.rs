use crate::lowlevel_api::*;
use crate::records::*;
use crate::*;
use log::{info, warn};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Instant;

pub type UpdateFn = Box<dyn Fn(Vec<TableRow>) -> Vec<TableRow>>;

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
}

impl Revealer {
    pub fn new(llapi: Arc<Mutex<LowLevelAPI>>, pool: mysql::Pool) -> Revealer {
        let revealer = Revealer {
            llapi: llapi,
            pool: pool,
            start: Instant::now(),
            updates: vec![],
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
        self.reveal_using_secretkey(did, table_info, pp_gen, reveal_pps, decrypt_cap, db)
    }

    // Note: Decorrelations are not revealed if not using EdnaSpeaksForRecords
    // We reveal in reverse order of how we disguised
    pub fn reveal_using_secretkey<Q: Queryable>(
        &mut self,
        did: DID,
        table_info: &HashMap<String, TableInfo>,
        pp_gen: &PseudoprincipalGenerator,
        reveal_pps: Option<RevealPPType>,
        decrypt_cap: records::DecryptCap,
        db: &mut Q,
    ) -> Result<(), mysql::Error> {
        // NOTE: We are currently not trying to reveal items with identifying
        // columns that are fks to the users table that have since been
        // decorrelated (would need to check all possible pps as fks)
        let start = time::Instant::now();
        let (drws, pks) = self
            .llapi
            .lock()
            .unwrap()
            .get_recs_and_privkeys(&decrypt_cap);
        warn!(
            "Edna: Get records for reveal: {}",
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
        let mut recorrelated_pps: HashSet<UID> = pks.keys().cloned().collect();
        warn!("Recorrelated pps pre-pruning: {:?}", recorrelated_pps);

        // remove pseudoprincipals that haven't been recorrelated yet
        for dr in drs.clone() {
            if dr.record.typ == REMOVE
                && dr.record.new_values.len() > 0
                && dr.record.new_values[0].table == pp_gen.table
            {
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
        warn!("Recorrelated pps: {:?}", recorrelated_pps);

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
        // this must be done in order to properly recorrelate sf_records later
        let start = time::Instant::now();

        // construct the graph of tables with removed items
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

        // insert all other removed records
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

        // NOTE: used to rewrite of path to correct oldest UID here. no need, since we do it upon
        // reveal?
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

        llapi.cleanup_records_of_disguise(did, &decrypt_cap);
        if !success {
            warn!("Reveal records failed, clearing anyways");
        }
        llapi.end_reveal(did);
        warn!("Reveal records total: {}", start.elapsed().as_micros());
        Ok(())
    }

    pub fn record_update(&mut self, f: UpdateFn) {
        self.updates
            .push((self.start.elapsed().as_secs(), Box::new(f)));
    }
}
