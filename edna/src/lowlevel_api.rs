use crate::records::*;
use crate::*;
use crypto_box::PublicKey;
use log::debug;
use std::collections::{HashMap, HashSet};

/*
 * Exposes the LowLevel Edna API
 * Non-Transaction: each call (if runs any SQL queries) may be interleaved with other application
 * DB SQL queries, or disguise SQL queries. DO NOT run concurrently.
 */
pub struct LowLevelAPI {
    pub pool: mysql::Pool,
    record_ctrler: RecordCtrler,
}

impl LowLevelAPI {
    pub fn new(pool: mysql::Pool, in_memory: bool, reset: bool, dryrun: bool) -> LowLevelAPI {
        let mut db = pool.get_conn().unwrap();
        LowLevelAPI {
            pool: pool,
            record_ctrler: RecordCtrler::new(&mut db, in_memory, reset, dryrun),
        }
    }

    // gets number of bytes in principaldata as well as encrypted store
    pub fn get_sizes(&self, dbname: &str) -> (usize, usize) {
        self.record_ctrler
            .get_sizes(&mut self.pool.get_conn().unwrap(), dbname)
    }

    pub fn get_priv_key(
        &self,
        uid: &UID,
        password: Option<String>,
        user_share: Option<UserData>,
    ) -> Option<DecryptCap> {
        self.record_ctrler.get_priv_key(uid, password, user_share)
    }

    //-----------------------------------------------------------------------------
    // Necessary to make Edna aware of all principals in the system
    // so Edna can link these to pseudoprincipals/do crypto stuff
    //-----------------------------------------------------------------------------

    // registers the princiapl with edna, giving them a private/public keypair
    pub fn register_principal(&mut self, uid: &UID, password: String) -> UserData {
        let mut db = self.pool.get_conn().unwrap();
        let user_share = self
            .record_ctrler
            .register_principal_secret_sharing(uid, &mut db, password);
        user_share
    }

    // registers the princiapl with edna, giving them a private/public keypair
    pub fn register_principal_without_sharing(&mut self, uid: &UID) -> DecryptCap {
        let mut db = self.pool.get_conn().unwrap();
        let privkey = self
            .record_ctrler
            .register_principal_private_key(uid, false, &mut db, true);
        privkey
    }

    // reregister a principal with a known publickey
    pub fn register_saved_principal(
        &mut self,
        uid: &UID,
        is_anon: bool,
        pubkey: &Option<PublicKey>,
        enc_locators_index: Index,
        persist: bool,
    ) {
        self.record_ctrler.register_saved_principal(
            uid,
            is_anon,
            pubkey,
            enc_locators_index,
            persist,
            &mut self.pool.get_conn().unwrap(),
        );
    }

    //-----------------------------------------------------------------------------
    // To register and end a disguise (and get the corresponding capabilities)
    //-----------------------------------------------------------------------------
    pub fn start_disguise(&mut self, invoking_user: Option<UID>) -> DID {
        self.record_ctrler.start_disguise(invoking_user)
    }

    pub fn end_disguise(&mut self) {
        self.record_ctrler
            .save_and_clear_disguise(&mut self.pool.get_conn().unwrap());
    }

    pub fn start_reveal(&self, did: DID) {
        debug!("Starting reveal of {}", did);
        //self.record_ctrler.start_reveal(&user, &password);
    }

    pub fn end_reveal(&mut self, did: DID) {
        debug!("Ending reveal of {}", did);
        self.record_ctrler
            .save_and_clear_reveal(&mut self.pool.get_conn().unwrap());
    }

    //-----------------------------------------------------------------------------
    // Get all records of a particular disguise
    // returns all the diff records and all the speaksfor record blobs
    // Additional function to get and mark records revealed (if records are retrieved for the
    // purpose of reversal)
    //-----------------------------------------------------------------------------
    pub fn get_locators(&mut self, pk: &DecryptCap) -> Vec<records::Locator> {
        self.record_ctrler.get_locators(&pk)
    }

    pub fn get_records(
        &mut self,
        decrypt_cap: &records::DecryptCap,
    ) -> (Vec<Vec<u8>>, Vec<Vec<u8>>, HashMap<UID, PrivkeyRecord>) {
        let (diff_records, sf_records, pk_records) = self.get_recs_and_privkeys(&decrypt_cap);
        (
            diff_records
                .iter()
                .map(|wrapper| wrapper.record_data.clone())
                .collect(),
            sf_records
                .iter()
                .map(|wrapper| wrapper.record_data.clone())
                .collect(),
            pk_records,
        )
    }

    pub fn get_recs_and_privkeys(
        &mut self,
        decrypt_cap: &records::DecryptCap,
    ) -> (
        Vec<DiffRecordWrapper>,
        Vec<SpeaksForRecordWrapper>,
        HashMap<UID, PrivkeyRecord>,
    ) {
        let mut diff_records = vec![];
        let mut sf_records = vec![];
        let mut pk_records = HashMap::new();
        if decrypt_cap.is_empty() {
            return (diff_records, sf_records, pk_records);
        }
        let locators = self.record_ctrler.get_locators(&decrypt_cap);
        for lc in &locators {
            let (dts, ots, pks) = self.record_ctrler.get_user_records(&decrypt_cap, &lc);
            diff_records.extend(dts.iter().cloned());
            sf_records.extend(ots.iter().cloned());
            for (new_uid, pk) in &pks {
                pk_records.insert(new_uid.clone(), pk.clone());
            }
        }
        (diff_records, sf_records, pk_records)
    }

    pub fn cleanup_records_of_disguise(
        &mut self,
        did: DID,
        decrypt_cap: &records::DecryptCap,
        seen_pps: &mut HashSet<UID>,
    ) {
        let mut db = self.pool.get_conn().unwrap();
        let locators = self.record_ctrler.get_locators(decrypt_cap);
        for lc in locators {
            self.record_ctrler
                .cleanup_user_records(did, decrypt_cap, &lc, seen_pps, &mut db);
        }
    }

    //-----------------------------------------------------------------------------
    // Save arbitrary diffs performed by the disguise for the purpose of later
    // restoring.
    //-----------------------------------------------------------------------------
    pub fn save_diff_record(&mut self, uid: UID, did: DID, data: Vec<u8>) {
        let tok = records::new_generic_diff_record_wrapper(&uid, did, data);
        self.record_ctrler.insert_user_diff_record_wrapper(&tok);
    }

    //-----------------------------------------------------------------------------
    // Save information about decorrelation/speaksfor
    //-----------------------------------------------------------------------------
    pub fn forget_principal(&mut self, uid: &UID, did: DID) {
        self.record_ctrler.mark_principal_to_forget(uid, did);
    }

    pub fn register_and_save_pseudoprincipal_record(
        &mut self,
        did: DID,
        old_uid: &UID,
        new_uid: &UID,
        record_bytes: Vec<u8>,
    ) {
        self.register_pseudoprincipal(did, old_uid, new_uid);
        self.insert_speaksfor_record(did, old_uid, new_uid, record_bytes);
    }

    pub fn register_pseudoprincipal(&mut self, did: DID, old_uid: &UID, new_uid: &UID) {
        self.record_ctrler
            .register_anon_principal(&old_uid, &new_uid, did);
    }

    pub fn insert_speaksfor_record(
        &mut self,
        did: DID,
        old_uid: &UID,
        new_uid: &UID,
        record_bytes: Vec<u8>,
    ) {
        self.record_ctrler
            .insert_speaksfor_record(&old_uid, &new_uid, did, record_bytes);
    }

    pub fn get_pseudoprincipals(
        &self,
        user: &UID,
        password: Option<String>,
        user_share: Option<(records::Share, records::Loc)>,
    ) -> HashSet<UID> {
        let mut decrypt_cap = vec![];
        let priv_key = self.get_priv_key(user, password, user_share);
        if let Some(key) = priv_key {
            decrypt_cap = key;
        }
        let locators = self.record_ctrler.get_locators(&decrypt_cap);
        let uids = self
            .record_ctrler
            .get_user_pseudoprincipals(&decrypt_cap, &locators);
        uids
    }

    pub fn principal_is_anon(&self, uid: &UID) -> bool {
        self.record_ctrler.principal_is_anon(uid)
    }
}
