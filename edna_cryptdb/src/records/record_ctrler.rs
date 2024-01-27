use crate::crypto::*;
use crate::helpers::*;
use crate::records::*;
use crate::TableRow;
use crate::{DID, UID};
use crypto_box::{PublicKey, SecretKey};
use log::{error, info, warn};
use mysql::prelude::*;
use num_bigint::BigInt;
use num_primes::Generator;
use pbkdf2::{
    password_hash::{PasswordHash, PasswordHasher, SaltString},
    Pbkdf2,
};
use rand::{rngs::OsRng, Rng, RngCore};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::hash::{Hash, Hasher};
use std::iter::repeat;
use std::mem::size_of_val;
use std::time;

pub type Loc = u64; // locator
pub type Index = u64; // index into shares map, enc locators map
pub type PrivKey = Vec<u8>; // decryption capability (e.g., private key)
pub type Share = [BigInt; 2];
pub type ShareValue = BigInt;
pub type UserData = (Share, Index);

#[derive(Clone)]
pub struct PrincipalData {
    pub pubkey: Option<PublicKey>,
    pub is_anon: bool,
    pub enc_locators_index: Index,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Bag {
    pub diffrecs: Vec<DiffRecordWrapper>,
    pub chainrecs: HashMap<UID, SFChainRecord>,
    pub random_padding: Vec<u8>,
}

impl Bag {
    pub fn new() -> Bag {
        let mut rng = rand::thread_rng();
        let size = rng.gen_range(512..4096);
        let mut padding: Vec<u8> = repeat(0u8).take(size).collect();
        rng.fill_bytes(&mut padding[..]);

        let mut bag: Bag = Default::default();
        bag.random_padding = padding;
        bag
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShareStore {
    pub share: Share,
    pub share_value: ShareValue,
    pub password_salt: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default, Hash)]
pub struct Locator {
    pub loc: Loc,
    // UID here is necessary because composed disguises return UIDs to the
    // client for other principals. UID marks the (pseudo or natural) principal
    // whose data is stored at the locator; this principal's public key should
    // be used to encrypt
    pub uid: UID,
    pub did: DID,
    // we need this to reveal deleted principal's data, as they no longer have
    // principaldata entries
    pub pubkey: Vec<u8>,
}

#[derive(Clone)]
pub struct RecordCtrler {
    // principal records are stored indexed by some large random num
    principal_data: HashMap<UID, PrincipalData>,

    // (p,d) capability -> set of record ciphertext for principal+disguise
    enc_map: HashMap<Loc, EncData>,

    shares_map: HashMap<Index, ShareStore>,

    // Note: this acts also as the "deleted principals" table referenced
    // by the paper, indexed via a hash of the user's password
    enc_locators_map: HashMap<Index, HashSet<EncData>>,

    // used for randomness and encryption
    rng: OsRng,
    prime: BigInt,

    // used to temporarily store keys used during disguises
    tmp_invoking_uid: Option<UID>,
    tmp_did: Option<DID>,
    tmp_remove_principals: HashSet<UID>,
    tmp_principals_to_insert: Vec<(UID, PrincipalData)>,
    tmp_bags: HashMap<UID, Bag>,

    // actually encrypt or not
    dryrun: bool,
}

impl RecordCtrler {
    pub fn new(
        db: &mut mysql::PooledConn,
        in_memory: bool,
        reset: bool,
        dryrun: bool,
    ) -> RecordCtrler {
        // generate a 256-bit prime randomly and read into a BigInt
        let prime_arr: [u8; 64];
        loop {
            let prime_gen = Generator::new_prime(512).to_bytes_le();
            let try_pa: Result<[u8; 64], _> = prime_gen.try_into();
            match try_pa {
                Ok(p) => {
                    prime_arr = p;
                    break;
                }
                Err(_) => continue,
            }
        }
        let prime = BigInt::from_bytes_le(num_bigint::Sign::Plus, &prime_arr);

        let mut tctrler = RecordCtrler {
            principal_data: HashMap::new(),
            enc_map: HashMap::new(),
            enc_locators_map: HashMap::new(),
            shares_map: HashMap::new(),
            rng: OsRng,
            prime: prime,
            tmp_remove_principals: HashSet::new(),
            tmp_principals_to_insert: vec![],
            tmp_bags: HashMap::new(),
            tmp_did: None,
            tmp_invoking_uid: None,
            dryrun: dryrun,
        };
        RecordPersister::init(db, in_memory, reset);
        let principal_rows = RecordPersister::get_principal_rows(db);
        for (uid, is_anon, pubkey, locs) in principal_rows {
            tctrler.register_saved_principal::<mysql::PooledConn>(
                &uid, is_anon, &pubkey, locs, false, db,
            );
        }

        // TODO get from persistent enc locs maps

        let share_rows = RecordPersister::get_share_store_rows(db);
        for share_info in share_rows {
            // loc, secretkey_share, share_func_value, password_salt
            let share = ShareStore {
                share: share_info.1.unwrap(),
                share_value: share_info.2.unwrap(),
                password_salt: share_info.3.unwrap().to_string(),
            };
            tctrler.shares_map.insert(share_info.0, share);
        }

        let encbag_rows = RecordPersister::get_encbag_rows(db);
        for (loc, edata) in encbag_rows {
            tctrler.enc_map.insert(loc, edata);
        }
        tctrler
    }

    // gets number of bytes in principaldata as well as encrypted store
    pub fn get_space_overhead(&self, db: &mut mysql::PooledConn, dbname: &str) -> (usize, usize) {
        let mut bytes = 0;
        for (key, pd) in self.principal_data.iter() {
            bytes += size_of_val(&*key);
            bytes += size_of_val(&pd.pubkey);
            bytes += size_of_val(&pd.enc_locators_index);
            bytes += size_of_val(&*pd);
        }
        bytes += size_of_val(&self.principal_data);
        error!("pdata {}", bytes);

        let mut edbytes = 0;
        for (l, ed) in self.enc_map.iter() {
            edbytes += size_of_val(&l);
            edbytes += size_of_val(&*ed);
            edbytes += size_of_val(&*ed.nonce);
            edbytes += size_of_val(&*ed.encdata);
            edbytes += size_of_val(&*ed.pubkey);
        }
        edbytes += size_of_val(&self.enc_map);
        error!("emap {}", edbytes);

        let mut elbytes = 0;
        for (l, els) in self.enc_locators_map.iter() {
            elbytes += size_of_val(&l);
            elbytes += size_of_val(&*els);
            for el in els.iter() {
                elbytes += size_of_val(&*el);
            }
        }
        elbytes += size_of_val(&self.enc_locators_map);
        error!("enclocs_map {}", elbytes);

        let mut ssbytes = 0;
        for (l, ss) in self.shares_map.iter() {
            ssbytes += size_of_val(&l);
            ssbytes += size_of_val(&*ss);
        }
        ssbytes += size_of_val(&self.shares_map);
        error!("ssmap {}", ssbytes);

        let persisted_bytes = RecordPersister::get_space_overhead(db, dbname);
        error!("persisted {}", persisted_bytes);

        (bytes + ssbytes + edbytes + elbytes, persisted_bytes)
    }

    /*
     * Disguising Functions
     */
    // sets tmp_did to a new random DID
    pub fn start_disguise(&mut self, acting_uid: Option<UID>) -> DID {
        let did = self.rng.next_u64();
        self.tmp_did = Some(did);
        self.tmp_invoking_uid = acting_uid;
        info!("Setting tmp did to {}", did);
        did
    }

    //
    // Invariant:
    // * Edna retains encrypted locators at opaque indexes corresponding to encrypted indexes encrypted with the user's public key
    // OLD: Edna used to only ever retain encrypted locators for pseudoprincipals
    // OLD: If NP is acting on behalf of a PP, Edna returns the PP
    //   locators for the NP and does not save them in the PP metadata
    //
    pub fn save_and_clear_disguise<Q: Queryable>(&mut self, db: &mut Q) {
        // this creates locators, so we have to do it first before deleting or
        // clearing principaldata
        let start = time::Instant::now();
        let bag_uids = self.tmp_bags.keys().cloned().collect::<Vec<_>>();
        for uid in &bag_uids {
            let start_update = time::Instant::now();
            let p = self
                .principal_data
                .get(uid)
                .expect(&format!("no user with uid {} when saving?", uid))
                .clone();

            // pubkey can be None if we're doing a dryrun
            let pubkey = match p.pubkey.as_ref() {
                None => vec![],
                Some(p) => p.as_bytes().to_vec(),
            };
            let lc = Locator {
                loc: self.rng.next_u64(),
                uid: uid.clone(),
                did: self.tmp_did.expect("No disguise?"),
                pubkey: pubkey,
            };
            let bag = self.tmp_bags.get(&uid.to_string()).unwrap().clone();
            self.update_bag_at_loc(&lc, &bag, db);
            warn!(
                "Edna: Inserted bag with {} dr, {} sfchains for uid {}: {}mus",
                bag.diffrecs.len(),
                bag.chainrecs.len(),
                uid,
                start_update.elapsed().as_micros(),
            );

            // Don't do the interactive mess since that would mean that we have
            // to iterate through private keys and find the right one...  Note:
            // This comment was from when we believed we wouldn't have to store
            // encrypted locators, since we could return them to the client (if
            // the client was interactive); however, it's just easier to store
            // them all encrypted at opaque indices.
            let start_enc = time::Instant::now();
            let enc_lc =
                encrypt_with_pubkey(&p.pubkey.as_ref(), &serialize_to_bytes(&lc), self.dryrun);
            info!(
                "Saving encrypted locator {:?} into {} for uid {}",
                lc, p.enc_locators_index, uid
            );
            warn!(
                "Save and clear disguise encrypt locator: {}mus",
                start_enc.elapsed().as_micros()
            );

            let start_ins = time::Instant::now();
            // insert into map...
            let hs = match self.enc_locators_map.get_mut(&p.enc_locators_index) {
                Some(enclcs) => {
                    enclcs.insert(enc_lc);
                    enclcs.clone()
                }
                None => {
                    let mut hs = HashSet::new();
                    hs.insert(enc_lc);
                    self.enc_locators_map
                        .insert(p.enc_locators_index, hs.clone());
                    hs
                }
            };
            // persist
            RecordPersister::update_enc_locs_at_index(p.enc_locators_index, &hs, db);
            warn!(
                "Save and clear disguise insert: {}mus",
                start_ins.elapsed().as_micros()
            );
        }
        self.commit_removed(db);
        self.clear_tmp();
        warn!(
            "Save and clear disguise: {}mus",
            start.elapsed().as_micros()
        );
    }

    pub fn save_and_clear_reveal<Q: Queryable>(&mut self, db: &mut Q) {
        self.commit_removed(db);
        self.clear_tmp();
    }

    pub fn commit_removed<Q: Queryable>(&mut self, db: &mut Q) {
        // actually remove the principals supposed to be removed
        let start = time::Instant::now();
        for uid in self.tmp_remove_principals.clone().iter() {
            info!(
                "Actually removing principal metadata and principal {}\n",
                uid
            );
            self.principal_data.remove(uid);
        }
        RecordPersister::remove_principals(&self.tmp_remove_principals, db);
        info!(
            "Edna: remove principal total: {}",
            start.elapsed().as_micros()
        );
        self.persist_inserted_principals::<Q>(db);
    }

    pub fn clear_tmp(&mut self) {
        self.tmp_remove_principals.clear();
        self.tmp_bags.clear();
        self.tmp_did = None;
        self.tmp_invoking_uid = None;
    }

    /*
     * REGISTRATION
     */
    pub fn register_saved_principal<Q: Queryable>(
        &mut self,
        uid: &UID,
        is_anon: bool,
        pubkey: &Option<PublicKey>,
        enc_locators_index: u64,
        persist: bool,
        db: &mut Q,
    ) {
        info!("Re-registering saved principal {}", uid);
        let pdata = PrincipalData {
            pubkey: pubkey.clone(),
            is_anon: is_anon,
            enc_locators_index: enc_locators_index,
        };
        self.mark_principal_to_insert(uid, &pdata);
        if persist {
            self.persist_inserted_principals::<Q>(db);
        }
        self.principal_data.insert(uid.clone(), pdata);
    }

    fn register_principal<Q: Queryable>(
        &mut self,
        uid: &UID,
        is_anon: bool,
        db: &mut Q,
        persist: bool,
    ) -> (PrivKey, Vec<u8> /*publickey*/) {
        if self.dryrun {
            // pk = uid as bytes
            let pk = get_pk_bytes(&uid.as_bytes().to_vec());
            let hash = {
                let mut hasher = DefaultHasher::new();
                base64::encode(pk).hash(&mut hasher);
                hasher.finish()
            };
            info!("Hash of {} is {}", base64::encode(pk), hash);
            let pdata = PrincipalData {
                pubkey: None,
                is_anon: is_anon,
                enc_locators_index: hash,
            };
            self.mark_principal_to_insert(uid, &pdata);
            if persist {
                self.persist_inserted_principals::<Q>(db);
            }
            self.principal_data.insert(uid.clone(), pdata);

            return (pk.to_vec(), vec![]);
        }
        let secretkey = SecretKey::generate(&mut self.rng);
        let pubkey = PublicKey::from(&secretkey);
        info!(
            "Registering principal {} with secretkey {} and pubkey {}",
            uid,
            base64::encode(secretkey.as_bytes()),
            base64::encode(pubkey.as_bytes()),
        );
        let pk_hash = {
            let mut hasher = DefaultHasher::new();
            (base64::encode(secretkey.as_bytes())).hash(&mut hasher);
            hasher.finish()
        };
        info!(
            "got pkhash for user {} pk {}: {}",
            uid,
            base64::encode(secretkey.as_bytes()),
            pk_hash
        );
        let pdata = PrincipalData {
            pubkey: Some(pubkey.clone()),
            is_anon: is_anon,
            // create index for enc_locators for this principal
            enc_locators_index: pk_hash,
        };
        self.mark_principal_to_insert(uid, &pdata);
        if persist {
            self.persist_inserted_principals::<Q>(db);
        }
        self.principal_data.insert(uid.clone(), pdata);

        // XXX PROXY CRYPTDB super hacky
        let pubkey_vec = pubkey.as_bytes().to_vec();
        warn!("Proxy: Register principal pubkey {} privkey {}", base64::encode(&pubkey_vec), base64::encode(secretkey.as_bytes().to_vec()));
        if uid.contains("malte") {
            query_drop(&format!("REGISTER {} ADMIN", base64::encode(&pubkey_vec)), db).unwrap();
        } else {
            query_drop(&format!("REGISTER {}", base64::encode(&pubkey_vec)), db).unwrap();
        }

        (secretkey.as_bytes().to_vec(), pubkey_vec)
    }

    pub fn register_principal_private_key<Q: Queryable>(
        &mut self,
        uid: &UID,
        is_anon: bool,
        db: &mut Q,
        persist: bool,
    ) -> PrivKey {
        let (secretkey, _pubkey) = self.register_principal(uid, is_anon, db, persist);
        secretkey
    }

    // registers the principal with edna, giving them a private/public keypair
    // breaks the private key into shares and returns the user's portion
    pub fn register_principal_secret_sharing<Q: Queryable>(
        &mut self,
        uid: &UID,
        db: &mut Q,
        password: String,
    ) -> UserData {
        let (secretkey, _) = self.register_principal(uid, false, db, true);
        if self.dryrun {
            return (
                [
                    BigInt::new(num_bigint::Sign::Plus, vec![0]),
                    BigInt::new(num_bigint::Sign::Plus, vec![0]),
                ],
                0,
            );
        }

        let start = time::Instant::now();
        let salt = SaltString::generate(&mut OsRng);
        let pass_info: String = Pbkdf2
            .hash_password(password.as_bytes(), &salt)
            .unwrap()
            .to_string();
        let _parsed_hash = PasswordHash::new(&pass_info).unwrap();
        let hash_pass_bigint = BigInt::from_bytes_le(num_bigint::Sign::Plus, pass_info.as_bytes());
        let secretkey_int = BigInt::from_bytes_le(num_bigint::Sign::Plus, &secretkey);
        info!("pbkdf2: {}mus", start.elapsed().as_micros());
        info!("hash_pass_bigint: {}", hash_pass_bigint);
        info!("private key int: {}", secretkey_int);

        let start = time::Instant::now();
        let sss = ShamirSecretSharing {
            threshold: 1,
            share_count: 3,
            prime: self.prime.clone(),
        };
        // returned format: vec < [h(p), f(h(p))], [rand1, f(rand1)], [rand2, f(rand2)] >
        let all_shares = sss.share(&secretkey_int, &hash_pass_bigint);
        info!("SSS share: {}mus", start.elapsed().as_micros());

        let mut uid_owned = uid.clone().to_owned();
        uid_owned.push_str(&password);
        let uid_pw_hash = {
            let mut hasher = DefaultHasher::new();
            uid_owned.hash(&mut hasher);
            hasher.finish()
        };
        info!(
            "got uid, password {} {}: hash {}",
            uid, password, uid_pw_hash
        );

        // save share for NP
        let perm_share = ShareStore {
            share: all_shares[1].clone(),
            share_value: all_shares[0][1].clone(),
            password_salt: salt.clone().as_str().to_string(),
        };

        // persist share info at share_loc
        self.shares_map.insert(uid_pw_hash, perm_share.clone());
        info!("edna-stored user share: {:?}", perm_share.share);
        info!(
            "user-stored user share: {:?}",
            (&all_shares[2], uid_pw_hash)
        );
        RecordPersister::persist_share(&vec![(uid_pw_hash, perm_share.clone())], db);

        (all_shares[2].clone(), uid_pw_hash)
    }

    pub fn register_pseudoprincipal<Q: Queryable>(
        &mut self,
        old_uid: &UID,
        new_uid: &UID,
        pp: TableRow,
        did: DID,
        db: &mut Q
    ) -> PrivKey {
        let start = time::Instant::now();
        let anon_uidstr = new_uid.trim_matches('\'');
        // save the anon principal as a new principal with a public key
        let (secretkey, pubkey) = self.register_principal(
            &anon_uidstr.to_string(),
            true,
            db,
            false,
        );
        info!(
            "Registering anon principal {} with secretkey {} and pubkey {}",
            anon_uidstr.to_string(),
            base64::encode(&secretkey),
            base64::encode(&pubkey),
        );

        // we need to record the pp in the speaksfor chain
        let chain_record = new_sfchain_record(
            old_uid.to_string(),
            anon_uidstr.to_string(),
            secretkey.clone(),
        );
        self.insert_chain_record(old_uid, did, &chain_record);

        // we also need to record a diff record for the pp
        // so we can delete the pp when we reverse this disguise
        let pp_record = new_generic_diff_record_wrapper(
            old_uid,
            did,
            edna_diff_record_to_bytes(&new_pseudoprincipal_record(
                pp,
                old_uid.clone(),
                new_uid.clone(),
            )),
        );
        self.insert_diff_record_wrapper(&pp_record);
        info!(
            "Edna: record anon principal {} for original {}",
            new_uid, old_uid
        );
        info!(
            "Edna: register anon principal: {}",
            start.elapsed().as_micros()
        );
        secretkey
    }

    pub fn principal_is_anon(&self, uid: &UID) -> bool {
        match self.principal_data.get(uid) {
            None => false,
            Some(p) => p.is_anon,
        }
    }

    fn mark_principal_to_insert(&mut self, uid: &UID, pdata: &PrincipalData) {
        self.tmp_principals_to_insert
            .push((uid.clone(), pdata.clone()));
    }

    fn persist_inserted_principals<Q: Queryable>(&mut self, db: &mut Q) {
        if self.tmp_principals_to_insert.is_empty() {
            return;
        }
        RecordPersister::persist_inserted_principals(&self.tmp_principals_to_insert, db);
        self.tmp_principals_to_insert.clear();
    }

    // forget metadata as soon as all locators are gone.
    // don't this even for pseudoprincipals; we'll either fail to recorrelate to pseudoprincipal can be restored
    // from recursive disguising!
    pub fn mark_principal_to_forget(&mut self, uid: &UID, did: DID) {
        let start = time::Instant::now();
        let p = self.principal_data.get_mut(uid).unwrap();
        let precord = new_generic_diff_record_wrapper(
            uid,
            did,
            edna_diff_record_to_bytes(&new_remove_principal_record(&p)),
        );
        self.insert_diff_record_wrapper(&precord);
        self.tmp_remove_principals.insert(uid.to_string());
        info!(
            "Edna: mark principal {} to remove : {}",
            uid,
            start.elapsed().as_micros()
        );
    }

    /*
     * PRINCIPAL TOKEN INSERT
     */
    // encrypts bag contents and inserts it
    fn update_bag_at_loc<Q: Queryable>(&mut self, lc: &Locator, bag: &Bag, db: &mut Q) {
        let popt = self.principal_data.get(&lc.uid);
        let pubkey = if popt.is_none() {
            warn!("UpdateBagAtLoc: no user with uid {} found", lc.uid,);
            Some(PublicKey::from(get_pk_bytes(&lc.pubkey)))
        } else {
            popt.unwrap().pubkey.clone()
        };
        // we just need the pubkey here! put in the loc?
        let plaintext = bincode::serialize(bag).unwrap();
        let enc_bag = encrypt_with_pubkey(&pubkey.as_ref(), &plaintext, self.dryrun);

        // insert the encrypted pppk into locating capability
        RecordPersister::update_enc_bag_at_loc(lc.loc, &enc_bag, db);
        self.enc_map.insert(lc.loc, enc_bag);
        info!("Edna: Saved bag for {}", lc.uid);
    }

    fn insert_chain_record(&mut self, old_uid: &UID, did: DID, pppk: &SFChainRecord) {
        let start = time::Instant::now();
        let p = self.principal_data.get_mut(old_uid);
        if p.is_none() {
            info!("InsertSFChainRecord: no user with uid {} found?", old_uid);
            return;
        }
        match self.tmp_bags.get_mut(old_uid) {
            Some(bag) => {
                // important: insert the mapping from new_uid to pppk
                bag.chainrecs.insert(pppk.new_uid.clone(), pppk.clone());
                info!(
                    "Got bag for {} and {} with and privkeys {}",
                    old_uid,
                    did,
                    bag.chainrecs.len()
                );
            }
            None => {
                let mut new_bag = Bag::new();
                new_bag.chainrecs.insert(pppk.new_uid.clone(), pppk.clone());
                info!(
                    "Got new_bag for {} and {} with and privkeys {}",
                    old_uid,
                    did,
                    new_bag.chainrecs.len()
                );
                self.tmp_bags.insert(old_uid.clone(), new_bag);
            }
        }
        info!(
            "Inserted privkey record for uid {} total: {}",
            old_uid,
            start.elapsed().as_micros()
        );
    }

    pub fn insert_diff_record_wrapper(&mut self, record: &DiffRecordWrapper) {
        let start = time::Instant::now();
        match self.tmp_bags.get_mut(&record.uid) {
            Some(bag) => {
                info!("Got bag for {} diff records", record.uid);
                bag.diffrecs.push(record.clone());
            }
            None => {
                let mut new_bag = Bag::new();
                info!("Got new bag for {} diff records", record.uid);
                new_bag.diffrecs.push(record.clone());
                self.tmp_bags.insert(record.uid.clone(), new_bag);
            }
        }
        info!(
            "Inserted diff record uid {} total: {}",
            record.uid,
            start.elapsed().as_micros()
        );
    }

    /*
     * GET TOKEN FUNCTIONS
     */

    // gets/decrypts bag at location, keeps contents (sf and diff), follows references in chainrecs:
    // if user is pseudoprincipal, get/decrypt that bag and add its contents; return all gathered contents
    // NOTE: we could have flag to remove locators so we don't traverse twice (e.g., have to call cleanup)
    // this would remove locators regardless of success in revealing
    pub fn get_user_records(
        &mut self,
        privkey: &PrivKey,
        lc: &Locator,
    ) -> (Vec<DiffRecordWrapper>, HashMap<UID, SFChainRecord>) {
        let mut diff_records = vec![];
        let mut pk_records = HashMap::new();
        if privkey.is_empty() {
            return (diff_records, pk_records);
        }
        if let Some(encbag) = self.enc_map.get(&lc.loc) {
            info!("Getting records of user {} with lc {}", lc.uid, lc.loc);
            let start = time::Instant::now();
            // decrypt record with privkey provided by client
            let (succeeded, plaintext) = decrypt_encdata(encbag, privkey, self.dryrun);
            if !succeeded {
                info!("Failed to decrypt bag {} with {}", lc.uid, privkey.len());
                return (diff_records, pk_records);
            }
            let mut bag: Bag = bincode::deserialize(&plaintext).unwrap();

            // we found a matching record for the disguise
            diff_records.append(&mut bag.diffrecs);
            info!(
                "Edna: Decrypted diff, pk records added {}, {}: {}mus",
                bag.diffrecs.len(),
                bag.chainrecs.len(),
                start.elapsed().as_micros(),
            );

            // get ALL new_uids regardless of disguise that record came from
            let mut new_uids = vec![];
            for (new_uid, pk) in &bag.chainrecs {
                new_uids.push((new_uid.clone(), pk.priv_key.clone()));
                pk_records.insert(new_uid.clone(), pk.clone());
            }
            // get all records of pseudoprincipal
            for (_new_uid, pk) in new_uids {
                let pplcs = self.get_locators(&pk);
                for lc in pplcs {
                    let (mut pp_diff_records, pp_pk_records) = self.get_user_records(&pk, &lc);
                    diff_records.append(&mut pp_diff_records);
                    for (new_uid, pk) in &pp_pk_records {
                        pk_records.insert(new_uid.clone(), pk.clone());
                    }
                }
            }
        }
        // return records matching disguise and the removed locs from this iteration
        (diff_records, pk_records)
    }

    // gets private key given uid and password or user share
    pub fn get_priv_key(
        &self,
        uid: &UID,
        password: Option<String>,
        user_data: Option<UserData>,
    ) -> Option<PrivKey> {
        info!("get_priv_key: user {} user share: {:?}", uid, user_data);
        if self.dryrun {
            if user_data != None {
                panic!("Oops can't use this in dryrun");
            } else {
                info!(
                    "Dryrun got pk bytes {}",
                    base64::encode(get_pk_bytes(&uid.as_bytes().to_vec()))
                );
                return Some(get_pk_bytes(&uid.as_bytes().to_vec()).to_vec());
            }
        }

        let mut shares: Vec<[BigInt; 2]> = vec![];
        if user_data != None {
            shares.push(user_data.clone().unwrap().0);

            if let Some(share) = self.shares_map.get(&user_data.clone().unwrap().1) {
                info!("getting users share");
                shares.push(share.share.clone());
            }
        } else {
            info!("using uid and pw");

            if password == None {
                info!("no password?");
                return None;
            }
            let password_str = password.unwrap();
            let mut uid_owned = uid.clone().to_owned();

            uid_owned.push_str(&password_str);
            let uid_pw_hash = {
                let mut hasher = DefaultHasher::new();
                uid_owned.hash(&mut hasher);
                hasher.finish()
            };
            info!(
                "got uid, password {} {}: hash {}",
                uid, password_str, uid_pw_hash
            );

            if let Some(share) = self.shares_map.get(&uid_pw_hash) {
                info!("getting users share");
                shares.push(share.share.clone());

                let start = time::Instant::now();
                let pass_info: String = Pbkdf2
                    .hash_password(password_str.as_bytes(), &share.password_salt)
                    .unwrap()
                    .to_string();
                let hash_pass_bigint =
                    BigInt::from_bytes_le(num_bigint::Sign::Plus, pass_info.as_bytes());
                info!("pbkdf2: {}mus", start.elapsed().as_micros());

                info!("hash_pass_bigint: {}", hash_pass_bigint);
                info!("user share: {:?}", share.share);
                let other_share = [hash_pass_bigint, share.share_value.clone()];
                shares.push(other_share);
            }
        }

        if shares.len() != 2 {
            info!("Unable to reconstruct due to too few shares");
            return None;
        }

        let start = time::Instant::now();
        let sss = ShamirSecretSharing {
            threshold: 1,
            share_count: 3,
            prime: self.prime.clone(),
        };
        let priv_key = sss.reconstruct(&shares);
        let pkbytes = get_pk_bytes(&priv_key.to_bytes_le().1);
        info!(
            "SSS reconstruct {}: {}mus",
            uid,
            start.elapsed().as_micros()
        );
        return Some(pkbytes.to_vec());
    }

    // gets locators given privkey
    pub fn get_locators(&self, privkey: &PrivKey) -> Vec<Locator> {
        let pk_hash = {
            let mut hasher = DefaultHasher::new();
            (base64::encode(privkey)).hash(&mut hasher);
            hasher.finish()
        };
        let mut lcs = vec![];
        if let Some(encls) = self.enc_locators_map.get(&pk_hash) {
            for enclc in encls {
                let (_, lcbytes) = decrypt_encdata(&enclc, privkey, self.dryrun);
                let lc: Locator = bincode::deserialize(&lcbytes).unwrap();
                lcs.push(lc);
            }
        }
        info!(
            "Got {} locators for pk {} hash {}",
            lcs.len(),
            base64::encode(privkey),
            pk_hash
        );
        return lcs;
    }

    // gets/decrypts bag at loc and gets diff/sfchains, if nothing or correct
    // disguise puts in an empty vec, follows uids in pk, removes matching
    // records of pseudoprincipals for each pseudoprincipal for which we hold a
    // private key; returns whether there exist diffs, owns, or sfchains at the given
    // location (respectively)
    pub fn cleanup_user_records(
        &mut self,
        did: DID,
        privkey: &PrivKey,
        lc: &Locator,
        db: &mut mysql::PooledConn,
    ) -> (bool, bool) {
        // delete locators + encrypted records
        // remove pseudoprincipal metadata if caps are empty
        let mut no_diffs_at_loc = true;
        let mut no_sfchains_at_loc = true;
        let mut changed = false;
        if privkey.is_empty() {
            return (false, false);
        }
        if let Some(encbag) = self.enc_map.get(&lc.loc) {
            let start = time::Instant::now();
            no_diffs_at_loc = false;

            let (success, plaintext) = decrypt_encdata(encbag, privkey, self.dryrun);
            if !success {
                info!("Could not decrypt encdata at {:?} with decryptcap", lc);
                return (false, false);
            }
            let mut bag: Bag = bincode::deserialize(&plaintext).unwrap();
            let records = bag.diffrecs.clone();
            info!(
                "EdnaCleanup: Decrypted diff records added {} total: {}",
                records.len(),
                start.elapsed().as_micros(),
            );
            // remove if we found a matching record for the disguise
            if records.is_empty() || records[0].did == did {
                no_diffs_at_loc = true;
                bag.diffrecs = vec![];
                changed |= !records.is_empty();
            }
            let records = bag.chainrecs.clone();
            info!(
                "EdnaCleanup: Decrypted pk records added {} total: {}",
                records.len(),
                start.elapsed().as_micros(),
            );
            // remove matching records of pseudoprincipals
            // for each pseudoprincipal for which we hold a private key
            let mut keep_sfchains = HashMap::new();
            for (new_uid, pkt) in &records {
                let pk_hash = {
                    let mut hasher = DefaultHasher::new();
                    (base64::encode(&pkt.priv_key)).hash(&mut hasher);
                    hasher.finish()
                };
                if let Some(encls) = self.enc_locators_map.get(&pk_hash) {
                    info!("Cleanup: Getting records of pseudoprincipal {}", new_uid,);
                    let mut empty = false;
                    for enclc in encls.clone() {
                        let (_, lcbytes) = decrypt_encdata(&enclc, &pkt.priv_key, self.dryrun);
                        let pplc: Locator = bincode::deserialize(&lcbytes).unwrap();
                        // for each locator that the pp has
                        // clean up the records at the locators
                        let (no_diffs_at_loc, no_sfchains_at_loc) =
                            self.cleanup_user_records(did, &pkt.priv_key, &pplc, db);

                        // remove loc from pp if nothing's left at that loc
                        if no_diffs_at_loc && no_sfchains_at_loc {
                            // remove the locator and also the bag that it points to!
                            info!(
                                "Removing locator {:?} from user {}'s encrypted locators",
                                pplc, new_uid
                            );
                            let encls_mut = self.enc_locators_map.get_mut(&pk_hash).unwrap();
                            encls_mut.remove(&enclc);
                            empty = encls_mut.is_empty();
                        }
                    }
                    let principal_removed = self.principal_data.get(new_uid).is_none();
                    // delete the principal if there are no more locators with non-empty bags,
                    // and the principal has actually been removed before
                    if principal_removed && empty {
                        info!(
                            "Removing entry for principal {} from enc_locators_map",
                            new_uid
                        );
                        self.enc_locators_map.remove(&pk_hash);
                        changed = true;
                        RecordPersister::remove_enc_locs_at_index(pk_hash, db);
                    } else {
                        // otherwise we need to keep the private key for this principal/keep this
                        // principal around.
                        keep_sfchains.insert(new_uid.clone(), pkt.clone());
                    }
                }
            }
            // if this is empty, yay! no more private keys at this principal
            no_sfchains_at_loc = keep_sfchains.is_empty();

            // actually remove locs
            if no_diffs_at_loc && no_sfchains_at_loc {
                let _enc_bag = self.enc_map.remove(&lc.loc);
                RecordPersister::remove_enc_bag_at_loc(lc.loc, db);
            } else if changed {
                bag.chainrecs = keep_sfchains;
                self.update_bag_at_loc(&lc, &bag, db);
            } else {
                info!("Bag of {} not changed and not empty", lc.uid);
            }
        }
        // return whether we removed bags
        (no_diffs_at_loc, no_sfchains_at_loc)
    }

    // for every loc, get/decrypt bag, add associated uids in chainrecs to set (done recursively),
    // return that set
    pub fn get_user_pseudoprincipals(
        &self,
        privkey: &PrivKey,
        locators: &Vec<Locator>,
    ) -> HashSet<UID> {
        let mut uids = HashSet::new();
        if privkey.is_empty() {
            return uids;
        }
        for lc in locators {
            if let Some(encbag) = self.enc_map.get(&lc.loc) {
                info!("Getting pps of user");
                let start = time::Instant::now();
                // decrypt record with privkey provided by client
                let (_, plaintext) = decrypt_encdata(encbag, privkey, self.dryrun);
                let bag: Bag = bincode::deserialize(&plaintext).unwrap();
                let records = bag.chainrecs;
                let mut new_uids = vec![];
                for pk in &records {
                    if uids.is_empty() {
                        // save the original user too
                        uids.insert(pk.1.old_uid.clone());
                    }
                    uids.insert(pk.1.new_uid.clone());
                    new_uids.push((pk.0.clone(), pk.1.priv_key.clone()));
                }
                // get all records of pseudoprincipal
                // note that the pp's metadata might be deleted by now
                for (new_uid, pk) in new_uids {
                    info!("Getting records of pseudoprincipal {}", new_uid);
                    let pplcs = self.get_locators(&pk);
                    let ppuids = self.get_user_pseudoprincipals(&pk, &pplcs);
                    uids.extend(ppuids.iter().cloned());
                }
                info!(
                    "Got records of pseudoprincipal: {}",
                    start.elapsed().as_micros()
                );
            }
        }
        uids
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{helpers, EdnaClient, RowVal};
    use mysql::Opts;
    use serde_json::from_str;

    fn start_logger() {
        let _ = env_logger::builder()
            // Include all events in tests
            .filter_level(log::LevelFilter::Warn)
            // Ensure events are captured by `cargo test`
            .is_test(true)
            // Ignore infos initializing the logger if tests race to configure it
            .try_init();
    }

    #[test]
    fn test_insert_user_diff_record_multi() {
        start_logger();
        let iters = 5;
        let dbname = "testRecordCtrlerUserMulti".to_string();
        helpers::init_db(true, "tester", "pass", "127.0.0.1", &dbname, "");
        EdnaClient::new(
            &format!("mysql://tester:pass@127.0.0.1/{}", dbname),
            true,
            false,
        );

        let url = format!("mysql://{}:{}@{}/{}", "tester", "pass", "127.0.0.1", dbname);
        let pool = mysql::Pool::new(Opts::from_url(&url).unwrap()).unwrap();
        let mut db = pool.get_conn().unwrap();
        let mut ctrler = RecordCtrler::new(&mut db, true, true, false);

        let pseudoprincipal_name = "user".to_string();
        let old_fk_value = 5;
        let fk_col = "fk_col".to_string();

        let mut user_shares = vec![];

        for u in 1..iters {
            let user_data = ctrler.register_principal_secret_sharing::<mysql::PooledConn>(
                &u.to_string(),
                &mut db,
                String::from("password"),
            );
            user_shares.push(user_data.clone());

            for _ in 1..iters {
                let d = ctrler.start_disguise(Some(u.to_string()));
                for i in 0..iters {
                    let mut remove_record = new_generic_diff_record_wrapper(
                        &u.to_string(),
                        d,
                        edna_diff_record_to_bytes(&new_delete_record(TableRow {
                            table: pseudoprincipal_name.clone(),
                            row: vec![RowVal::new(
                                fk_col.clone(),
                                (old_fk_value + (i as u64)).to_string(),
                            )],
                        })),
                    );
                    remove_record.uid = u.to_string();
                    ctrler.insert_diff_record_wrapper(&remove_record);
                }
                ctrler.save_and_clear_disguise::<mysql::PooledConn>(&mut db);
            }
        }
        ctrler.clear_tmp();

        for u in 1..iters {
            let priv_key1 = ctrler
                .get_priv_key(&u.to_string(), Some(String::from("password")), None)
                .unwrap();
            let priv_key2 = ctrler
                .get_priv_key(&u.to_string(), None, Some(user_shares[u - 1].clone()))
                .unwrap();
            assert_eq!(priv_key1, priv_key2);
            let locators = ctrler.get_locators(&priv_key1);
            assert_eq!(locators.len(), iters - 1);
            let (diff_records, _) = ctrler.get_user_records(&priv_key1, &locators[0]);
            assert_eq!(diff_records.len(), (iters as usize));
            for i in 0..iters {
                let dt = edna_diff_record_from_bytes(&diff_records[i].record_data);
                let old_val = from_str::<u64>(&dt.old_values[0].row[0].value()).unwrap();

                assert!(old_val < old_fk_value || old_val <= old_fk_value + (iters as u64));
            }
        }
    }

    #[test]
    fn test_insert_user_record_privkey() {
        start_logger();
        let iters = 5;
        let dbname = "testRecordCtrlerUserPK".to_string();
        helpers::init_db(true, "tester", "pass", "127.0.0.1", &dbname, "");
        EdnaClient::new(
            &format!("mysql://tester:pass@127.0.0.1/{}", dbname),
            true,
            false,
        );

        let url = format!("mysql://{}:{}@{}/{}", "tester", "pass", "127.0.0.1", dbname);
        let pool = mysql::Pool::new(Opts::from_url(&url).unwrap()).unwrap();
        let mut db = pool.get_conn().unwrap();
        let mut ctrler = RecordCtrler::new(&mut db, true, true, false);

        let pseudoprincipal_name = "user".to_string();
        let old_fk_value = 5;
        let fk_col = "fk_col".to_string();

        let mut rng = OsRng;
        let mut user_shares = vec![];

        for u in 1..iters {
            let user_data = ctrler.register_principal_secret_sharing::<mysql::PooledConn>(
                &u.to_string(),
                &mut db,
                String::from("password"),
            );
            user_shares.push(user_data.clone());

            for i in 1..iters {
                let d = ctrler.start_disguise(Some(u.to_string()));
                let remove_record = new_generic_diff_record_wrapper(
                    &u.to_string(),
                    d,
                    edna_diff_record_to_bytes(&new_delete_record(TableRow {
                        table: pseudoprincipal_name.clone(),
                        row: vec![RowVal::new(
                            fk_col.clone(),
                            (old_fk_value + (i as u64)).to_string(),
                        )],
                    })),
                );
                ctrler.insert_diff_record_wrapper(&remove_record);

                let anon_uid: u64 = rng.next_u64();
                // create an anonymous user
                // and insert some record for the anon user
                let pp = TableRow {
                    table: pseudoprincipal_name.clone(),
                    row: vec![RowVal::new("uid".to_string(), anon_uid.to_string())],
                };
                ctrler.register_pseudoprincipal(&u.to_string(), &anon_uid.to_string(), pp, d, &mut db);
                ctrler.save_and_clear_disguise::<mysql::PooledConn>(&mut db);
            }

            // check principal data
            ctrler
                .principal_data
                .get(&u.to_string())
                .expect("failed to get user?");

            let priv_key1 = ctrler
                .get_priv_key(&u.to_string(), Some(String::from("password")), None)
                .unwrap();
            let priv_key2 = ctrler
                .get_priv_key(
                    &u.to_string(),
                    None,
                    Some(user_shares[u as usize - 1].clone()),
                )
                .unwrap();

            let locators = ctrler.get_locators(&priv_key1);
            assert_eq!(locators.len(), iters - 1);

            assert_eq!(priv_key1, priv_key2);
            let (diff_records, _) = ctrler.get_user_records(&priv_key1, &locators[0]);

            assert_eq!(diff_records.len(), 2); // TODO lily check
            let dt = edna_diff_record_from_bytes(&diff_records[0].record_data);
            let old_val = from_str::<u64>(&dt.old_values[0].row[0].value()).unwrap();
            assert!(old_val < old_fk_value || old_val <= old_fk_value + (iters as u64));
        }
    }
}

#[test]
fn test_make_pw_hash() {
    let password = b"not password";
    let salt = SaltString::generate(&mut OsRng);
    let pass_info: String = Pbkdf2.hash_password(password, &salt).unwrap().to_string();
    let hash_pass_bigint = BigInt::from_bytes_le(num_bigint::Sign::Plus, pass_info.as_bytes());

    println!("{:?}", pass_info);
    println!("{:?}", hash_pass_bigint);
}

#[test]
fn test_default_hasher() {
    let email_str1 = String::from("u password");

    let uid_pw_hash1 = {
        let mut hasher = DefaultHasher::new();
        email_str1.hash(&mut hasher);
        hasher.finish()
    };

    let email_str2 = String::from("u password");

    let uid_pw_hash2 = {
        let mut hasher = DefaultHasher::new();
        email_str2.hash(&mut hasher);
        hasher.finish()
    };

    println!("{}", uid_pw_hash1);
    println!("{}", uid_pw_hash2);
}
