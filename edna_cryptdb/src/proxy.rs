extern crate mysql;
use crate::crypto::*;
use crate::*;
use base64;
use crypto_box::{PublicKey, SecretKey};
use log::{debug, info};
use msql_srv::*;
use mysql::Opts;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::io;
use std::iter::repeat;
use std::mem::size_of_val;
use std::sync::{Arc, Mutex};

const UID_COL: usize = 0;
const INDEX_COL: usize = 1;

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct RowKey {
    // ids
    pub row_ids: Vec<String>,
    pub enc_row_ids: Vec<String>,

    // key info
    pub symkey: Vec<u8>,
    pub iv: Vec<u8>,
}

impl Hash for RowKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.row_ids.hash(state);
    }
}

// table --> uid --> index value --> set of keys
pub type IndexValue = String;
pub type TableKeys = HashMap<TableName, HashMap<UID, HashMap<IndexValue, HashSet<Arc<RowKey>>>>>;

#[derive(Clone)]
pub struct ProxyState {
    tables_to_encrypt: HashMap<String, HashSet<String>>,
    logged_in_keys_plaintext: HashMap<PublicKey, TableKeys>,
    logged_in_keys_enc: HashMap<PublicKey, TableKeys>,
    access_keys_plaintext: HashMap<PublicKey, EncData>,
    access_keys_enc: HashMap<PublicKey, EncData>,
    admin_access_keys: HashMap<TableName, HashSet<EncData>>,
    admin_pubkey: Option<PublicKey>,
    edna_secretkey: SecretKey,
    nonce: Vec<u8>,
    keys_to_delete: HashMap<TableName, Vec<Arc<RowKey>>>,
}

impl ProxyState {
    pub fn get_space_for_keys(&self) {
        let mut edbytes = 0;
        for (pk, ed) in self.access_keys_plaintext.iter() {
            edbytes += size_of_val(&pk);
            edbytes += size_of_val(&*ed);
            edbytes += size_of_val(&*ed.nonce);
            edbytes += size_of_val(&*ed.encdata);
            edbytes += size_of_val(&*ed.pubkey);
            info!("access keys plaintext pk {}", size_of_val(&*pk));
            info!("access keys plaintext ed pointer {}", size_of_val(&*ed));
            info!("access keys plaintext nonce {}", size_of_val(&*ed.nonce));
            info!(
                "access keys plaintext encdata {}",
                size_of_val(&*ed.encdata)
            );
            info!("access keys plaintext pubkey {}", size_of_val(&*ed.pubkey));
        }
        warn!("access keys plaintext {}", edbytes);
        let mut eedbytes = 0;
        for (pk, ed) in self.access_keys_enc.iter() {
            eedbytes += size_of_val(&pk);
            eedbytes += size_of_val(&*ed);
            eedbytes += size_of_val(&*ed.nonce);
            eedbytes += size_of_val(&*ed.encdata);
            eedbytes += size_of_val(&*ed.pubkey);
            info!("access keys enc pk {}", size_of_val(&*pk));
            info!("access keys enc ed pointer {}", size_of_val(&*ed));
            info!("access keys enc nonce {}", size_of_val(&*ed.nonce));
            info!("access keys enc encdata {}", size_of_val(&*ed.encdata));
            info!("access keys enc pubkey {}", size_of_val(&*ed.pubkey));
        }
        warn!("access keys enc {}", eedbytes);
        let mut aedbytes = 0;
        for (pk, ed) in self.admin_access_keys.iter() {
            aedbytes += size_of_val(&pk);
            aedbytes += size_of_val(&*ed);
            info!("admin keys pk {}", size_of_val(&*pk));
            info!("admin keys ed hashset pointer {}", size_of_val(&*ed));
            for el in ed.iter() {
                aedbytes += size_of_val(&*el);
                aedbytes += size_of_val(&*el.nonce);
                aedbytes += size_of_val(&*el.encdata);
                aedbytes += size_of_val(&*el.pubkey);
                info!("admin keys encdata pointer {}", size_of_val(&*el));
                info!("admin keys ed nonce {}", size_of_val(&*el.nonce));
                info!("admin keys ed encdata {}", size_of_val(&*el.encdata));
                info!("admin keys ed pubkey {}", size_of_val(&*el.pubkey));
            }
        }
        warn!("admin keys {}", aedbytes);
        warn!("Total: {}", edbytes + eedbytes + aedbytes);
    }
}

#[derive(Clone)]
pub struct Proxy {
    dryrun: bool,
    pool: mysql::Pool,
    state: Arc<Mutex<ProxyState>>,
}

impl Proxy {
    pub fn new(
        host: &str,
        user: &str,
        pass: &str,
        dbname: &str,
        to_encrypt: HashMap<String, HashSet<String>>,
        dryrun: bool,
    ) -> Proxy {
        // assumes the db is already initialized
        let url = format!("mysql://{}:{}@{}/{}", user, pass, host, dbname);
        let pool = mysql::Pool::new(Opts::from_url(&url).unwrap()).unwrap();
        let mut db = pool.get_conn().unwrap();
        db.query_drop("SET max_heap_table_size = 4294967295;")
            .unwrap();

        // hacky stuff for det encryption with admin pubkey
        let mut rng = crypto_box::rand_core::OsRng;
        let secretkey = SecretKey::generate(&mut rng);
        let nonce = crypto_box::generate_nonce(&mut rng);

        info!("Returning proxy!");
        Proxy {
            dryrun: dryrun,
            pool: pool.clone(),
            state: Arc::new(Mutex::new(ProxyState {
                admin_access_keys: HashMap::new(),
                admin_pubkey: None,
                access_keys_plaintext: HashMap::new(),
                access_keys_enc: HashMap::new(),
                logged_in_keys_enc: HashMap::new(),
                logged_in_keys_plaintext: HashMap::new(),
                tables_to_encrypt: to_encrypt,
                keys_to_delete: HashMap::new(),
                edna_secretkey: secretkey,
                nonce: nonce.to_vec(),
            })),
        }
    }

    fn save_key_for_users(state: &mut ProxyState, rowkey: Arc<RowKey>, table: &str) {
        for (_pk, mut keys) in &mut state.logged_in_keys_plaintext {
            let start = time::Instant::now();
            Proxy::insert_key(&mut keys, table, rowkey.clone(), false);
            warn!(
                "Temp saved plaintext {} key: insert single user: {}mus",
                &table,
                start.elapsed().as_micros()
            );
        }
        for (_pk, mut keys) in &mut state.logged_in_keys_enc {
            let start = time::Instant::now();
            Proxy::insert_key(&mut keys, table, rowkey.clone(), true);
            warn!(
                "Temp saved enc {} key: insert single user: {}mus",
                &table,
                start.elapsed().as_micros()
            );
        }
        // also save for admin
        let admin_pubkey = state.admin_pubkey.as_ref().unwrap().clone();
        let encrypt_start = time::Instant::now();
        let bytes = bincode::serialize(&rowkey).unwrap();
        let encdata_admin =
            helpers::encrypt_det_with_pubkey(&admin_pubkey, &state.edna_secretkey, &state.nonce, &bytes);
        match state.admin_access_keys.get_mut(table) {
            Some(table_keys) => {
                table_keys.insert(encdata_admin);
                warn!(
                    "Saving {} admin keys, current len is {}",
                    table,
                    table_keys.len()
                );
            }
            None => {
                let mut hs = HashSet::new();
                hs.insert(encdata_admin);
                warn!("Saving {} admin keys, current len is 1", table);
                state.admin_access_keys.insert(table.to_string(), hs);
            }
        }
        warn!(
            "Saved {} key: admin insert: {}mus",
            &table,
            encrypt_start.elapsed().as_micros()
        );
    }

    fn save_and_clear_logged_in(&self, state: &mut ProxyState, pk: Option<PublicKey>) {
        let start = time::Instant::now();

        // delete keys to delete prior to saving
        for (table, delete_keys) in &state.keys_to_delete {
            for (_pk, pk_keys) in &mut state.logged_in_keys_plaintext {
                for k in delete_keys {
                    Proxy::remove_key(pk_keys, table, k.clone(), false);
                }
            }
            for (_pk, pk_keys) in &mut state.logged_in_keys_enc {
                for k in delete_keys {
                    Proxy::remove_key(pk_keys, table, k.clone(), true);
                }
            }

            let admin_pubkey = state.admin_pubkey.as_ref().unwrap().clone();
            if let Some(table_keys) = state.admin_access_keys.get_mut(table) {
                warn!(
                    "PREDeleting {} admin keys, current len is {}",
                    table,
                    table_keys.len()
                );
                let encrypt_start = time::Instant::now();
                for k in delete_keys {
                    let bytes = bincode::serialize(&k).unwrap();
                    let encdata_admin = helpers::encrypt_det_with_pubkey(
                        &admin_pubkey,
                        &state.edna_secretkey,
                        &state.nonce,
                        &bytes,
                    );
                    table_keys.remove(&encdata_admin);
                }
                warn!(
                    "POSTDeleting {} admin keys, current len is {}: {}mus",
                    table,
                    table_keys.len(),
                    encrypt_start.elapsed().as_micros()
                );
            }
            warn!(
                "Deleted {} {} keys: {}mus",
                &table,
                delete_keys.len(),
                start.elapsed().as_micros()
            );
        }
        state.keys_to_delete.clear();

        // actually encrypt and store user keys as access keys
        if let Some(pk) = &pk {
            if pk != state.admin_pubkey.as_ref().unwrap() {
                let keys = state.logged_in_keys_plaintext.get(&pk).unwrap();
                let bytes = bincode::serialize(&keys).unwrap();
                let encdata_princ = encrypt_with_pubkey(&Some(&pk), &bytes, self.dryrun);
                state
                    .access_keys_plaintext
                    .insert(pk.clone(), encdata_princ);

                let keys = state.logged_in_keys_enc.get(&pk).unwrap();
                let bytes = bincode::serialize(&keys).unwrap();
                let encdata_princ = encrypt_with_pubkey(&Some(&pk), &bytes, self.dryrun);
                state.access_keys_enc.insert(pk.clone(), encdata_princ);
            }
            state
                .logged_in_keys_plaintext
                .retain(|pubkey, _| pubkey != pk);
            state.logged_in_keys_enc.retain(|pubkey, _| pubkey != pk);
            warn!(
                "logout {} and {} users logged in still: {}mus",
                state.logged_in_keys_plaintext.len(),
                state.logged_in_keys_enc.len(),
                start.elapsed().as_micros()
            );
        } else {
            // insert the encrypted blob into access keys
            for (pk, keys) in &state.logged_in_keys_plaintext {
                warn!("logout plaintext pk with {} table keys", keys.len());
                if pk != state.admin_pubkey.as_ref().unwrap() {
                    let bytes = bincode::serialize(&keys).unwrap();
                    let encdata_princ = encrypt_with_pubkey(&Some(pk), &bytes, self.dryrun);
                    state
                        .access_keys_plaintext
                        .insert(pk.clone(), encdata_princ);
                }
            }
            // do the same for enc
            for (pk, keys) in &state.logged_in_keys_enc {
                warn!("logout enc pk with {} table keys", keys.len());
                if pk != state.admin_pubkey.as_ref().unwrap() {
                    let bytes = bincode::serialize(&keys).unwrap();
                    let encdata_princ = encrypt_with_pubkey(&Some(pk), &bytes, self.dryrun);
                    state.access_keys_enc.insert(pk.clone(), encdata_princ);
                }
            }
            state.logged_in_keys_plaintext.clear();
            state.logged_in_keys_enc.clear();
            warn!(
                "logout all encrypted access keys: {}mus",
                start.elapsed().as_micros()
            );
        }
    }

    fn remove_key(keys: &mut TableKeys, table: &TableName, rowkey: Arc<RowKey>, is_enc: bool) {
        let (uid, index_val) = if !is_enc {
            (&rowkey.row_ids[UID_COL], &rowkey.row_ids[INDEX_COL])
        } else {
            (&rowkey.enc_row_ids[UID_COL], &rowkey.enc_row_ids[INDEX_COL])
        };
        if let Some(tablekeys) = keys.get_mut(table) {
            if let Some(userkeys) = tablekeys.get_mut(uid) {
                if let Some(valkeys) = userkeys.get_mut(index_val) {
                    valkeys.remove(&rowkey);
                    warn!("Removed valkey uid {} index {}: len {}", uid, index_val, 1);
                }
            }
        }
    }

    fn insert_key(keys: &mut TableKeys, table: &str, rowkey: Arc<RowKey>, is_enc: bool) {
        let (uid, index_val) = if !is_enc {
            (&rowkey.row_ids[UID_COL], &rowkey.row_ids[INDEX_COL])
        } else {
            (&rowkey.enc_row_ids[UID_COL], &rowkey.enc_row_ids[INDEX_COL])
        };
        match keys.get_mut(table) {
            Some(user2vals) => match user2vals.get_mut(uid) {
                Some(val2valkeys) => match val2valkeys.get_mut(index_val) {
                    Some(valkeys) => {
                        valkeys.insert(rowkey.clone());
                        warn!(
                            "KEYS: Inserted valkey uid {} index {}: len {}",
                            uid,
                            index_val,
                            valkeys.len()
                        );
                    }
                    None => {
                        let mut valkeys = HashSet::new();
                        valkeys.insert(rowkey.clone());
                        val2valkeys.insert(index_val.to_string(), valkeys);
                        warn!(
                            "KEYS: New index val, Inserted valkey uid {} index {}: len {}",
                            uid, index_val, 1
                        );
                    }
                },
                None => {
                    let mut val2valkeys = HashMap::new();
                    let mut valkeys = HashSet::new();
                    valkeys.insert(rowkey.clone());
                    val2valkeys.insert(index_val.to_string(), valkeys);
                    user2vals.insert(uid.to_string(), val2valkeys);
                    warn!(
                        "KEYS: New uid, Inserted valkey uid {} index {}: len {}",
                        uid, index_val, 1
                    );
                }
            },
            None => {
                let mut user2vals = HashMap::new();
                let mut val2valkeys = HashMap::new();
                let mut valkeys = HashSet::new();
                valkeys.insert(rowkey.clone());
                warn!(
                    "KEYS: New table {}, Inserted valkey uid {} index {}: len {}",
                    table, uid, index_val, 1
                );
                val2valkeys.insert(index_val.to_string(), valkeys);
                user2vals.insert(uid.to_string(), val2valkeys);
                keys.insert(table.to_string(), user2vals);
            }
        }
    }

    pub fn get_keys(
        keys: &TableKeys,
        table: &str,
        uids: &Vec<UID>,
        vals: &Vec<String>,
    ) -> HashSet<Arc<RowKey>> {
        let mut match_keys = HashSet::new();
        match keys.get(table) {
            Some(user2vals) => {
                for u in uids {
                    match user2vals.get(u) {
                        Some(val2valkeys) => {
                            info!("User {} keys for table {} are {:?}", u, table, val2valkeys);
                            for v in vals {
                                match val2valkeys.get(v) {
                                    Some(valkeys) => {
                                        match_keys.extend(
                                            valkeys
                                                .iter()
                                                .map(|vk| vk.clone())
                                                .collect::<HashSet<Arc<RowKey>>>(),
                                        );
                                    }
                                    None => (),
                                }
                            }
                            if vals.is_empty() {
                                for (_, valkeys) in val2valkeys {
                                    match_keys.extend(
                                        valkeys
                                            .iter()
                                            .map(|vk| vk.clone())
                                            .collect::<HashSet<Arc<RowKey>>>(),
                                    );
                                }
                            }
                        }
                        None => (),
                    }
                }
                // there were no UIDs so just get all the keys matching vals
                if uids.is_empty() {
                    for (uid, val2valkeys) in user2vals {
                        info!(
                            "User {} keys for table {} are {:?}",
                            uid, table, val2valkeys
                        );
                        for v in vals {
                            match val2valkeys.get(v) {
                                Some(valkeys) => {
                                    match_keys.extend(
                                        valkeys
                                            .iter()
                                            .map(|vk| vk.clone())
                                            .collect::<HashSet<Arc<RowKey>>>(),
                                    );
                                }
                                None => (),
                            }
                        }
                        if vals.is_empty() {
                            for (_, valkeys) in val2valkeys {
                                match_keys.extend(
                                    valkeys
                                        .iter()
                                        .map(|vk| vk.clone())
                                        .collect::<HashSet<Arc<RowKey>>>(),
                                );
                            }
                        }
                    }
                }
            }
            None => {
                warn!("No keys for table {}", table);
            }
        }
        info!(
            "KEYS: Got {} {} keys for {:?} uids {:?} vals",
            match_keys.len(),
            table,
            uids,
            vals
        );
        match_keys
    }

    pub fn get_uid_and_val(ids: &Vec<String>, cols: &Vec<String>) -> (Vec<UID>, Vec<String>) {
        let mut uids = vec![];
        let mut vals = vec![];
        for (i, id) in ids.iter().enumerate() {
            let col_index = std::cmp::min(i, cols.len() - 1);
            let col = &cols[col_index];
            match col.as_str() {
                "email" => uids.push(id.clone()),
                "lec" | "apikey" => vals.push(id.clone()),
                _ => debug!("col {} not a uid or index col", col),
            }
        }
        (uids, vals)
    }

    fn get_uid_and_val_expr(e: &Expr) -> (Vec<UID>, Vec<String>) {
        let ids = helpers::get_expr_values(&e);
        let cols = helpers::get_expr_cols(&e);
        let mut uids = vec![];
        let mut vals = vec![];
        for (i, id) in ids.iter().enumerate() {
            let col_index = std::cmp::min(i, cols.len() - 1);
            let col = &cols[col_index];
            match col.as_str() {
                "email" => uids.push(id.clone()),
                "lec" | "apikey" => vals.push(id.clone()),
                _ => debug!("col {} not a uid or index col", col),
            }
        }
        (uids, vals)
    }

    fn has_col_to_encrypt(e: &Expr, enccols: &HashSet<String>) -> bool {
        let cols = helpers::get_expr_cols(&e);
        for c in &cols {
            if enccols.contains(c) {
                return true;
            }
        }
        false
    }

    fn check_key_matches_row_ids(e: &Expr, key: &RowKey) -> bool {
        let ids = helpers::get_expr_values(&e);
        let cols = helpers::get_expr_cols(&e);
        let mut matched_cols = HashMap::new();
        debug!("Checking ids: {:?}, {:?}", ids, cols);

        // XXX Note: this assertion doesn't hold because sometimes the predicate will be over
        // multiple values (e.g., email IN (foo, bar, baz))
        // Use the heuristic for now that this predicate will always be the last one
        //assert_eq!(cols.len(), ids.len());

        // IN LIST: we want to make sure that at least one encrypted value in the list matches the row_id val
        // thus, if every column has ONE matching value, then we should return true
        for col in &cols {
            matched_cols.insert(col, false);
        }

        // check that the key row_ids contains all the ids selected over
        for (i, id) in ids.iter().enumerate() {
            let col_index = std::cmp::min(i, cols.len() - 1);
            let col = &cols[col_index];
            let check_index = match col.as_str() {
                "email" => 0,
                "lec" => 1,
                "q" => 2,
                "apikey" => 1,
                "is_admin" => 2,
                _ => {
                    debug!("No match for col? {}", col);
                    // this is ok, we'll just ignore it so set to true
                    matched_cols.insert(col, true);
                    0
                }
            };
            if &key.row_ids[check_index] != id {
                debug!(
                    "CHECK NO MATCH: Skipping encryption with key col {}\n\t{}\n\t{}",
                    col, id, key.row_ids[check_index]
                );
            } else {
                matched_cols.insert(col, true);
                debug!(
                    "CHECK MATCH: Found matching val {} for col {}",
                    key.row_ids[check_index], col
                );
            }
        }
        for (_, b) in matched_cols {
            if !b {
                return false;
            }
        }
        true
    }
}

impl<W: io::Write> MysqlShim<W> for Proxy {
    type Error = io::Error;

    fn on_prepare(&mut self, q: &str, _info: StatementMetaWriter<W>) -> io::Result<()> {
        info!("Prepare {}", q);
        Ok(())
        //unimplemented!("Nope can't prepare right now");
    }
    fn on_execute(
        &mut self,
        _: u32,
        _: ParamParser,
        _results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        info!("Exec");
        Ok(())
        //unimplemented!("Nope can't execute right now");
    }

    fn on_close(&mut self, _: u32) {
        info!("Close");
    }

    fn on_init(&mut self, schema: &str, _: InitWriter<'_, W>) -> Result<(), Self::Error> {
        info!("Init schema {}", schema);
        self.pool
            .get_conn()
            .unwrap()
            .query_drop(format!("use {}", schema))
            .unwrap();
        Ok(())
    }

    fn on_query(&mut self, query: &str, results: QueryResultWriter<W>) -> io::Result<()> {
        let start = time::Instant::now();

        if self.dryrun {
            if query.contains("REGISTER") || query.contains("LOGIN") || query.contains("LOGOUT") {
                results.completed(1, 1).unwrap();
            } else {
                let mut db = self.pool.get_conn().unwrap();
                let res = db.query_iter(query);
                helpers::write_mysql_answer_rows(results, res).unwrap();
            }
            warn!(
                "No Crypto Query {}: {}mus",
                query,
                start.elapsed().as_micros()
            );
            return Ok(());
        }

        info!("On Query {}", query);
        let mut state = self.state.lock().unwrap();
        if query.contains("SPACE") {
            let qparts: Vec<&str> = query.split_whitespace().collect();
            let tag = qparts[1];
            warn!("Space {}", tag);
            state.get_space_for_keys();
            results.completed(1, 1).unwrap();
            return Ok(());
        } else if query.contains("REGISTER") {
            /*
             * "REGISTER pubkey ADMIN"
             */
            let qparts: Vec<&str> = query.split_whitespace().collect();
            assert!(qparts.len() >= 2);

            let pkbytes = base64::decode(&qparts[1]).unwrap();
            let pubkey = PublicKey::from(get_pk_bytes(&pkbytes));
            let hm: HashMap<TableName, HashSet<RowKey>> = HashMap::new();
            let bytes = bincode::serialize(&hm).unwrap();
            let encdata = encrypt_with_pubkey(&Some(&pubkey), &bytes, self.dryrun);
            state
                .access_keys_plaintext
                .insert(pubkey.clone(), encdata.clone());
            state.access_keys_enc.insert(pubkey.clone(), encdata);
            if qparts.len() > 2 {
                state.admin_pubkey = Some(pubkey);
                info!("Created admin user with pubkey {}", qparts[1]);
            } else {
                info!("Created user with pubkey {}", qparts[1]);
            }
            results.completed(1, 1).unwrap();
            warn!("Register: {}mus", start.elapsed().as_micros());
            return Ok(());
        } else if query.contains("FORGET") {
            /*
             * "FORGET pubkey"
             */
            let qparts: Vec<&str> = query.split_whitespace().collect();
            assert!(qparts.len() >= 2);
            let pkbytes = base64::decode(&qparts[1]).unwrap();
            let pubkey = PublicKey::from(get_pk_bytes(&pkbytes));
            state.access_keys_plaintext.remove(&pubkey);
            state.access_keys_enc.remove(&pubkey);
            info!("Removed user with pubkey {:?}", &pkbytes);
            results.completed(1, 1).unwrap();
            warn!("Forget: {}mus", start.elapsed().as_micros());
            return Ok(());
        } else if query.contains("LOGIN") {
            /*
             * LOGIN privkey
             */
            let start = time::Instant::now();
            let qparts: Vec<&str> = query.split_whitespace().collect();
            assert_eq!(qparts.len(), 2);
            let skbytes = base64::decode(&qparts[1]).unwrap();
            let skey = SecretKey::from(get_pk_bytes(&skbytes));
            let pubkey = PublicKey::from(&skey);

            if state.logged_in_keys_plaintext.contains_key(&pubkey) {
                warn!(
                    "Already logged-in user with pubkey {}, {} users' keys in total: {}mus",
                    base64::encode(&pubkey),
                    state.logged_in_keys_plaintext.len(),
                    start.elapsed().as_micros()
                );
                results.completed(1, 1).unwrap();
                return Ok(());
            }

            info!("Logging in user with pubkey {}", base64::encode(&pubkey));
            // special case the admin
            if &pubkey == state.admin_pubkey.as_ref().unwrap() {
                let start = time::Instant::now();
                let mut table_keys_plaintext = HashMap::new();
                let mut table_keys_enc = HashMap::new();
                for (table, enc_keys) in state.admin_access_keys.iter() {
                    for ek in enc_keys {
                        let (success, rowkey_bytes) = decrypt_encdata(&ek, &skbytes, self.dryrun);
                        assert!(success);
                        let rowkey: Arc<RowKey> = bincode::deserialize(&rowkey_bytes).unwrap();
                        Proxy::insert_key(&mut table_keys_plaintext, table, rowkey.clone(), false);
                        Proxy::insert_key(&mut table_keys_enc, table, rowkey, true);
                    }
                }
                state
                    .logged_in_keys_plaintext
                    .insert(pubkey.clone(), table_keys_plaintext);
                state.logged_in_keys_enc.insert(pubkey, table_keys_enc);
                warn!(
                    "Admin login {} table's keys: {}mus",
                    state.admin_access_keys.len(),
                    start.elapsed().as_micros()
                );
            } else {
                // PLAINTEXT
                let start = time::Instant::now();
                let enc_table_keys = state.access_keys_plaintext.get(&pubkey).unwrap();
                let (success, plaintext) = decrypt_encdata(&enc_table_keys, &skbytes, self.dryrun);
                assert!(success);
                let table_keys: TableKeys = bincode::deserialize(&plaintext).unwrap();
                warn!(
                    "Inserting Plaintext User login {} table's keys: {}mus",
                    table_keys.len(),
                    start.elapsed().as_micros()
                );
                state
                    .logged_in_keys_plaintext
                    .insert(pubkey.clone(), table_keys);

                // ENC
                let start = time::Instant::now();
                let enc_table_keys = state.access_keys_enc.get(&pubkey).unwrap();
                let (success, plaintext) = decrypt_encdata(&enc_table_keys, &skbytes, self.dryrun);
                assert!(success);
                let table_keys: TableKeys = bincode::deserialize(&plaintext).unwrap();
                warn!(
                    "Inserting Enc User login {} table's keys: {}mus",
                    table_keys.len(),
                    start.elapsed().as_micros()
                );
                state.logged_in_keys_enc.insert(pubkey, table_keys);
            }
            warn!(
                "TOTAL: Logged in with {} users' keys: {}mus",
                state.logged_in_keys_plaintext.len(),
                start.elapsed().as_micros()
            );
            results.completed(1, 1).unwrap();
            return Ok(());
        } else if query.contains("LOGOUT") {
            /*
             * LOGOUT [privkey]
             */
            let qparts: Vec<&str> = query.split_whitespace().collect();
            if qparts.len() < 2 {
                self.save_and_clear_logged_in(&mut state, None);
            } else {
                let skbytes = base64::decode(&qparts[1]).unwrap();
                let skey = SecretKey::from(get_pk_bytes(&skbytes));
                let pubkey = PublicKey::from(&skey);
                self.save_and_clear_logged_in(&mut state, Some(pubkey));
                info!("Logging out user with pubkey {}", qparts[1]);
            }
            results.completed(1, 1).unwrap();
            warn!("TOTAL Logout: {}mus", start.elapsed().as_micros());
            return Ok(());
        } else {
            // everything else
            use sql_parser::ast::*;
            let stmt: Option<Statement>;
            match helpers::get_single_parsed_stmt(query) {
                Err(_) => {
                    helpers::write_mysql_answer_rows(
                        results,
                        self.pool.get_conn().unwrap().query_iter(query.to_string()),
                    )
                    .unwrap();
                    return Ok(());
                }
                Ok(s) => stmt = Some(s),
            }
            match stmt.unwrap() {
                Statement::Select(s) => {
                    let mut new_s = s.query.clone();
                    match &s.query.body {
                        SetExpr::Select(select) => {
                            let table = &helpers::get_tables_of_twj(&select.from)[0].0;
                            if let Some(enccols) = state.tables_to_encrypt.get(table) {
                                let mut new_select = select.clone();
                                // exists data that will actually be returned
                                let mut exists_data = false;
                                new_select.selection = match &select.selection {
                                    None => {
                                        exists_data = true;
                                        None
                                    }
                                    Some(e) => {
                                        let mut select_expr = e.clone();
                                        if !Proxy::has_col_to_encrypt(e, enccols) {
                                            warn!("No col to encrypt! {}", select);
                                            exists_data = true;
                                            Some(select_expr)
                                        } else {
                                            let (uids, index_vals) = Proxy::get_uid_and_val_expr(e);
                                            for (_, table_keys) in &state.logged_in_keys_plaintext {
                                                let keys = Proxy::get_keys(
                                                    &table_keys,
                                                    table,
                                                    &uids,
                                                    &index_vals,
                                                );
                                                // encrypt values in where clause, select on all possible encrypted values using the row
                                                // keys accessible to logged in user that match the predicated encrypted values
                                                // XXX no joins for now!
                                                let mut nencrypts = 0;
                                                let select_start = time::Instant::now();
                                                warn!(
                                                    "Select: Going to iterate through {} keys",
                                                    keys.len()
                                                );
                                                'keyloop: for key in keys {
                                                    // check that the key row_ids contains all the ids selected over
                                                    if !Proxy::check_key_matches_row_ids(&e, &key) {
                                                        warn!(
                                                            "Select: Skipping encryption with key"
                                                        );
                                                        continue 'keyloop;
                                                    }
                                                    nencrypts += 1;
                                                    exists_data = true;
                                                    let encrypted_expr =
                                                        helpers::encrypt_expr_values(
                                                            &e, &key, &key.iv, enccols,
                                                        );
                                                    if &select_expr == e {
                                                        select_expr = encrypted_expr;
                                                    } else {
                                                        select_expr = Expr::BinaryOp {
                                                            left: Box::new(select_expr),
                                                            op: BinaryOperator::Or,
                                                            right: Box::new(encrypted_expr),
                                                        };
                                                    }
                                                }
                                                warn!(
                                                    "Select: Encrypt with {} keys: {}mus",
                                                    nencrypts,
                                                    select_start.elapsed().as_micros()
                                                );
                                            }
                                            Some(select_expr)
                                        }
                                    }
                                };
                                if exists_data {
                                    // decrypt the results
                                    let select_start = time::Instant::now();
                                    new_s.body = SetExpr::Select(new_select);
                                    let mut db = self.pool.get_conn().unwrap();
                                    let rows = db.query_iter(new_s.to_string());
                                    warn!(
                                        "Select: DB {}:\n\tQuery Time {}mus",
                                        new_s,
                                        select_start.elapsed().as_micros()
                                    );

                                    let select_start = time::Instant::now();
                                    helpers::write_decrypted_mysql_answer_rows(
                                        &s.to_string(),
                                        results,
                                        &state.logged_in_keys_enc,
                                        &table,
                                        &enccols,
                                        rows,
                                    )
                                    .unwrap();
                                    warn!(
                                        "Select: Write decrypted rows {}mus",
                                        select_start.elapsed().as_micros()
                                    );
                                } else {
                                    warn!(
                                        "Select: No rows that matched to decrypt {}mus",
                                        start.elapsed().as_micros()
                                    );
                                    // return nothing
                                    results.completed(0, 0).unwrap();
                                }
                                warn!("Select {} total : {}mus", s, start.elapsed().as_micros());
                                return Ok(());
                            } else {
                                helpers::write_mysql_answer_rows(
                                    results,
                                    self.pool.get_conn().unwrap().query_iter(query),
                                )
                                .unwrap();
                                return Ok(());
                            }
                        }
                        _ => unimplemented!("Unsupported select statement"),
                    }
                }
                Statement::Insert(s) => {
                    let table = &s.table_name.to_string();
                    if let Some(enccols) = state.tables_to_encrypt.get(table) {
                        // HACK
                        let enccol_ixs = if table == "answers" {
                            vec![0, 3, 4]
                        } else {
                            vec![0, 1, 2, 3]
                        };
                        let mut new_s = s.clone();
                        match &s.source {
                            InsertSource::Query(q) => {
                                let mut new_q = q.clone();
                                match &q.body {
                                    SetExpr::Values(vs) => {
                                        let insert_start = time::Instant::now();
                                        // generate a symmetric key to encrypt the row
                                        let mut key: Vec<u8> =
                                            repeat(0u8).take(AES_BYTES).collect();
                                        let mut rng = rand::thread_rng();
                                        rng.fill_bytes(&mut key[..]);
                                        let mut iv: Vec<u8> = repeat(0u8).take(AES_BYTES).collect();
                                        rng.fill_bytes(&mut iv[..]);
                                        warn!("new_key: {}mus", insert_start.elapsed().as_micros());
                                        let row_ids = vs.0[0]
                                            .iter()
                                            .map(|e| {
                                                helpers::trim_quotes(&e.to_string()).to_string()
                                            })
                                            .collect();
                                        let mut rowkey = RowKey {
                                            row_ids: row_ids,
                                            enc_row_ids: vec![],
                                            symkey: key,
                                            iv: iv.clone(),
                                        };
                                        // XXX assumes only one row being inserted
                                        assert_eq!(vs.0.len(), 1);
                                        let new_vs: Vec<Vec<Expr>> = vs
                                            .0
                                            .iter()
                                            .map(|row| {
                                                let mut newrow = vec![];
                                                for (i, e) in row.iter().enumerate() {
                                                    if enccol_ixs.contains(&i) {
                                                        newrow.push(helpers::encrypt_expr_values(
                                                            &e, &rowkey, &iv, enccols,
                                                        ));
                                                    } else {
                                                        newrow.push(e.clone());
                                                    }
                                                }
                                                newrow
                                            })
                                            .collect();
                                        rowkey.enc_row_ids = new_vs[0]
                                            .iter()
                                            .map(|e| {
                                                helpers::trim_quotes(&e.to_string()).to_string()
                                            })
                                            .collect();
                                        new_q.body = SetExpr::Values(Values(new_vs.clone()));
                                        warn!(
                                            "Insert: Encrypt expr {}mus",
                                            insert_start.elapsed().as_micros()
                                        );

                                        // save both the encrypted row IDs (to check for encrypted returned
                                        // row matches) and the unecrypted ones (to check for plaintext predicate matches)
                                        let insert_start = time::Instant::now();

                                        Proxy::save_key_for_users(
                                            &mut state,
                                            Arc::new(rowkey),
                                            &s.table_name.to_string(),
                                        );
                                        warn!(
                                            "Insert: Save key {}mus",
                                            insert_start.elapsed().as_micros()
                                        );
                                    }
                                    _ => unimplemented!("Bad values inserted"),
                                }
                                new_s.source = InsertSource::Query(new_q);
                            }
                            _ => unimplemented!("Bad insert query"),
                        }
                        info!("Insert of encrypted row: {}", new_s);
                        let insert_start = time::Instant::now();
                        helpers::write_mysql_answer_rows(
                            results,
                            self.pool.get_conn().unwrap().query_iter(new_s.to_string()),
                        )
                        .unwrap();
                        warn!("Insert: DB {}mus", insert_start.elapsed().as_micros());
                    } else {
                        helpers::write_mysql_answer_rows(
                            results,
                            self.pool.get_conn().unwrap().query_iter(query),
                        )
                        .unwrap();
                    }
                    warn!("Insert {}: {}mus", s, start.elapsed().as_micros());
                    return Ok(());
                }
                Statement::Delete(s) => {
                    // encrypt any values in predicate
                    if let Some(enccols) = state.tables_to_encrypt.get(&s.table_name.to_string()) {
                        let mut new_s = s.clone();
                        if let Some(select) = &s.selection {
                            // predicate selects on all possible encrypted values using the row
                            // keys accessible to logged in user
                            let mut keys_to_delete = vec![];
                            let table = s.table_name.to_string();
                            let (uids, index_vals) = Proxy::get_uid_and_val_expr(&select);

                            // if there aren't any rows in the selection predicate to encrypt,
                            // simply issue the query
                            // we also then need to find the keys to delete
                            if !Proxy::has_col_to_encrypt(select, enccols) {
                                let mut conn = self.pool.get_conn().unwrap();
                                let res = conn.query_iter(new_s.to_string()).unwrap();
                                let affected_rows = res.affected_rows();
                                info!("Delete of {} encrypted row: {}", affected_rows, new_s);
                                if affected_rows > 0 {
                                    for (_, table_keys) in &state.logged_in_keys_plaintext {
                                        let keys = Proxy::get_keys(
                                            &table_keys,
                                            &table,
                                            &uids,
                                            &index_vals,
                                        );
                                        for key in keys {
                                            // remove the key if deleted
                                            info!("Delete key of encrypted row {:?}", key.row_ids);
                                            if Proxy::check_key_matches_row_ids(&select, &key) {
                                                keys_to_delete.push(key.clone());
                                            }
                                        }
                                    }
                                }
                            } else {
                                for (_, table_keys) in &state.logged_in_keys_plaintext {
                                    let keys =
                                        Proxy::get_keys(&table_keys, &table, &uids, &index_vals);
                                    warn!("Delete: Going to iterate through {} keys", keys.len());
                                    'keyloop: for key in keys {
                                        // check that the key enc_row_ids contains all the ids selected over
                                        let delete_start = time::Instant::now();
                                        if !Proxy::check_key_matches_row_ids(&select, &key) {
                                            debug!("Delete: Skipping encryption with key");
                                            continue 'keyloop;
                                        }
                                        info!("Delete: encrypting with key {:?}", key.row_ids);
                                        let encrypted_expr = helpers::encrypt_expr_values(
                                            &select, &key, &key.iv, enccols,
                                        );
                                        warn!(
                                            "Delete : Encrypt expr {}mus",
                                            delete_start.elapsed().as_micros()
                                        );
                                        let delete_start = time::Instant::now();
                                        new_s.selection = Some(encrypted_expr);
                                        let mut conn = self.pool.get_conn().unwrap();
                                        let res = conn.query_iter(new_s.to_string()).unwrap();
                                        let affected_rows = res.affected_rows();
                                        info!(
                                            "Delete of {} encrypted row: {}",
                                            affected_rows, new_s
                                        );
                                        if affected_rows > 0 {
                                            // remove the key if deleted
                                            info!("Delete key of encrypted row {:?}", key.row_ids);
                                            keys_to_delete.push(key.clone());
                                        }
                                        warn!(
                                            "Delete : DB query and delete key {}mus",
                                            delete_start.elapsed().as_micros()
                                        );
                                    }
                                }
                            }
                            match state.keys_to_delete.get_mut(&table) {
                                Some(table_keys) => table_keys.append(&mut keys_to_delete),
                                None => {
                                    state.keys_to_delete.insert(table, keys_to_delete);
                                }
                            }
                        }
                        results.completed(1, 1).unwrap();
                    } else {
                        helpers::write_mysql_answer_rows(
                            results,
                            self.pool.get_conn().unwrap().query_iter(query.to_string()),
                        )
                        .unwrap();
                    }
                    warn!("Delete {}: {}mus", s, start.elapsed().as_micros());
                    return Ok(());
                }
                Statement::Update(s) => {
                    if let Some(enccols) = state.tables_to_encrypt.get(&s.table_name.to_string()) {
                        // send update statement for each row belonging to the principal given the
                        // select statement
                        // NOTE: sends one update for every key!!!
                        let mut new_s = s.clone();
                        let mut keys_to_save = vec![];
                        let table = s.table_name.to_string();
                        let mut keys_to_delete = vec![];
                        //assert!(!s.selection.is_none());
                        let select = s.selection.as_ref().unwrap();
                        let (uids, index_vals) = Proxy::get_uid_and_val_expr(&select);

                        'update_loop: for (pk, table_keys) in &state.logged_in_keys_plaintext {
                            // NOTE: this introduces some variability because we can't tell apriori
                            // from the public key whether one of the principal's keys will match or not
                            let keys = Proxy::get_keys(&table_keys, &table, &uids, &index_vals);
                            warn!(
                                "Update: Going to iterate through {} keys for {}",
                                keys.len(),
                                base64::encode(pk)
                            );
                            'keyloop: for key in keys {
                                // check that the key enc_row_ids contains all the ids selected over
                                let update_start = time::Instant::now();
                                let key_matches_select_ids =
                                    Proxy::check_key_matches_row_ids(&select, &key);
                                if !key_matches_select_ids {
                                    debug!("Update: Skipping encryption with key");
                                    //warn!("Update: Check should encrypt, skipping {}mus", keyloop_start.elapsed().as_micros());
                                    continue 'keyloop;
                                }
                                warn!(
                                    "Update: Check should encrypt {}mus",
                                    update_start.elapsed().as_micros()
                                );
                                info!("Update: Encrypted with key {:?}", key.row_ids);
                                // encrypt values to update using the row's symmetric key
                                let update_start = time::Instant::now();
                                new_s.assignments = s
                                    .assignments
                                    .iter()
                                    .map(|a| {
                                        if enccols.contains(&a.id.to_string()) {
                                            Assignment {
                                                id: a.id.clone(),
                                                value: helpers::encrypt_expr_values(
                                                    &a.value, &key, &key.iv, enccols,
                                                ),
                                            }
                                        } else {
                                            a.clone()
                                        }
                                    })
                                    .collect();

                                // update predicates to encrypted values
                                let encselect =
                                    helpers::encrypt_expr_values(&select, &key, &key.iv, enccols);
                                warn!(
                                    "Update: Encrypt expr and assignments {}mus",
                                    update_start.elapsed().as_micros()
                                );

                                let update_start = time::Instant::now();
                                new_s.selection = Some(encselect);
                                info!("Update: encrypted DB query {}", new_s);
                                let mut db = self.pool.get_conn().unwrap();
                                let res = db.query_iter(new_s.to_string()).unwrap();
                                let affected_rows = res.affected_rows();
                                drop(res);
                                warn!("Update: DB query {}mus", update_start.elapsed().as_micros());

                                info!(
                                    "Update {} encrypted rows: \n{}\n{:?}",
                                    affected_rows, new_s, key.row_ids
                                );

                                if affected_rows > 0 {
                                    // update rowkeys to new values!
                                    // XXX TODO do for all identifier columns of the table, not
                                    // just users...
                                    let mut newkey = (*key).clone();
                                    for (i, a) in new_s.assignments.iter().enumerate() {
                                        // updating a user col
                                        if a.id.to_string().contains("email") {
                                            // Remove old key
                                            keys_to_delete.push(key.clone());

                                            // Save new key
                                            newkey.row_ids[0] = helpers::trim_quotes(
                                                &s.assignments[i].value.to_string(),
                                            )
                                            .to_string();
                                            newkey.enc_row_ids[0] =
                                                helpers::trim_quotes(&a.value.to_string())
                                                    .to_string();
                                            keys_to_save.push(newkey);
                                            break;
                                        }
                                    }
                                    break 'update_loop;
                                }
                            }
                        }
                        match state.keys_to_delete.get_mut(&table) {
                            Some(table_keys) => table_keys.append(&mut keys_to_delete),
                            None => {
                                state.keys_to_delete.insert(table, keys_to_delete);
                            }
                        }
                        let update_start = time::Instant::now();
                        for newkey in keys_to_save {
                            Proxy::save_key_for_users(
                                &mut state,
                                Arc::new(newkey),
                                &s.table_name.to_string(),
                            );
                        }
                        warn!(
                            "Update: save all new keys {}mus",
                            update_start.elapsed().as_micros()
                        );
                        results.completed(1, 1).unwrap();
                    } else {
                        //info!("Update of unencrypted row: {}", s);
                        helpers::write_mysql_answer_rows(
                            results,
                            self.pool.get_conn().unwrap().query_iter(query.to_string()),
                        )
                        .unwrap();
                    }
                    warn!("Update {}: {}mus", s, start.elapsed().as_micros());
                    return Ok(());
                }
                _ => {
                    /*info!(
                        "Not a select, insert, delete, or update statement: {}",
                        query
                    );*/
                    helpers::write_mysql_answer_rows(
                        results,
                        self.pool.get_conn().unwrap().query_iter(query.to_string()),
                    )
                    .unwrap();
                }
            }
        }
        return Ok(());
    }
}
