extern crate base64;
extern crate mysql;
extern crate ordered_float;

use log::{error, warn};
use mysql::prelude::*;
use mysql::IsolationLevel::Serializable;
use mysql::Opts;
use mysql::TxOpts;
use serde::{Deserialize, Serialize};
use sql_parser::ast::*;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::*;

pub mod crypto;
pub mod gen_value;
pub mod helpers;
pub mod highlevel_api;
pub mod lowlevel_api;
pub mod predicate;
pub mod proxy;
pub mod records;

// disguise ID
pub type DID = u64;
// user ID
pub type UID = String;
pub type ColName = String;
pub type TableName = String;

#[derive(Clone, Serialize, Deserialize)]
pub struct PseudoprincipalGenerator {
    pub name: TableName,
    pub id_col: ColName,
    pub cols: Vec<String>,
    pub val_generation: Vec<gen_value::GenValue>,
}

impl PseudoprincipalGenerator {
    pub fn get_vals(&self) -> Vec<String> {
        let mut vals: Vec<String> = vec![];
        for genval in &self.val_generation {
            vals.push(gen_value::gen_strval(genval));
        }
        vals
    }
    pub fn get_vals_with_fk(&self, fk_col: &ColName, fk_val: &str) -> Vec<String> {
        let mut vals: Vec<String> = vec![];
        for (i, genval) in self.val_generation.iter().enumerate() {
            if &self.cols[i] == fk_col {
                vals.push(fk_val.to_string());
            } else {
                vals.push(gen_value::gen_strval(genval));
            }
        }
        vals
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct TableInfo {
    pub name: TableName,
    pub id_cols: Vec<ColName>,
    pub owner_fk_cols: Vec<ColName>,
    // table, referenced_column, fk column
    pub other_fk_cols: Vec<(TableName, ColName, ColName)>,
}

#[derive(Clone, Serialize, Deserialize)]
pub enum Transformation {
    // XXX Note: we don't have a "RemovePermanent" option because this is just
    // SQL. We could add support for it though.
    Remove {
        pred: String,
        from: String,
    },
    Modify {
        pred: String,
        // which table/joined tables to select from
        from: String,
        // name of column
        col: ColName,
        // how to generate a modified value
        gen_value: gen_value::GenValue,
    },
    // decorrelates all ownership columns of items matching the predicate
    // if there is an invoking UID, this only decorrelates columns correlating to the invoking UID
    // otherwise, this decorrelates all columns if no user is invoking the disguise
    Decor {
        pred: String,
        // which table/joined tables to select from
        from: String,
        // all rows with matching values for these columns will have the same associated
        // pseudoprincipal
        group_by_cols: Vec<String>,
        user_fk_cols: Vec<String>,
    },
}

pub type DisguiseSpec = HashMap<String, Vec<Transformation>>;

#[derive(Clone, PartialEq)]
pub enum RevealPPType {
    Delete,
    Restore,
    Retain,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Disguise {
    pub user: Option<UID>,
    pub table_disguises: HashMap<String, Vec<Transformation>>,
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct RowVal(ColName, String);
impl RowVal {
    pub fn column(&self) -> String {
        self.0.clone()
    }
    pub fn value(&self) -> String {
        self.1.clone()
    }
    pub fn new(c: ColName, r: String) -> RowVal {
        RowVal(c, r)
    }
}

pub struct EdnaClient {
    pub in_memory: bool,
    pub pool: mysql::Pool,
    pub hlapi: highlevel_api::HighLevelAPI,
    pub llapi: Arc<Mutex<lowlevel_api::LowLevelAPI>>,
}

impl EdnaClient {
    pub fn new(
        user: &str,
        password: &str,
        host: &str,
        dbname: &str,
        in_memory: bool,
        proxy: bool,
        dryrun: bool,
    ) -> EdnaClient {
        let url = if proxy {
            format!("mysql://127.0.0.1:{}", 62292)
        } else {
            format!("mysql://{}:{}@{}/{}", user, password, host, dbname)
        };
        let pool = mysql::Pool::new(Opts::from_url(&url).unwrap()).unwrap();

        let llapi = Arc::new(Mutex::new(lowlevel_api::LowLevelAPI::new(
            pool.clone(),
            in_memory,
            true, // for now, reset encdata each time
            dryrun,
        )));
        EdnaClient {
            pool: pool.clone(),
            in_memory: in_memory,
            hlapi: highlevel_api::HighLevelAPI::new(
                llapi.clone(),
                pool.clone(),
                in_memory,
                true, // reset each time for now
            ),
            llapi: llapi,
        }
    }

    pub fn get_sizes(&self, dbname: &str) -> (usize, usize) {
        let locked_llapi = self.llapi.lock().unwrap();
        let bytes = locked_llapi.get_sizes(dbname);
        drop(locked_llapi);
        error!("RCTRLER MEMORY BYTES\t {}", bytes.0);
        error!("RCTRLER PERSISTED BYTES\t {}", bytes.1);
        let hlapi_bytes = self.hlapi.get_sizes(dbname);
        error!(
            "HLAPI BYTES\t MEM {}, PERSIST {}",
            hlapi_bytes.0, hlapi_bytes.1
        );
        bytes
    }
    //-----------------------------------------------------------------------------
    // Necessary to make Edna aware of all principals in the system
    // so Edna can link these to pseudoprincipals/do crypto stuff
    // UID is the foreign-key ID of the principal
    //-----------------------------------------------------------------------------
    pub fn register_principal(&mut self, uid: &UID, password: String) -> records::UserData {
        let mut locked_llapi = self.llapi.lock().unwrap();
        let user_share = locked_llapi.register_principal(uid, password);
        drop(locked_llapi);
        user_share
    }

    pub fn register_principal_without_using_secret_sharing(
        &mut self,
        uid: &UID,
    ) -> records::DecryptCap {
        let mut locked_llapi = self.llapi.lock().unwrap();
        let privkey = locked_llapi.register_principal_without_sharing(uid);
        drop(locked_llapi);
        privkey
    }

    //-----------------------------------------------------------------------------
    // To register and end a disguise (and get the corresponding capabilities)
    //-----------------------------------------------------------------------------
    pub fn start_disguise(&self, invoking_user: Option<UID>) -> DID {
        let mut locked_llapi = self.llapi.lock().unwrap();
        locked_llapi.start_disguise(invoking_user)
    }

    pub fn end_disguise(&self) {
        let mut locked_llapi = self.llapi.lock().unwrap();
        locked_llapi.end_disguise();
        drop(locked_llapi);
    }

    pub fn start_reveal(&self, did: DID) {
        let locked_llapi = self.llapi.lock().unwrap();
        locked_llapi.start_reveal(did);
    }

    pub fn end_reveal(&mut self, did: DID) {
        let mut locked_llapi = self.llapi.lock().unwrap();
        locked_llapi.end_reveal(did);
    }

    //-----------------------------------------------------------------------------
    // Get all records of a particular disguise
    // returns all the diff records and all the speaksfor record blobs
    // Additional function to get and mark records revealed (if records are retrieved for the
    // purpose of reversal)
    //-----------------------------------------------------------------------------
    // note that this does not interface with the HLAPI's ability to track produced pps
    pub fn cleanup_records_of_disguise(&self, did: DID, decrypt_cap: &records::DecryptCap) {
        let mut locked_llapi = self.llapi.lock().unwrap();
        locked_llapi.cleanup_records_of_disguise(did, &decrypt_cap, &mut HashSet::new());
        drop(locked_llapi);
    }

    pub fn get_records_of_disguise(
        &self,
        _did: DID,
        decrypt_cap: &records::DecryptCap,
    ) -> (
        Vec<Vec<u8>>,
        Vec<Vec<u8>>,
        HashMap<UID, records::PrivkeyRecord>,
    ) {
        let mut locked_llapi = self.llapi.lock().unwrap();
        let res = locked_llapi.get_records(decrypt_cap);
        drop(locked_llapi);
        res
    }

    //-----------------------------------------------------------------------------
    // Save arbitrary diffs performed by the disguise for the purpose of later
    // restoring.
    //-----------------------------------------------------------------------------
    pub fn save_diff_record(&self, uid: UID, did: DID, data: Vec<u8>) {
        let mut locked_llapi = self.llapi.lock().unwrap();
        locked_llapi.save_diff_record(uid, did, data);
        drop(locked_llapi);
    }

    //-----------------------------------------------------------------------------
    // Save information about decorrelation/speaksfor
    //-----------------------------------------------------------------------------
    pub fn forget_principal(&mut self, uid: &UID, did: DID) {
        let mut locked_llapi = self.llapi.lock().unwrap();
        locked_llapi.forget_principal(uid, did);
        drop(locked_llapi);
    }

    pub fn register_and_save_pseudoprincipal_record(
        &self,
        did: DID,
        old_uid: &UID,
        new_uid: &UID,
        record_bytes: &Vec<u8>,
    ) {
        let mut locked_llapi = self.llapi.lock().unwrap();
        locked_llapi.register_and_save_pseudoprincipal_record(
            did,
            old_uid,
            new_uid,
            record_bytes.clone(),
        );
        drop(locked_llapi);
    }

    /**********************************************************************
     * If using the high-level spec API where Edna performs DB statements
     **********************************************************************/
    pub fn get_pseudoprincipals(
        &self,
        uid: UID,
        password: Option<String>,
        user_share: Option<(records::Share, records::Loc)>,
    ) -> HashSet<UID> {
        let locked_llapi = self.llapi.lock().unwrap();
        let uids = locked_llapi.get_pseudoprincipals(&uid, password, user_share);
        drop(locked_llapi);
        uids
    }

    // UID is the foreign-key ID of the principal
    pub fn apply_disguise(
        &mut self,
        for_user: UID,
        disguise_spec_json: &str,
        table_info_json: &str,
        guise_gen_json: &str,
        password: Option<String>,
        user_share: Option<(records::Share, records::Loc)>,
        use_txn: bool,
    ) -> Result<DID, mysql::Error> {
        warn!("EDNA: APPLYING Disguise");
        let table_infos: HashMap<TableName, TableInfo> =
            serde_json::from_str(table_info_json).unwrap();
        let guise_gen: PseudoprincipalGenerator = serde_json::from_str(guise_gen_json).unwrap();
        let disguise_spec: DisguiseSpec = serde_json::from_str(disguise_spec_json).unwrap();
        let disguise = Disguise {
            user: if for_user == "NULL" {
                None
            } else {
                Some(for_user)
            },
            table_disguises: disguise_spec,
        };
        let mut db = self.pool.get_conn()?;
        if use_txn {
            let txopts = TxOpts::default();
            txopts.set_isolation_level(Some(Serializable));
            let mut txn = db.start_transaction(txopts)?;
            let res = self.hlapi.apply(
                &disguise,
                &table_infos,
                &guise_gen,
                &mut txn,
                password,
                user_share,
            );
            txn.commit()?;
            return res;
        } else {
            return self.hlapi.apply(
                &disguise,
                &table_infos,
                &guise_gen,
                &mut db,
                password,
                user_share,
            );
        }
    }

    // UID is the foreign-key ID of the principal
    pub fn reveal_disguise(
        &mut self,
        uid: UID,
        did: DID,
        table_info_json: &str,
        guise_gen_json: &str,
        reveal_pps: Option<RevealPPType>,
        password: Option<String>,
        user_share: Option<(records::Share, records::Loc)>,
        use_txn: bool,
    ) -> Result<(), mysql::Error> {
        warn!("EDNA: REVERSING Disguise {}", did);
        let table_infos: HashMap<TableName, TableInfo> =
            serde_json::from_str(table_info_json).unwrap();
        let guise_gen: PseudoprincipalGenerator = serde_json::from_str(guise_gen_json).unwrap();
        let mut db = self.pool.get_conn()?;
        let user = if uid == "NULL" { None } else { Some(&uid) };
        if use_txn {
            let txopts = TxOpts::default();
            txopts.set_isolation_level(Some(Serializable));
            let mut txn = db.start_transaction(txopts)?;
            self.hlapi.reveal(
                user,
                did,
                &table_infos,
                &guise_gen,
                reveal_pps,
                &mut txn,
                password,
                user_share,
            )?;
            txn.commit()?;
        } else {
            self.hlapi.reveal(
                user,
                did,
                &table_infos,
                &guise_gen,
                reveal_pps,
                &mut db,
                password,
                user_share,
            )?;
        }
        Ok(())
    }
}
