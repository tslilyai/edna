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
pub mod disguiser;
pub mod gen_value;
pub mod helpers;
pub mod lowlevel_api;
pub mod predicate;
pub mod proxy;
pub mod records;
pub mod revealer;

/// disguise ID
pub type DID = u64;
/// user ID
pub type UID = String;
pub type ColName = String;
pub type TableName = String;
pub type UpdateFn = Arc<Mutex<dyn Fn(Vec<TableRow>) -> Vec<TableRow> + Send + Sync>>;

#[derive(Clone)]
pub struct Update {
    t: u64,
    upfn: UpdateFn,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct PseudoprincipalGenerator {
    pub table: TableName,
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
pub struct ForeignKey {
    pub to_table: TableName,
    pub to_col: ColName,
    pub from_table: TableName,
    pub from_col: ColName,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct TableInfo {
    pub table: TableName,
    pub id_cols: Vec<ColName>,
    pub owner_fks: Vec<ForeignKey>,
    pub other_fks: Vec<ForeignKey>,
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
    // decorrelates all ownership of items matching the predicate
    // if there is an invoking UID, this only decorrelates pointers to the invoking UID
    // otherwise, this decorrelates all pointers to any user
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

pub type DisguiseSpec = HashMap<TableName, Vec<Transformation>>;

// how pseudoprincipals with to-references should be handled during reveal
#[derive(Copy, Clone, PartialEq)]
pub enum RevealPPType {
    Delete,  // remove referencing objects and pp
    Restore, // restore ownership of referencing objects to np, remove pp
    Retain,  // keep referencing objects and pp
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
    pub fn set_value(&mut self, r: &str) {
        self.1 = r.to_string();
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, Default, PartialEq, Eq, Hash)]
pub struct TableRow {
    pub row: Vec<RowVal>,
    pub table: TableName,
}

pub struct EdnaClient {
    pub in_memory: bool,
    pub pool: mysql::Pool,
    pub disguiser: disguiser::Disguiser,
    pub revealer: revealer::Revealer,
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
            disguiser: disguiser::Disguiser::new(
                llapi.clone(),
                pool.clone(),
                in_memory,
                true, // reset each time for now
            ),
            revealer: revealer::Revealer::new(llapi.clone(), pool.clone()),
            llapi: llapi,
        }
    }

    pub fn get_space_overhead(&self, dbname: &str) -> (usize, usize) {
        let locked_llapi = self.llapi.lock().unwrap();
        let bytes = locked_llapi.get_space_overhead(dbname);
        drop(locked_llapi);
        error!("RCTRLER MEMORY BYTES\t {}", bytes.0);
        error!("RCTRLER PERSISTED BYTES\t {}", bytes.1);

        // TODO disguiser, revealer, record_ctrler
        bytes
    }

    ///-----------------------------------------------------------------------------
    /// Necessary to make Edna aware of all principals in the system
    /// so Edna can link these to pseudoprincipals/do crypto stuff
    /// UID is the foreign-key ID of the principal
    ///-----------------------------------------------------------------------------
    pub fn register_principal(&mut self, uid: &UID, password: String) -> records::UserData {
        let mut locked_llapi = self.llapi.lock().unwrap();
        let user_share = locked_llapi.register_principal(uid, password);
        drop(locked_llapi);
        user_share
    }

    pub fn register_principal_without_using_secret_sharing(
        &mut self,
        uid: &UID,
    ) -> records::PrivKey {
        let mut locked_llapi = self.llapi.lock().unwrap();
        let privkey = locked_llapi.register_principal_without_sharing(uid);
        drop(locked_llapi);
        privkey
    }

    ///-----------------------------------------------------------------------------
    /// To register and end a disguise (and get the corresponding capabilities)
    ///-----------------------------------------------------------------------------
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

    ///-----------------------------------------------------------------------------
    /// cleanup_records_of_disguise
    /// - Get all records of a particular disguise
    /// - Returns all the diff records and all the speaksfor record blobs
    /// - Additional function to get and mark records revealed (if records are retrieved for the
    /// purpose of reversal)
    ///-----------------------------------------------------------------------------
    /// note that this does not interface with the disguiser's ability to track produced pps
    pub fn cleanup_records_of_disguise(&self, did: DID, privkey: &records::PrivKey) {
        let mut locked_llapi = self.llapi.lock().unwrap();
        locked_llapi.cleanup_records_of_disguise(did, &privkey);
        drop(locked_llapi);
    }

    pub fn get_records_of_disguise(
        &self,
        _did: DID,
        privkey: &records::PrivKey,
    ) -> (Vec<Vec<u8>>, HashMap<UID, records::SFChainRecord>) {
        let mut locked_llapi = self.llapi.lock().unwrap();
        let res = locked_llapi.get_records(privkey);
        drop(locked_llapi);
        res
    }

    ///-----------------------------------------------------------------------------
    /// Save arbitrary diffs performed by the disguise for the purpose of later
    /// restoring.
    ///-----------------------------------------------------------------------------
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

    pub fn register_pseudoprincipal(&self, did: DID, old_uid: &UID, new_uid: &UID, pp: TableRow) {
        let mut locked_llapi = self.llapi.lock().unwrap();
        locked_llapi.register_pseudoprincipal(did, old_uid, new_uid, pp);
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
    pub fn apply_disguise_rust(
        &mut self,
        for_user: UID,
        disguise_spec: DisguiseSpec,
        table_infos: HashMap<TableName, TableInfo>,
        guise_gen: PseudoprincipalGenerator,
        password: Option<String>,
        user_share: Option<(records::Share, records::Loc)>,
        use_txn: bool,
    ) -> Result<DID, mysql::Error> {
        warn!("EDNA: APPLYING Disguise");
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
            let res = self.disguiser.apply(
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
            return self.disguiser.apply(
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
            let res = self.disguiser.apply(
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
            return self.disguiser.apply(
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
    pub fn reveal_disguise_rust(
        &mut self,
        uid: UID,
        did: DID,
        table_infos: HashMap<TableName, TableInfo>,
        guise_gen: PseudoprincipalGenerator,
        reveal_pps: Option<RevealPPType>,
        allow_singlecolumn_reveals: bool,
        password: Option<String>,
        user_share: Option<(records::Share, records::Loc)>,
        use_txn: bool,
    ) -> Result<(), mysql::Error> {
        warn!("EDNA: REVERSING Disguise {}", did);
        let start = time::Instant::now();
        let mut db = self.pool.get_conn()?;
        let user = if uid == "NULL" { None } else { Some(&uid) };
        if use_txn {
            let txopts = TxOpts::default();
            txopts.set_isolation_level(Some(Serializable));
            let mut txn = db.start_transaction(txopts)?;
            self.revealer.reveal(
                user,
                did,
                &table_infos,
                &guise_gen,
                reveal_pps,
                allow_singlecolumn_reveals,
                &mut txn,
                password,
                user_share,
            )?;
            let txnstart = time::Instant::now();
            txn.commit()?;
            warn!("commit txn took {}mus", txnstart.elapsed().as_micros());
        } else {
            self.revealer.reveal(
                user,
                did,
                &table_infos,
                &guise_gen,
                reveal_pps,
                allow_singlecolumn_reveals,
                &mut db,
                password,
                user_share,
            )?;
        }
        warn!("reveal_disguise took {}mus", start.elapsed().as_micros());
        Ok(())
    }
    // UID is the foreign-key ID of the principal
    pub fn reveal_disguise(
        &mut self,
        uid: UID,
        did: DID,
        table_info_json: &str,
        guise_gen_json: &str,
        reveal_pps: Option<RevealPPType>,
        allow_singlecolumn_reveals: bool,
        password: Option<String>,
        user_share: Option<(records::Share, records::Loc)>,
        use_txn: bool,
    ) -> Result<(), mysql::Error> {
        warn!("EDNA: REVERSING Disguise {}", did);
        let start = time::Instant::now();
        let table_infos: HashMap<TableName, TableInfo> =
            serde_json::from_str(table_info_json).unwrap();
        let guise_gen: PseudoprincipalGenerator = serde_json::from_str(guise_gen_json).unwrap();
        warn!(
            "reveal_disguise deserialize took {}mus",
            start.elapsed().as_micros()
        );
        let start = time::Instant::now();
        let mut db = self.pool.get_conn()?;
        let user = if uid == "NULL" { None } else { Some(&uid) };
        if use_txn {
            let txopts = TxOpts::default();
            txopts.set_isolation_level(Some(Serializable));
            let mut txn = db.start_transaction(txopts)?;
            self.revealer.reveal(
                user,
                did,
                &table_infos,
                &guise_gen,
                reveal_pps,
                allow_singlecolumn_reveals,
                &mut txn,
                password,
                user_share,
            )?;
            let txnstart = time::Instant::now();
            txn.commit()?;
            warn!("commit txn took {}mus", txnstart.elapsed().as_micros());
        } else {
            self.revealer.reveal(
                user,
                did,
                &table_infos,
                &guise_gen,
                reveal_pps,
                allow_singlecolumn_reveals,
                &mut db,
                password,
                user_share,
            )?;
        }
        warn!("reveal_disguise took {}mus", start.elapsed().as_micros());
        Ok(())
    }

    pub fn record_update<F>(&mut self, f: F)
    where
        F: Fn(Vec<TableRow>) -> Vec<TableRow> + 'static + Send + Sync,
    {
        let start = time::Instant::now();
        let mut locked_llapi = self.llapi.lock().unwrap();
        locked_llapi
            .record_ctrler
            .record_update(Arc::new(Mutex::new(f)));
        drop(locked_llapi);
        warn!("record_update took {}mus", start.elapsed().as_micros());
    }
}
