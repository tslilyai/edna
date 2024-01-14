use crate::crypto::*;
use crate::helpers::*;
use crate::records::*;
use crate::UID;
use base64;
use bincode;
use crypto_box::PublicKey;
use log::{debug, error, info};
use mysql::prelude::*;
use num_bigint::BigInt;
use std::collections::HashSet;
use std::str::FromStr;
use std::time;

// NOTE we could instead just save blobs of the entire state, rather than per-bag/per-principal
// granularity
const PRINCIPAL_TABLE: &'static str = "EdnaPrincipals";
const SHARES_TABLE: &'static str = "EdnaShares";
const BAGTABLE: &'static str = "EdnaBags";
const ENCLOCSTABLE: &'static str = "EdnaEncLocs";
const UID_COL: &'static str = "uid";

pub struct RecordPersister {}

impl RecordPersister {
    pub fn init(db: &mut mysql::PooledConn, in_memory: bool, reset: bool) {
        db.query_drop("SET max_heap_table_size = 4294967295;")
            .unwrap();
        let _engine = if in_memory { "InnoDB" } else { "InnoDB" };

        if reset {
            db.query_drop(format!("DROP TABLE IF EXISTS {}", PRINCIPAL_TABLE))
                .unwrap();
            db.query_drop(format!("DROP TABLE IF EXISTS {}", SHARES_TABLE))
                .unwrap();
            db.query_drop(format!("DROP TABLE IF EXISTS {}", BAGTABLE))
                .unwrap();
            db.query_drop(format!("DROP TABLE IF EXISTS {}", ENCLOCSTABLE))
                .unwrap();
        }

        // create principals table
        db.query_drop(format!("CREATE TABLE IF NOT EXISTS {} ({} varchar(255), is_anon tinyint, pubkey varchar(1024), locs varchar(2048), PRIMARY KEY ({})) ENGINE = InnoDB;",
        PRINCIPAL_TABLE, UID_COL, UID_COL)).unwrap();

        // create shares table
        db.query_drop(format!("CREATE TABLE IF NOT EXISTS {} (loc BIGINT UNSIGNED, edna_share_x_val varchar(1024), edna_share_y_value varchar(1024), user_share_value varchar(1024), password_salt varchar(1024), PRIMARY KEY (loc)) ENGINE = InnoDB;",
        SHARES_TABLE)).unwrap();

        // create encbags table
        // InnoDB for blobs
        db.query_drop(format!(
        "CREATE TABLE IF NOT EXISTS {} (loc BIGINT UNSIGNED, encbag MEDIUMBLOB, PRIMARY KEY (loc)) ENGINE = InnoDB;",
        BAGTABLE)).unwrap();

        // create enclocss table
        db.query_drop(format!(
        "CREATE TABLE IF NOT EXISTS {} (id BIGINT UNSIGNED, enclocs MEDIUMBLOB, PRIMARY KEY (id)) ENGINE = InnoDB;",
        ENCLOCSTABLE)).unwrap();
    }

    pub fn get_space_overhead(db: &mut mysql::PooledConn, dbname: &str) -> usize {
        // principaldata
        let rows = get_query_rows_str(
            &format!(
                "SELECT \
                (DATA_LENGTH + INDEX_LENGTH) AS `Size (MB)` \
                FROM information_schema.TABLES \
                WHERE TABLE_SCHEMA = \'{}\' AND TABLE_NAME = \'{}\'",
                dbname, PRINCIPAL_TABLE
            ),
            db,
        )
        .unwrap();
        let pbytes = usize::from_str(&rows[0][0].value()).unwrap();

        // encbags
        let rows = get_query_rows_str(
            &format!(
                "SELECT \
                (DATA_LENGTH + INDEX_LENGTH) AS `Size (MB)` \
                FROM information_schema.TABLES \
                WHERE TABLE_SCHEMA = \'{}\' AND TABLE_NAME = \'{}\'",
                dbname, BAGTABLE
            ),
            db,
        )
        .unwrap();
        let ebbytes = usize::from_str(&rows[0][0].value()).unwrap();

        // locs
        let rows = get_query_rows_str(
            &format!(
                "SELECT \
                (DATA_LENGTH + INDEX_LENGTH) AS `Size (MB)` \
                FROM information_schema.TABLES \
                WHERE TABLE_SCHEMA = \'{}\' AND TABLE_NAME = \'{}\'",
                dbname, ENCLOCSTABLE
            ),
            db,
        )
        .unwrap();
        let elsbytes = usize::from_str(&rows[0][0].value()).unwrap();

        // shares
        let rows = get_query_rows_str(
            &format!(
                "SELECT \
                (DATA_LENGTH + INDEX_LENGTH) AS `Size (MB)` \
                FROM information_schema.TABLES \
                WHERE TABLE_SCHEMA = \'{}\' AND TABLE_NAME = \'{}\'",
                dbname, SHARES_TABLE
            ),
            db,
        )
        .unwrap();
        let ssbytes = usize::from_str(&rows[0][0].value()).unwrap();

        // everything
        let rows = get_query_rows_str(
            &format!(
                "SELECT sum( data_length + index_length) as 'Size (MB)'   
            FROM     information_schema.TABLES
            WHERE TABLE_SCHEMA = \'{}\';",
                dbname
            ),
            db,
        )
        .unwrap();
        let allbytes = usize::from_str(&rows[0][0].value()).unwrap();
        error!(
            "allbytes, pbytes, ssbytes, ebbytes, elbytes: {}, {}, {}, {}, {}",
            allbytes, pbytes, ssbytes, ebbytes, elsbytes
        );
        allbytes
    }

    pub fn get_principal_rows(
        db: &mut mysql::PooledConn,
    ) -> Vec<(UID, bool, Option<PublicKey>, Index)> {
        let mut ret = vec![];
        let principal_rows =
            get_query_rows_str(&format!("SELECT * FROM {}", PRINCIPAL_TABLE), db).unwrap();
        for row in principal_rows {
            let uid: UID = row[0].value();
            let is_anon: bool = row[1].value() == "1";
            let pkbytes = base64::decode(&row[2].value()).unwrap();
            let pubkey = if pkbytes.is_empty() {
                None
            } else {
                Some(PublicKey::from(get_pk_bytes(pkbytes)))
            };
            let index: Index = u64::from_str(&row[3].value()).unwrap();
            ret.push((uid, is_anon, pubkey, index));
        }
        ret
    }

    pub fn get_encbag_rows(db: &mut mysql::PooledConn) -> Vec<(Loc, EncData)> {
        let mut ret = vec![];
        let encbag_rows = get_query_rows_str(&format!("SELECT * FROM {}", BAGTABLE), db).unwrap();
        for row in encbag_rows {
            let loc: Loc = u64::from_str(&row[0].value()).unwrap();
            let edata_bytes = base64::decode(&row[1].value()).unwrap();
            let edata = bincode::deserialize(&edata_bytes).unwrap();
            ret.push((loc, edata));
        }
        ret
    }

    pub fn update_principal_locs<Q: Queryable>(uid: &str, loc_caps: &HashSet<EncData>, db: &mut Q) {
        let uidstr = uid.trim_matches('\'');
        db.query_drop(&format!(
            "UPDATE {} SET {} = \'{}\' WHERE {} = \'{}\'",
            PRINCIPAL_TABLE,
            "locs",
            base64::encode(&bincode::serialize(loc_caps).unwrap()),
            UID_COL,
            uidstr
        ))
        .unwrap();
    }

    pub fn get_share_store_rows(
        db: &mut mysql::PooledConn,
    ) -> Vec<(
        Loc,
        Option<Share>,
        Option<ShareValue>,
        Option<String>,
        HashSet<EncData>,
    )> {
        let mut ret = vec![];
        let share_store_rows =
            get_query_rows_str(&format!("SELECT * FROM {}", SHARES_TABLE), db).unwrap();
        for row in share_store_rows {
            let loc: Loc = u64::from_str(&row[0].value()).unwrap();

            let key_int_x_val = BigInt::from_str(&row[1].value()).unwrap();
            let key_int_y_val = BigInt::from_str(&row[2].value()).unwrap();
            let secretkey_share: Option<Share> = Some([key_int_x_val, key_int_y_val]);

            let share_func_int_val = BigInt::from_str(&row[3].value()).unwrap();
            let share_func_value: Option<ShareValue> = Some(share_func_int_val);

            let password_salt: Option<String> = Some(row[4].value());

            let locs_bytes = base64::decode(&row[5].value()).unwrap();
            let locs = bincode::deserialize(&locs_bytes).unwrap();
            ret.push((loc, secretkey_share, share_func_value, password_salt, locs));
        }
        ret
    }

    pub fn persist_share<Q: Queryable>(shares_to_insert: &Vec<(Loc, ShareStore)>, db: &mut Q) {
        let start = time::Instant::now();
        let mut values = vec![];
        for (loc, share_store) in shares_to_insert {
            let edna_share_x_val = share_store.share[0].to_string();
            let edna_share_y_value = share_store.share[1].to_string();
            let user_share_value = share_store.share_value.to_string();
            let password_salt = share_store.password_salt.as_str();
            // order: edna_share_x_val, edna_share_y_value, user_share_value, password_salt
            values.push(format!(
                "(\'{}\', \'{}\', \'{}\', \'{}\', \'{}\')",
                loc, edna_share_x_val, edna_share_y_value, user_share_value, password_salt
            ));
        }
        let insert_q = format!(
            "INSERT INTO {} (loc, edna_share_x_val, edna_share_y_value, user_share_value, password_salt) \
               VALUES {};",
            SHARES_TABLE,
            values.join(", "),
        );
        //debug!("Persist Principals insert q {}", insert_q);
        db.query_drop(&insert_q).unwrap();
        debug!(
            "Edna persist {} shares: {}",
            shares_to_insert.len(),
            start.elapsed().as_micros()
        );
    }

    pub fn persist_inserted_principals<Q: Queryable>(
        principals_to_insert: &Vec<(UID, PrincipalData)>,
        db: &mut Q,
    ) {
        let start = time::Instant::now();
        let mut values = vec![];
        for (uid, pdata) in principals_to_insert {
            let pubkey_vec = match &pdata.pubkey {
                Some(pk) => pk.as_bytes().to_vec(),
                None => vec![],
            };
            let v: Vec<String> = vec![];
            let empty_vec = base64::encode(&bincode::serialize(&v).unwrap());
            let uid = uid.trim_matches('\'');
            // order: uid, is_anon, edna_share_x_value, edna_share_y_value, user_share_value, password_salt, email_pw_hash, pubkey
            values.push(format!(
                "(\'{}\', {}, \'{}\', \'{}\')",
                uid,
                if pdata.is_anon { 1 } else { 0 },
                base64::encode(&pubkey_vec),
                empty_vec
            ));
        }
        let insert_q = format!(
            "INSERT INTO {} ({}, is_anon, pubkey, locs) \
               VALUES {} ON DUPLICATE KEY UPDATE {} = VALUES({});",
            PRINCIPAL_TABLE,
            UID_COL,
            values.join(", "),
            "locs",
            "locs",
        );
        //debug!("Persist Principals insert q {}", insert_q);
        db.query_drop(&insert_q).unwrap();
        debug!(
            "Edna persist {} principals: {}",
            principals_to_insert.len(),
            start.elapsed().as_micros()
        );
    }

    pub fn remove_principals<Q: Queryable>(uids: &HashSet<String>, db: &mut Q) {
        if uids.is_empty() {
            return;
        }
        let start = time::Instant::now();
        let stmt = format!(
            "DELETE FROM {} WHERE {} IN ({})",
            PRINCIPAL_TABLE,
            UID_COL,
            uids.iter()
                .map(|uid| format!("\'{}\'", uid.trim_matches('\'')))
                .collect::<Vec<String>>()
                .join(",")
        );
        db.query_drop(stmt.clone()).unwrap();
        info!("{} total: {}", stmt, start.elapsed().as_micros());
    }

    pub fn update_enc_bag_at_loc<Q: Queryable>(loc: u64, encbag: &EncData, db: &mut Q) {
        let start = time::Instant::now();
        let bytes = base64::encode(&bincode::serialize(encbag).unwrap());
        let val = format!("({}, \'{}\')", loc, bytes);

        let insert_q = format!(
            "INSERT INTO {} (loc, encbag) \
               VALUES {} ON DUPLICATE KEY UPDATE encbag = VALUES(encbag);",
            BAGTABLE, val
        );
        info!("Insert encbag {}", insert_q);
        db.query_drop(&insert_q).unwrap();
        info!("{} total: {}", insert_q, start.elapsed().as_micros());
    }

    pub fn update_enc_locs_at_index<Q: Queryable>(
        index: u64,
        enclocs: &HashSet<EncData>,
        db: &mut Q,
    ) {
        let start = time::Instant::now();
        let insert_q = format!(
            "INSERT INTO {} (id, enclocs) \
               VALUES ({}, \'{}\') ON DUPLICATE KEY UPDATE enclocs = VALUES(enclocs);",
            ENCLOCSTABLE,
            index,
            base64::encode(&bincode::serialize(enclocs).unwrap()),
        );
        db.query_drop(&insert_q).unwrap();
        info!("{} total: {}", insert_q, start.elapsed().as_micros());
    }

    pub fn remove_enc_bag_at_loc<Q: Queryable>(loc: u64, db: &mut Q) {
        let start = time::Instant::now();
        db.query_drop(format!("DELETE FROM {} WHERE loc = {}", BAGTABLE, loc))
            .unwrap();
        info!(
            "EncBag delete {} total: {}",
            loc,
            start.elapsed().as_micros()
        );
    }

    pub fn remove_enc_locs_at_index<Q: Queryable>(index: u64, db: &mut Q) {
        debug!("Remove encloc at {}", index);
        db.query_drop(format!("DELETE FROM {} WHERE id = {}", ENCLOCSTABLE, index))
            .unwrap();
    }
}
