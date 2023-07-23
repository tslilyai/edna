use crate::UID;
use log::debug;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
//use log::error;
//use std::mem::size_of_val;

#[derive(Default, Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PrivkeyRecord {
    pub old_uid: UID,
    pub new_uid: UID,
    pub priv_key: Vec<u8>,
}

pub fn find_path_to(map: &HashMap<UID, Vec<PrivkeyRecord>>, to: &UID) -> Vec<PrivkeyRecord> {
    let mut path = vec![];
    let mut q = VecDeque::new();
    q.push_back(to);

    while let Some(to) = q.pop_front() {
        if let Some(vs) = map.get(to) {
            for child in vs {
                debug!("Path: {}->{}", child.old_uid, child.new_uid);
                path.push(child.clone());
                q.push_back(&child.old_uid);
            }
        }
    }
    return path;
}

pub fn privkey_record_from_bytes(bytes: &Vec<u8>) -> PrivkeyRecord {
    bincode::deserialize(bytes).unwrap()
}
pub fn privkey_records_from_bytes(bytes: &Vec<u8>) -> Vec<PrivkeyRecord> {
    bincode::deserialize(bytes).unwrap()
}
pub fn new_privkey_record(old_uid: UID, new_uid: UID, priv_key: Vec<u8>) -> PrivkeyRecord {
    let mut record: PrivkeyRecord = Default::default();
    record.old_uid = old_uid;
    record.new_uid = new_uid;
    record.priv_key = priv_key;
    /*error!("PK DATA: new_uid {}, pk {}, all: {}",
        size_of_val(&*record.new_uid),
        size_of_val(&*record.priv_key),
        size_of_val(&record),
    );*/
    record
}
