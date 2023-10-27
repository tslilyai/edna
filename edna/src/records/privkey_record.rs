use crate::UID;
use log::debug;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};

#[derive(Default, Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PrivkeyRecord {
    pub old_uid: UID,
    pub new_uid: UID,
    pub priv_key: Vec<u8>,
}

pub fn find_old_uid(
    map: &HashMap<UID, Vec<PrivkeyRecord>>,
    from: &UID,
    recorrelated: &HashSet<UID>,
) -> Option<UID> {
    let mut path = vec![];
    let mut q = VecDeque::new();
    q.push_back(from);

    while let Some(from) = q.pop_front() {
        if let Some(vs) = map.get(from) {
            for child in vs {
                debug!("Path: {}->{}", child.old_uid, child.new_uid);
                path.push(child.clone());
                if recorrelated.get(&child.old_uid).is_none() {
                    return Some(child.old_uid.clone());
                }
                q.push_back(&child.old_uid);
            }
        }
    }
    return None;
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
    record
}
