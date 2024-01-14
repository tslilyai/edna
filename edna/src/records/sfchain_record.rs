use crate::*;
use crate::{UID};
use log::debug;
use serde::{Deserialize, Serialize};
//use std::mem::size_of_val;
use std::collections::{HashMap, HashSet, VecDeque};

#[derive(Default, Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct SFChainRecord {
    pub old_uid: UID,
    pub new_uid: UID,
    pub priv_key: Vec<u8>,
}

pub fn find_old_uid(
    map: &HashMap<UID, Vec<SFChainRecord>>,
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

pub fn find_path_to(map: &HashMap<UID, Vec<SFChainRecord>>, to: &UID) -> Vec<SFChainRecord> {
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

pub fn sfchain_record_from_bytes(bytes: &Vec<u8>) -> SFChainRecord {
    bincode::deserialize(bytes).unwrap()
}

pub fn sfchain_records_from_bytes(bytes: &Vec<u8>) -> Vec<SFChainRecord> {
    bincode::deserialize(bytes).unwrap()
}

pub fn new_sfchain_record(old_uid: UID, new_uid: UID, priv_key: Vec<u8>) -> SFChainRecord {
    let mut record: SFChainRecord = Default::default();
    record.old_uid = old_uid;
    record.new_uid = new_uid;
    record.priv_key = priv_key;
    record
}
