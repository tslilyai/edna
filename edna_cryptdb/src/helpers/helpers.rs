use crate::*;
use log::debug;
use serde::Serialize;
use std::collections::{HashMap, HashSet};

pub fn size_of_vec<T>(vec: &Vec<T>) -> usize {
    std::mem::size_of_val(vec) + vec.capacity() * std::mem::size_of::<T>()
}

pub fn serialize_to_bytes<T: Serialize>(item: &T) -> Vec<u8> {
    bincode::serialize(item).unwrap()
}

pub fn get_value_of_col(row: &Vec<RowVal>, col: &str) -> Option<String> {
    for rv in row {
        if &rv.column() == col {
            return Some(rv.value().clone());
        }
    }
    debug!("No value for col {} in row {:?}", col, row);
    None
}

pub fn set_value_of_col(row: &mut Vec<RowVal>, col: &str, val: &str) {
    for rv in row {
        if &rv.column() == col {
            rv.set_value(val);
        }
    }
}

pub fn equal_rows_with_ids(r1: &Vec<RowVal>, r2: &Vec<RowVal>) -> bool {
    // note that we want to just compare the columns that exist, assuming that
    // they contain all the IDs for the row
    let mut r1 = r1;
    let mut r2 = r2;
    if r2.len() < r1.len() {
        let tmp = r1;
        r1 = r2;
        r2 = tmp;
    }
    // check all the columns in the smaller row (r1)
    // let mut hm = HashMap::new();
    // for rv in r2 {hm.insert(rv.column(), rv.value());}
    for (ix, rv) in r1.iter().enumerate() {
        let v1 = rv.value().replace("\'", "");
        let v2 = r2[ix].value().replace("\'", "");
        //if let Some(v) = hm.get(&rv.column()) {
            // compare the stripped versions
        if v1 != v2 {
            return false; 
        }
    }
    return true;
}

pub fn get_ids(id_cols: &Vec<String>, row: &Vec<RowVal>) -> Vec<RowVal> {
    id_cols
        .iter()
        .map(|id_col| RowVal::new(id_col.clone(), get_value_of_col(row, &id_col).unwrap()))
        .collect()
}

pub fn get_owners(owner_cols: &Vec<String>, row: &Vec<RowVal>) -> HashSet<UID> {
    owner_cols
        .iter()
        .map(|id_col| get_value_of_col(row, &id_col).unwrap())
        .collect()
}

pub fn merge_vector_hashmaps<T: Clone>(
    h1: &HashMap<String, Vec<T>>,
    h2: &HashMap<String, Vec<T>>,
) -> HashMap<String, Vec<T>> {
    let mut hm = h1.clone();
    for (k, vs1) in hm.iter_mut() {
        if let Some(vs2) = h2.get(k) {
            vs1.extend_from_slice(vs2);
        }
    }
    for (k, vs2) in h2.iter() {
        if let Some(vs1) = hm.get_mut(k) {
            vs1.extend_from_slice(vs2);
        } else {
            hm.insert(k.to_string(), vs2.clone());
        }
    }
    hm
}

pub fn max(a: u64, b: u64) -> u64 {
    if a >= b {
        a
    } else {
        b
    }
}
