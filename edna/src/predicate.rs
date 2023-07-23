use crate::helpers::*;
use crate::records::*;
use crate::UID;
use log::debug;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::str::FromStr;
use std::*;

#[derive(Clone, Serialize, Deserialize, Eq, PartialEq)]
pub enum PredSpec {
    True,
    False,
    Eq {
        col: String,
        val: String,
    },
    BitwiseAnd {
        col: String,
        val: u64,
    },
    NotEq {
        col: String,
        val: String,
    },
    And(Vec<PredSpec>),
    Or(Vec<PredSpec>),
    Join {
        tab1: String,
        tab2: String,
        col1: String,
        col2: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BinOp {
    Gt,
    Lt,
    GtEq,
    LtEq,
    Eq,
    NotEq,
    And,
    Or,
    Plus,
    Minus,
    BitwiseAnd,
    BitwiseOr,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Pred {
    ColInList {
        col: String,
        vals: Vec<String>,
        neg: bool,
    },

    ColColCmp {
        col1: String,
        col2: String,
        op: BinOp,
    },

    ColValCmp {
        col: String,
        val: String,
        op: BinOp,
    },
    Bool(bool),
    And(Vec<Pred>),
    Or(Vec<Pred>),
    Join {
        tab1: String,
        tab2: String,
        col1: String,
        col2: String,
    },
}

impl ToString for Pred {
    fn to_string(&self) -> String {
        use Pred::*;
        match self {
            ColInList { col, vals, neg } => {
                let strvals: Vec<String> = vals.iter().map(|v| format!("\'{}\'", v)).collect();
                match neg {
                    true => format!("{} NOT IN ({})", col, strvals.join(",")),
                    false => format!("{} IN ({})", col, strvals.join(",")),
                }
            }

            ColColCmp { col1, col2, op } => {
                use BinOp::*;
                match op {
                    Gt => format!("{} > {}", col1, col2),
                    Lt => format!("{} < {}", col1, col2),
                    GtEq => format!("{} >= {}", col1, col2),
                    LtEq => format!("{} <= {}", col1, col2),
                    Eq => format!("{} = {}", col1, col2),
                    NotEq => format!("{} != {}", col1, col2),
                    And => format!("{} AND {}", col1, col2),
                    Or => format!("{} OR {}", col1, col2),
                    _ => unimplemented!("No support for op {:?}", op),
                }
            }
            ColValCmp { col, val, op } => {
                use BinOp::*;
                match op {
                    Gt => format!("{} > {}", col, val),
                    Lt => format!("{} < {}", col, val),
                    GtEq => format!("{} >= {}", col, val),
                    LtEq => format!("{} <= {}", col, val),
                    // escape strings
                    Eq => {
                        if val.chars().all(char::is_numeric) {
                            format!("{} = {}", col, val)
                        } else {
                            format!("{} = '{}'", col, val)
                        }
                    }
                    NotEq => {
                        if val.chars().all(char::is_numeric) {
                            format!("{} != {}", col, val)
                        } else {
                            format!("{} != '{}'", col, val)
                        }
                    }
                    And => format!("{} AND {}", col, val),
                    Or => format!("{} OR {}", col, val),
                    BitwiseAnd => format!("{} & {}", col, val),
                    _ => unimplemented!("No support for op {:?}", op),
                }
            }
            Bool(b) => b.to_string(),
            Pred::Or(ps) => {
                let ps: Vec<String> = ps.iter().map(|p| p.to_string()).collect();
                format!("({})", ps.join(" OR "))
            }
            Pred::And(ps) => {
                let ps: Vec<String> = ps.iter().map(|p| p.to_string()).collect();
                format!("({})", ps.join(" AND "))
            }
            Pred::Join {
                tab1,
                tab2,
                col1,
                col2,
            } => {
                format!("{}.{} = {}.{}", tab1, col1, tab2, col2)
            }
        }
    }
}

// adds a filter to pred to filter by ownership by UID, if UID exists
pub fn owner_filter_pred(
    table: &str,
    uid: &Option<UID>,
    owner_cols: &Vec<String>,
    sf_records: &Vec<SpeaksForRecordWrapper>,
) -> Pred {
    let p = match uid {
        None => PredSpec::True,
        Some(uid) => {
            assert!(!owner_cols.is_empty());
            let mut init_pred = vec![];
            for fk in owner_cols {
                init_pred.push(PredSpec::Eq {
                    col: format!("{}.{}", table, fk),
                    val: uid.clone(),
                });
            }
            PredSpec::Or(init_pred)
        }
    };
    predspec_to_owned_pred(owner_cols, sf_records, &p)
}

// rewrites pred with owners as specified in the sf_records
fn predspec_to_owned_pred(
    owner_cols: &Vec<String>,
    sf_records: &Vec<SpeaksForRecordWrapper>,
    p: &PredSpec,
) -> Pred {
    use PredSpec::*;
    match p {
        True => Pred::Bool(true),
        False => Pred::Bool(false),
        Join {
            tab1,
            tab2,
            col1,
            col2,
        } => Pred::Join {
            tab1: tab1.clone(),
            tab2: tab2.clone(),
            col1: col1.clone(),
            col2: col2.clone(),
        },
        And(ps) => {
            let mut init_pred = vec![];
            for pt in ps {
                init_pred.push(predspec_to_owned_pred(owner_cols, sf_records, pt));
            }
            Pred::And(init_pred)
        }
        Or(ps) => {
            let mut init_pred = vec![];
            for pt in ps {
                init_pred.push(predspec_to_owned_pred(owner_cols, sf_records, pt));
            }
            Pred::Or(init_pred)
        }
        BitwiseAnd { col, val } => Pred::ColValCmp {
            col: col.clone(),
            val: val.to_string(),
            op: BinOp::BitwiseAnd,
        },
        // if col is a fk_col, match against all UIDs that the specified
        // val-uid user can speak for (including the original).
        // otherwise, just match against the value
        Eq { col, val } => {
            let mut new_owners: Vec<String> = vec![val.clone()];
            let mut found = false;
            let col_end = col.split(".").last().unwrap();
            for fk_col in owner_cols {
                if col_end == fk_col {
                    found = true;
                    new_owners.append(
                        &mut sf_records
                            .iter()
                            .filter(|ot| &ot.old_uid == val)
                            .map(|ot| ot.new_uid.to_string())
                            .collect(),
                    );
                    break;
                }
            }
            if found && !sf_records.is_empty() {
                Pred::ColInList {
                    col: col.clone(),
                    vals: new_owners,
                    neg: false,
                }
            } else {
                Pred::ColValCmp {
                    col: col.clone(),
                    val: val.clone(),
                    op: BinOp::Eq,
                }
            }
        }
        // if col is a fk_col, match against all UIDs that the specified
        // val=UID can speak for (including the original).
        // otherwise, just match against the value
        NotEq { col, val } => {
            let mut found = false;
            let mut new_owners: Vec<String> = vec![val.clone()];
            let col_end = col.split(".").last().unwrap();
            for fk_col in owner_cols {
                if col_end == fk_col {
                    found = true;
                    new_owners.append(
                        &mut sf_records
                            .iter()
                            .filter(|ot| &ot.old_uid == val)
                            .map(|ot| ot.new_uid.to_string())
                            .collect(),
                    );
                    break;
                }
            }
            if found && !sf_records.is_empty() {
                Pred::ColInList {
                    col: col.clone(),
                    vals: new_owners,
                    neg: true,
                }
            } else {
                Pred::ColValCmp {
                    col: col.clone(),
                    val: val.clone(),
                    op: BinOp::NotEq,
                }
            }
        }
    }
}

pub fn diff_record_matches_pred(pred: &Pred, name: &str, t: &EdnaDiffRecord) -> bool {
    if t.table != name {
        return false;
    }
    if predicate_applies_with_col(pred, &t.col, &t.old_val)
        || predicate_applies_with_col(pred, &t.col, &t.new_val)
    {
        //debug!("Pred: SpeaksForRecord matched pred {:?}! Pushing matching to len {}\n", pred, matching.len());
        return true;
    }
    false
}

pub fn get_speaksfor_records_matching_pred(
    pred: &Pred,
    fk_col: &str,
    records: &Vec<SpeaksForRecordWrapper>,
) -> Vec<SpeaksForRecordWrapper> {
    let mut matching = vec![];
    for t in records {
        if predicate_applies_with_col(pred, fk_col, &t.old_uid)
            || predicate_applies_with_col(pred, fk_col, &t.new_uid)
        {
            debug!(
                "Pred: SpeaksForRecord matched pred {:?}! Pushing matching to len {}\n",
                pred,
                matching.len()
            );
            matching.push(t.clone());
        }
    }
    matching
}

fn predicate_applies_with_col(p: &Pred, c: &str, v: &str) -> bool {
    use Pred::*;
    let mut ret = false;
    match p {
        Pred::Or(ps) => {
            for p in ps {
                if predicate_applies_with_col(p, c, &v) {
                    ret = true;
                    break;
                }
            }
        }
        Pred::And(ps) => {
            let mut all_true = true;
            for clause in ps {
                if !predicate_applies_with_col(clause, c, &v) {
                    all_true = false;
                    break;
                }
            }
            ret = all_true;
        }
        ColInList { col, vals, neg } => {
            let found = col == c && vals.iter().find(|v2| v2.to_string() == v).is_some();
            ret = found != *neg;
        }
        ColColCmp { .. } => unimplemented!("No speaksfor comparison of cols"),
        ColValCmp { col, val, op } => {
            if c == col {
                ret = vals_satisfy_cmp(&v.to_string(), &val, &op);
            } else {
                ret = false;
            }
        }
        Bool(b) => ret = *b,
        _ => unimplemented!("No support for pred"),
    }
    debug!(
        "Predicate {:?} applies with col {} and val {}? {}\n",
        p, c, v, ret
    );
    ret
}

pub fn compute_op(lval: &str, rval: &str, op: &BinOp) -> String {
    let v1 = f64::from_str(lval).unwrap();
    let v2 = f64::from_str(rval).unwrap();
    match op {
        BinOp::Plus => (v1 + v2).to_string(),
        BinOp::Minus => (v1 - v2).to_string(),
        _ => unimplemented!("bad compute binop"),
    }
}

pub fn vals_satisfy_cmp(lval: &str, rval: &str, op: &BinOp) -> bool {
    let cmp = string_vals_cmp(&lval, &rval);
    match op {
        BinOp::Eq => cmp == Ordering::Equal,
        BinOp::NotEq => cmp != Ordering::Equal,
        BinOp::Lt => cmp == Ordering::Less,
        BinOp::Gt => cmp == Ordering::Greater,
        BinOp::LtEq => cmp != Ordering::Greater,
        BinOp::GtEq => cmp != Ordering::Less,
        _ => unimplemented!("bad binop"),
    }
}
