use crate::crypto::EncData;
use crate::helpers::*;
use crate::proxy::{RowKey, TableKeys};
use crate::*;
use crypto_box::{aead::Aead, PublicKey, SecretKey};
use log::{debug, info, warn};
use std::collections::HashSet;
use std::time;

pub fn encrypt_det_with_pubkey(
    pubkey: &PublicKey,
    secretkey: &SecretKey,
    nonce: &Vec<u8>,
    bytes: &Vec<u8>,
) -> EncData {
    let start = time::Instant::now();
    // this generates a new secret key each time
    let edna_pubkey = PublicKey::from(secretkey);
    let salsabox = crypto_box::Box::new(pubkey, secretkey);
    let encrypted = salsabox
        .encrypt(nonce.as_slice().into(), &bytes[..])
        .unwrap();
    info!("pubkey encrypt: {}", start.elapsed().as_micros());
    EncData {
        encdata: encrypted,
        nonce: nonce.to_vec(),
        pubkey: edna_pubkey.as_bytes().to_vec(),
    }
}

pub fn string_to_common_val(val: &str) -> mysql_common::value::Value {
    if val.to_lowercase() == "NULL" {
        mysql_common::value::Value::NULL
    } else {
        match val.parse::<i64>() {
            Ok(i) => mysql_common::value::Value::Int(i),
            _ => mysql_common::value::Value::Bytes(
                trim_quotes(&std::str::from_utf8(&base64::decode(val).unwrap()).unwrap())
                    .as_bytes()
                    .to_vec(),
            ),
        }
    }
}

pub fn write_decrypted_mysql_answer_rows<W: io::Write>(
    q: &str,
    results: msql_srv::QueryResultWriter<W>,
    logged_in_keys_enc: &HashMap<PublicKey, TableKeys>,
    table: &str,
    enccols: &HashSet<String>,
    rows: mysql::Result<mysql::QueryResult<mysql::Text>>,
) -> Result<(), mysql::Error> {
    match rows {
        Ok(rows) => {
            let start = time::Instant::now();
            let cols: Vec<_> = rows
                .columns()
                .as_ref()
                .into_iter()
                .map(|c| msql_srv::Column {
                    table: c.table_str().to_string(),
                    column: c.name_str().to_string(),
                    coltype: get_msql_srv_coltype(&c.column_type()),
                    colflags: msql_srv::ColumnFlags::from_bits(c.flags().bits()).unwrap(),
                })
                .collect();
            let strcols = cols.iter().map(|c| c.column.to_string()).collect();
            let mut writer = results.start(&cols)?;
            let mut nrows = 0;
            for row in rows {
                let vals = row.unwrap().unwrap();
                let strvals: Vec<String> = vals
                    .iter()
                    .map(|v| trim_quotes(&v.as_sql(false)).to_string())
                    .collect();
                let mut ndecrypt = 0;
                'decrypt_loop: for (_pk, table_keys) in logged_in_keys_enc {
                    let (uids, index_vals) = proxy::Proxy::get_uid_and_val(&strvals, &strcols);
                    let keys = proxy::Proxy::get_keys(&table_keys, table, &uids, &index_vals);
                    'keyloop: for rk in keys {
                        for (i, id) in strvals.iter().enumerate() {
                            let col = &cols[i].column.to_string();
                            let mut check = true;
                            let (enc, col_index) = match col.as_str() {
                                "email" => (true, 0),
                                "lec" => (false, 1),
                                "q" => (false, 2),
                                "apikey" => (true, 1),
                                "is_admin" => (true, 2),
                                _ => {
                                    debug!("No match for col {}, skipping ID check", col);
                                    check = false;
                                    (false, 3)
                                }
                            };
                            if check {
                                let rids = if enc { &rk.enc_row_ids } else { &rk.row_ids };
                                if &rids[col_index] != id {
                                    debug!(
                                        "DECRYPT NO MATCH:\n\t col {}:\n\t{}\n\t{}!",
                                        col, id, rk.enc_row_ids[col_index]
                                    );
                                    continue 'keyloop;
                                } else {
                                    debug!(
                                        "DECRYPT MATCH Key rowids:\n\t col {}:\n\t{}\n\t{}!",
                                        col, id, rk.enc_row_ids[col_index]
                                    );
                                }
                            }
                        }
                        // we get here when all the checked IDs matched!
                        // this means that this key probably encrypted this row, so we can use
                        // it to decrypt it too
                        for (i, id) in strvals.iter().enumerate() {
                            let col = &cols[i].column.to_string();
                            if enccols.contains(col) {
                                debug!("Going to decrypt {} col {} with key", id, col);
                                ndecrypt += 1;
                                let (success, plainv) = crypto::decrypt_with_aes(&id, &rk.symkey);
                                if success {
                                    debug!(
                                        "Decrypted val (not yet quote-trimmed) to {}",
                                        std::str::from_utf8(
                                            &base64::decode(trim_quotes(&plainv)).unwrap()
                                        )
                                        .unwrap()
                                    );
                                }
                                writer.write_col(string_to_common_val(&plainv))?;
                            } else {
                                writer.write_col(string_to_common_val(&id))?;
                            }
                        }
                        break 'decrypt_loop;
                    }
                    warn!("Decrypt {} cols with keys", ndecrypt);
                }
                debug!("Writer finishing row!");
                nrows += 1;
                writer.end_row()?;
            }
            warn!(
                "{} returned {} rows with {} cols!: {}mus",
                q,
                nrows,
                cols.len(),
                start.elapsed().as_micros()
            );
            writer.finish()?;
        }
        Err(e) => {
            warn!("{:?}", e);
            results.error(
                msql_srv::ErrorKind::ER_BAD_SLAVE,
                format!("{:?}", e).as_bytes(),
            )?;
        }
    }
    Ok(())
}
pub fn get_tablefactor_name(relation: &TableFactor) -> (TableName, TableName) {
    match relation {
        // direct tables referencing user table changes to pp table name
        TableFactor::Table { name, alias, .. } => match alias {
            Some(a) => (name.to_string(), a.to_string()),
            None => (name.to_string(), name.to_string()),
        },
        _ => unimplemented!("Unsupported relation"),
    }
}

pub fn get_tables_of_twj(twjs: &Vec<TableWithJoins>) -> Vec<(TableName, TableName)> {
    let mut names = vec![];
    for twj in twjs {
        names.push(get_tablefactor_name(&twj.relation));
        for j in &twj.joins {
            names.push(get_tablefactor_name(&j.relation));
        }
    }
    names
}
pub fn get_expr_cols(e: &Expr) -> Vec<String> {
    let mut vals = vec![];
    match e {
        Expr::Identifier(v) => vals.push(trim_quotes(&v[0].to_string()).to_string()),
        Expr::Value(v) => match v {
            Value::Boolean(_) => (),
            _ => vals.push(trim_quotes(&v.to_string()).to_string()),
        },
        Expr::IsNull { expr, .. } => vals.append(&mut get_expr_cols(expr)),
        Expr::InList { expr, .. } => {
            vals.append(&mut get_expr_cols(expr));
        }
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::Eq | BinaryOperator::NotEq => vals.append(&mut get_expr_cols(left)),
            BinaryOperator::And | BinaryOperator::Or => {
                vals.append(&mut get_expr_cols(left));
                vals.append(&mut get_expr_cols(right));
            }
            _ => unimplemented!("Bad binary operator!"),
        },
        Expr::Nested(e) => vals.append(&mut get_expr_cols(e)),
        _ => unimplemented!("No identifier tracking for expr {:?}", e),
    }
    vals
}

pub fn get_expr_values(e: &Expr) -> Vec<String> {
    let mut vals = vec![];
    match e {
        Expr::Identifier(v) => vals.push(trim_quotes(&v[0].to_string()).to_string()),
        Expr::Value(v) => match v {
            Value::Boolean(_) => (),
            _ => vals.push(trim_quotes(&v.to_string()).to_string()),
        },
        Expr::IsNull { expr, .. } => vals.append(&mut get_expr_values(expr)),
        Expr::InList { list, .. } => {
            for le in list {
                vals.append(&mut get_expr_values(le));
            }
        }
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::Eq | BinaryOperator::NotEq => vals.append(&mut get_expr_values(right)),
            BinaryOperator::And | BinaryOperator::Or => {
                vals.append(&mut get_expr_values(left));
                vals.append(&mut get_expr_values(right));
            }
            _ => unimplemented!("Bad binary operator!"),
        },
        Expr::Nested(e) => vals.append(&mut get_expr_values(e)),
        _ => unimplemented!("No identifier tracking for expr {:?}", e),
    }
    vals
}

pub fn encrypt_expr_values(
    e: &Expr,
    key: &RowKey,
    iv: &Vec<u8>,
    enccols: &HashSet<String>,
) -> Expr {
    let new_e = match e {
        // XXX hack, some values are identifiers
        // TODO huh??????
        Expr::Identifier(v) => {
            let val = trim_quotes(&v[0].to_string()).to_string();
            let cipher = crypto::encrypt_with_aes(&val, &key.symkey, iv);
            Expr::Value(Value::String(cipher))
        }
        Expr::Value(v) => match v {
            Value::Boolean(..) => e.clone(),
            _ => {
                let val = trim_quotes(&v.to_string()).to_string();
                let cipher = crypto::encrypt_with_aes(&val, &key.symkey, iv);
                Expr::Value(Value::String(cipher))
            }
        },
        Expr::IsNull { expr, negated } => Expr::IsNull {
            expr: Box::new(encrypt_expr_values(expr, key, iv, enccols)),
            negated: *negated,
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            warn!("inlist: {}", expr);
            assert!(!negated);
            let col = expr.to_string();
            if !enccols.contains(&col) {
                e.clone()
            } else {
                let check_index = match col.as_str() {
                    "email" => 0,
                    "lec" => 1,
                    "q" => 2,
                    "apikey" => 1,
                    "is_admin" => 2,
                    _ => {
                        return e.clone();
                    }
                };
                for id in list {
                    let strid = id.to_string();
                    let strid = trim_quotes(&strid);
                    info!(
                        "col {} id is {}, check index {} is {}",
                        col, strid, check_index, key.row_ids[check_index]
                    );
                    // we know there has to be a match
                    if key.row_ids[check_index] == strid {
                        info!("Found match for key!");
                        return Expr::BinaryOp {
                            left: expr.clone(),
                            op: BinaryOperator::Eq,
                            right: Box::new(encrypt_expr_values(id, key, iv, enccols)),
                        };
                    }
                }
                error!("No matching value to encrypt for key?");
                e.clone()
            }
        }
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::Eq | BinaryOperator::NotEq => {
                if !enccols.contains(&left.to_string()) {
                    e.clone()
                } else {
                    warn!("binop: {}", left);
                    Expr::BinaryOp {
                        left: left.clone(),
                        op: op.clone(),
                        right: Box::new(encrypt_expr_values(right, key, iv, enccols)),
                    }
                }
            }
            BinaryOperator::And | BinaryOperator::Or => Expr::BinaryOp {
                left: Box::new(encrypt_expr_values(left, key, iv, enccols)),
                op: op.clone(),
                right: Box::new(encrypt_expr_values(right, key, iv, enccols)),
            },
            _ => unimplemented!("Bad binary operator!"),
        },
        Expr::Nested(e) => Expr::Nested(Box::new(encrypt_expr_values(e, key, iv, enccols))),
        /*Expr::UnaryOp { op, expr} => {}*/
        _ => unimplemented!("No identifier tracking for expr {:?}", e),
    };
    debug!(
        "Encrypting expr value
        \t{} -> {}
        \t key/iv {}/{}",
        e,
        new_e,
        base64::encode(&key.symkey),
        base64::encode(iv)
    );
    new_e
}

pub fn decrypt_expr_values(e: &Expr, key: &Vec<u8>) -> Expr {
    //info!("Decrypting expr {}", e);
    match e {
        Expr::Identifier(v) => {
            let val = trim_quotes(&v[0].to_string()).to_string();
            let (_, cipher) = crypto::decrypt_with_aes(&val, key);
            Expr::Value(Value::String(cipher))
        }
        Expr::Value(v) => match v {
            Value::Boolean(_) => e.clone(),
            _ => {
                let val = trim_quotes(&v.to_string()).to_string();
                let (_, cipher) = crypto::decrypt_with_aes(&val, key);
                Expr::Value(Value::String(cipher))
            }
        },
        Expr::IsNull { expr, negated } => Expr::IsNull {
            expr: Box::new(decrypt_expr_values(expr, key)),
            negated: *negated,
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(decrypt_expr_values(expr, key)),
            list: list.iter().map(|le| decrypt_expr_values(le, key)).collect(),
            negated: *negated,
        },
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::Eq | BinaryOperator::NotEq => Expr::BinaryOp {
                left: Box::new(decrypt_expr_values(left, key)),
                op: op.clone(),
                right: Box::new(decrypt_expr_values(right, key)),
            },
            _ => unimplemented!("Bad binary operator!"),
        },
        Expr::Nested(e) => Expr::Nested(Box::new(decrypt_expr_values(e, key))),
        /*Expr::UnaryOp { op, expr} => {}*/
        _ => unimplemented!("No identifier tracking for expr {:?}", e),
    }
}
