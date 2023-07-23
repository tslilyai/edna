extern crate libc;

use crate::args;
use libc::*;
use mysql::prelude::*;
use mysql::Opts;
pub use mysql::Value;
use mysql::*;
use std::ffi::CStr;
use std::fs::File;
use std::io::Write;
use std::str;
use std::time;

const ADMIN_INSERT : &'static str = "INSERT INTO users VALUES ('malte@cs.brown.edu', 'b4bc3cef020eb6dd20defa1a7a8340dee889bc2164612e310766e69e45a1d5a7', 1, 0, 0, NULL);";

const QUERY_FILE: &'static str = "queries.txt";

#[link(name = "refmonws", kind = "static")]
extern "C" {
    pub fn rewrite_query(
        email: *const c_char,
        sql: *const c_char,
        rewrite_q: *const *mut c_char,
        cellblind: bool,
    );
}

pub struct MySqlBackend {
    pub log: slog::Logger,
    pool: mysql::Pool,
    queryfile: File,
    _schema: String,
}

impl MySqlBackend {
    pub fn new(dbname: &str, log: Option<slog::Logger>, args: &args::Args) -> Result<Self> {
        let log = match log {
            None => slog::Logger::root(slog::Discard, o!()),
            Some(l) => l,
        };

        let schema = std::fs::read_to_string("src/schema.sql")?;
        let file = File::create(QUERY_FILE).unwrap();

        debug!(
            log,
            "Connecting to MySql DB and initializing schema {}...", dbname
        );
        let mut pool =
            mysql::Pool::new(Opts::from_url(&format!("mysql://tester:pass@127.0.0.1:3306")).unwrap())
                .unwrap();
        let mut db = pool.get_conn().unwrap();

        db.query_drop(format!("DROP DATABASE IF EXISTS {};", dbname))
            .unwrap();
        db.query_drop(format!("CREATE DATABASE {};", dbname))
            .unwrap();
        // reconnect
        pool = mysql::Pool::new(
            Opts::from_url(&format!("mysql://tester:pass@127.0.0.1:3306/{}", dbname)).unwrap(),
        )
        .unwrap();
        let mut db = pool.get_conn().unwrap();
        //db.query_drop("SET max_heap_table_size = 4294967295;")?;

        for line in schema.lines() {
            if line.starts_with("--") || line.is_empty() {
                continue;
            }
            let line = if line.contains("VIEW") {
                line.to_string()
            } else {
                process_schema_stmt(line, false /*in_memory*/)
            };
            info!(log, "schema {}", line);

            db.query_drop(line).unwrap();
        }

        debug!(log, "Initializing with lectures, answers, and questions..");

        // initialize for testing
        if args.benchmark {
            db.query_drop(ADMIN_INSERT).unwrap();
            for l in 0..args.nlec {
                db.query_drop(&format!("INSERT INTO lectures VALUES ({}, 'lec{}');", l, l))
                    .unwrap();
                for q in 0..args.nqs {
                    db.query_drop(&format!(
                        "INSERT INTO questions VALUES ({}, {}, 'lec{}question{}');",
                        l, q, l, q
                    ))
                    .unwrap();
                    for u in 0..args.nusers {
                        db.query_drop(&format!("INSERT INTO answers VALUES ('{}@mail.edu', {}, {}, 'lec{}q{}answer{}', '1000-01-01 00:00:00');", 
                                u, l, q, l, q, u)).unwrap();
                    }
                }
            }
        }

        Ok(MySqlBackend {
            pool: pool,
            log: log,
            queryfile: file,
            _schema: schema.to_owned(),
        })
    }

    pub fn rewrite_query_ffi(&self, email: &str, q: &str) -> String {
        let email = std::ffi::CString::new(email).unwrap();
        let q_char = std::ffi::CString::new(q.to_lowercase()).unwrap();
        let cellblind = q.contains("users");
        let mut buf: Vec<i8> = vec![0, 100];
        let rewrite_q: *mut c_char = buf.as_mut_ptr();
        let start = time::Instant::now();
        unsafe {
            rewrite_query(
                email.as_ptr(),
                q_char.as_ptr(),
                &rewrite_q as *const *mut c_char,
                cellblind,
            )
        };
        debug!(
            self.log,
            "Time to rewrite: {}mus",
            start.elapsed().as_micros()
        );
        let c_str: &CStr = unsafe { CStr::from_ptr(rewrite_q) };
        let str_slice: &str = c_str.to_str().unwrap();
        str_slice.to_string()
    }

    pub fn handle(&self) -> mysql::PooledConn {
        self.pool.get_conn().unwrap()
    }

    pub fn exec_batch<I, P>(&mut self, sql: &str, params: I) -> ()
    where
        P: Into<Params>,
        I: IntoIterator<Item = P>,
    {
        let mut db = self.handle();
        db.exec_batch(sql, params).expect("failed to exec batch");
        self.queryfile.write_all(sql.as_bytes()).unwrap();
        self.queryfile.write_all("\n".as_bytes()).unwrap();
    }

    pub fn query(&mut self, email: &str, sql: &str) -> Vec<Vec<Value>> {
        // ignore views
        let new_q = if sql.to_lowercase().contains("select")
            && !sql.to_lowercase().contains("lec_qcount")
        {
            self.rewrite_query_ffi(email, sql)
        } else {
            sql.to_string()
        };
        let mut db = self.handle();
        let start = time::Instant::now();
        let res = db
            .query_iter(new_q.clone())
            .expect(&format!("query \'{}\' failed", new_q));
        info!(
            self.log,
            "query {}: {}mus",
            new_q,
            start.elapsed().as_micros()
        );
        let mut rows = vec![];
        for row in res {
            let rowvals = row.unwrap().unwrap();
            let vals: Vec<Value> = rowvals.iter().map(|v| v.clone().into()).collect();
            rows.push(vals);
        }
        debug!(
            self.log,
            "executed query {}, got {} rows",
            new_q,
            rows.len()
        );
        //self.queryfile.write_all(sql.as_bytes()).unwrap();
        //self.queryfile.write_all("\n".as_bytes()).unwrap();
        return rows;
    }

    fn do_insert(&mut self, table: &str, vals: Vec<Value>, replace: bool) {
        let op = if replace { "REPLACE" } else { "INSERT" };
        let q = format!(
            "{} INTO {} VALUES ({})",
            op,
            table,
            vals.iter().map(|_| "?").collect::<Vec<&str>>().join(",")
        );
        debug!(self.log, "executed insert query {} for row {:?}", q, vals);

        let q_full = format!(
            "{} INTO {} VALUES ({})",
            op,
            table,
            vals.iter()
                .map(|v| v.as_sql(true))
                .collect::<Vec<String>>()
                .join(",")
        );
        self.handle().exec_drop(q, vals).expect(&format!(
            "failed to insert into {}, query {}!",
            table, q_full
        ));
    }

    pub fn insert(&mut self, table: &str, vals: Vec<Value>) {
        self.do_insert(table, vals, false);
    }

    pub fn replace(&mut self, table: &str, vals: Vec<Value>) {
        self.do_insert(table, vals, true);
    }
}

fn process_schema_stmt(stmt: &str, in_memory: bool) -> String {
    // get rid of unsupported types
    let mut new = stmt.replace(r"int unsigned", "int");

    // remove semicolon
    new = new.trim_matches(';').to_string();

    // get rid of DEFAULT/etc. commands after query
    let mut end_index = new.len();
    if let Some(i) = new.find("DEFAULT CHARSET") {
        end_index = i;
    } else if let Some(i) = new.find("default charset") {
        end_index = i;
    }
    new.truncate(end_index);

    if in_memory {
        new = new.replace(r"mediumtext", "varchar(255)");
        new = new.replace(r"tinytext", "varchar(255)");
        new = new.replace(r" text ", " varchar(255) ");
        new = new.replace(r" text,", " varchar(255),");
        new = new.replace(r" text)", " varchar(255))");
        new = new.replace(r"FULLTEXT", "");
        new = new.replace(r"fulltext", "");
        new = new.replace(r"InnoDB", "MEMORY");
        if !new.contains("MEMORY") {
            new.push_str(" ENGINE = MEMORY");
        }
    } else if !new.contains("ENGINE") && !new.contains("engine") {
        new.push_str(" ENGINE = InnoDB");
    }
    new.push_str(";");
    new
}
