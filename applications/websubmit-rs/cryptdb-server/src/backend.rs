use crate::args;
use edna_cryptdb::EdnaClient;
use mysql::prelude::*;
use mysql::Opts;
pub use mysql::Value;
use mysql::*;
use std::time;

pub struct MySqlBackend {
    pub handle: mysql::Conn,
    pub log: slog::Logger,
    pub edna: EdnaClient,
    pub crypto: bool,
    _schema: String,
    url: String,
}

impl MySqlBackend {
    pub fn new(
        url: &str,
        dbname: &str,
        log: Option<slog::Logger>,
        args: &args::Args,
    ) -> Result<Self> {
        let log = match log {
            None => slog::Logger::root(slog::Discard, o!()),
            Some(l) => l,
        };

        let schema = std::fs::read_to_string(&args.schema)?;

        debug!(
            log,
            "Connecting to MySql DB and initializing schema {}...", dbname
        );
        let mut db = mysql::Conn::new(Opts::from_url(url).unwrap()).unwrap();
        assert_eq!(db.ping(), true);

        let edna = EdnaClient::new(&url, false, args.crypto);

        Ok(MySqlBackend {
            handle: db,
            log: log,
            _schema: schema.to_owned(),
            url: url.to_string(),
            edna: edna,
            crypto: args.crypto,
        })
    }

    fn reconnect(&mut self) {
        self.handle = mysql::Conn::new(Opts::from_url(&self.url).unwrap()).unwrap();
    }

    pub fn query_iter(&mut self, sql: &str) -> Vec<Vec<Value>> {
        // lily: turn this into a single query
        let start = time::Instant::now();
        loop {
            match self.handle.query_iter(sql) {
                Err(e) => {
                    debug!(
                        self.log,
                        "query \'{}\' failed ({}), reconnecting to database", sql, e
                    );
                }
                Ok(res) => {
                    warn!(
                        self.log,
                        "query {}: {}mus",
                        sql,
                        start.elapsed().as_micros()
                    );
                    let start = time::Instant::now();
                    let mut rows = vec![];
                    for row in res {
                        let rowvals = row.unwrap().unwrap();
                        let vals: Vec<Value> = rowvals.iter().map(|v| v.clone().into()).collect();
                        rows.push(vals);
                    }
                    debug!(
                        self.log,
                        "query {} parsing: {}mus",
                        sql,
                        start.elapsed().as_micros()
                    );
                    debug!(self.log, "executed query {}, got {} rows", sql, rows.len());
                    return rows;
                }
            }
            self.reconnect();
        }
    }

    fn do_insert(&mut self, table: &str, vals: Vec<Value>, replace: bool) {
        let _op = if replace { "REPLACE" } else { "INSERT" };
        let q = format!(
            "INSERT INTO {} VALUES ({})",
            table,
            vals.iter()
                .map(|v| v.as_sql(true))
                .collect::<Vec<String>>()
                .join(",")
        );
        debug!(self.log, "executed insert query {} for row {:?}", q, vals);
        while let Err(e) = self.handle.query_drop(q.clone()) {
            debug!(
                self.log,
                "failed to insert into {}, query {} ({}), reconnecting to database", table, q, e
            );
            self.reconnect();
        }
    }

    pub fn insert(&mut self, table: &str, vals: Vec<Value>) {
        self.do_insert(table, vals, false);
    }

    pub fn update(&mut self, table: &str, keys: Vec<(&str, String)>, vals: Vec<(&str, String)>) {
        let q = format!(
            "UPDATE {} SET {} WHERE {}",
            table,
            vals.iter()
                .map(|(c, v)| format!("{} = {}", c, v))
                .collect::<Vec<String>>()
                .join(","),
            keys.iter()
                .map(|(c, v)| format!("{} = {}", c, v))
                .collect::<Vec<String>>()
                .join(" AND ")
        );
        debug!(self.log, "executed update query {} for row {:?}", q, vals);
        while let Err(e) = self.handle.query_drop(&q) {
            debug!(
                self.log,
                "failed to insert into {}, query {} ({}), reconnecting to database", table, q, e
            );
            self.reconnect();
        }
    }
}
