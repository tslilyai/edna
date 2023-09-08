use crate::args;
use edna::EdnaClient;
use mysql::prelude::*;
use mysql::Opts;
pub use mysql::Value;
use mysql::*;
use std::time;

pub struct MySqlBackend {
    pub handle: mysql::Conn,
    pub log: slog::Logger,
    pub edna: EdnaClient,
    pub is_baseline: bool,
    url: String,
}

impl MySqlBackend {
    pub fn new(dbname: &str, log: Option<slog::Logger>, args: &args::Args) -> Result<Self> {
        let log = match log {
            None => slog::Logger::root(slog::Discard, o!()),
            Some(l) => l,
        };
        let url = format!(
            "mysql://{}:{}@127.0.0.1:{}/{}",
            args.config.mysql_user, args.config.mysql_pass, args.port, args.class
        );
        debug!(log, "Connecting to MySql DB {}...", dbname);
        let mut db = mysql::Conn::new(Opts::from_url(&url).unwrap()).unwrap();
        assert_eq!(db.ping(), true);

        let edna = EdnaClient::new(
            &args.config.mysql_user,
            &args.config.mysql_pass,
            &format!("127.0.0.1:{}", args.port),
            &args.class,
            true,
            false,
            args.dryrun,
        );

        Ok(MySqlBackend {
            handle: db,
            log: log,
            url: url.to_string(),
            edna: edna,
            is_baseline: args.is_baseline,
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
                    warn!(
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
                        let start = time::Instant::now();
                        let rowvals = row.unwrap().unwrap();
                        let vals: Vec<Value> = rowvals.iter().map(|v| v.clone().into()).collect();
                        debug!(
                            self.log,
                            "Collecting rowval took {}mus",
                            start.elapsed().as_micros()
                        );
                        let start = time::Instant::now();
                        rows.push(vals);
                        debug!(
                            self.log,
                            "pushing vals took {}mus",
                            start.elapsed().as_micros()
                        );
                    }
                    warn!(
                        self.log,
                        "query {} parsing {} rows: {}mus",
                        sql,
                        rows.len(),
                        start.elapsed().as_micros()
                    );
                    debug!(self.log, "executed query {}, got {} rows", sql, rows.len());
                    return rows;
                }
            }
            self.reconnect();
        }
    }

    pub fn exec_batch<P>(&mut self, stmt: &str, params: Vec<P>)
    where
        P: Into<Params> + Clone,
    {
        while let Err(e) = self.handle.exec_batch(stmt, &params) {
            warn!(
                self.log,
                "failed to perform query {} ({}), reconnecting to database", stmt, e
            );
            self.reconnect();
        }
    }

    pub fn query_drop(&mut self, q: &str) {
        let start = time::Instant::now();
        while let Err(e) = self.handle.query_drop(q) {
            warn!(
                self.log,
                "failed to perform query {} ({}), reconnecting to database", q, e
            );
            self.reconnect();
        }
        warn!(self.log, "query {}: {}mus", q, start.elapsed().as_micros());
    }

    fn do_insert(&mut self, table: &str, vals: Vec<Value>, replace: bool) {
        let start = time::Instant::now();
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
            warn!(
                self.log,
                "failed to insert into {}, query {} ({}), reconnecting to database", table, q, e
            );
            self.reconnect();
        }
        warn!(self.log, "query {}: {}mus", q, start.elapsed().as_micros());
    }

    pub fn insert(&mut self, table: &str, vals: Vec<Value>) {
        self.do_insert(table, vals, false);
    }

    pub fn update(&mut self, table: &str, vals: Vec<(&str, String)>) {
        let start = time::Instant::now();
        let q = format!(
            "INSERT INTO {} ({}) VALUES ({}) ON DUPLICATE KEY UPDATE {}",
            table,
            vals.iter()
                .map(|(c, _)| format!("{}", c))
                .collect::<Vec<String>>()
                .join(","),
            vals.iter()
                .map(|(_, v)| format!("{}", v))
                .collect::<Vec<String>>()
                .join(","),
            vals.iter()
                .map(|(c, v)| format!("{} = {}", c, v))
                .collect::<Vec<String>>()
                .join(","),
        );
        debug!(self.log, "executed update query {} for row {:?}", q, vals);
        while let Err(e) = self.handle.query_drop(&q) {
            warn!(
                self.log,
                "failed to insert into {}, query {} ({}), reconnecting to database", table, q, e
            );
            self.reconnect();
        }
        warn!(self.log, "query {}: {}mus", q, start.elapsed().as_micros());
    }
}
