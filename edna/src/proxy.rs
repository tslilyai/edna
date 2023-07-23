extern crate mysql;
use crate::*;
use log::info;
use msql_srv::*;
use mysql::Opts;
use std::io;

#[derive(Clone)]
pub struct Proxy {
    pool: mysql::Pool,
}

impl Proxy {
    pub fn new(host: &str, user: &str, pass: &str, dbname: &str) -> Proxy {
        // assumes the db is already initialized
        let url = format!("mysql://{}:{}@{}/{}", user, pass, host, dbname);
        let pool = mysql::Pool::new(Opts::from_url(&url).unwrap()).unwrap();
        info!("Returning proxy!");
        Proxy { pool: pool.clone() }
    }
}

impl<W: io::Write> MysqlShim<W> for Proxy {
    type Error = io::Error;

    fn on_prepare(&mut self, q: &str, _info: StatementMetaWriter<W>) -> io::Result<()> {
        info!("Prepare {}", q);
        Ok(())
        //unimplemented!("Nope can't prepare right now");
    }
    fn on_execute(
        &mut self,
        _: u32,
        _: ParamParser,
        _results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        info!("Exec");
        Ok(())
        //unimplemented!("Nope can't execute right now");
    }

    fn on_close(&mut self, _: u32) {
        info!("Close");
    }

    fn on_init(&mut self, schema: &str, _: InitWriter<'_, W>) -> Result<(), Self::Error> {
        info!("Init schema {}", schema);
        self.pool
            .get_conn()
            .unwrap()
            .query_drop(format!("use {}", schema))
            .unwrap();
        Ok(())
    }

    fn on_query(&mut self, query: &str, results: QueryResultWriter<W>) -> io::Result<()> {
        let start = time::Instant::now();
        let mut db = self.pool.get_conn().unwrap();
        let res = db.query_iter(query);
        helpers::write_mysql_answer_rows(results, res).unwrap();
        warn!("Query {}: {}mus", query, start.elapsed().as_micros());
        return Ok(());
    }
}
