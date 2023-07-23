use crate::config;
use clap::{App, Arg};
use std::str::FromStr;

#[cfg_attr(rustfmt, rustfmt_skip)]
const WEBSUBMIT_USAGE: &'static str = "\
EXAMPLES:
  websubmit -i csci2390
  websubmit -i csci2390 -c csci2390-f19.toml";

#[derive(Clone, Debug)]
pub struct Args {
    pub class: String,
    pub nusers: usize,
    pub nlec: usize,
    pub nqs: usize,
    pub prime: bool,
    pub proxy: bool,
    pub dryrun: bool,
    pub is_baseline: bool,
    pub port: usize,
    pub benchmark: bool,
    pub config: config::Config,
    pub schema: String,
}

pub fn parse_args() -> Args {
    let args = App::new("websubmit")
        .version("0.0.1")
        .about("Class submission system.")
        .arg(
            Arg::with_name("schema")
                .long("schema")
                .takes_value(true)
                .value_name("SCHEMA")
                .default_value("/data/repository/websubmit-rs/edna-server/src/schema.sql")
        )
        .arg(
            Arg::with_name("config")
                .short("c")
                .long("config")
                .takes_value(true)
                .value_name("CONFIG_FILE")
                .default_value("/data/repository/applications/websubmit-rs/edna-server/sample-config.toml")
                .help("Path to the configuration file for the deployment."),
        )
        .arg(
            Arg::with_name("class")
                .short("i")
                .long("class-id")
                .takes_value(true)
                .value_name("CLASS_ID")
                .default_value("myclass")
                .help("Short textual identifier for the class hosted (used as Noria deployment name)."),
        )
        .arg(
            Arg::with_name("nusers")
                .short("u")
                .long("nusers")
                .takes_value(true)
                .value_name("NUSERS")
                .default_value("0")
        )
        .arg(
            Arg::with_name("nlec")
                .short("l")
                .long("nlec")
                .takes_value(true)
                .value_name("NLEC")
                .default_value("0")
        ).arg(
            Arg::with_name("nqs")
                .short("q")
                .long("nqs")
                .takes_value(true)
                .value_name("NQS")
                .default_value("0")
        ).arg(
            Arg::with_name("prime")
                .short("p")
                .long("prime")
                .takes_value(true)
                .value_name("PRIME")
                .default_value("true")
        ).arg(
            Arg::with_name("proxy")
                .short("p")
                .long("proxy")
                .takes_value(true)
                .value_name("proxy")
                .default_value("true")
         ).arg(
            Arg::with_name("port")
                .short("p")
                .long("port")
                .takes_value(true)
                .value_name("port")
                .default_value("true")
         ).arg(
            Arg::with_name("benchmark")
                .short("b")
                .long("benchmark")
                .takes_value(true)
                .value_name("BENCHMARK")
                .default_value("false")
        ).arg(
            Arg::with_name("baseline")
                .long("baseline")
                .takes_value(true)
                .value_name("BASELINE")
                .default_value("false")
        ).arg(
            Arg::with_name("dryrun")
                .long("dryrun")
                .takes_value(true)
                .value_name("DRYRUN")
                .default_value("false")
        )

        .after_help(WEBSUBMIT_USAGE)
        .get_matches();
    let config = config::parse(args.value_of("config").expect("Failed to parse config!"))
        .expect("failed to parse config");
    Args {
        is_baseline: bool::from_str(args.value_of("baseline").unwrap()).unwrap(),
        class: String::from(args.value_of("class").unwrap()),
        nusers: usize::from_str(args.value_of("nusers").unwrap()).unwrap(),
        nlec: usize::from_str(args.value_of("nlec").unwrap()).unwrap(),
        nqs: usize::from_str(args.value_of("nqs").unwrap()).unwrap(),
        prime: bool::from_str(args.value_of("prime").unwrap()).unwrap(),
        proxy: bool::from_str(args.value_of("proxy").unwrap()).unwrap(),
        port: usize::from_str(args.value_of("port").unwrap()).unwrap(),
        benchmark: bool::from_str(args.value_of("benchmark").unwrap()).unwrap(),
        schema: String::from(args.value_of("schema").unwrap()),
        dryrun: bool::from_str(args.value_of("dryrun").unwrap()).unwrap(),
        config: config,
    }
}
