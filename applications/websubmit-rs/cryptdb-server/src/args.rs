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
    pub port: usize,
    pub prime: bool,
    pub crypto: bool,
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
                .default_value("src/schema.sql")
                //.default_value("/home/tslilyai/disguises/applications/websubmit-rs/server/src/schema.sql")
                //.default_value("/home/tslilyai/Documents/MIT/Research/DataPrivacy/disguising/applications/websubmit-rs/server/src/schema.sql")
        )
        .arg(
            Arg::with_name("config")
                .short("c")
                .long("config")
                .takes_value(true)
                .value_name("CONFIG_FILE")
                .default_value("/home/tslilyai/data_disguises_workspace/applications/websubmit-rs/server/sample-config.toml")
                .help("Path to the configuration file for the deployment."),
        )
        .arg(
            Arg::with_name("port")
                .long("port")
                .takes_value(true)
                .value_name("PORT")
                .default_value("62292")
                .help("Port")
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
            Arg::with_name("crypto")
                .short("p")
                .long("crypto")
                .takes_value(true)
                .value_name("PRIME")
                .default_value("true")
         ).arg(
            Arg::with_name("prime")
                .short("p")
                .long("prime")
                .takes_value(true)
                .value_name("PRIME")
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
        )
        .after_help(WEBSUBMIT_USAGE)
        .get_matches();
    let mut config = config::parse(args.value_of("config").expect("Failed to parse config!"))
        .expect("failed to parse config {}");
    config.is_baseline = bool::from_str(args.value_of("baseline").unwrap()).unwrap();
    Args {
        class: String::from(args.value_of("class").unwrap()),
        nusers: usize::from_str(args.value_of("nusers").unwrap()).unwrap(),
        nlec: usize::from_str(args.value_of("nlec").unwrap()).unwrap(),
        nqs: usize::from_str(args.value_of("nqs").unwrap()).unwrap(),
        port: usize::from_str(args.value_of("port").unwrap()).unwrap(),
        prime: bool::from_str(args.value_of("prime").unwrap()).unwrap(),
        crypto: bool::from_str(args.value_of("crypto").unwrap()).unwrap(),
        benchmark: bool::from_str(args.value_of("benchmark").unwrap()).unwrap(),
        schema: String::from(args.value_of("schema").unwrap()),
        config: config,
    }
}
