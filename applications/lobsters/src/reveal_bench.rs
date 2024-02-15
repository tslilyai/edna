use edna::{helpers, EdnaClient, RowVal, TableRow};
use log::warn;
use mysql::prelude::*;
use std::fs::{OpenOptions};
use std::io::Write;
use std::str::FromStr;
use std::time;
use std::time::Duration;

const TABLEINFO_JSON: &'static str = include_str!("./disguises/table_info_updated.json");
const PPGEN_JSON: &'static str = include_str!("./disguises/pp_gen_updated.json");
const GDPR_JSON: &'static str = include_str!("./disguises/gdpr_disguise.json");

pub fn run_disguise_reveal_test(
    edna: &mut EdnaClient,
    db: &mut mysql::PooledConn,
    num_updates: usize,
    use_txn: bool,
    nusers: usize,
) {
    let mut delete_durations = vec![];
    let mut restore_durations = vec![];

    let uid = nusers; // always test most expensive user

    // UNSUB
    let start = time::Instant::now();
    let did = edna
        .apply_disguise(
            uid.to_string(),
            GDPR_JSON,
            TABLEINFO_JSON,
            PPGEN_JSON,
            None, //Some(uid.to_string()),
            None,
            use_txn,
        )
        .unwrap();
    delete_durations.push(start.elapsed());
    warn!("Ran unsub 1: {}", start.elapsed().as_micros());

    // RESUB
    let start = time::Instant::now();
    edna.reveal_disguise(
        uid.to_string(),
        did,
        TABLEINFO_JSON,
        PPGEN_JSON,
        Some(edna::RevealPPType::Restore),
        true, // allow partial row reveals
        Some(uid.to_string()),
        None,
        use_txn,
    )
    .unwrap();
    restore_durations.push(start.elapsed());
    warn!("Ran resub no updates: {}", start.elapsed().as_micros());
    print_update_stats(&delete_durations, &restore_durations);
}

fn print_update_stats(
    delete_durations: &Vec<Duration>,
    restore_durations: &Vec<Duration>,
) {
    let filename = format!(
      "../../results/lobsters_results/update_stats.csv",
    );

    // print out stats
    let mut f = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&filename)
        .unwrap();

    writeln!(
        f,
        "{}",
        delete_durations
            .iter()
            .map(|d| format!(
                "{}",
                d.as_micros().to_string()
            ))
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
    writeln!(
        f,
        "{}",
        restore_durations
            .iter()
            .map(|d| format!(
                "{}",
                d.as_micros().to_string()
            ))
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
}
