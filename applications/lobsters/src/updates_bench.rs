use crate::migrations::*;
use edna::EdnaClient;
use log::warn;
use std::fs::OpenOptions;
use std::io::Write;
use std::time;
use std::time::Duration;

const TABLEINFO_JSON: &'static str = include_str!("./disguises/table_info.json");
const PPGEN_JSON: &'static str = include_str!("./disguises/pp_gen.json");
const TABLEINFO_JSON_UPDATED: &'static str = include_str!("./migrations/table_info_updated.json");
const PPGEN_JSON_UPDATED: &'static str = include_str!("./migrations/pp_gen_updated.json");

const GDPR_JSON: &'static str = include_str!("./disguises/gdpr_disguise.json");

pub fn run_updates_test(
    edna: &mut EdnaClient,
    db: &mut mysql::PooledConn,
    use_txn: bool,
    uid: usize,
) {
    let mut delete_durations = vec![];
    let mut updated_restore_durations = vec![];
    let mut restore_durations = vec![];

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
    warn!("Ran unsub 2: {}", start.elapsed().as_micros());

    // apply schema updates!
    normalize_url::apply(db);
    addusersettingshowemail::apply(db);
    story_text::apply(db);

    // record one-by-one, so they count as separate updates in Edna
    edna.record_update(normalize_url::update);
    edna.record_update(addusersettingshowemail::update);
    edna.record_update(story_text::update);

    // RESUB
    let start = time::Instant::now();
    edna.reveal_disguise(
        uid.to_string(),
        did,
        TABLEINFO_JSON_UPDATED,
        PPGEN_JSON_UPDATED,
        Some(edna::RevealPPType::Restore),
        true, // allow partial row reveals
        Some(uid.to_string()),
        None,
        use_txn,
    )
    .unwrap();
    updated_restore_durations.push(start.elapsed());
    warn!("Ran resub updates: {}", start.elapsed().as_micros());

    print_update_stats(
        &delete_durations,
        &restore_durations,
        &updated_restore_durations,
    );
}
fn print_update_stats(
    delete_durations: &Vec<Duration>,
    restore_durations: &Vec<Duration>,
    updated_restore_durations: &Vec<Duration>,
) {
    let filename = format!("../../results/lobsters_results/update_stats.csv",);

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
            .map(|d| format!("{}", d.as_micros().to_string()))
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
    writeln!(
        f,
        "{}",
        restore_durations
            .iter()
            .map(|d| format!("{}", d.as_micros().to_string()))
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
    writeln!(
        f,
        "{}",
        updated_restore_durations
            .iter()
            .map(|d| format!("{}", d.as_micros().to_string()))
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
}
