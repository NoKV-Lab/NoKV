//! A local owner session lives exactly as long as its process.

#![cfg(unix)]

use std::io::{BufRead as _, BufReader, Read as _};
use std::process::{Command, Stdio};

use nokv_control::{
    ControlError, ControlStore, LocalControlStore, LogicalShardId, LogicalShardState, NodeId,
    PlacementGeneration, RecoveryPublication, RootId, RootPlacement, RootPlacementLifecycle,
};

const OWNER_DIRECTORY_ENV: &str = "NOKV_LOCAL_OWNER_PROCESS_DIRECTORY";
const SHARD: LogicalShardId = LogicalShardId::from_bytes([9; 16]);

fn node(name: &str) -> NodeId {
    NodeId::new(name).unwrap()
}

fn empty_publication() -> RecoveryPublication {
    RecoveryPublication {
        checkpoint: None,
        log: None,
        durable_lsn: 0,
    }
}

/// Child mode: own the shard, report it, and hold the session until killed.
fn serve_as_child_owner(directory: &str) {
    let store = LocalControlStore::open(directory).unwrap();
    let lease = store
        .acquire_owner(&SHARD, node("node-a"), "127.0.0.1:7001".to_owned())
        .unwrap();
    store.mark_serving(&lease, empty_publication()).unwrap();
    println!("serving {}", lease.owner_epoch.get());
    let _ = std::io::stdin().read_to_end(&mut Vec::new());
    unreachable!("the parent kills this owner before closing its stdin");
}

#[test]
fn a_killed_owner_process_admits_exactly_one_successor() {
    if let Ok(directory) = std::env::var(OWNER_DIRECTORY_ENV) {
        serve_as_child_owner(&directory);
        return;
    }

    let directory = tempfile::tempdir().unwrap();
    let store = LocalControlStore::open(directory.path()).unwrap();
    store.create_logical_shard(SHARD).unwrap();
    store
        .create_root_placement(RootPlacement {
            root_id: RootId::from_bytes([9; 16]),
            logical_shard_id: SHARD,
            placement_generation: PlacementGeneration::new(1).unwrap(),
            lifecycle: RootPlacementLifecycle::Provisioning,
        })
        .unwrap();

    let mut owner = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "a_killed_owner_process_admits_exactly_one_successor",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(OWNER_DIRECTORY_ENV, directory.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(owner.stdout.take().unwrap()).lines();
    let reported = loop {
        let line = lines
            .next()
            .expect("child owner exited before serving")
            .unwrap();
        // libtest prints the test name without a newline before the body runs.
        if let Some((_, epoch)) = line.rsplit_once("serving ") {
            break epoch.parse::<u64>().unwrap();
        }
    };
    assert_eq!(reported, 1);

    let record = store.get_logical_shard(&SHARD).unwrap().unwrap();
    assert_eq!(record.state, LogicalShardState::Serving);
    let epoch = record.owner_epoch.unwrap();
    assert!(matches!(
        store.acquire_successor(&SHARD, epoch, node("node-b"), "127.0.0.1:7002".to_owned()),
        Err(ControlError::PreviousOwnerSessionLive { .. })
    ));

    owner.kill().unwrap();
    owner.wait().unwrap();

    let successor = store
        .acquire_successor(&SHARD, epoch, node("node-b"), "127.0.0.1:7002".to_owned())
        .unwrap();
    assert_eq!(successor.owner_epoch.get(), epoch.get() + 1);
    assert!(matches!(
        store.acquire_successor(&SHARD, epoch, node("node-c"), "127.0.0.1:7003".to_owned()),
        Err(ControlError::StaleOwnerEpoch { .. })
    ));
    store.mark_serving(&successor, empty_publication()).unwrap();
    assert_eq!(
        store.prove_owner(&successor).unwrap().owner_epoch,
        Some(successor.owner_epoch)
    );
}
