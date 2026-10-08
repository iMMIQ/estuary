use super::deploy::*;
use super::worker::{reset_restart_backoff, schedule_restart};
use super::*;

#[test]
fn worker_restart_backoff_is_bounded_and_resets() {
    let mut slot = SlotRuntime {
        id: SlotId::A,
        release: PathBuf::from("release"),
        child: None,
        must_be_ready: false,
        started_at: None,
        restart_failures: 0,
        restart_not_before: std::time::Instant::now(),
    };
    for _ in 0..20 {
        schedule_restart(&mut slot);
    }
    assert_eq!(slot.restart_failures, 20);
    assert!(slot.restart_not_before <= std::time::Instant::now() + RESTART_MAX_BACKOFF);
    reset_restart_backoff(&mut slot);
    assert_eq!(slot.restart_failures, 0);
}

#[test]
fn atomic_symlink_replaces_existing_target() {
    let root = std::env::temp_dir().join(format!("estuary-link-{}", uuid::Uuid::now_v7()));
    fs::create_dir_all(root.join("one")).unwrap();
    fs::create_dir_all(root.join("two")).unwrap();
    let link = root.join("current");
    atomic_symlink(&root.join("one"), &link).unwrap();
    assert_eq!(link.canonicalize().unwrap(), root.join("one"));
    atomic_symlink(&root.join("two"), &link).unwrap();
    assert_eq!(link.canonicalize().unwrap(), root.join("two"));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn release_validation_rejects_nested_paths() {
    let root = std::env::temp_dir().join(format!("estuary-release-{}", uuid::Uuid::now_v7()));
    fs::create_dir_all(root.join("valid").join("nested")).unwrap();
    assert!(validate_release_dir(&root, &root.join("valid")).is_ok());
    assert!(validate_release_dir(&root, &root.join("valid/nested")).is_err());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn startup_repairs_a_slot_link_after_its_release_was_deleted() {
    let root = std::env::temp_dir().join(format!("estuary-layout-{}", uuid::Uuid::now_v7()));
    let releases = root.join("releases");
    let current = releases.join("current");
    let deleted = releases.join("deleted");
    let state = root.join("state");
    fs::create_dir_all(&current).unwrap();
    fs::create_dir_all(state.join("slots/a")).unwrap();
    fs::create_dir_all(state.join("slots/b")).unwrap();
    atomic_symlink(&current, &state.join("current")).unwrap();
    atomic_symlink(&deleted, &state.join("slots/a/current")).unwrap();

    let config = SupervisorConfig {
        settings: Settings::default(),
        database: root.join("estuary.db"),
        release_root: releases,
        state_root: state,
        runtime_dir: root.join("run"),
        slot_a_admin: "127.0.0.1:19091".parse().unwrap(),
        slot_b_admin: "127.0.0.1:19092".parse().unwrap(),
        start_timeout: Duration::from_secs(1),
        drain_timeout: Duration::from_secs(1),
    };
    ensure_state_layout(&config).unwrap();
    assert_eq!(
        read_release_link(&config.slot_link(SlotId::A)).unwrap(),
        current
    );
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn deploy_authorization_accepts_bearer_and_basic_passwords() {
    assert_eq!(
        deploy_authorization_token("Bearer secret").as_deref(),
        Some("secret")
    );
    assert_eq!(
        deploy_authorization_token("Basic dXNlcjpzZWNyZXQ=").as_deref(),
        Some("secret")
    );
}

#[test]
fn matching_links_do_not_finalize_an_interrupted_rollout_before_startup() {
    let root = std::env::temp_dir().join(format!("estuary-recovery-{}", uuid::Uuid::now_v7()));
    let releases = root.join("releases");
    let previous = releases.join("previous");
    let target = releases.join("target");
    let state = root.join("state");
    let runtime = root.join("run");
    fs::create_dir_all(&previous).unwrap();
    fs::create_dir_all(&target).unwrap();
    fs::create_dir_all(state.join("slots/a")).unwrap();
    fs::create_dir_all(state.join("slots/b")).unwrap();
    fs::create_dir_all(&runtime).unwrap();
    atomic_symlink(&target, &state.join("current")).unwrap();
    atomic_symlink(&target, &state.join("slots/a/current")).unwrap();
    atomic_symlink(&target, &state.join("slots/b/current")).unwrap();

    let config = SupervisorConfig {
        settings: Settings::default(),
        database: root.join("estuary.db"),
        release_root: releases,
        state_root: state,
        runtime_dir: runtime,
        slot_a_admin: "127.0.0.1:9090".parse().unwrap(),
        slot_b_admin: "127.0.0.1:19092".parse().unwrap(),
        start_timeout: Duration::from_secs(1),
        drain_timeout: Duration::from_secs(1),
    };
    let journal = RolloutJournal {
        target,
        previous_a: previous.clone(),
        previous_b: previous,
        phase: "slot_b".to_owned(),
    };
    write_json_atomic(&config.journal_file(), &journal).unwrap();

    assert!(recover_rollout_state(&config).unwrap().is_some());
    assert!(config.journal_file().exists());
    assert!(config.freeze_file().exists());
    fs::remove_dir_all(root).unwrap();
}
