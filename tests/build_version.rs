use std::{
    fs,
    path::PathBuf,
    process::{Command, Output},
};

struct VersionFixture {
    root: PathBuf,
}

impl VersionFixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!("estuary-version-{}", uuid::Uuid::now_v7()));
        fs::create_dir_all(root.join("deploy")).unwrap();
        fs::write(
            root.join("Cargo.toml"),
            "[package]\nname = \"version-fixture\"\nversion = \"0.7.0\"\n",
        )
        .unwrap();
        fs::copy(
            concat!(env!("CARGO_MANIFEST_DIR"), "/deploy/build-version.sh"),
            root.join("deploy/build-version.sh"),
        )
        .unwrap();
        Self { root }
    }

    fn git(&self, args: &[&str]) -> String {
        let output = Command::new("git")
            .args([
                "-c",
                "user.name=Version Test",
                "-c",
                "user.email=version@example.com",
            ])
            .args(args)
            .current_dir(&self.root)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    }

    fn resolve(&self, override_version: Option<&str>) -> Output {
        let mut command = Command::new("sh");
        command
            .arg(self.root.join("deploy/build-version.sh"))
            .env_remove("ESTUARY_BUILD_VERSION");
        if let Some(version) = override_version {
            command.env("ESTUARY_BUILD_VERSION", version);
        }
        command.output().unwrap()
    }

    fn version(&self) -> String {
        let output = self.resolve(None);
        assert!(output.status.success());
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    }

    fn cargo_version(&self) -> String {
        let output = Command::new("cargo")
            .args(["run", "--quiet", "--offline"])
            .current_dir(&self.root)
            .env_remove("ESTUARY_BUILD_VERSION")
            .env("CARGO_TARGET_DIR", self.root.join("target"))
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    }
}

impl Drop for VersionFixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.root).unwrap();
    }
}

#[test]
fn versions_follow_exact_release_tags_and_subsequent_commits() {
    let fixture = VersionFixture::new();
    fixture.git(&["init"]);
    fixture.git(&["add", "."]);
    fixture.git(&["-c", "commit.gpgsign=false", "commit", "-m", "initial"]);
    let first = format!(
        "0.7.0+{}",
        fixture.git(&["rev-parse", "--short=12", "HEAD"])
    );
    assert_eq!(fixture.version(), first);

    fixture.git(&["tag", "unrelated"]);
    assert_eq!(fixture.version(), first);
    fixture.git(&["tag", "v0.7.0"]);
    assert_eq!(fixture.version(), "0.7.0");

    fixture.git(&[
        "-c",
        "commit.gpgsign=false",
        "commit",
        "--allow-empty",
        "-m",
        "next",
    ]);
    let next = format!(
        "0.7.0+{}",
        fixture.git(&["rev-parse", "--short=12", "HEAD"])
    );
    assert_ne!(first, next);
    assert_eq!(fixture.version(), next);

    fixture.git(&["tag", "-d", "v0.7.0"]);
    fixture.git(&[
        "-c",
        "tag.gpgsign=false",
        "tag",
        "-a",
        "v0.7.0",
        "-m",
        "release",
    ]);
    assert_eq!(fixture.version(), "0.7.0");
    fixture.git(&["pack-refs", "--all", "--prune"]);
    assert_eq!(fixture.version(), "0.7.0");
}

#[test]
fn archives_use_explicit_metadata_or_unknown_hash() {
    let fixture = VersionFixture::new();
    assert_eq!(fixture.version(), "0.7.0+unknown");
    for version in ["0.7.0", "0.7.0+abcdef123456"] {
        let output = fixture.resolve(Some(version));
        assert!(output.status.success());
        assert_eq!(String::from_utf8(output.stdout).unwrap().trim(), version);
    }
    for version in ["../release", "0.7.0\nother", "version with spaces"] {
        let output = fixture.resolve(Some(version));
        assert!(!output.status.success(), "{version:?}");
    }
}

#[test]
fn cargo_rebuilds_when_head_or_release_tags_change() {
    let fixture = VersionFixture::new();
    fs::copy(
        concat!(env!("CARGO_MANIFEST_DIR"), "/build.rs"),
        fixture.root.join("build.rs"),
    )
    .unwrap();
    fs::create_dir(fixture.root.join("src")).unwrap();
    fs::write(
        fixture.root.join("src/main.rs"),
        "fn main() { println!(\"{}\", env!(\"ESTUARY_BUILD_VERSION\")); }",
    )
    .unwrap();
    fixture.git(&["init"]);
    fixture.git(&["add", "."]);
    fixture.git(&["-c", "commit.gpgsign=false", "commit", "-m", "initial"]);
    let initial = fixture.version();
    assert_eq!(fixture.cargo_version(), initial);
    fixture.git(&["tag", "v0.7.0"]);
    assert_eq!(fixture.cargo_version(), "0.7.0");
    fixture.git(&["pack-refs", "--all", "--prune"]);
    assert_eq!(fixture.cargo_version(), "0.7.0");
    fixture.git(&[
        "-c",
        "commit.gpgsign=false",
        "commit",
        "--allow-empty",
        "-m",
        "next",
    ]);
    assert_ne!(fixture.version(), initial);
    assert_eq!(fixture.cargo_version(), fixture.version());

    // Linked worktrees keep HEAD and the shared refs in different directories.
    let linked = fixture.root.join("linked");
    fixture.git(&["worktree", "add", "--detach", linked.to_str().unwrap()]);
    let worktree = VersionFixture { root: linked };
    assert_eq!(worktree.cargo_version(), fixture.version());
    fixture.git(&["tag", "-d", "v0.7.0"]);
    worktree.git(&["tag", "v0.7.0"]);
    assert_eq!(worktree.cargo_version(), "0.7.0");
}

#[test]
fn cli_reports_the_same_build_version_as_the_library() {
    let output = Command::new(env!("CARGO_BIN_EXE_estuary"))
        .arg("--version")
        .output()
        .unwrap();
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap().trim(),
        format!("estuary {}", estuary::VERSION)
    );
}
