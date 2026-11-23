use std::{
    env,
    path::{Path, PathBuf},
    process::Command,
};

fn main() {
    let manifest_dir = PathBuf::from(
        env::var("CARGO_MANIFEST_DIR").expect("Cargo must provide the manifest directory"),
    );
    let root = resolve_repo_root(&manifest_dir);

    println!("cargo:rerun-if-changed=build.rs");
    println!(
        "cargo:rerun-if-changed={}",
        root.join("Cargo.toml").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        root.join("deploy/build-version.sh").display()
    );
    println!("cargo:rerun-if-env-changed=ESTUARY_BUILD_VERSION");

    // Resolve Git's paths so branch updates and tags also invalidate builds in
    // linked worktrees, where .git is a file rather than a directory.
    for name in ["HEAD", "refs", "packed-refs"] {
        if let Some(raw) = git_output(&root, &["rev-parse", "--git-path", name]) {
            let path = Path::new(&raw);
            let full = if path.is_absolute() {
                path.to_path_buf()
            } else {
                root.join(path)
            };
            if full.exists() {
                println!("cargo:rerun-if-changed={}", full.display());
            }
        }
    }

    let output = Command::new("sh")
        .arg("deploy/build-version.sh")
        .current_dir(&root)
        .output()
        .expect("failed to run build version resolver");
    assert!(
        output.status.success(),
        "build version resolver failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let version = String::from_utf8(output.stdout).expect("build version must be UTF-8");
    let version = version.trim();
    if version.ends_with("+unknown") {
        println!(
            "cargo:warning=Git metadata is unavailable; set ESTUARY_BUILD_VERSION for an identifiable build"
        );
    }
    println!("cargo:rustc-env=ESTUARY_BUILD_VERSION={version}");
}

fn resolve_repo_root(manifest_dir: &Path) -> PathBuf {
    if manifest_dir.join("deploy/build-version.sh").exists() {
        return manifest_dir.to_path_buf();
    }
    manifest_dir
        .parent()
        .and_then(Path::parent)
        .map_or_else(|| manifest_dir.to_path_buf(), Path::to_path_buf)
}

fn git_output(root: &Path, args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(root)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8(output.stdout).ok()?.trim().to_owned())
}
