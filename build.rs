use std::{env, path::Path, process::Command};

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=Cargo.toml");
    println!("cargo:rerun-if-changed=deploy/build-version.sh");
    println!("cargo:rerun-if-env-changed=ESTUARY_BUILD_VERSION");

    // Resolve Git's paths so branch updates and tags also invalidate builds in
    // linked worktrees, where .git is a file rather than a directory.
    for name in ["HEAD", "refs", "packed-refs"] {
        if let Some(path) =
            git_output(&["rev-parse", "--git-path", name]).filter(|path| Path::new(path).exists())
        {
            println!("cargo:rerun-if-changed={path}");
        }
    }

    let root = env::var("CARGO_MANIFEST_DIR").expect("Cargo must provide the manifest directory");
    let output = Command::new("sh")
        .arg("deploy/build-version.sh")
        .current_dir(root)
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

fn git_output(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8(output.stdout).ok()?.trim().to_owned())
}
