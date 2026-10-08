use std::{
    fs::{self, File},
    io::{Read, Write},
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Context, Result, bail};
use serde::Serialize;

pub(super) fn validate_release(release_root: &Path, release: &Path) -> Result<PathBuf> {
    let release = validate_release_dir(release_root, release)?;
    let binary = release.join("estuary");
    if !binary.is_file() {
        bail!("release binary is missing: {}", binary.display());
    }
    Ok(binary)
}

pub(super) fn validate_release_dir(release_root: &Path, release: &Path) -> Result<PathBuf> {
    let root = release_root
        .canonicalize()
        .with_context(|| format!("invalid release root {}", release_root.display()))?;
    let release = release
        .canonicalize()
        .with_context(|| format!("invalid release directory {}", release.display()))?;
    if release.parent() != Some(root.as_path()) {
        bail!("release must be an immediate child of {}", root.display());
    }
    Ok(release)
}

pub(super) fn stage_release(release_root: &Path, binary: &Path) -> Result<PathBuf> {
    let binary = binary
        .canonicalize()
        .with_context(|| format!("invalid candidate binary {}", binary.display()))?;
    let output = Command::new(&binary)
        .arg("--version")
        .output()
        .context("failed to execute candidate binary")?;
    if !output.status.success() {
        bail!("candidate --version failed: {}", output.status);
    }
    let stdout = String::from_utf8(output.stdout).context("candidate version is not UTF-8")?;
    let version = stdout
        .split_whitespace()
        .nth(1)
        .context("candidate did not report a version")?;
    if !safe_version(version) {
        bail!("candidate reported an unsafe version: {version}");
    }
    fs::create_dir_all(release_root)?;
    let release = release_root.join(version);
    let destination = release.join("estuary");
    if destination.exists() {
        if file_hash(&destination)? != file_hash(&binary)? {
            bail!("release {version} already exists with different content");
        }
        return release
            .canonicalize()
            .context("failed to resolve existing release");
    }

    fs::create_dir(&release)
        .with_context(|| format!("failed to create release {}", release.display()))?;
    let temporary = release.join(".estuary.tmp");
    fs::copy(&binary, &temporary).context("failed to copy candidate binary")?;
    fs::set_permissions(&temporary, fs::Permissions::from_mode(0o755))?;
    File::open(&temporary)?.sync_all()?;
    fs::rename(&temporary, &destination)?;
    File::open(&release)?.sync_all()?;
    release
        .canonicalize()
        .context("failed to resolve staged release")
}

pub(super) fn safe_version(version: &str) -> bool {
    !version.is_empty()
        && version
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._+-".contains(&byte))
}

pub(super) fn file_hash(path: &Path) -> Result<blake3::Hash> {
    let mut file = File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize())
}

pub(super) fn read_release_link(link: &Path) -> Result<PathBuf> {
    link.canonicalize()
        .with_context(|| format!("failed to resolve release link {}", link.display()))
}

pub(super) fn atomic_symlink(target: &Path, link: &Path) -> Result<()> {
    let parent = link
        .parent()
        .with_context(|| format!("link has no parent: {}", link.display()))?;
    fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(
        ".{}.{}.tmp",
        link.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("link"),
        uuid::Uuid::now_v7()
    ));
    std::os::unix::fs::symlink(target, &temporary)?;
    fs::rename(&temporary, link)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

pub(super) fn write_json_atomic(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path
        .parent()
        .with_context(|| format!("state file has no parent: {}", path.display()))?;
    fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(".rollout.{}.tmp", uuid::Uuid::now_v7()));
    let mut file = File::create(&temporary)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    fs::rename(&temporary, path)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

pub(super) fn remove_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("failed to remove {}", path.display())),
    }
}
