use std::{
    env,
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
};

fn main() {
    println!("cargo:rerun-if-changed=build.rs");

    // In debug/test builds, assets are read live from `web/dist` at runtime so
    // frontend changes never force Rust recompilation and backend unit builds do
    // not fail when `web/dist` has not been built yet.
    if env::var_os("CARGO_CFG_DEBUG_ASSERTIONS").is_some() {
        return;
    }

    let manifest_dir = PathBuf::from(
        env::var("CARGO_MANIFEST_DIR").expect("Cargo must provide the manifest directory"),
    );
    let dist_dir = manifest_dir.join("../../web/dist");
    println!("cargo:rerun-if-changed={}", dist_dir.display());
    assert!(
        dist_dir.join("index.html").is_file(),
        "web/dist/index.html is missing at {}; run `cd web && bun run build` before release builds",
        dist_dir.display()
    );

    let mut files = Vec::new();
    collect_files(&dist_dir, &dist_dir, &mut files);
    files.sort_by(|(a, _), (b, _)| a.cmp(b));

    let mut generated = String::from("pub(crate) static FILES: &[(&str, &[u8])] = &[\n");
    for (rel, abs) in &files {
        let abs = fs::canonicalize(abs).expect("embedded asset path must canonicalize");
        writeln!(
            generated,
            "    ({rel:?}, include_bytes!({:?})),",
            abs.to_string_lossy()
        )
        .expect("writing to String cannot fail");
    }
    generated.push_str("];\n");

    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("Cargo must provide OUT_DIR"));
    fs::write(out_dir.join("embedded_assets.rs"), generated)
        .expect("failed to write embedded_assets.rs");
}

fn collect_files(root: &Path, dir: &Path, out: &mut Vec<(String, PathBuf)>) {
    println!("cargo:rerun-if-changed={}", dir.display());
    let mut entries: Vec<_> = fs::read_dir(dir)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", dir.display()))
        .map(|entry| entry.expect("failed to read directory entry"))
        .collect();
    entries.sort_by_key(fs::DirEntry::file_name);

    for entry in entries {
        let path = entry.path();
        if path.is_dir() {
            collect_files(root, &path, out);
        } else if path.is_file() {
            println!("cargo:rerun-if-changed={}", path.display());
            let rel = path
                .strip_prefix(root)
                .expect("entry must be inside dist root")
                .components()
                .map(|c| c.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");
            out.push((rel, path));
        }
    }
}
