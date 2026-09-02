use std::process::Command;

fn git_value(args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .args(args)
        .current_dir("../..")
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn source_state() -> &'static str {
    let output = Command::new("git")
        .args(["status", "--porcelain", "--untracked-files=normal"])
        .current_dir("../..")
        .output();
    match output {
        Ok(output) if output.status.success() && output.stdout.is_empty() => "clean",
        Ok(output) if output.status.success() => "worktree",
        _ => "unknown",
    }
}

fn main() {
    println!("cargo:rerun-if-changed=../../Cargo.toml");
    println!("cargo:rerun-if-changed=../../README.md");
    println!("cargo:rerun-if-changed=../../libs/myko");

    let revision =
        git_value(&["rev-parse", "--short=10", "HEAD"]).unwrap_or_else(|| "unknown".to_owned());
    let branch = git_value(&["branch", "--show-current"]).unwrap_or_else(|| "detached".to_owned());

    println!("cargo:rustc-env=MYKO_SOURCE_REVISION={revision}");
    println!("cargo:rustc-env=MYKO_SOURCE_BRANCH={branch}");
    println!("cargo:rustc-env=MYKO_SOURCE_STATE={}", source_state());
}
