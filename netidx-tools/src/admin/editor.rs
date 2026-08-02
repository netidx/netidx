//! `visudo`-style editor loop. Renders the current value as JSON,
//! drops it in a temp file, spawns the operator's `$VISUAL` / `$EDITOR`
//! (falling back to the platform's own default), validates the result, and
//! offers a re-edit prompt on failure.

use anyhow::{Context, Result};
use std::{
    io::{BufRead, IsTerminal, Write},
    path::Path,
    process::Command,
};

/// Edit `initial` in the operator's editor; loop until `validate`
/// accepts the contents (or the operator aborts). On success returns
/// the validated value.
pub fn edit_with_validation<T, F>(initial: &str, validate: F) -> Result<T>
where
    F: Fn(&str) -> Result<T>,
{
    let mut tmp = tempfile::Builder::new()
        .prefix("netidx-admin-")
        .suffix(".json")
        .tempfile()
        .context("creating editor temp file")?;
    tmp.as_file_mut()
        .write_all(initial.as_bytes())
        .context("seeding editor temp file")?;
    tmp.as_file_mut().sync_all().ok();
    let temp_path = tmp.into_temp_path();
    let path: &Path = &temp_path;
    loop {
        spawn_editor(path)?;
        let edited = std::fs::read_to_string(path)
            .with_context(|| format!("reading edited file {:?}", path))?;
        match validate(&edited) {
            Ok(v) => return Ok(v),
            Err(e) => {
                eprintln!("\nvalidation failed: {e:#}\n");
                if !std::io::stdin().is_terminal() {
                    bail!(
                        "validation failed and stdin is not a TTY; cannot prompt for re-edit"
                    );
                }
                if !confirm("re-edit? [Y/n] ") {
                    bail!("edit aborted; temp file discarded");
                }
            }
        }
    }
}

/// The editor to use when the operator has named none. `vi` is the unix
/// answer and is not on a Windows host at all, so falling back to it there
/// turns "you did not set $EDITOR" into "vi: not found".
fn default_editor() -> &'static str {
    if cfg!(windows) { "notepad" } else { "vi" }
}

fn spawn_editor(path: &Path) -> Result<()> {
    let editor = std::env::var("VISUAL")
        .ok()
        .or_else(|| std::env::var("EDITOR").ok())
        .unwrap_or_else(|| default_editor().to_string());
    let mut parts = editor.split_whitespace();
    let cmd = parts.next().context("VISUAL/EDITOR is empty")?;
    let status = Command::new(cmd)
        .args(parts)
        .arg(path)
        .status()
        .with_context(|| format!("launching editor {editor:?}"))?;
    if !status.success() {
        bail!("editor {editor:?} exited with {status}");
    }
    Ok(())
}

fn confirm(prompt: &str) -> bool {
    print!("{prompt}");
    std::io::stdout().flush().ok();
    let stdin = std::io::stdin();
    let mut line = String::new();
    // EOF (0 bytes) and IO errors both count as "no".
    match stdin.lock().read_line(&mut line) {
        Ok(0) | Err(_) => return false,
        Ok(_) => {}
    }
    let answer = line.trim().to_lowercase();
    answer.is_empty() || answer == "y" || answer == "yes"
}
