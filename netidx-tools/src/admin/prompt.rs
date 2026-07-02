//! Interactive prompts for missing CLI arguments.
//!
//! Three levels of "required-ness" for a CLI argument:
//!
//! - **Level 0** — no prompt. The argument has a silent default
//!   (clap `default_value`, or `.unwrap_or(...)` at the call
//!   site). Nothing in this module handles level 0; there's nothing
//!   to ask.
//! - **Level 1** — prompt, but offer a default. Blank input (just
//!   pressing return) takes the shown default. A non-TTY caller gets
//!   the default silently — there's a usable value either way, the
//!   prompt just lets an interactive operator override it. See
//!   [`string_with_default`], [`parsed_with_default`],
//!   [`choice_with_default`].
//! - **Level 2** — prompt and require a non-empty answer. A non-TTY
//!   caller bails with an error naming the flag they'd have passed.
//!   See [`required_string`], [`required_path`], [`required_parsed`].
//!
//! Kept in the CLI layer per the project convention that the
//! `netidx-admin` library stays free of user-IO.

use anyhow::{Context, Result, anyhow};
use std::{
    fmt::Display,
    io::{BufRead, IsTerminal, Write},
    path::PathBuf,
    str::FromStr,
};

/// Is stdin a real terminal we can prompt on?
///
/// In test builds this is unconditionally `false`. `cargo test` does
/// not redirect the test binary's stdin — it inherits the launching
/// terminal's stdin, so a live `is_terminal()` here returns `true`
/// whenever the suite is run from a terminal, and every prompt-driven
/// test then blocks forever on input that never comes. (It passes in
/// CI only because CI hands the runner a non-TTY stdin — i.e. the
/// behaviour was a property of the environment, not the test.) Pinning
/// it to `false` makes the non-interactive branches deterministic
/// regardless of how the suite was launched; the interactive branches
/// are smoke-tested against the built binary.
///
/// `pub(super)` so the other `admin` submodules share this single
/// definition rather than re-checking `is_terminal()` themselves.
#[cfg(not(test))]
pub(super) fn stdin_is_tty() -> bool {
    std::io::stdin().is_terminal()
}

#[cfg(test)]
pub(super) fn stdin_is_tty() -> bool {
    false
}

/// Wrap `s` in ANSI bold, but only when stdout is a real terminal —
/// otherwise the escape codes would land in a pipe or log.
fn bold(s: &str) -> String {
    if std::io::stdout().is_terminal() {
        format!("\x1b[1m{s}\x1b[0m")
    } else {
        s.to_string()
    }
}

/// Print `prompt` and read one line. Returns `Ok(None)` on EOF
/// (operator pressed Ctrl-D) so callers can distinguish "blank line"
/// from "no more input". The returned string is trimmed of the
/// trailing newline only — interior whitespace is preserved.
///
/// Callers must check `stdin_is_tty()` before calling; reading from
/// a non-TTY here would block a pipeline.
fn read_line(prompt: &str) -> Result<Option<String>> {
    print!("{prompt}");
    std::io::stdout().flush().context("flushing stdout for prompt")?;
    let mut line = String::new();
    let n = std::io::stdin()
        .lock()
        .read_line(&mut line)
        .context("reading prompt response")?;
    if n == 0 {
        return Ok(None); // EOF
    }
    Ok(Some(line.trim_end_matches(['\n', '\r']).to_string()))
}

// ---- level 2: prompt, require a non-empty answer -----------------------

/// Prompt for a required string. Blank input re-prompts; a non-TTY
/// caller, or EOF at the prompt, bails with an error naming the flag.
pub fn required_string(label: &str, provided: Option<String>) -> Result<String> {
    if let Some(s) = provided {
        return Ok(s);
    }
    if !stdin_is_tty() {
        anyhow::bail!(
            "{label} is required (stdin is not a TTY so I cannot prompt; \
             pass the corresponding --flag)"
        );
    }
    loop {
        match read_line(&format!("{label}: "))? {
            None => anyhow::bail!("{label} is required (got EOF at the prompt)"),
            Some(line) if line.is_empty() => {
                eprintln!("{label} must not be empty; please enter a value");
                continue;
            }
            Some(line) => return Ok(line),
        }
    }
}

/// Prompt for a required filesystem path. Same semantics as
/// [`required_string`].
pub fn required_path(label: &str, provided: Option<PathBuf>) -> Result<PathBuf> {
    let s = required_string(label, provided.map(|p| p.to_string_lossy().into_owned()))?;
    Ok(PathBuf::from(s))
}

/// Prompt for a required value parseable from a string. Re-prompts on
/// parse failure (a typo is recoverable on a TTY); a non-TTY caller,
/// or EOF at the prompt, bails.
pub fn required_parsed<T>(label: &str, provided: Option<T>) -> Result<T>
where
    T: FromStr,
    T::Err: Display,
{
    if let Some(v) = provided {
        return Ok(v);
    }
    if !stdin_is_tty() {
        anyhow::bail!(
            "{label} is required (stdin is not a TTY so I cannot prompt; \
             pass the corresponding --flag)"
        );
    }
    loop {
        match read_line(&format!("{label}: "))? {
            None => anyhow::bail!("{label} is required (got EOF at the prompt)"),
            Some(line) if line.is_empty() => {
                eprintln!("{label} must not be empty; please enter a value");
                continue;
            }
            Some(line) => match line.parse::<T>() {
                Ok(v) => return Ok(v),
                Err(e) => {
                    eprintln!("invalid {label}: {e}; please try again");
                    continue;
                }
            },
        }
    }
}

/// Prompt for a required value parsed by a caller-supplied `parse`
/// function — the level-2 analogue of [`optional_with`], and the
/// closure-taking sibling of [`required_parsed`] for when turning the
/// typed line into a value is more than a `FromStr` (e.g. resolving a
/// hostname and defaulting an omitted port). Re-prompts on parse error
/// (a typo is recoverable on a TTY); a non-TTY caller, or EOF at the
/// prompt, bails. The error is shown with its full context chain.
// Only the unix-only admin commands (delegation / perms admin) call this
// today; keep it available cross-platform but don't warn on Windows.
#[cfg_attr(not(unix), allow(dead_code))]
pub fn required_with<T>(
    label: &str,
    mut parse: impl FnMut(&str) -> Result<T>,
) -> Result<T> {
    if !stdin_is_tty() {
        anyhow::bail!(
            "{label} is required (stdin is not a TTY so I cannot prompt; \
             pass the corresponding --flag)"
        );
    }
    loop {
        match read_line(&format!("{label}: "))? {
            None => anyhow::bail!("{label} is required (got EOF at the prompt)"),
            Some(line) if line.is_empty() => {
                eprintln!("{label} must not be empty; please enter a value");
                continue;
            }
            Some(line) => match parse(&line) {
                Ok(v) => return Ok(v),
                Err(e) => {
                    eprintln!("invalid {label}: {e:#}; please try again");
                    continue;
                }
            },
        }
    }
}

// ---- level 1: prompt, offer a default ----------------------------------

/// Prompt for a string, offering `default`. Blank input (or EOF, or a
/// non-TTY caller) takes `default`.
///
/// No caller wires this up today — every level-1 arg so far is
/// either an enum (`choice_with_default`) or parseable
/// (`parsed_with_default`). It's kept because a plain
/// string-with-default is the most basic shape of the level-1
/// system and leaving it out would be the odd omission; the first
/// free-text level-1 arg should use it rather than reinventing it.
#[allow(dead_code)]
pub fn string_with_default(
    label: &str,
    provided: Option<String>,
    default: &str,
) -> Result<String> {
    if let Some(s) = provided {
        return Ok(s);
    }
    if !stdin_is_tty() {
        return Ok(default.to_string());
    }
    match read_line(&format!("{label} [{default}]: "))? {
        None => Ok(default.to_string()),
        Some(line) if line.is_empty() => Ok(default.to_string()),
        Some(line) => Ok(line),
    }
}

/// Parse a string error into the "internal: default doesn't parse"
/// shape. A level-1 default that fails to parse is a caller bug, not
/// operator error — surface it as such rather than as a confusing
/// "invalid input".
fn parse_default<T>(label: &str, default: &str) -> Result<T>
where
    T: FromStr,
    T::Err: Display,
{
    default.parse::<T>().map_err(|e| {
        anyhow!("internal error: default {default:?} for {label} does not parse: {e}")
    })
}

/// Prompt for a parseable value, offering `default` (given as the
/// string form so the displayed hint and the fallback value share a
/// single source of truth). Blank input / EOF / non-TTY take the
/// default; a bad answer on a TTY re-prompts.
pub fn parsed_with_default<T>(
    label: &str,
    provided: Option<T>,
    default: &str,
) -> Result<T>
where
    T: FromStr,
    T::Err: Display,
{
    if let Some(v) = provided {
        return Ok(v);
    }
    if !stdin_is_tty() {
        return parse_default(label, default);
    }
    loop {
        match read_line(&format!("{label} [{default}]: "))? {
            None => return parse_default(label, default),
            Some(line) if line.is_empty() => return parse_default(label, default),
            Some(line) => match line.parse::<T>() {
                Ok(v) => return Ok(v),
                Err(e) => {
                    eprintln!("invalid {label}: {e}; please try again");
                    continue;
                }
            },
        }
    }
}

/// Prompt for one of a fixed set of `choices`, offering `default`.
/// The choice list is shown with the default rendered in bold.
/// Blank input / EOF / non-TTY take the default; a value outside the
/// set re-prompts (the parse step is what rejects it, so `T`'s
/// `FromStr` is the authority on what's valid).
pub fn choice_with_default<T>(
    label: &str,
    provided: Option<T>,
    choices: &[&str],
    default: &str,
) -> Result<T>
where
    T: FromStr,
    T::Err: Display,
{
    if let Some(v) = provided {
        return Ok(v);
    }
    if !stdin_is_tty() {
        return parse_default(label, default);
    }
    let rendered = choices
        .iter()
        .map(|c| if *c == default { bold(c) } else { c.to_string() })
        .collect::<Vec<_>>()
        .join(", ");
    let hint = format!("{label} [{rendered}]: ");
    loop {
        match read_line(&hint)? {
            None => return parse_default(label, default),
            Some(line) if line.is_empty() => return parse_default(label, default),
            // The displayed options are authoritative: validate against
            // them before parsing. `T` is often `String` (whose `FromStr`
            // never fails), so without this an off-list answer would slip
            // through to whatever catch-all consumes the parsed value.
            Some(line) if !choices.contains(&line.as_str()) => {
                eprintln!(
                    "invalid {label}: {line:?} is not one of [{}]; please try again",
                    choices.join(", ")
                );
                continue;
            }
            Some(line) => match line.parse::<T>() {
                Ok(v) => return Ok(v),
                Err(e) => {
                    eprintln!("invalid {label}: {e}; please try again");
                    continue;
                }
            },
        }
    }
}

// ---- level 1 (optional): prompt, default = `None` ----------------------

/// Prompt for an optional parseable value. Blank input / EOF / non-TTY
/// returns `None`. A bad answer on a TTY re-prompts (blank still
/// escapes to `None`). Used when "no value" is itself a meaningful
/// choice — e.g. "is there a network-wide parent resolver? [none]:".
pub fn optional_parsed<T>(label: &str, provided: Option<T>) -> Result<Option<T>>
where
    T: FromStr,
    T::Err: Display,
{
    if let Some(v) = provided {
        return Ok(Some(v));
    }
    if !stdin_is_tty() {
        return Ok(None);
    }
    loop {
        match read_line(&format!("{label} [none]: "))? {
            None => return Ok(None),
            Some(line) if line.is_empty() => return Ok(None),
            Some(line) => match line.parse::<T>() {
                Ok(v) => return Ok(Some(v)),
                Err(e) => {
                    eprintln!("invalid {label}: {e}; try again (or blank for none)");
                    continue;
                }
            },
        }
    }
}

/// Prompt for an optional value parsed by a caller-supplied `parse`
/// function — for when turning the typed line into a value is more than
/// a `FromStr` (e.g. resolving a hostname to addresses and defaulting an
/// omitted port). Blank input / EOF / non-TTY returns `None`. A parse
/// error on a TTY re-prompts (blank still escapes to `None`); the error
/// is shown with its full context chain.
pub fn optional_with<T>(
    label: &str,
    mut parse: impl FnMut(&str) -> Result<T>,
) -> Result<Option<T>> {
    if !stdin_is_tty() {
        return Ok(None);
    }
    loop {
        match read_line(&format!("{label} [none]: "))? {
            None => return Ok(None),
            Some(line) if line.is_empty() => return Ok(None),
            Some(line) => match parse(&line) {
                Ok(v) => return Ok(Some(v)),
                Err(e) => {
                    eprintln!("invalid {label}: {e:#}; try again (or blank for none)");
                    continue;
                }
            },
        }
    }
}

// ---- yes/no confirmation ------------------------------------------------

/// Ask a yes/no question. Blank input and EOF take `default`; a
/// non-TTY caller also takes `default` — a confirm that has a
/// sensible default is safe to auto-answer when nobody's watching.
/// (A confirm that *must* be answered by a human should not be a
/// confirm — gate on a level-2 prompt downstream instead, which is
/// what the CA-create flow does: `confirm` says "yes" unattended,
/// then the CA common-name prompt bails because it's level 2.)
/// Unrecognised input re-prompts.
pub fn confirm(question: &str, default: bool) -> Result<bool> {
    if !stdin_is_tty() {
        return Ok(default);
    }
    let hint = if default {
        format!("{question} [{}/n]: ", bold("Y"))
    } else {
        format!("{question} [y/{}]: ", bold("N"))
    };
    loop {
        match read_line(&hint)? {
            None => return Ok(default),
            Some(line) => match line.trim().to_ascii_lowercase().as_str() {
                "" => return Ok(default),
                "y" | "yes" => return Ok(true),
                "n" | "no" => return Ok(false),
                _ => {
                    eprintln!("please answer 'y' or 'n'");
                    continue;
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // In test builds `stdin_is_tty()` is pinned to `false` (see its
    // doc comment), so these deterministically exercise the
    // provided-value and non-TTY branches. The interactive branches
    // are smoke-tested against the built binary.

    #[test]
    fn provided_value_short_circuits_every_level() {
        assert_eq!(required_string("x", Some("v".into())).unwrap(), "v");
        assert_eq!(
            required_path("x", Some(PathBuf::from("/p"))).unwrap(),
            PathBuf::from("/p"),
        );
        assert_eq!(required_parsed::<u32>("x", Some(7)).unwrap(), 7);
        assert_eq!(string_with_default("x", Some("v".into()), "d").unwrap(), "v",);
        assert_eq!(parsed_with_default::<u32>("x", Some(7), "9").unwrap(), 7,);
        assert_eq!(
            choice_with_default::<u32>("x", Some(7), &["7", "9"], "9").unwrap(),
            7,
        );
    }

    #[test]
    fn level_2_bails_without_tty() {
        assert!(required_string("flag", None).is_err());
        assert!(required_path("flag", None).is_err());
        assert!(required_parsed::<u32>("flag", None).is_err());
    }

    #[test]
    fn level_1_uses_default_without_tty() {
        assert_eq!(string_with_default("flag", None, "deflt").unwrap(), "deflt",);
        assert_eq!(parsed_with_default::<u32>("flag", None, "65534").unwrap(), 65534,);
        assert_eq!(
            choice_with_default::<u32>("flag", None, &["1", "2"], "2").unwrap(),
            2,
        );
    }

    #[test]
    fn optional_parsed_provided_round_trips() {
        assert_eq!(optional_parsed::<u32>("flag", Some(42)).unwrap(), Some(42),);
    }

    #[test]
    fn optional_parsed_non_tty_returns_none() {
        assert_eq!(optional_parsed::<u32>("flag", None).unwrap(), None);
    }

    #[test]
    fn confirm_non_tty_returns_default() {
        assert!(confirm("proceed?", true).unwrap());
        assert!(!confirm("proceed?", false).unwrap());
    }

    #[test]
    fn level_1_internal_default_parse_failure_is_an_error() {
        // A default that doesn't parse is a caller bug; the non-TTY
        // path surfaces it rather than silently doing something else.
        let r = parsed_with_default::<u32>("flag", None, "not-a-number");
        assert!(r.is_err());
        assert!(format!("{:#}", r.unwrap_err()).contains("internal error"));
    }
}
