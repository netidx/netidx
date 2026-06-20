//! `netidx conf <role> …` — per-system-role command groups.
//!
//! Each role template (workstation / resolver / publisher) is a
//! top-level command whose subcommands are *actions on that role's
//! install*: `install` today, with `status` / `update` / `join`
//! lifecycle ops layered on. The install engine and the shared
//! discovery / prompt / config helpers live in [`super::init`]; these
//! modules are the thin per-role command surface over it.

pub(crate) mod publisher;
pub(crate) mod resolver;
pub(crate) mod workstation;
