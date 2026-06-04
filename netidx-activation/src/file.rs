//! On-disk schema for activation units.
//!
//! A unit file is JSON containing a single [`Unit`]. Defaults match
//! the historical schema previously embedded in `netidx-tools`. Builders
//! are exposed (via `derive_builder`) so the schema can be constructed
//! programmatically by configuration tooling.

use anyhow::{bail, Result};
use derive_builder::Builder;
use netidx_core::path::Path;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};

/// What to do when a process exits.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum Restart {
    /// Don't restart.
    No,
    /// Restart immediately.
    Yes,
    /// Restart, but no more often than once every N seconds.
    RateLimited(f64),
}

impl Default for Restart {
    fn default() -> Self {
        Self::RateLimited(1.0)
    }
}

/// How to populate the child's environment.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum Environment {
    /// Inherit the supervisor's environment, then apply these overrides.
    Inherit(BTreeMap<String, String>),
    /// Discard the supervisor's environment, use exactly these.
    Replace(BTreeMap<String, String>),
}

impl Default for Environment {
    fn default() -> Self {
        Self::Inherit(BTreeMap::new())
    }
}

/// What causes the unit to start.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum Trigger {
    /// Start as soon as the supervisor is up.
    OnStart,
    /// Start when any of these paths is subscribed to.
    OnAccess(BTreeSet<Path>),
}

impl Default for Trigger {
    fn default() -> Self {
        Self::OnStart
    }
}

/// How to spawn the supervised process.
#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
#[serde(deny_unknown_fields)]
pub struct ProcessCfg {
    /// Path to the executable.
    #[builder(setter(into))]
    pub exe: String,

    /// Arguments passed after `exe`.
    #[serde(default)]
    #[builder(default)]
    pub args: Vec<String>,

    /// Working directory.
    #[serde(default)]
    #[builder(setter(into, strip_option), default)]
    pub working_directory: Option<PathBuf>,

    /// Drop to this uid before exec.
    #[serde(default)]
    #[builder(setter(strip_option), default)]
    pub uid: Option<u32>,

    /// Drop to this gid before exec.
    #[serde(default)]
    #[builder(setter(strip_option), default)]
    pub gid: Option<u32>,

    /// What to do when the process exits.
    #[serde(default)]
    #[builder(default)]
    pub restart: Restart,

    /// File to redirect stdin from.
    #[serde(default)]
    #[builder(setter(into, strip_option), default)]
    pub stdin: Option<PathBuf>,

    /// File to append stdout to.
    #[serde(default)]
    #[builder(setter(into, strip_option), default)]
    pub stdout: Option<PathBuf>,

    /// File to append stderr to.
    #[serde(default)]
    #[builder(setter(into, strip_option), default)]
    pub stderr: Option<PathBuf>,

    /// How to populate the child's environment.
    #[serde(default)]
    #[builder(default)]
    pub environment: Environment,
}

/// A complete activation unit. The on-disk format of a `<name>.unit` file.
#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
#[serde(deny_unknown_fields)]
pub struct Unit {
    /// What causes this unit to start. Defaults to `OnStart`.
    #[serde(default)]
    #[builder(default)]
    pub trigger: Trigger,
    /// How to run the supervised process.
    pub process: ProcessCfg,
}

/// Check that no two units claim the same `OnAccess` trigger path.
/// `units` yields `(unit_name, unit)` pairs. This is the single
/// implementation of the cross-unit trigger-conflict invariant, shared
/// by the daemon's `load_units` (runtime) and the `netidx-conf`
/// editor's pre-save `validate`, so the two can't drift.
pub fn check_trigger_conflicts<'a, I>(units: I) -> Result<()>
where
    I: IntoIterator<Item = (&'a str, &'a Unit)>,
{
    let mut claimed: BTreeSet<&Path> = BTreeSet::new();
    for (name, unit) in units {
        if let Trigger::OnAccess(paths) = &unit.trigger {
            for p in paths {
                if !claimed.insert(p) {
                    bail!(
                        "conflicting OnAccess trigger {p}: unit {name} \
                         claims a path already claimed by another unit"
                    );
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_unit() -> Unit {
        UnitBuilder::default()
            .process(
                ProcessCfgBuilder::default()
                    .exe("/usr/bin/echo")
                    .args(vec!["hello".to_string(), "world".to_string()])
                    .uid(1000u32)
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap()
    }

    #[test]
    fn round_trip() {
        let u = sample_unit();
        let json = serde_json::to_string_pretty(&u).unwrap();
        let back: Unit = serde_json::from_str(&json).unwrap();
        let json2 = serde_json::to_string_pretty(&back).unwrap();
        assert_eq!(json, json2);
    }

    #[test]
    fn determinism_env_inherit() {
        let mut env = BTreeMap::new();
        for i in 0..50 {
            env.insert(format!("VAR_{i}"), format!("value_{i}"));
        }
        let u = UnitBuilder::default()
            .process(
                ProcessCfgBuilder::default()
                    .exe("/bin/true")
                    .environment(Environment::Inherit(env))
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();
        let a = serde_json::to_string_pretty(&u).unwrap();
        let b = serde_json::to_string_pretty(&u).unwrap();
        assert_eq!(a, b);
        // Re-serialize after a round trip too.
        let parsed: Unit = serde_json::from_str(&a).unwrap();
        let c = serde_json::to_string_pretty(&parsed).unwrap();
        assert_eq!(a, c);
    }

    #[test]
    fn defaults_minimal_serialize() {
        // Only required field is process.exe.
        let u = UnitBuilder::default()
            .process(ProcessCfgBuilder::default().exe("/bin/true").build().unwrap())
            .build()
            .unwrap();
        let json = serde_json::to_string(&u).unwrap();
        let back: Unit = serde_json::from_str(&json).unwrap();
        assert!(matches!(back.trigger, Trigger::OnStart));
        assert!(matches!(back.process.restart, Restart::RateLimited(s) if (s - 1.0).abs() < f64::EPSILON));
    }

    #[test]
    fn deny_unknown_fields() {
        let bad = r#"{"process": {"exe": "/bin/true", "bogus_field": 1}}"#;
        let r: Result<Unit, _> = serde_json::from_str(bad);
        assert!(r.is_err());
    }

    #[test]
    fn legacy_format_compat() {
        // A unit JSON that mirrors how units have historically been
        // written: trigger omitted, no environment block, no
        // working_directory.
        let json = r#"{
            "process": {
                "exe": "/usr/bin/true",
                "args": ["--foo", "bar"],
                "restart": {"RateLimited": 0.5}
            }
        }"#;
        let u: Unit = serde_json::from_str(json).unwrap();
        assert_eq!(u.process.exe, "/usr/bin/true");
        assert_eq!(u.process.args, vec!["--foo", "bar"]);
    }
}
