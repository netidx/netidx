use anyhow::Result;
use graphix_package_core::{run, testing::FuseExpect};
use netidx_admin_proto::fingerprint::Fingerprint;
use netidx_value::Value;
use std::sync::LazyLock;

/// A real fingerprint (of fixed bytes) so tests exercise the canonical
/// grouped-base32 form without hardcoding a stale rendering.
static FP: LazyLock<Fingerprint> = LazyLock::new(|| Fingerprint::of_der(b"graphix"));

const PARSE_GARBAGE: &str = r#"
  netidx_admin::parse_fingerprint("not a fingerprint")
"#;

run!(parse_garbage, PARSE_GARBAGE, |v: Result<&Value>| {
    matches!(v, Ok(Value::Error(_)))
});

run!(
    parse_roundtrip,
    format!(r#"netidx_admin::parse_fingerprint("{}")"#, FP.text()),
    |v: Result<&Value>| {
        match v {
            Ok(Value::Array(pairs)) => pairs.iter().any(|p| match p {
                Value::Array(kv) => {
                    kv[0] == Value::from("code") && kv[1] == Value::from(FP.text())
                }
                _ => false,
            }),
            _ => false,
        }
    }
);

run!(
    identicon_of_fingerprint,
    format!(
        r#"netidx_admin::identicon({{code: "{}", short: "{}"}})"#,
        FP.text(),
        FP.short()
    ),
    |v: Result<&Value>| {
        match v {
            // {cells, color} — a struct, not an error
            Ok(Value::Array(pairs)) => pairs.len() == 2,
            _ => false,
        }
    }
);

const LOCAL_BAD_PATH: &str = r#"
  netidx_admin::local(#cfg_path: "/nonexistent/admin-server.json", true)
"#;

run!(
    local_bad_path,
    LOCAL_BAD_PATH,
    |v: Result<&Value>| { matches!(v, Ok(Value::Error(_))) };
    FuseExpect::None
);

const CONNECT_REFUSED: &str = r#"
{
  let c = netidx_admin::connect("127.0.0.1:1");
  select netidx_admin::events(c) {
    `Done(r) => r,
    _ => never()
  }
}
"#;

run!(
    connect_refused,
    CONNECT_REFUSED,
    |v: Result<&Value>| { matches!(v, Ok(Value::Error(_))) };
    FuseExpect::None
);

// mDNS browse fires and produces a value (an array, or an error where
// the network stack forbids mDNS) — the wiring test, not a discovery
// assertion.
run!(
    discover_produces,
    r#"netidx_admin::discover(#timeout: duration:0.25s, true)"#,
    |v: Result<&Value>| { matches!(v, Ok(Value::Array(_)) | Ok(Value::Error(_))) };
    FuseExpect::None
);

// A remote-plane op minted against a Local target must refuse at the
// mint site with a value, not a ceremony.
graphix_package_core::run_with_tempdir! {
    name: remote_op_on_local_target_errors,
    code: "netidx_admin::list_queue(netidx_admin::local(#cfg_path: \"{}\", true)$)",
    setup: |dir| {
        let p = dir.path().join("admin-server.json");
        std::fs::write(&p, "{{}}").unwrap();
        p
    },
    expect_error
}

// PINS A GRAPHIX TRAP (findings 2026-08-18): an explicit type
// predicate on an ABSTRACT type typechecks but matches NOTHING at
// runtime (`is_a` refuses to claim what it can't verify), so the arm
// is guaranteed dead and the wildcard wins. The `$`/`?` operators are
// the designed dissectors for [T, Error] unions. This pin flips when
// graphix either refuses the predicate at compile time or makes it
// verifiable.
graphix_package_core::run_with_tempdir! {
    name: abstract_type_predicate_is_dead_at_runtime,
    code: "select netidx_admin::local(#cfg_path: \"{}\", true) {{ netidx_admin::Target as t => \"target\", _ => \"other\" }}",
    setup: |dir| {
        let p = dir.path().join("admin-server.json");
        std::fs::write(&p, "{{}}").unwrap();
        p
    },
    expect: |v: ::netidx::subscriber::Value| -> ::anyhow::Result<()> {
        match v {
            ::netidx::subscriber::Value::String(s) if &*s == "other" => Ok(()),
            v => panic!("expected the documented dead-arm behavior, got: {v:?}"),
        }
    }
}
