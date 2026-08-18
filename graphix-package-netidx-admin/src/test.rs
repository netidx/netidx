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
