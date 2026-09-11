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

// An explicit type predicate on an ABSTRACT type is a nominal tag
// test since graphix's nominal-abstract-types work (2026-08-22): over
// a scrutinee that can hold the type it compiles and dispatches
// exactly by AbstractId; over a disjoint scrutinee it is a dead arm,
// refused with the type's NAME (graphix 2026-08-31 — the id→name
// diagnostic registry; it printed the word "abstract" before). This
// pin's two earlier forms tracked the pre-nominal semantics: first
// the silent dead arm, then the wholesale compile refusal.
#[tokio::test]
async fn abstract_type_predicate_is_nominal() -> anyhow::Result<()> {
    let (tx, _rx) = tokio::sync::mpsc::channel(10);
    let ctx = graphix_package_core::testing::init(tx, &crate::TEST_REGISTER).await?;
    let e = ctx
        .rt
        .compile(arcstr::literal!("select 42 { netidx_admin::Target as t => 0, _ => 1 }"))
        .await;
    match e {
        Ok(_) => panic!("an abstract predicate matched a disjoint scrutinee"),
        Err(e) => {
            let msg = format!("{e:#}");
            assert!(
                msg.contains("pattern Target will never match"),
                "wrong error: {msg}"
            );
        }
    }
    ctx.rt
        .compile(arcstr::literal!(
            "|v: [netidx_admin::Target, i64]| -> i64 select v { \
             netidx_admin::Target as _ => 0, i64 as _ => 1 }"
        ))
        .await
        .map(|_| ())
        .map_err(|e| anyhow::anyhow!("the nominal tag test failed to compile: {e:#}"))
}

/// Milestone timing per the findings-log discipline: registration
/// (all stdlib + this package's packed-AST decode + typecheck) and
/// the compile of the app main (both tabs under the pump). Run by
/// hand at every ~1k lines of `.gx`:
/// The session image of the app, cold against warm, wall clock,
/// fusion off (the image cannot hold kernels); run by hand:
/// `cargo test -p graphix-package-netidx-admin milestone_image -- --ignored --nocapture`
#[tokio::test]
#[ignore = "timing, run by hand"]
async fn milestone_image() -> anyhow::Result<()> {
    use graphix_compiler::{CFlag, expr::Source};
    use graphix_package_core::testing::init_with_session;
    use graphix_rt::RegistrationImage;
    let prog = arcstr::literal!("netidx_admin::tui::app::app(#server: \"127.0.0.1:1\")");
    let (tx, _rx) = tokio::sync::mpsc::channel(10);
    let (reg_tx, _reg_rx) = tokio::sync::oneshot::channel();
    let (prog_tx, prog_rx) = tokio::sync::oneshot::channel();
    let t0 = std::time::Instant::now();
    let cold = init_with_session(
        tx,
        &crate::TEST_REGISTER,
        CFlag::FusionDisabled.into(),
        RegistrationImage::Save(reg_tx),
        Some(Source::Internal(prog)),
        Some(prog_tx),
    )
    .await?;
    let cold_t = t0.elapsed();
    let image = prog_rx.await??;
    cold.shutdown().await;
    let mut warm_t = Vec::new();
    for _ in 0..5 {
        // The previous runtime tears down on this executor after its
        // shutdown returns; a start measured over it would count that.
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let (tx, _rx) = tokio::sync::mpsc::channel(10);
        let t = std::time::Instant::now();
        let warm = init_with_session(
            tx,
            &crate::TEST_REGISTER,
            CFlag::FusionDisabled.into(),
            RegistrationImage::Load(image.clone()),
            None,
            None,
        )
        .await?;
        warm_t.push(t.elapsed());
        warm.shutdown().await;
    }
    eprintln!(
        "milestone image: cold {cold_t:?}, warm {warm_t:?}, image {} bytes",
        image.len()
    );
    Ok(())
}

/// `cargo test -p graphix-package-netidx-admin milestone_timing -- --ignored --nocapture`
#[tokio::test]
#[ignore = "milestone timing, run by hand"]
async fn milestone_timing() -> anyhow::Result<()> {
    let (tx, _rx) = tokio::sync::mpsc::channel(10);
    let t0 = std::time::Instant::now();
    let ctx = graphix_package_core::testing::init(tx, &crate::TEST_REGISTER).await?;
    let reg = t0.elapsed();
    let prog = arcstr::literal!("netidx_admin::tui::app::app(#server: \"127.0.0.1:1\")");
    let t1 = std::time::Instant::now();
    ctx.rt.compile(prog.clone()).await?;
    let compile = t1.elapsed();
    let stats = ctx.rt.fusion_stats().await?;
    eprintln!(
        "milestone: registration {reg:?}, app-main compile {compile:?} \
         (kernels attempted {} fused {})",
        stats.attempted, stats.fused
    );
    // the blocker profile: failure reasons with quoted names elided
    let mut reasons: std::collections::HashMap<String, usize> = Default::default();
    for f in &stats.failed {
        let mut key = String::new();
        let mut quoted = false;
        for c in f.reason.chars() {
            match (c, quoted) {
                ('`', false) => {
                    quoted = true;
                    key.push_str("`_`");
                }
                ('`', true) => quoted = false,
                (_, true) => (),
                (c, false) => key.push(c),
            }
        }
        if let Some((i, _)) = key.char_indices().nth(110) {
            key.truncate(i);
        }
        *reasons.entry(key).or_default() += 1;
    }
    let mut reasons: Vec<_> = reasons.into_iter().collect();
    reasons.sort_by(|a, b| b.1.cmp(&a.1));
    for (reason, n) in reasons.iter().take(15) {
        eprintln!("milestone:   {n:5} {reason}");
    }
    // the same compile with fusion off: the JIT's share of startup
    let (tx, _rx) = tokio::sync::mpsc::channel(10);
    let ctx = graphix_package_core::testing::init_with_flags_and_setup(
        tx,
        &crate::TEST_REGISTER,
        vec![],
        enumflags2::BitFlags::from(graphix_compiler::CFlag::FusionDisabled),
        |_| {},
    )
    .await?;
    let t2 = std::time::Instant::now();
    ctx.rt.compile(prog).await?;
    eprintln!("milestone: app-main compile without fusion {:?}", t2.elapsed());
    Ok(())
}
