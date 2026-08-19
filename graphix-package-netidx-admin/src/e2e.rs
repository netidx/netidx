//! The ceremony machinery against a LIVE admin domain — the first
//! integration coverage any admin frontend has had. One founded domain
//! serves both flows, interactive first (the session it caches would
//! otherwise silence the questions the first flow asserts).

use anyhow::{Result, bail};
use graphix_package_core::testing;
use graphix_rt::GXEvent;
use netidx_admin::testing::TestAdminDomain;
use netidx_value::Value;
use std::time::Duration;

/// Compile one graphix block against the package and wait for its value.
async fn run_program(prog: String, timeout_s: u64) -> Result<Value> {
    let (tx, mut rx) = tokio::sync::mpsc::channel(10);
    let ctx = testing::init(tx, &crate::TEST_REGISTER).await?;
    let e = ctx.rt.compile(arcstr::ArcStr::from(prog)).await?;
    let eid = e.exprs[0].id;
    let timeout = tokio::time::sleep(Duration::from_secs(timeout_s));
    tokio::pin!(timeout);
    loop {
        tokio::select! {
            _ = &mut timeout => bail!("timeout waiting for the program's value"),
            batch = rx.recv() => match batch {
                None => bail!("runtime died"),
                Some(mut batch) => {
                    for ev in batch.drain(..) {
                        match ev {
                            GXEvent::Env(_) => (),
                            GXEvent::Diagnostic(_, d) => eprintln!("{d}"),
                            GXEvent::Updated(id, v) if id == eid => return Ok(v),
                            GXEvent::Updated(id, v) => eprintln!("aux {id:?}: {v}"),
                        }
                    }
                }
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn ceremonies_against_a_live_domain() -> Result<()> {
    let d = TestAdminDomain::start().await?;
    // ── the interactive path: the security gesture and the password
    // arrive as QUESTIONS, answered from graphix ──
    let prog = format!(
        r#"{{
  let c = netidx_admin::connect(#admin: "{admin}", "{listen}");
  let ev = netidx_admin::events(c);
  let answers = select ev {{
    `ConfirmIdentity(q) => netidx_admin::answer(q.id, `Confirm(true)),
    `Secret(q) => netidx_admin::answer(q.id, `Secret("{pw}")),
    _ => never()
  }};
  select ev {{
    `Done(r) => r,
    _ => never()
  }}
}}"#,
        admin = d.admin,
        listen = d.listen,
        pw = d.password,
    );
    let v = run_program(prog, 60).await?;
    match &v {
        Value::Abstract(_) => (),
        v => bail!("interactive connect did not yield a target: {v}"),
    }
    // ── provided credentials ride the cached session, and the target
    // drives a real roster query and a remote ceremony ──
    let prog = format!(
        r#"{{
  let c = netidx_admin::connect(#admin: "{admin}", #password: "{pw}", "{listen}");
  let ev = netidx_admin::events(c);
  let answers = select ev {{
    `ConfirmIdentity(q) => netidx_admin::answer(q.id, `Confirm(true)),
    _ => never()
  }};
  let t = select ev {{
    `Done(r) => r$,
    _ => never()
  }};
  let admins = netidx_admin::list_admins(t);
  let queue = select netidx_admin::events(netidx_admin::list_queue(t)) {{
    `Done(r) => r,
    _ => never()
  }};
  {{ admins, queue }}
}}"#,
        admin = d.admin,
        listen = d.listen,
        pw = d.password,
    );
    let v = run_program(prog, 60).await?;
    let fields = match &v {
        Value::Array(fields) => fields,
        v => bail!("expected a struct, got {v}"),
    };
    let get = |name: &str| {
        fields.iter().find_map(|p| match p {
            Value::Array(kv) if matches!(&kv[0], Value::String(s) if &**s == name) => {
                Some(kv[1].clone())
            }
            _ => None,
        })
    };
    match get("admins") {
        Some(Value::Array(rows)) => {
            assert!(!rows.is_empty(), "roster empty");
            let root = format!("{}", Value::Array(rows.clone()));
            assert!(root.contains("root"), "no root admin in {root}");
        }
        v => bail!("bad admins: {v:?}"),
    }
    match get("queue") {
        Some(Value::Array(rows)) => assert!(rows.is_empty(), "fresh queue not empty"),
        v => bail!("bad queue: {v:?}"),
    }
    Ok(())
}
