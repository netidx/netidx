//! The ceremony machinery against a LIVE admin domain — the first
//! integration coverage any admin frontend has had. One founded domain
//! serves both flows, interactive first (the session it caches would
//! otherwise silence the questions the first flow asserts).

use anyhow::{Context, Result, bail};
use graphix_package_core::testing;
use graphix_rt::GXEvent;
use netidx_admin::testing::TestAdminDomain;
use netidx_value::Value;
use std::time::Duration;

/// Compile one graphix block against the package and wait for its value.
pub(crate) async fn run_program(prog: String, timeout_s: u64) -> Result<Value> {
    let (tx, mut rx) = tokio::sync::mpsc::channel(10);
    let ctx = testing::init(tx, &crate::TEST_REGISTER).await?;
    let e = ctx.rt.compile(arcstr::ArcStr::from(prog)).await?;
    let eid = e.exprs.last().context("empty program")?.id;
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

/// The reset-password route through the package API, in graphix: a
/// role admin minted with a one-time password is refused a session
/// with the ROUTING variant, `change_password_at` replaces the password
/// over a fresh password session without re-asking the gesture (the
/// `#glyph` confirmed during the first connect), and the new password
/// then opens a session whose `info` names the admin.
#[tokio::test(flavor = "multi_thread")]
async fn reset_password_routes_to_change_password_at() -> Result<()> {
    let d = TestAdminDomain::start().await?;
    let prog = format!(
        r#"{{
  let confirm = |ev: netidx_admin::Event<'a>| select ev {{
    `ConfirmIdentity(q) => netidx_admin::answer(q.id, `Confirm(true)),
    _ => never()
  }};
  let c = netidx_admin::connect(#admin: "{admin}", #password: "{pw}", "{listen}");
  let ev = netidx_admin::events(c);
  confirm(ev);
  let glyph: [netidx_admin::Fingerprint, null] = null;
  glyph <- select ev {{
    `ConfirmIdentity(q) => q.identity.fingerprint,
    _ => never()
  }};
  let t = netidx_admin::result(c)$;
  let policy: netidx_admin::Policy = {{
    allowed_san: [],
    max_validity: duration:3600.s,
    id_map_groups: [],
    server_enroll_scopes: [],
    server_enroll_roles: [],
    perms_edit_scopes: [],
    may_manage_admins: false,
    service_control_scopes: []
  }};
  let one_time = netidx_admin::add_role_admin("alice", policy, t)$;
  let refused = netidx_admin::connect(#admin: "alice", #password: one_time, "{listen}");
  confirm(netidx_admin::events(refused));
  let must_change: string = never();
  {{
    catch(e) select (e.0).error {{
      `PasswordChangeRequired(name) => must_change <- name,
      _ => never()
    }};
    netidx_admin::result(refused)?
  }};
  let changed = netidx_admin::change_password_at(
    #admin: must_change,
    #password: must_change ~ one_time,
    #glyph: must_change ~ glyph,
    #new_password: "a-brand-new-password",
    must_change ~ "{listen}"
  );
  confirm(netidx_admin::events(changed));
  let done = netidx_admin::result(changed)$;
  let again = netidx_admin::connect(
    #admin: "alice",
    #password: "a-brand-new-password",
    #glyph: done ~ glyph,
    done ~ "{listen}"
  );
  confirm(netidx_admin::events(again));
  select netidx_admin::info(netidx_admin::result(again)$) {{
    `Remote({{ admin, identity, .. }}) => "[admin]@[identity.domain]",
    `Local(_) => "local"
  }}
}}"#,
        admin = d.admin,
        listen = d.listen,
        pw = d.password,
    );
    let v = run_program(prog, 90).await?;
    match &v {
        Value::String(s) if s.starts_with("alice@") => Ok(()),
        v => bail!("the route did not end in alice's session: {v}"),
    }
}
