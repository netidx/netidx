//! The modal question pump against a LIVE admin domain, driven through
//! the headless TUI harness — the first end-to-end test of the Graphix
//! admin TUI's core: a real connect ceremony's questions render as
//! modals, key events answer them, and the ceremony's result lands.

use anyhow::{Result, bail};
use crossterm::event::{Event, KeyCode, KeyEvent};
use graphix_package_tui::testing::TuiTestHarness;
use netidx_admin::testing::TestAdminDomain;
use netidx_value::Value;
use std::time::{Duration, Instant};

fn key(code: KeyCode) -> Event {
    Event::Key(KeyEvent::new(code, crossterm::event::KeyModifiers::NONE))
}

async fn type_text(h: &mut TuiTestHarness, s: &str) -> Result<()> {
    for c in s.chars() {
        h.dispatch_event(key(KeyCode::Char(c))).await?;
    }
    Ok(())
}

/// Wait for a text modal, replace whatever it pre-seeded with `s`, and
/// submit it.
async fn answer_text(h: &mut TuiTestHarness, label: &str, s: &str) -> Result<()> {
    let what = format!("the {label} modal");
    wait_render(h, Duration::from_secs(60), &what, |lines| {
        lines.iter().any(|l| l.contains(label))
    })
    .await?;
    for _ in 0..64 {
        h.dispatch_event(key(KeyCode::Backspace)).await?;
    }
    type_text(h, s).await?;
    h.dispatch_event(key(KeyCode::Enter)).await
}

/// The remote tab's app-main composition: the tab under the shared pump.
fn remote_tab_program(listen: &str) -> String {
    format!(
        r#"
use tui::input_handler::{{self, *}};
use tui::layout::{{self, *}};
use tui::overlay::{{self, *}};
use tui::text::{{self, *}};

let q: netidx_admin::Question = never();
let r = netidx_admin::tui::remote::remote(#q: &q, #server: "{listen}");
let p = netidx_admin::tui::pump(q);
let layers = array::concat(r.layers, p.layers);
let result = overlay(#layers: &layers,
  input_handler(#handle: &r.handle,
    &layout(#direction: &`Vertical, &[
      child(#constraint: `Min(1), r.view),
      child(#constraint: `Length(1), text(&[r.status]))
    ])))
"#
    )
}

/// From the connect screen (address pre-filled): start the ceremony and
/// answer the gesture, the admin name, and the password through the
/// pump's modals.
async fn connect_via_modals(
    h: &mut TuiTestHarness,
    d: &TestAdminDomain,
    admin: &str,
    password: &str,
) -> Result<()> {
    wait_render(h, Duration::from_secs(60), "the connect screen", |lines| {
        lines.iter().any(|l| l.contains("Connect to an admin domain"))
    })
    .await?;
    h.dispatch_event(key(KeyCode::Enter)).await?;
    if d.gesture_expected() {
        wait_render(h, Duration::from_secs(60), "the identity modal", |lines| {
            lines.iter().any(|l| l.contains("Confirm the CA's identity"))
        })
        .await?;
        h.dispatch_event(key(KeyCode::Char('a'))).await?;
    }
    // no #admin was passed, so the ceremony asks for the name too; the
    // field pre-seeds the question's default (the OS username)
    answer_text(h, "admin name", admin).await?;
    answer_text(h, "admin password", password).await
}

/// Drain + render until `pred` is true of the buffer lines, or fail
/// with the last render after `timeout`.
async fn wait_render(
    h: &mut TuiTestHarness,
    timeout: Duration,
    what: &str,
    pred: impl Fn(&[String]) -> bool,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        h.drain().await?;
        let lines = h.render_lines()?;
        if pred(&lines) {
            return Ok(());
        }
        if Instant::now() > deadline {
            bail!("timeout waiting for {what}; last render:\n{}", lines.join("\n"));
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn pump_drives_a_live_connect() -> Result<()> {
    let d = TestAdminDomain::start().await?;
    let prog = format!(
        r#"
use tui::overlay::{{self, *}};
use tui::paragraph::{{self, *}};

let c = netidx_admin::connect(#admin: "{admin}", "{listen}");
let p = netidx_admin::tui::pump(netidx_admin::questions(c));
let status = "waiting";
{{
  catch(e) status <- e ~ "failed";
  let t = netidx_admin::result(c)?;
  status <- t ~ "connected"
}};
let result = overlay(#layers: &p.layers, paragraph(&"base"))
"#,
        admin = d.admin,
        listen = d.listen,
    );
    let mut h =
        TuiTestHarness::with_register(&prog, crate::TEST_REGISTER, 80, 24).await?;
    h.watch("test::status").await?;
    // the security gesture: the ConfirmIdentity modal comes up with the
    // CA's glyph and grouped fingerprint
    if d.gesture_expected() {
        wait_render(&mut h, Duration::from_secs(60), "the identity modal", |lines| {
            lines.iter().any(|l| l.contains("Confirm the CA's identity"))
        })
        .await?;
        h.dispatch_event(key(KeyCode::Char('a'))).await?;
    }
    // the password question renders as a masked text modal
    wait_render(&mut h, Duration::from_secs(60), "the password modal", |lines| {
        lines.iter().any(|l| l.contains("admin password"))
    })
    .await?;
    // type the password and submit; the echo must be masked
    for c in d.password.chars() {
        h.dispatch_event(key(KeyCode::Char(c))).await?;
    }
    let lines = h.render_lines()?;
    assert!(
        !lines.iter().any(|l| l.contains(&d.password)),
        "the password echoed in clear text:\n{}",
        lines.join("\n")
    );
    assert!(
        lines.iter().any(|l| l.contains('•')),
        "no masked echo rendered:\n{}",
        lines.join("\n")
    );
    h.dispatch_event(key(KeyCode::Enter)).await?;
    // the ceremony finishes: the modal closes and the result lands
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        h.drain().await?;
        match h.get_watched("test::status") {
            Some(Value::String(s)) if &**s == "connected" => break,
            Some(Value::String(s)) if &**s == "failed" => {
                bail!("the connect ceremony failed")
            }
            _ if Instant::now() > deadline => bail!("timeout waiting for the result"),
            _ => (),
        }
    }
    let lines = h.render_lines()?;
    assert!(
        !lines.iter().any(|l| l.contains("admin password")),
        "the modal did not close:\n{}",
        lines.join("\n")
    );
    Ok(())
}

/// The remote tab end to end against a live domain: connect through
/// the pump's modals (identity gesture, admin name, password), land on
/// the panel menu, open the enrollment queue, walk back, and open the
/// roster — every panel load a real ceremony over the session.
#[tokio::test(flavor = "multi_thread")]
async fn remote_tab_drives_the_panels() -> Result<()> {
    let d = TestAdminDomain::start().await?;
    let prog = remote_tab_program(&d.listen.to_string());
    let mut h =
        TuiTestHarness::with_register(&prog, crate::TEST_REGISTER, 100, 30).await?;
    connect_via_modals(&mut h, &d, &d.admin, &d.password).await?;
    // the ceremony finishes and the panel menu names the session
    let admin = d.admin.clone();
    wait_render(&mut h, Duration::from_secs(60), "the panel menu", |lines| {
        lines.iter().any(|l| l.contains(&format!("as {admin}")))
            && lines.iter().any(|l| l.contains("Enrollment Queue"))
    })
    .await?;
    // open the queue: its load ceremony runs against the session
    h.dispatch_event(key(KeyCode::Enter)).await?;
    wait_render(&mut h, Duration::from_secs(60), "the queue panel", |lines| {
        lines.iter().any(|l| l.contains("a approve"))
    })
    .await?;
    // back to the menu, then down twice to the roster
    h.dispatch_event(key(KeyCode::Esc)).await?;
    wait_render(&mut h, Duration::from_secs(60), "the menu again", |lines| {
        lines.iter().any(|l| l.contains("certificate-enrollment requests"))
    })
    .await?;
    h.dispatch_event(key(KeyCode::Char('j'))).await?;
    h.dispatch_event(key(KeyCode::Char('j'))).await?;
    h.dispatch_event(key(KeyCode::Enter)).await?;
    // the roster table renders the founding admin and the detail pane
    let admin = d.admin.clone();
    wait_render(&mut h, Duration::from_secs(60), "the roster panel", |lines| {
        lines.iter().any(|l| l.contains(&admin))
            && lines.iter().any(|l| l.contains("may manage admins"))
    })
    .await?;
    Ok(())
}

/// The reset-password route through the TUI: a role admin's one-time
/// password is refused at connect, the tab routes into the
/// from-scratch change-password ceremony (no second identity gesture —
/// the confirmed glyph rides along), the new password is asked twice,
/// and the new password then connects. From the roster, `c` changes the
/// session's own password and sends the operator back to connect.
#[tokio::test(flavor = "multi_thread")]
async fn remote_tab_routes_a_reset_password() -> Result<()> {
    let d = TestAdminDomain::start().await?;
    let one_time = d.mint_role_admin("alice").await?;
    let prog = remote_tab_program(&d.listen.to_string());
    let mut h =
        TuiTestHarness::with_register(&prog, crate::TEST_REGISTER, 100, 30).await?;
    connect_via_modals(&mut h, &d, "alice", &one_time).await?;
    // refused with PasswordChangeRequired: the change-password ceremony
    // asks the current password (the gesture, if any, was confirmed once
    // already and rides along as the glyph), then the new one twice
    answer_text(&mut h, "admin password", &one_time).await?;
    answer_text(&mut h, "new password", "a-brand-new-password").await?;
    answer_text(&mut h, "confirm password", "a-brand-new-password").await?;
    wait_render(&mut h, Duration::from_secs(60), "the changed toast", |lines| {
        lines.iter().any(|l| l.contains("Password changed"))
    })
    .await?;
    h.dispatch_event(key(KeyCode::Enter)).await?;
    // the new password opens a session
    connect_via_modals(&mut h, &d, "alice", "a-brand-new-password").await?;
    wait_render(&mut h, Duration::from_secs(60), "alice's menu", |lines| {
        lines.iter().any(|l| l.contains("as alice"))
    })
    .await?;
    // the roster's own-password change: new password twice, then back
    // to the connect screen
    h.dispatch_event(key(KeyCode::Char('j'))).await?;
    h.dispatch_event(key(KeyCode::Char('j'))).await?;
    h.dispatch_event(key(KeyCode::Enter)).await?;
    wait_render(&mut h, Duration::from_secs(60), "the roster panel", |lines| {
        lines.iter().any(|l| l.contains("change my password"))
    })
    .await?;
    h.dispatch_event(key(KeyCode::Char('c'))).await?;
    answer_text(&mut h, "new password", "yet-another-password").await?;
    answer_text(&mut h, "confirm password", "yet-another-password").await?;
    wait_render(&mut h, Duration::from_secs(60), "the changed toast", |lines| {
        lines.iter().any(|l| l.contains("Password changed"))
    })
    .await?;
    h.dispatch_event(key(KeyCode::Enter)).await?;
    wait_render(&mut h, Duration::from_secs(60), "the connect screen", |lines| {
        lines.iter().any(|l| l.contains("Connect to an admin domain"))
    })
    .await?;
    Ok(())
}

/// The services, perms and read-gate flows against a domain whose only
/// server is the CA: the server pick and the cluster pick run their
/// ceremonies and land on their empty-list states, and `g` on the CA's
/// row refuses a read gate for a server that runs no resolver.
#[tokio::test(flavor = "multi_thread")]
async fn remote_tab_opens_the_services_and_perms_screens() -> Result<()> {
    let d = TestAdminDomain::start().await?;
    let prog = remote_tab_program(&d.listen.to_string());
    let mut h =
        TuiTestHarness::with_register(&prog, crate::TEST_REGISTER, 100, 30).await?;
    connect_via_modals(&mut h, &d, &d.admin, &d.password).await?;
    wait_render(&mut h, Duration::from_secs(60), "the panel menu", |lines| {
        lines.iter().any(|l| l.contains("Services"))
    })
    .await?;
    // Services is the sixth entry: its server pick lists nothing here
    for _ in 0..5 {
        h.dispatch_event(key(KeyCode::Char('j'))).await?;
    }
    h.dispatch_event(key(KeyCode::Enter)).await?;
    wait_render(&mut h, Duration::from_secs(60), "the empty server pick", |lines| {
        lines.iter().any(|l| l.contains("Pick an admin server"))
            && lines
                .iter()
                .any(|l| l.contains("no registered admin server runs a resolver"))
    })
    .await?;
    h.dispatch_event(key(KeyCode::Esc)).await?;
    // Permissions is the seventh: no active resolver cluster to pick
    h.dispatch_event(key(KeyCode::Char('j'))).await?;
    h.dispatch_event(key(KeyCode::Enter)).await?;
    wait_render(&mut h, Duration::from_secs(60), "the empty cluster pick", |lines| {
        lines.iter().any(|l| l.contains("Pick a resolver cluster"))
            && lines.iter().any(|l| l.contains("no active resolver cluster"))
    })
    .await?;
    h.dispatch_event(key(KeyCode::Esc)).await?;
    // Servers (fourth): the CA row has no read gate, and `g` says so
    for _ in 0..3 {
        h.dispatch_event(key(KeyCode::Char('k'))).await?;
    }
    h.dispatch_event(key(KeyCode::Enter)).await?;
    wait_render(&mut h, Duration::from_secs(60), "the servers panel", |lines| {
        lines.iter().any(|l| l.contains("Gate"))
            && lines.iter().any(|l| l.contains("no resolver, so no read gate"))
    })
    .await?;
    h.dispatch_event(key(KeyCode::Char('g'))).await?;
    wait_render(&mut h, Duration::from_secs(60), "the no-gate toast", |lines| {
        lines.iter().any(|l| l.contains("No read gate"))
    })
    .await?;
    h.dispatch_event(key(KeyCode::Enter)).await?;
    wait_render(&mut h, Duration::from_secs(60), "the toast dismissed", |lines| {
        !lines.iter().any(|l| l.contains("No read gate"))
    })
    .await?;
    Ok(())
}
