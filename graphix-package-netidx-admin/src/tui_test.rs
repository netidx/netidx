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
    wait_render(&mut h, Duration::from_secs(60), "the identity modal", |lines| {
        lines.iter().any(|l| l.contains("Confirm the CA's identity"))
    })
    .await?;
    // accept it
    h.dispatch_event(key(KeyCode::Char('a'))).await?;
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
    let prog = format!(
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
"#,
        listen = d.listen,
    );
    let mut h =
        TuiTestHarness::with_register(&prog, crate::TEST_REGISTER, 100, 30).await?;
    wait_render(&mut h, Duration::from_secs(60), "the connect screen", |lines| {
        lines.iter().any(|l| l.contains("Connect to an admin domain"))
    })
    .await?;
    // the address is pre-filled; Enter starts the connect ceremony
    h.dispatch_event(key(KeyCode::Enter)).await?;
    wait_render(&mut h, Duration::from_secs(60), "the identity modal", |lines| {
        lines.iter().any(|l| l.contains("Confirm the CA's identity"))
    })
    .await?;
    h.dispatch_event(key(KeyCode::Char('a'))).await?;
    // no #admin was passed, so the ceremony asks for the name too
    wait_render(&mut h, Duration::from_secs(60), "the admin name modal", |lines| {
        lines.iter().any(|l| l.contains("admin name"))
    })
    .await?;
    // the name field pre-seeds the question's default (the OS
    // username) — clear it before typing the real admin name
    for _ in 0..64 {
        h.dispatch_event(key(KeyCode::Backspace)).await?;
    }
    for c in d.admin.chars() {
        h.dispatch_event(key(KeyCode::Char(c))).await?;
    }
    h.dispatch_event(key(KeyCode::Enter)).await?;
    wait_render(&mut h, Duration::from_secs(60), "the password modal", |lines| {
        lines.iter().any(|l| l.contains("admin password"))
    })
    .await?;
    for c in d.password.chars() {
        h.dispatch_event(key(KeyCode::Char(c))).await?;
    }
    h.dispatch_event(key(KeyCode::Enter)).await?;
    // the ceremony finishes and the panel menu comes up
    wait_render(&mut h, Duration::from_secs(60), "the panel menu", |lines| {
        lines.iter().any(|l| l.contains("Admin domain"))
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
