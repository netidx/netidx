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
use tui;
use tui::overlay;
use tui::paragraph;
use netidx_admin;

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
