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

/// Accept the identity gesture if the ceremony asks it: whether it does
/// depends on whether the domain's CA is the one in the user CA
/// directory, which the tests do not control — so wait for whichever
/// of the gesture and the next question comes up.
async fn accept_gesture_if_asked(h: &mut TuiTestHarness, next: &str) -> Result<()> {
    wait_render(
        h,
        Duration::from_secs(60),
        "the gesture or the next question",
        |lines| {
            lines
                .iter()
                .any(|l| l.contains("Confirm the CA's identity") || l.contains(next))
        },
    )
    .await?;
    let asked = h.render_lines()?.iter().any(|l| l.contains("Confirm the CA's identity"));
    if asked {
        h.dispatch_event(key(KeyCode::Char('a'))).await?;
    }
    Ok(())
}

/// From the connect screen (address pre-filled): start the ceremony and
/// answer the gesture (if asked), the admin name, and the password
/// through the pump's modals.
async fn connect_via_modals(
    h: &mut TuiTestHarness,
    admin: &str,
    password: &str,
) -> Result<()> {
    wait_render(h, Duration::from_secs(60), "the connect screen", |lines| {
        lines.iter().any(|l| l.contains("Connect to an admin domain"))
    })
    .await?;
    h.dispatch_event(key(KeyCode::Enter)).await?;
    accept_gesture_if_asked(h, "admin name").await?;
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
    accept_gesture_if_asked(&mut h, "admin password").await?;
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
    connect_via_modals(&mut h, &d.admin, &d.password).await?;
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
    connect_via_modals(&mut h, "alice", &one_time).await?;
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
    connect_via_modals(&mut h, "alice", "a-brand-new-password").await?;
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
    connect_via_modals(&mut h, &d.admin, &d.password).await?;
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

/// The landing screen: with no address passed the tab starts on the
/// registry — empty here, the bookmarks live under the test's config
/// redirect — and `c` reaches connect-by-address. A session established
/// there is remembered to the bookmarks file and, back on the landing
/// after a disconnect, re-verified at its address and listed; Enter on
/// it connects with the saved fingerprint and the cached session, so
/// the menu comes up with no question asked.
#[tokio::test(flavor = "multi_thread")]
async fn landing_remembers_and_reverifies_a_domain() -> Result<()> {
    let d = TestAdminDomain::start().await?;
    let prog = remote_tab_program("");
    let mut h =
        TuiTestHarness::with_register(&prog, crate::TEST_REGISTER, 100, 30).await?;
    wait_render(&mut h, Duration::from_secs(60), "the empty landing", |lines| {
        lines.iter().any(|l| l.contains("No saved admin domains yet"))
    })
    .await?;
    h.dispatch_event(key(KeyCode::Char('c'))).await?;
    wait_render(&mut h, Duration::from_secs(60), "the connect screen", |lines| {
        lines.iter().any(|l| l.contains("Connect to an admin domain"))
    })
    .await?;
    type_text(&mut h, &d.listen.to_string()).await?;
    connect_via_modals(&mut h, &d.admin, &d.password).await?;
    wait_render(&mut h, Duration::from_secs(60), "the panel menu", |lines| {
        lines.iter().any(|l| l.contains("Enrollment Queue"))
    })
    .await?;
    // the session was recorded
    let book = std::path::PathBuf::from(std::env::var("XDG_CONFIG_HOME")?)
        .join("netidx-admin-tui")
        .join("admin-domains.json");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(text) = std::fs::read_to_string(&book)
            && text.contains(&d.domain)
        {
            break;
        }
        if Instant::now() > deadline {
            bail!("the bookmarks file never recorded {}", d.domain);
        }
        h.drain().await?;
    }
    // disconnect: the landing re-verifies the saved domain and lists it
    h.dispatch_event(key(KeyCode::Esc)).await?;
    let domain = d.domain.clone();
    let listen = d.listen.to_string();
    wait_render(&mut h, Duration::from_secs(60), "the verified domain", |lines| {
        lines.iter().any(|l| l.contains(&domain) && l.contains(&listen))
    })
    .await?;
    // Enter connects with the saved fingerprint and the session the
    // first connect cached: no gesture, no name, no password — the
    // menu comes straight up
    h.dispatch_event(key(KeyCode::Enter)).await?;
    let admin = d.admin.clone();
    wait_render(&mut h, Duration::from_secs(60), "the panel menu again", |lines| {
        lines.iter().any(|l| l.contains(&format!("as {admin}")))
            && lines.iter().any(|l| l.contains("Enrollment Queue"))
    })
    .await?;
    Ok(())
}

/// The roster's policy editor: `a` opens the form, a new role admin is
/// minted with the policy typed into it (the one-time password toasted),
/// and `e` on that admin edits the policy in place — the detail pane
/// shows the new validity.
#[tokio::test(flavor = "multi_thread")]
async fn roster_adds_and_edits_an_admin() -> Result<()> {
    let d = TestAdminDomain::start().await?;
    let prog = remote_tab_program(&d.listen.to_string());
    let mut h =
        TuiTestHarness::with_register(&prog, crate::TEST_REGISTER, 100, 34).await?;
    connect_via_modals(&mut h, &d.admin, &d.password).await?;
    wait_render(&mut h, Duration::from_secs(60), "the panel menu", |lines| {
        lines.iter().any(|l| l.contains("Admin Roster"))
    })
    .await?;
    h.dispatch_event(key(KeyCode::Char('j'))).await?;
    h.dispatch_event(key(KeyCode::Char('j'))).await?;
    h.dispatch_event(key(KeyCode::Enter)).await?;
    wait_render(&mut h, Duration::from_secs(60), "the roster panel", |lines| {
        lines.iter().any(|l| l.contains("e edit policy"))
    })
    .await?;
    // add: name, allowed SAN, validity, groups, scopes, roles, perms, manage, service
    h.dispatch_event(key(KeyCode::Char('a'))).await?;
    wait_render(&mut h, Duration::from_secs(60), "the add form", |lines| {
        lines.iter().any(|l| l.contains("Add a role admin"))
    })
    .await?;
    for (i, text) in ["bob", "*.example", "12h", "", "/apps", "Resolver", "", "no", ""]
        .iter()
        .enumerate()
    {
        if i > 0 {
            h.dispatch_event(key(KeyCode::Tab)).await?;
        }
        for _ in 0..40 {
            h.dispatch_event(key(KeyCode::Backspace)).await?;
        }
        type_text(&mut h, text).await?;
    }
    h.dispatch_event(key(KeyCode::Enter)).await?;
    wait_render(&mut h, Duration::from_secs(60), "the one-time password", |lines| {
        lines.iter().any(|l| l.contains("One-time password"))
    })
    .await?;
    h.dispatch_event(key(KeyCode::Enter)).await?;
    wait_render(&mut h, Duration::from_secs(60), "bob in the roster", |lines| {
        lines.iter().any(|l| l.contains("bob"))
    })
    .await?;
    // select bob — the roster lists the signing slots, then root, then
    // bob — and edit
    for _ in 0..3 {
        h.dispatch_event(key(KeyCode::Char('j'))).await?;
    }
    wait_render(&mut h, Duration::from_secs(60), "bob's detail", |lines| {
        lines.iter().any(|l| l.contains("max validity 12h"))
    })
    .await?;
    h.dispatch_event(key(KeyCode::Char('e'))).await?;
    wait_render(&mut h, Duration::from_secs(60), "the edit form", |lines| {
        lines.iter().any(|l| l.contains("Edit bob's policy"))
    })
    .await?;
    // validity is the second field
    h.dispatch_event(key(KeyCode::Tab)).await?;
    for _ in 0..40 {
        h.dispatch_event(key(KeyCode::Backspace)).await?;
    }
    type_text(&mut h, "36h").await?;
    h.dispatch_event(key(KeyCode::Enter)).await?;
    wait_render(&mut h, Duration::from_secs(60), "the policy toast", |lines| {
        lines.iter().any(|l| l.contains("Policy updated"))
    })
    .await?;
    h.dispatch_event(key(KeyCode::Enter)).await?;
    wait_render(&mut h, Duration::from_secs(60), "the edited validity", |lines| {
        lines.iter().any(|l| l.contains("max validity 36h"))
    })
    .await?;
    Ok(())
}

/// This host's install record for the Local tab, removed when the test
/// ends — the fixture's config root is shared by every test in the
/// process, and a leftover record would seed the landing screen of the
/// next one.
struct InstallRecordGuard(std::path::PathBuf);

impl Drop for InstallRecordGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// The row offset from the `Status` item to the item labeled `label`
/// in the rendered action list.
fn rows_below_status(lines: &[String], label: &str) -> Result<usize> {
    let row = |what: &str| {
        lines
            .iter()
            .position(|l| l.contains(what))
            .ok_or_else(|| anyhow::anyhow!("no {what} row in:\n{}", lines.join("\n")))
    };
    let status = row("Status")?;
    let target = row(label)?;
    Ok(target.saturating_sub(status))
}

/// The Local tab over a CA founded on this host: detection reads the
/// install record, the action list offers the CA's local admin tools,
/// the status card shows the domain and its glyph once the credential
/// probe lands, and Admins opens the roster over the control socket.
#[tokio::test(flavor = "multi_thread")]
async fn local_tab_detects_a_ca_install_and_opens_its_roster() -> Result<()> {
    use netidx_admin::provenance::{AdminDomainIdentity, InstallRecord, InstallRole};
    let d = TestAdminDomain::start().await?;
    let record = InstallRecord::new(
        InstallRole::Ca,
        "/",
        "tls",
        Some(AdminDomainIdentity::new(&d.domain, &d.fingerprint)),
        Some(d.listen),
    );
    let path = netidx_admin::paths::user_install_record()?;
    std::fs::write(&path, serde_json::to_vec_pretty(&record)?)?;
    let _guard = InstallRecordGuard(path);
    let prog = "let result = netidx_admin::tui::app::app()";
    let mut h =
        TuiTestHarness::with_register(prog, crate::TEST_REGISTER, 120, 40).await?;
    // the install's role and service state title the action list; the
    // admin-server config names the CA, so its local admin tools are
    // offered before any probe answers
    wait_render(&mut h, Duration::from_secs(60), "the CA's action list", |lines| {
        lines.iter().any(|l| l.contains("CA ("))
            && lines.iter().any(|l| l.contains("Admins"))
            && lines.iter().any(|l| l.contains("Back Up This Install"))
    })
    .await?;
    // something is installed, so the app shows its tab bar
    let lines = h.render_lines()?;
    assert!(
        lines.iter().any(|l| l.contains("Local") && l.contains("Admin Domain")),
        "no tab bar:\n{}",
        lines.join("\n")
    );
    // the credential probe lands and the list grows
    wait_render(&mut h, Duration::from_secs(60), "the credential actions", |lines| {
        lines.iter().any(|l| l.contains("Rotate Recovery Password"))
    })
    .await?;
    // Status is the first item: its card names the domain and carries
    // the CA glyph and the probe's verdicts
    h.dispatch_event(key(KeyCode::Enter)).await?;
    let domain = d.domain.clone();
    wait_render(&mut h, Duration::from_secs(60), "the status card", move |lines| {
        lines.iter().any(|l| l.contains("Admin domain") && l.contains(&domain))
            && lines.iter().any(|l| l.contains("CA glyph"))
            && lines.iter().any(|l| l.contains("Recovery slot") && l.contains("set"))
    })
    .await?;
    // any key closes it
    h.dispatch_event(key(KeyCode::Esc)).await?;
    wait_render(&mut h, Duration::from_secs(10), "the card to close", |lines| {
        !lines.iter().any(|l| l.contains("any key to close"))
    })
    .await?;
    // Admins: the roster over this host's control socket
    let n = rows_below_status(&h.render_lines()?, "Admins")?;
    for _ in 0..n {
        h.dispatch_event(key(KeyCode::Down)).await?;
    }
    h.dispatch_event(key(KeyCode::Enter)).await?;
    let admin = d.admin.clone();
    wait_render(&mut h, Duration::from_secs(60), "the roster", move |lines| {
        lines.iter().any(|l| l.contains("Admin Roster"))
            && lines.iter().any(|l| l.contains(&admin) && l.contains("role"))
    })
    .await?;
    // Esc backs straight out to the action list — there is no panel menu
    // behind a local panel
    h.dispatch_event(key(KeyCode::Esc)).await?;
    wait_render(&mut h, Duration::from_secs(10), "the action list again", |lines| {
        lines.iter().any(|l| l.contains("CA ("))
            && !lines.iter().any(|l| l.contains("Admin Roster"))
    })
    .await?;
    Ok(())
}

/// The Local tab's Services surface over the fixture root's activation
/// directory, with no supervisor running: the empty list, a unit
/// created through the form (the name plus the template's fields),
/// listed as not loaded, then deleted after a confirmation.
#[tokio::test(flavor = "multi_thread")]
async fn services_surface_creates_and_deletes_a_unit() -> Result<()> {
    use netidx_admin::provenance::{AdminDomainIdentity, InstallRecord, InstallRole};
    let d = TestAdminDomain::start().await?;
    let record = InstallRecord::new(
        InstallRole::Ca,
        "/",
        "tls",
        Some(AdminDomainIdentity::new(&d.domain, &d.fingerprint)),
        Some(d.listen),
    );
    let path = netidx_admin::paths::user_install_record()?;
    std::fs::write(&path, serde_json::to_vec_pretty(&record)?)?;
    let _guard = InstallRecordGuard(path);
    // the unit directory the surface manages: the user one, which
    // exists on a host that runs a supervisor
    let units_dir = netidx_admin::paths::user_activation_dir()?;
    std::fs::create_dir_all(&units_dir)?;
    let prog = "let result = netidx_admin::tui::app::app()";
    let mut h =
        TuiTestHarness::with_register(prog, crate::TEST_REGISTER, 120, 40).await?;
    wait_render(&mut h, Duration::from_secs(60), "the action list", |lines| {
        lines.iter().any(|l| l.contains("CA ("))
            && lines.iter().any(|l| l.contains("Services"))
    })
    .await?;
    let n = rows_below_status(&h.render_lines()?, "Services")?;
    for _ in 0..n {
        h.dispatch_event(key(KeyCode::Down)).await?;
    }
    h.dispatch_event(key(KeyCode::Enter)).await?;
    wait_render(&mut h, Duration::from_secs(60), "the empty services list", |lines| {
        lines.iter().any(|l| l.contains("(no units)"))
    })
    .await?;
    // create: the form opens on the name field, seeded from the template
    h.dispatch_event(key(KeyCode::Char('c'))).await?;
    wait_render(&mut h, Duration::from_secs(10), "the unit form", |lines| {
        lines.iter().any(|l| l.contains("New unit"))
            && lines.iter().any(|l| l.contains("/path/to/executable"))
    })
    .await?;
    type_text(&mut h, "myunit").await?;
    h.dispatch_event(key(KeyCode::Enter)).await?;
    wait_render(&mut h, Duration::from_secs(60), "the saved toast", |lines| {
        lines.iter().any(|l| l.contains("Unit saved"))
    })
    .await?;
    assert!(units_dir.join("myunit.unit").is_file(), "the unit file was not written");
    h.dispatch_event(key(KeyCode::Enter)).await?;
    // listed, with no supervisor to load it
    wait_render(&mut h, Duration::from_secs(60), "the unit in the list", |lines| {
        lines.iter().any(|l| l.contains("myunit"))
            && lines.iter().any(|l| l.contains("not loaded"))
            && lines.iter().any(|l| l.contains("/path/to/executable"))
    })
    .await?;
    // delete, confirmed
    h.dispatch_event(key(KeyCode::Char('d'))).await?;
    wait_render(&mut h, Duration::from_secs(10), "the delete confirmation", |lines| {
        lines.iter().any(|l| l.contains("Delete unit"))
    })
    .await?;
    h.dispatch_event(key(KeyCode::Char('y'))).await?;
    wait_render(&mut h, Duration::from_secs(60), "the deleted toast", |lines| {
        lines.iter().any(|l| l.contains("Unit deleted"))
    })
    .await?;
    assert!(!units_dir.join("myunit.unit").exists(), "the unit file was not removed");
    h.dispatch_event(key(KeyCode::Enter)).await?;
    wait_render(&mut h, Duration::from_secs(60), "the empty list again", |lines| {
        lines.iter().any(|l| l.contains("(no units)"))
    })
    .await?;
    // Esc backs out to the action list
    h.dispatch_event(key(KeyCode::Esc)).await?;
    wait_render(&mut h, Duration::from_secs(10), "the action list again", |lines| {
        lines.iter().any(|l| l.contains("CA ("))
            && !lines.iter().any(|l| l.contains("(no units)"))
    })
    .await?;
    Ok(())
}

/// A fresh machine: the welcome dialog, the role menu, and a dry-run
/// preview of the Workstation role reaching the guided install's first
/// question through the pump — cancelled there, which the tab reports
/// as the install failing. Then, with a CA install recorded, the
/// teardown's chained question: the confirmation, then whether to
/// destroy the CA, cancelled before anything runs.
#[tokio::test(flavor = "multi_thread")]
async fn fresh_machine_previews_an_install_and_a_teardown_asks_about_the_ca() -> Result<()>
{
    use netidx_admin::provenance::{AdminDomainIdentity, InstallRecord, InstallRole};
    let d = TestAdminDomain::start().await?;
    let record_path = netidx_admin::paths::user_install_record()?;
    let _ = std::fs::remove_file(&record_path);
    let prog = "let result = netidx_admin::tui::app::app()";
    let mut h =
        TuiTestHarness::with_register(prog, crate::TEST_REGISTER, 120, 40).await?;
    wait_render(&mut h, Duration::from_secs(60), "the welcome dialog", |lines| {
        lines.iter().any(|l| l.contains("Welcome to netidx"))
    })
    .await?;
    // no tab bar on a fresh machine
    let lines = h.render_lines()?;
    assert!(
        !lines.iter().any(|l| l.contains("Local") && l.contains("Admin Domain")),
        "a fresh machine showed the tab bar:\n{}",
        lines.join("\n")
    );
    h.dispatch_event(key(KeyCode::Enter)).await?;
    wait_render(&mut h, Duration::from_secs(10), "the role menu", |lines| {
        lines.iter().any(|l| l.contains("Set Up This Machine"))
            && lines.iter().any(|l| l.contains("Restore from Backup"))
            && !lines.iter().any(|l| l.contains("Welcome to netidx"))
    })
    .await?;
    // Workstation is the second choice; p previews it
    h.dispatch_event(key(KeyCode::Down)).await?;
    wait_render(&mut h, Duration::from_secs(10), "the workstation blurb", |lines| {
        lines.iter().any(|l| l.contains("full netidx node"))
    })
    .await?;
    h.dispatch_event(key(KeyCode::Char('p'))).await?;
    // the guided flow's first question, or a preview that finished
    // without one
    wait_render(
        &mut h,
        Duration::from_secs(60),
        "the install's first question",
        |lines| {
            lines
                .iter()
                .any(|l| l.contains("Esc cancel") || l.contains("Workstation preview"))
        },
    )
    .await?;
    if !h.render_lines()?.iter().any(|l| l.contains("Workstation preview")) {
        h.dispatch_event(key(KeyCode::Esc)).await?;
        wait_render(&mut h, Duration::from_secs(60), "the cancelled install", |lines| {
            lines.iter().any(|l| l.contains("Install failed"))
        })
        .await?;
    }
    h.dispatch_event(key(KeyCode::Enter)).await?;
    // now a CA install is recorded: the tab re-detects on R and offers
    // Uninstall, which asks twice before it touches anything
    let record = InstallRecord::new(
        InstallRole::Ca,
        "/",
        "tls",
        Some(AdminDomainIdentity::new(&d.domain, &d.fingerprint)),
        Some(d.listen),
    );
    std::fs::write(&record_path, serde_json::to_vec_pretty(&record)?)?;
    let _guard = InstallRecordGuard(record_path);
    h.dispatch_event(key(KeyCode::Char('R'))).await?;
    wait_render(&mut h, Duration::from_secs(60), "the CA's action list", |lines| {
        lines.iter().any(|l| l.contains("CA ("))
            && lines.iter().any(|l| l.contains("Uninstall"))
    })
    .await?;
    h.dispatch_event(key(KeyCode::Char('u'))).await?;
    wait_render(&mut h, Duration::from_secs(10), "the uninstall confirmation", |lines| {
        lines.iter().any(|l| l.contains("Remove this install?"))
    })
    .await?;
    h.dispatch_event(key(KeyCode::Char('y'))).await?;
    wait_render(&mut h, Duration::from_secs(10), "the CA question", |lines| {
        lines.iter().any(|l| l.contains("This install owns a CA"))
            && lines.iter().any(|l| l.contains("Keep the CA directory"))
    })
    .await?;
    h.dispatch_event(key(KeyCode::Esc)).await?;
    wait_render(&mut h, Duration::from_secs(10), "the question dismissed", |lines| {
        !lines.iter().any(|l| l.contains("This install owns a CA"))
            && lines.iter().any(|l| l.contains("CA ("))
    })
    .await?;
    Ok(())
}

/// Key-to-settled latency of the whole app on a fresh machine, the
/// runtime side of the milestone table (`milestone_timing` is the
/// compile side). Reports, per key kind, the time from dispatch to the
/// last update batch the key produced (the harness's quiescence wait
/// excluded), plus the app build, the first frame and a bare render.
/// Run by hand in BOTH profiles:
/// `cargo test [--release] -p graphix-package-netidx-admin milestone_latency -- --ignored --nocapture`
#[tokio::test(flavor = "multi_thread")]
#[ignore = "milestone latency, run by hand"]
async fn milestone_latency() -> Result<()> {
    fn report(what: &str, mut v: Vec<Duration>) {
        v.sort();
        let n = v.len();
        let ms = |d: Duration| d.as_secs_f64() * 1e3;
        eprintln!(
            "milestone latency: {what}: n={n} p50={:.2}ms p95={:.2}ms max={:.2}ms",
            ms(v[n / 2]),
            ms(v[n * 95 / 100]),
            ms(v[n - 1])
        );
    }
    let d = TestAdminDomain::start().await?;
    let record_path = netidx_admin::paths::user_install_record()?;
    let _ = std::fs::remove_file(&record_path);
    let t0 = Instant::now();
    let prog = "let result = netidx_admin::tui::app::app()";
    let mut h =
        TuiTestHarness::with_register(prog, crate::TEST_REGISTER, 120, 40).await?;
    let build = t0.elapsed();
    wait_render(&mut h, Duration::from_secs(60), "the welcome dialog", |lines| {
        lines.iter().any(|l| l.contains("Welcome to netidx"))
    })
    .await?;
    let first_frame = t0.elapsed();
    eprintln!("milestone latency: app build {build:?}, first frame {first_frame:?}");
    h.dispatch_event(key(KeyCode::Enter)).await?;
    wait_render(&mut h, Duration::from_secs(10), "the role menu", |lines| {
        lines.iter().any(|l| l.contains("Set Up This Machine"))
    })
    .await?;
    // the role menu: a list selection plus the role's blurb per key
    let mut moves = Vec::new();
    for _ in 0..25 {
        moves.push(h.dispatch_event_timed(key(KeyCode::Down)).await?);
        moves.push(h.dispatch_event_timed(key(KeyCode::Up)).await?);
    }
    report("role menu Down/Up", moves);
    // the guided install's first question through the pump: a modal
    // text field, typed into and erased
    h.dispatch_event(key(KeyCode::Down)).await?;
    h.dispatch_event(key(KeyCode::Char('p'))).await?;
    wait_render(
        &mut h,
        Duration::from_secs(60),
        "the install's first question",
        |lines| {
            lines
                .iter()
                .any(|l| l.contains("Esc cancel") || l.contains("Workstation preview"))
        },
    )
    .await?;
    if h.render_lines()?.iter().any(|l| l.contains("Esc cancel")) {
        let mut typed = Vec::new();
        for c in "the quick brown fox jumps".chars() {
            typed.push(h.dispatch_event_timed(key(KeyCode::Char(c))).await?);
        }
        report("modal text field Char", typed);
        let mut erased = Vec::new();
        for _ in 0..25 {
            erased.push(h.dispatch_event_timed(key(KeyCode::Backspace)).await?);
        }
        report("modal text field Backspace", erased);
        h.dispatch_event(key(KeyCode::Esc)).await?;
    }
    let mut renders = Vec::new();
    for _ in 0..50 {
        let t = Instant::now();
        let _ = h.render()?;
        renders.push(t.elapsed());
    }
    report("bare render", renders);
    drop(d);
    Ok(())
}
