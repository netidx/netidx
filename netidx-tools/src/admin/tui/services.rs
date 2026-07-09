//! The Local tab's **Services** surface: list, control, and locally define the
//! activation units on *this* machine.
//!
//! It talks **directly** to the local activation supervisor
//! ([`netidx_activation::control`]) and unit directory
//! ([`netidx_admin::activation::ActivationDir`]) — never through an admin
//! server. That is deliberate and load-bearing: a unit is an arbitrary command
//! line, so defining one is equivalent to running code on the box. **Unit
//! definition (create / edit / delete) is therefore local-only** — this module
//! is the *sole* path to `ActivationDir`, and no remote/cluster code ever
//! constructs a [`ServicesAction`]. Cluster-scope service control (the Cluster
//! tab's `Service` panel) can only start/stop/restart units defined here.

use super::{
    action::{Action, Outcome},
    answer::{EditValidator, TuiAnswerer},
    theme,
};
use anyhow::{Context, Result, bail};
use crossterm::event::KeyCode;
use netidx_activation::control::{
    ControlOp, ControlRequest, ControlResponse, UnitState, control,
};
use netidx_admin::activation::{
    ActivationDir, ProcessCfgBuilder, Unit, UnitBuilder, validate,
};
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Style},
    text::{Line, Span},
    widgets::{List, ListItem, ListState, Paragraph, Wrap},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

/// One row in the Services list: an on-disk unit definition merged with its live
/// run-state from the local supervisor.
#[derive(Clone)]
pub(super) struct ServiceRow {
    /// Basename, no `.unit` suffix.
    pub(super) name: String,
    /// Present in the unit directory (vs. only reported by the supervisor).
    pub(super) defined: bool,
    /// Live run-state; `None` if the supervisor didn't report this unit (e.g. a
    /// just-created unit before the next reload).
    pub(super) state: Option<UnitState>,
    exe: String,
    args: Vec<String>,
    trigger: String,
    restart: String,
}

impl ServiceRow {
    /// Build a display row from a cluster member's reported unit (the Cluster
    /// tab's remote services panel). The member pre-formats the definition
    /// fields, so this is a pure mapping — no `ActivationDir` access, which
    /// stays local-only.
    pub(super) fn from_service_unit(su: &netidx_admin::admin_proto::ServiceUnit) -> ServiceRow {
        let (exe, args, trigger, restart) = match &su.definition {
            Some(d) => (d.exe.clone(), d.args.clone(), d.trigger.clone(), d.restart.clone()),
            None => (String::new(), Vec::new(), String::new(), String::new()),
        };
        ServiceRow {
            name: su.unit.clone(),
            defined: su.definition.is_some(),
            state: Some(su.state.clone()),
            exe,
            args,
            trigger,
            restart,
        }
    }
}

/// The refreshed rows a completed services op applies to [`ServicesState`].
pub(super) struct ServicesUpdate {
    pub(super) rows: Vec<ServiceRow>,
}

/// A local activation-services op. Every variant carries the units dir it acts
/// on (always `default_units_dir()`), so the whole surface touches exactly one
/// on-box directory.
pub(super) enum ServicesAction {
    /// `list()` + `control(Status)`, merged → rows. Quiet (no toast).
    Refresh { units_dir: PathBuf },
    /// start/stop/restart one unit (`Some`) or all (`None`); then re-list.
    Control { units_dir: PathBuf, op: ControlOp, unit: Option<String> },
    /// Editor on a generic template → validate → save → reload → re-list.
    Create { units_dir: PathBuf, name: String },
    /// Editor on the unit's current JSON → validate → save → reload → re-list.
    Edit { units_dir: PathBuf, name: String },
    /// remove → reload → re-list. Confirm-gated at the Action layer.
    Delete { units_dir: PathBuf, name: String },
}

impl ServicesAction {
    /// Progress-header label while the op runs.
    pub(super) fn label(&self) -> String {
        match self {
            ServicesAction::Refresh { .. } => "Checking services",
            ServicesAction::Control { op, .. } => match op {
                ControlOp::Start => "Starting service",
                ControlOp::Stop => "Stopping service",
                ControlOp::Restart => "Restarting service",
                _ => "Services",
            },
            ServicesAction::Create { .. } => "Creating unit",
            ServicesAction::Edit { .. } => "Editing unit",
            ServicesAction::Delete { .. } => "Deleting unit",
        }
        .to_string()
    }

    /// A yes/no confirmation to require before running (Stop + Delete), or `None`.
    pub(super) fn confirm_message(&self) -> Option<String> {
        match self {
            ServicesAction::Control { op: ControlOp::Stop, unit: Some(u), .. } => {
                Some(format!("Stop unit {u:?}? It stays down until started."))
            }
            ServicesAction::Control { op: ControlOp::Stop, unit: None, .. } => {
                Some("Stop this machine's netidx services? Running units stop.".to_string())
            }
            ServicesAction::Delete { name, .. } => Some(format!(
                "Delete unit {name:?}? This removes its definition file and reloads \
                 the supervisor."
            )),
            _ => None,
        }
    }
}

/// Run a services op. Takes the answerer so create/edit can drive the
/// terminal-suspending `$EDITOR` flow; the rest ignore it.
pub(super) async fn run(ans: &mut TuiAnswerer, action: ServicesAction) -> Result<Outcome> {
    match action {
        ServicesAction::Refresh { units_dir } => {
            Ok(Outcome::services_rows(svc_rows(&units_dir).await?))
        }
        ServicesAction::Control { units_dir, op, unit } => {
            let units: Vec<String> = unit.into_iter().collect();
            match control(&units_dir, &ControlRequest { op, units }).await? {
                ControlResponse::Ok { .. } => {}
                ControlResponse::Err { reason } => bail!("{reason}"),
            }
            let title = match op {
                ControlOp::Start => "Service started",
                ControlOp::Stop => "Service stopped",
                ControlOp::Restart => "Service restarted",
                _ => "Services",
            };
            Ok(Outcome::services_after(title, Vec::new(), svc_rows(&units_dir).await?))
        }
        ServicesAction::Create { units_dir, name } => {
            let ad = ActivationDir::open(Some(&units_dir))?;
            // The rest of the set, for the cross-unit trigger-conflict check.
            let validate = unit_validator(ad.list()?, name.clone());
            let edited = ans.edit(template_unit_json(), validate).await?;
            let unit: Unit = serde_json::from_str(&edited).context("parsing the edited unit")?;
            ad.save(&name, &unit)?;
            reload(&units_dir).await?;
            Ok(Outcome::services_after(
                "Unit created",
                vec![format!("Created unit {name:?}.")],
                svc_rows(&units_dir).await?,
            ))
        }
        ServicesAction::Edit { units_dir, name } => {
            let ad = ActivationDir::open(Some(&units_dir))?;
            let seed = serde_json::to_string_pretty(&ad.get(&name)?)
                .context("serializing the current unit")?;
            // Exclude this unit's own (old) definition from the conflict check.
            let mut others = ad.list()?;
            others.remove(&name);
            let validate = unit_validator(others, name.clone());
            let edited = ans.edit(seed, validate).await?;
            let unit: Unit = serde_json::from_str(&edited).context("parsing the edited unit")?;
            ad.save(&name, &unit)?;
            reload(&units_dir).await?;
            Ok(Outcome::services_after(
                "Unit saved",
                vec![format!("Saved unit {name:?}.")],
                svc_rows(&units_dir).await?,
            ))
        }
        ServicesAction::Delete { units_dir, name } => {
            ActivationDir::open(Some(&units_dir))?.remove(&name)?;
            reload(&units_dir).await?;
            Ok(Outcome::services_after(
                "Unit deleted",
                vec![format!("Deleted unit {name:?}.")],
                svc_rows(&units_dir).await?,
            ))
        }
    }
}

/// The definitions (from disk) merged with live run-states (from the supervisor)
/// into display rows. All I/O lives here, off the render frame.
async fn svc_rows(units_dir: &Path) -> Result<Vec<ServiceRow>> {
    let defs = ActivationDir::open(Some(units_dir))?.list()?;
    let statuses = match control(
        units_dir,
        &ControlRequest { op: ControlOp::Status, units: Vec::new() },
    )
    .await?
    {
        ControlResponse::Ok { units } => units,
        ControlResponse::Err { reason } => bail!("{reason}"),
    };
    let mut states: BTreeMap<String, UnitState> =
        statuses.into_iter().map(|u| (u.unit, u.state)).collect();
    let mut names: BTreeSet<String> = defs.keys().cloned().collect();
    names.extend(states.keys().cloned());
    let rows = names
        .into_iter()
        .map(|name| {
            let def = defs.get(&name);
            let (exe, args, trigger, restart) = match def {
                Some(u) => (
                    u.process.exe.clone(),
                    u.process.args.clone(),
                    u.trigger.to_string(),
                    u.process.restart.to_string(),
                ),
                None => (String::new(), Vec::new(), String::new(), String::new()),
            };
            ServiceRow {
                defined: def.is_some(),
                state: states.remove(&name),
                name,
                exe,
                args,
                trigger,
                restart,
            }
        })
        .collect();
    Ok(rows)
}

/// Reload the supervisor so it picks up an on-disk unit change.
async fn reload(units_dir: &Path) -> Result<()> {
    match control(units_dir, &ControlRequest { op: ControlOp::Reload, units: Vec::new() }).await? {
        ControlResponse::Ok { .. } => Ok(()),
        ControlResponse::Err { reason } => bail!("{reason}"),
    }
}

/// The `$EDITOR` validator for a unit: parse the JSON, run the cross-unit
/// trigger-conflict check against the rest of the set, return normalized JSON.
/// `others` captured so the check runs inside the re-edit loop.
fn unit_validator(others: BTreeMap<String, Unit>, name: String) -> EditValidator {
    Box::new(move |s: &str| -> Result<String> {
        let unit: Unit = serde_json::from_str(s).context("not a valid unit (JSON)")?;
        let mut set = others.clone();
        set.insert(name.clone(), unit.clone());
        validate(&set)?;
        serde_json::to_string_pretty(&unit).context("serializing unit")
    })
}

/// A generic unit template the create flow seeds the editor with. Built via the
/// builders so it round-trips the `deny_unknown_fields` decoder.
fn template_unit_json() -> String {
    let unit = UnitBuilder::default()
        .process(
            ProcessCfgBuilder::default()
                .exe("/path/to/executable")
                .build()
                .expect("template process cfg"),
        )
        .build()
        .expect("template unit");
    serde_json::to_string_pretty(&unit).expect("serializing template unit")
}

/// A row's bare status word + color for the Status pane (the pid is a separate
/// line).
fn state_word(row: &ServiceRow) -> (&'static str, Color) {
    match (&row.state, row.defined) {
        (Some(UnitState::Running { .. }), _) => ("running", theme::OK),
        (Some(UnitState::Stopped), _) => ("stopped", theme::WARN),
        (Some(UnitState::Died), _) => ("died", theme::ACCENT),
        (Some(UnitState::NotStarted), _) => ("not started", theme::HINT_FG),
        // Defined but the supervisor hasn't loaded it yet, or reported but no
        // file — both clear on the next reload/refresh.
        (None, true) => ("not loaded (press r)", theme::WARN),
        (None, false) => ("removed (press r)", theme::HINT_FG),
    }
}

/// Which sub-screen of the Services surface is showing.
enum SvcScreen {
    List,
    /// Inline unit-name entry before the editor opens on a fresh template.
    NamePrompt { input: String },
}

pub(super) struct ServicesState {
    /// The one directory this surface touches — always `default_units_dir()`,
    /// passed to both `ActivationDir` and `control()`. Never the user-default
    /// (which would desync from a system-dir supervisor).
    units_dir: PathBuf,
    rows: Vec<ServiceRow>,
    list: ListState,
    screen: SvcScreen,
    error: Option<String>,
}

impl ServicesState {
    pub(super) fn new(units_dir: PathBuf) -> ServicesState {
        ServicesState {
            units_dir,
            rows: Vec::new(),
            list: ListState::default(),
            screen: SvcScreen::List,
            error: None,
        }
    }

    /// On the unit list (vs. the name prompt) — the host closes the surface on
    /// Esc only from here.
    pub(super) fn at_list(&self) -> bool {
        matches!(self.screen, SvcScreen::List)
    }

    /// The name prompt captures keystrokes so a name with `q`/`l`/`j`/`k` isn't
    /// eaten by list navigation or the global shortcuts.
    pub(super) fn capturing_text(&self) -> bool {
        matches!(self.screen, SvcScreen::NamePrompt { .. })
    }

    pub(super) fn apply(&mut self, update: ServicesUpdate) {
        self.rows = update.rows;
        if self.rows.is_empty() {
            self.list.select(None);
        } else {
            let sel = self.list.selected().unwrap_or(0).min(self.rows.len() - 1);
            self.list.select(Some(sel));
        }
    }

    fn selected_name(&self) -> Option<String> {
        self.list.selected().and_then(|i| self.rows.get(i)).map(|r| r.name.clone())
    }

    pub(super) fn on_key(&mut self, code: KeyCode) -> Option<Action> {
        if let SvcScreen::NamePrompt { input } = &mut self.screen {
            match code {
                KeyCode::Char(c) => input.push(c),
                KeyCode::Backspace => {
                    input.pop();
                }
                KeyCode::Esc => {
                    self.screen = SvcScreen::List;
                    self.error = None;
                }
                KeyCode::Enter => {
                    let name = input.trim().to_string();
                    if name.is_empty() {
                        self.error = Some("a unit name is required".to_string());
                    } else if self.rows.iter().any(|r| r.name == name) {
                        self.error = Some(format!("unit {name:?} already exists"));
                    } else {
                        self.error = None;
                        self.screen = SvcScreen::List;
                        return Some(Action::Services(ServicesAction::Create {
                            units_dir: self.units_dir.clone(),
                            name,
                        }));
                    }
                }
                _ => {}
            }
            return None;
        }
        let ud = self.units_dir.clone();
        let control = |op, unit| Some(Action::Services(ServicesAction::Control { units_dir: ud.clone(), op, unit }));
        match code {
            KeyCode::Up | KeyCode::Char('k') => {
                if !self.rows.is_empty() {
                    let i = self.list.selected().unwrap_or(0).saturating_sub(1);
                    self.list.select(Some(i));
                }
                None
            }
            KeyCode::Down | KeyCode::Char('j') => {
                if !self.rows.is_empty() {
                    let i = self.list.selected().map_or(0, |i| (i + 1).min(self.rows.len() - 1));
                    self.list.select(Some(i));
                }
                None
            }
            KeyCode::Char('s') => self.selected_name().and_then(|u| control(ControlOp::Start, Some(u))),
            KeyCode::Char('t') => self.selected_name().and_then(|u| control(ControlOp::Stop, Some(u))),
            KeyCode::Char('R') => self.selected_name().and_then(|u| control(ControlOp::Restart, Some(u))),
            KeyCode::Char('c') => {
                self.screen = SvcScreen::NamePrompt { input: String::new() };
                self.error = None;
                None
            }
            KeyCode::Char('e') => self
                .selected_name()
                .map(|name| Action::Services(ServicesAction::Edit { units_dir: ud, name })),
            KeyCode::Char('d') => self
                .selected_name()
                .map(|name| Action::Services(ServicesAction::Delete { units_dir: ud, name })),
            KeyCode::Char('r') => Some(Action::Services(ServicesAction::Refresh { units_dir: ud })),
            _ => None,
        }
    }

    /// The tool's keys for the App gutter (the tab bar is hidden while this
    /// surface is up).
    pub(super) fn gutter(&self) -> &'static str {
        match self.screen {
            SvcScreen::List => {
                " s start · t stop · R restart · c create · e edit · d delete · r refresh · Esc back "
            }
            SvcScreen::NamePrompt { .. } => " Enter open editor · Esc back ",
        }
    }

    pub(super) fn render(&mut self, f: &mut Frame, area: Rect) {
        if let SvcScreen::NamePrompt { input } = &self.screen {
            super::remote::render_form(
                f,
                area,
                "New unit",
                &[
                    "Enter a name for the new unit (no .unit suffix).",
                    "The editor then opens on a generic template.",
                ],
                input,
                self.error.as_deref(),
                "",
            );
            return;
        }
        render_units(f, area, &self.rows, &self.list, " Services ");
    }
}

/// Render the shared Services view — a narrow unit-name list on the left, the
/// selected unit's run Status (top-right) and Definition (bottom-right) stacked
/// beside it. Used by both the Local surface here and the Cluster tab's remote
/// services panel, so the two look and behave identically (the remote one just
/// omits create / edit / delete). `title` names the list block.
pub(super) fn render_units(
    f: &mut Frame,
    area: Rect,
    rows: &[ServiceRow],
    list: &ListState,
    title: &str,
) {
    let label = |s: &str| Span::styled(s.to_string(), theme::hint_style());
    let val = |s: String| Span::styled(s, theme::panel_style());
    let sel = list.selected().unwrap_or(0).min(rows.len().saturating_sub(1));
    // Narrow name list on the left; the freed width goes to the stacked
    // Status (top) and Definition (bottom) panes on the right.
    let cols = Layout::horizontal([Constraint::Length(28), Constraint::Min(0)]).split(area);
    let right = Layout::vertical([Constraint::Length(4), Constraint::Min(0)]).split(cols[1]);
    let items: Vec<ListItem> = if rows.is_empty() {
        vec![ListItem::new(Line::from(Span::styled("(no units)", theme::hint_style())))]
    } else {
        rows.iter().map(|r| ListItem::new(r.name.clone())).collect()
    };
    let mut st = list.clone();
    let widget = List::new(items)
        .style(theme::panel_style())
        .block(theme::panel_block().title(Span::styled(title.to_string(), theme::title_style())))
        .highlight_style(theme::selected_style())
        .highlight_symbol("▸ ");
    f.render_stateful_widget(widget, cols[0], &mut st);
    // Top-right: the selected unit's run status.
    let status: Vec<Line> = match rows.get(sel) {
        Some(r) => {
            let (word, color) = state_word(r);
            let pid = match &r.state {
                Some(UnitState::Running { pid: Some(p) }) => p.to_string(),
                _ => "—".to_string(),
            };
            vec![
                Line::from(vec![
                    label("status: "),
                    Span::styled(word, Style::default().bg(theme::PANEL_BG).fg(color)),
                ]),
                Line::from(vec![label("pid:    "), val(pid)]),
            ]
        }
        None => vec![Line::from(label("No unit selected."))],
    };
    f.render_widget(
        Paragraph::new(status)
            .style(theme::panel_style())
            .block(theme::panel_block().title(Span::styled(" Status ", theme::title_style()))),
        right[0],
    );
    // Bottom-right: the selected unit's definition.
    let detail: Vec<Line> = match rows.get(sel) {
        Some(r) if r.defined => vec![
            Line::from(vec![label("exe:     "), val(r.exe.clone())]),
            Line::from(vec![label("args:    "), val(r.args.join(" "))]),
            Line::from(vec![label("trigger: "), val(r.trigger.clone())]),
            Line::from(vec![label("restart: "), val(r.restart.clone())]),
        ],
        Some(_) => vec![Line::from(label(
            "Reported by the supervisor with no definition file — press r to refresh.",
        ))],
        None => vec![Line::from(label("No unit selected."))],
    };
    f.render_widget(
        Paragraph::new(detail)
            .wrap(Wrap { trim: true })
            .style(theme::panel_style())
            .block(theme::panel_block().title(Span::styled(" Definition ", theme::title_style()))),
        right[1],
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::{Terminal, backend::TestBackend};

    fn render(state: &mut ServicesState, w: u16, h: u16) -> String {
        let mut t = Terminal::new(TestBackend::new(w, h)).unwrap();
        t.draw(|f| state.render(f, f.area())).unwrap();
        t.backend().buffer().content().iter().map(|c| c.symbol()).collect()
    }

    fn a_unit() -> ServiceRow {
        ServiceRow {
            name: "resolver".to_string(),
            defined: true,
            state: Some(UnitState::Running { pid: Some(42) }),
            exe: "/usr/bin/netidx".to_string(),
            args: vec!["resolver".to_string()],
            trigger: "OnStart".to_string(),
            restart: "rate-limited (1s)".to_string(),
        }
    }

    #[test]
    fn lists_units_with_status_and_definition_panes() {
        let mut s = ServicesState::new(PathBuf::from("/nonexistent/units"));
        s.apply(ServicesUpdate { rows: vec![a_unit()] });
        let out = render(&mut s, 110, 20);
        assert!(out.contains("resolver"), "unit name missing: {out:?}");
        // Status pane: status word + pid on their own lines.
        assert!(out.contains("status:") && out.contains("running"), "status pane missing: {out:?}");
        assert!(out.contains("pid:") && out.contains("42"), "pid line missing: {out:?}");
        // Definition pane: the selected unit's exe.
        assert!(out.contains("/usr/bin/netidx"), "definition exe missing: {out:?}");
        // Keybinds live in the App gutter, not the surface.
        let g = s.gutter();
        assert!(g.contains("c create") && g.contains("d delete"), "gutter keys missing: {g:?}");
    }

    #[test]
    fn create_opens_name_prompt_and_captures_text() {
        let mut s = ServicesState::new(PathBuf::from("/nonexistent/units"));
        assert!(s.on_key(KeyCode::Char('c')).is_none());
        assert!(s.capturing_text(), "name prompt should capture text");
        for ch in "myunit".chars() {
            s.on_key(KeyCode::Char(ch));
        }
        let out = render(&mut s, 100, 20);
        assert!(out.contains("New unit"), "name-prompt title missing: {out:?}");
        assert!(out.contains("myunit"), "typed name not captured: {out:?}");
    }

    #[test]
    fn stop_is_confirm_gated() {
        let stop = ServicesAction::Control {
            units_dir: PathBuf::from("/x"),
            op: ControlOp::Stop,
            unit: Some("resolver".to_string()),
        };
        assert!(stop.confirm_message().is_some(), "stop must be confirmed");
        let start = ServicesAction::Control {
            units_dir: PathBuf::from("/x"),
            op: ControlOp::Start,
            unit: Some("resolver".to_string()),
        };
        assert!(start.confirm_message().is_none(), "start must not be confirmed");
    }
}
