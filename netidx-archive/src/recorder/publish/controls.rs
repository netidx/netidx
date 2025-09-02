use super::{SessionBCastMsg, SessionUpdate};
use crate::{
    logfile::Seek,
    recorder::{publish::ClusterCmd, State},
};
use anyhow::Result;
use arcstr::{literal, ArcStr};
use chrono::prelude::*;
use futures::channel::mpsc;
use log::{info, warn};
use netidx::{
    path::Path,
    publisher::{ClId, PublishFlags, Publisher, UpdateBatch, Val, Value, WriteRequest},
};
use netidx_netproto::glob::{Glob, GlobSet};
use netidx_protocols::{
    rpc::server::{RpcCall, RpcReply},
    rpc_err,
};
use poolshark::global::GPooled;
use std::{collections::HashMap, ops::Bound, time::Duration};
use tokio::sync::broadcast;
use uuid::Uuid;

pub(crate) static START_DOC: &'static str = "The timestamp you want to replay to start at, or Unbounded for the beginning of the archive. This can also be an offset from now in terms of [+-][0-9]+[.]?[0-9]*[yMdhms], e.g. -1.5d. Default Unbounded.";
pub(crate) static END_DOC: &'static str = "Time timestamp you want to replay end at, or Unbounded for the end of the archive. This can also be an offset from now in terms of [+-][0-9]+[.]?[0-9]*[yMdhms], e.g. -1.5d. default Unbounded";
pub(crate) static SPEED_DOC: &'static str = "How fast you want playback to run, e.g 1 = realtime speed, 10 = 10x realtime, 0.5 = 1/2 realtime, Unlimited = as fast as data can be read and sent. Default is 1";
pub(crate) static STATE_DOC: &'static str = "The current state of playback, {pause, play, tail}. Tail, seek to the end of the archive and play any new messages that arrive. Default pause.";
pub(crate) static POS_DOC: &'static str = "The current playback position. Null if the archive is empty, or the timestamp of the current record. Set to any timestamp where start <= t <= end to seek. Set to [+-][0-9]+ to seek a specific number of batches, e.g. +1 to single step forward -1 to single step back. Set to [+-][0-9]+[yMdhmsu] to step forward or back that amount of time, e.g. -1y step back 1 year. -1u to step back 1 microsecond. set to 'beginning' to seek to the beginning and 'end' to seek to the end. By default the initial position is set to 'beginning' when opening the archive.";
pub(crate) static PLAY_AFTER_DOC: &'static str =
    "Start playing after waiting the specified timeout";
pub(crate) static FILTER_DOC: &'static str = "Only publish paths matching the specified filter. e.g. [\"/**\"] would match everything";

fn parse_speed(v: Value) -> Result<Option<f64>> {
    match v.clone().cast_to::<f64>() {
        Ok(speed) => Ok(Some(speed)),
        Err(_) => match v.cast_to::<ArcStr>() {
            Err(_) => bail!("expected a float, or unlimited"),
            Ok(s) => {
                if s.trim().to_lowercase().as_str() == "unlimited" {
                    Ok(None)
                } else {
                    bail!("expected a float, or unlimited")
                }
            }
        },
    }
}

pub(crate) fn parse_bound(v: Value) -> Result<Bound<DateTime<Utc>>> {
    match v {
        Value::DateTime(ts) => Ok(Bound::Included(ts)),
        Value::String(c) if c.trim().to_lowercase().as_str() == "unbounded" => {
            Ok(Bound::Unbounded)
        }
        v => match v.cast_to::<Seek>()? {
            Seek::Beginning => Ok(Bound::Unbounded),
            Seek::End => Ok(Bound::Unbounded),
            Seek::Absolute(ts) => Ok(Bound::Included(ts)),
            Seek::TimeRelative(offset) => Ok(Bound::Included(Utc::now() + offset)),
            Seek::BatchRelative(_) => bail!("invalid bound"),
        },
    }
}

fn get_bound(r: WriteRequest) -> Option<Bound<DateTime<Utc>>> {
    match parse_bound(r.value) {
        Ok(b) => Some(b),
        Err(e) => {
            if let Some(reply) = r.send_result {
                reply.send(Value::error(format!("{}", e)))
            }
            None
        }
    }
}

pub(crate) fn parse_filter(globs: Vec<ArcStr>) -> Result<GlobSet> {
    let globs: Vec<Glob> =
        globs.into_iter().map(|s| Glob::new(s)).collect::<Result<_>>()?;
    Ok(GlobSet::new(true, globs)?)
}

#[derive(Debug, Clone)]
pub(super) struct NewSessionConfig {
    pub(super) client: ClId,
    pub(super) start: Bound<DateTime<Utc>>,
    pub(super) end: Bound<DateTime<Utc>>,
    pub(super) speed: Option<f64>,
    pub(super) pos: Option<Seek>,
    pub(super) state: Option<State>,
    pub(super) play_after: Option<Duration>,
    pub(super) filter: Vec<ArcStr>,
}

impl NewSessionConfig {
    pub(super) fn new(
        mut req: RpcCall,
        start: Value,
        end: Value,
        speed: Value,
        pos: Option<Seek>,
        state: Option<State>,
        play_after: Option<Duration>,
        filter: Vec<ArcStr>,
    ) -> Option<(NewSessionConfig, RpcReply)> {
        let start = match parse_bound(start) {
            Ok(s) => s,
            Err(e) => rpc_err!(req.reply, format!("invalid start {}", e)),
        };
        let end = match parse_bound(end) {
            Ok(s) => s,
            Err(e) => rpc_err!(req.reply, format!("invalid end {}", e)),
        };
        let speed = match parse_speed(speed) {
            Ok(s) => s,
            Err(e) => rpc_err!(req.reply, format!("invalid speed {}", e)),
        };
        if let Err(e) = parse_filter(filter.clone()) {
            rpc_err!(req.reply, format!("could not parse filter {}", e))
        }
        let s = NewSessionConfig {
            client: req.client,
            start,
            end,
            speed,
            pos,
            state,
            play_after,
            filter,
        };
        Some((s, req.reply))
    }
}

pub(super) struct Controls {
    _start_doc: Val,
    start_ctl: Val,
    _end_doc: Val,
    end_ctl: Val,
    _speed_doc: Val,
    speed_ctl: Val,
    _state_doc: Val,
    state_ctl: Val,
    _pos_doc: Val,
    pos_ctl: Val,
}

impl Controls {
    pub(super) async fn new(
        session_base: &Path,
        publisher: &Publisher,
        control_tx: &mpsc::Sender<GPooled<Vec<WriteRequest>>>,
    ) -> Result<Self> {
        let _start_doc = publisher.publish(
            session_base.append("control/start/doc"),
            Value::String(literal!(START_DOC)),
        )?;
        let _end_doc = publisher.publish(
            session_base.append("control/end/doc"),
            Value::String(literal!(END_DOC)),
        )?;
        let _speed_doc = publisher.publish(
            session_base.append("control/speed/doc"),
            Value::String(literal!(SPEED_DOC)),
        )?;
        let _state_doc = publisher.publish(
            session_base.append("control/state/doc"),
            Value::String(literal!(STATE_DOC)),
        )?;
        let _pos_doc = publisher.publish(
            session_base.append("control/pos/doc"),
            Value::String(literal!(POS_DOC)),
        )?;
        let start_ctl = publisher.publish_with_flags(
            PublishFlags::USE_EXISTING,
            session_base.append("control/start/current"),
            Value::String(literal!("Unbounded")),
        )?;
        publisher.writes(start_ctl.id(), control_tx.clone());
        let end_ctl = publisher.publish_with_flags(
            PublishFlags::USE_EXISTING,
            session_base.append("control/end/current"),
            Value::String(literal!("Unbounded")),
        )?;
        publisher.writes(end_ctl.id(), control_tx.clone());
        let speed_ctl = publisher.publish_with_flags(
            PublishFlags::USE_EXISTING,
            session_base.append("control/speed/current"),
            Value::F64(1.),
        )?;
        publisher.writes(speed_ctl.id(), control_tx.clone());
        let state_ctl = publisher.publish_with_flags(
            PublishFlags::USE_EXISTING,
            session_base.append("control/state/current"),
            Value::String(literal!("pause")),
        )?;
        publisher.writes(state_ctl.id(), control_tx.clone());
        let pos_ctl = publisher.publish_with_flags(
            PublishFlags::USE_EXISTING,
            session_base.append("control/pos/current"),
            Value::Null,
        )?;
        publisher.writes(pos_ctl.id(), control_tx.clone());
        publisher.flushed().await;
        Ok(Controls {
            _start_doc,
            start_ctl,
            _end_doc,
            end_ctl,
            _speed_doc,
            speed_ctl,
            _state_doc,
            state_ctl,
            _pos_doc,
            pos_ctl,
        })
    }

    pub(super) fn process_update(&self, batch: &mut UpdateBatch, m: SessionUpdate) {
        fn bound_to_val(b: Bound<DateTime<Utc>>) -> Value {
            match b {
                Bound::Unbounded => Value::String(literal!("Unbounded")),
                Bound::Included(ts) | Bound::Excluded(ts) => Value::DateTime(ts),
            }
        }
        match m {
            SessionUpdate::Pos(pos) => self.pos_ctl.update_changed(batch, pos),
            SessionUpdate::End(e) => self.end_ctl.update_changed(batch, bound_to_val(e)),
            SessionUpdate::Start(s) => {
                self.start_ctl.update_changed(batch, bound_to_val(s))
            }
            SessionUpdate::State(st) => {
                self.state_ctl.update_changed(
                    batch,
                    match st {
                        State::Pause => "pause",
                        State::Play => "play",
                        State::Tail => "tail",
                    },
                );
            }
            SessionUpdate::Speed(sp) => self.speed_ctl.update_changed(batch, sp),
        }
    }

    pub(super) fn process_writes(
        &self,
        session_bcast: &mut broadcast::Sender<SessionBCastMsg>,
        session_id: Uuid,
        mut batch: GPooled<Vec<WriteRequest>>,
    ) {
        let mut inst = HashMap::new();
        for req in batch.drain(..) {
            inst.insert(req.id, req);
        }
        for (_, req) in inst {
            if req.id == self.start_ctl.id() {
                info!("set start {}: {}", session_id, req.value);
                if let Some(new_start) = get_bound(req) {
                    let _ = session_bcast
                        .send(SessionBCastMsg::Command(ClusterCmd::SetStart(new_start)));
                }
            } else if req.id == self.end_ctl.id() {
                info!("set end {}: {}", session_id, req.value);
                if let Some(new_end) = get_bound(req) {
                    let _ = session_bcast
                        .send(SessionBCastMsg::Command(ClusterCmd::SetEnd(new_end)));
                }
            } else if req.id == self.speed_ctl.id() {
                info!("set speed {}: {}", session_id, req.value);
                match parse_speed(req.value) {
                    Ok(speed) => {
                        let _ = session_bcast
                            .send(SessionBCastMsg::Command(ClusterCmd::SetSpeed(speed)));
                    }
                    Err(e) => {
                        warn!("tried to set invalid speed {}", e);
                        if let Some(reply) = req.send_result {
                            reply.send(Value::error(format!("{}", e)));
                        }
                    }
                }
            } else if req.id == self.state_ctl.id() {
                info!("set state {}: {}", session_id, req.value);
                match req.value.cast_to::<State>() {
                    Ok(state) => {
                        let _ = session_bcast
                            .send(SessionBCastMsg::Command(ClusterCmd::SetState(state)));
                    }
                    Err(e) => {
                        warn!("tried to set invalid state {}", e);
                        if let Some(reply) = req.send_result {
                            reply.send(Value::error(format!("{}", e)))
                        }
                    }
                }
            } else if req.id == self.pos_ctl.id() {
                info!("set pos {}: {}", session_id, req.value);
                match req.value.cast_to::<Seek>() {
                    Ok(pos) => {
                        let _ = session_bcast
                            .send(SessionBCastMsg::Command(ClusterCmd::SeekTo(pos)));
                    }
                    Err(e) => {
                        warn!("invalid set pos {}", e);
                        if let Some(reply) = req.send_result {
                            reply.send(Value::error(format!("{}", e)))
                        }
                    }
                }
            }
        }
    }
}
