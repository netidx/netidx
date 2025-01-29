use crate::{
    expr::{ExprId, ModPath},
    parser,
    vm::{BindId, Ctx, Event, ExecCtx, Node},
};
use anyhow::Result;
use fxhash::FxHashMap;
use netidx::{
    publisher::{Publisher, PublisherBuilder, Value},
    resolver_server,
    subscriber::{Subscriber, SubscriberBuilder},
};
use smallvec::SmallVec;
use std::collections::HashMap;

struct TestCtx {
    by_ref: FxHashMap<BindId, SmallVec<[ExprId; 3]>>,
    to_update: Vec<(ExprId, BindId, Value)>,
    resolver: resolver_server::Server,
    publisher: Publisher,
    subscriber: Subscriber,
}

impl TestCtx {
    async fn new() -> Result<Self> {
        let resolver = {
            use resolver_server::config::{self, file};
            let cfg = file::ConfigBuilder::default()
                .member_servers(vec![file::MemberServerBuilder::default()
                    .auth(file::Auth::Anonymous)
                    .addr("127.0.0.1:0".parse()?)
                    .build()?])
                .build()?;
            let cfg = config::Config::from_file(cfg)?;
            resolver_server::Server::new(cfg, false, 0).await?
        };
        let addr = *resolver.local_addr();
        let cfg = {
            use netidx::config::{self, file, DefaultAuthMech};
            let cfg = file::ConfigBuilder::default()
                .addrs(vec![(addr, file::Auth::Anonymous)])
                .default_auth(DefaultAuthMech::Anonymous)
                .default_bind_config("local")
                .build()?;
            config::Config::from_file(cfg)?
        };
        let publisher = PublisherBuilder::new(cfg.clone()).build().await?;
        let subscriber = SubscriberBuilder::new(cfg).build()?;
        Ok(Self {
            by_ref: HashMap::default(),
            to_update: vec![],
            resolver,
            publisher,
            subscriber,
        })
    }
}

impl Ctx for TestCtx {
    fn call_rpc(
        &mut self,
        name: netidx::path::Path,
        args: Vec<(netidx::chars::Chars, netidx::publisher::Value)>,
        ref_by: ExprId,
        id: crate::vm::RpcCallId,
    ) {
        unimplemented!()
    }

    fn clear(&mut self) {
        self.by_ref.clear();
        self.to_update.clear();
    }

    fn durable_subscribe(
        &mut self,
        flags: netidx::subscriber::UpdatesFlags,
        path: netidx::path::Path,
        ref_by: ExprId,
    ) -> netidx::subscriber::Dval {
        unimplemented!()
    }

    fn unsubscribe(
        &mut self,
        path: netidx::path::Path,
        dv: netidx::subscriber::Dval,
        ref_by: ExprId,
    ) {
        unimplemented!()
    }

    fn set_timer(
        &mut self,
        id: crate::vm::TimerId,
        timeout: std::time::Duration,
        ref_by: ExprId,
    ) {
        unimplemented!()
    }

    fn ref_var(&mut self, id: BindId, ref_by: ExprId) {
        let refs = self.by_ref.entry(id).or_default();
        if !refs.contains(&ref_by) {
            refs.push(ref_by);
        }
    }

    fn unref_var(&mut self, id: BindId, ref_by: ExprId) {
        unimplemented!()
    }

    fn set_var(&mut self, id: BindId, value: Value) {
        if let Some(refs) = self.by_ref.get(&id) {
            match &**refs {
                [] => (),
                [eids @ .., eid] => {
                    for eid in eids {
                        self.to_update.push((*eid, id, value.clone()));
                    }
                    self.to_update.push((*eid, id, value))
                }
            }
        }
    }
}

struct TestState {
    exprs: FxHashMap<ExprId, Node<TestCtx, ()>>,
    ctx: ExecCtx<TestCtx, ()>,
}

impl TestState {
    async fn new() -> Result<Self> {
        Ok(Self { exprs: HashMap::default(), ctx: ExecCtx::new(TestCtx::new().await?) })
    }
}

#[tokio::test(flavor = "current_thread")]
async fn basic_arith() -> Result<()> {
    let mut state = TestState::new().await?;
    let e = parser::parse_modexpr("let v = (((1 + 1) * 2) / 2) - 1")?;
    let eid = e.id;
    let mut n = Node::compile(&mut state.ctx, &ModPath::root(), e);
    assert_eq!(n.update(&mut state.ctx, &Event::Init), None);
    assert_eq!(state.ctx.user.to_update.len(), 1);
    let (up_eid, _, v) = &state.ctx.user.to_update[0];
    assert_eq!(up_eid, &eid);
    assert_eq!(v, &Value::I64(1));
    Ok(())
}
