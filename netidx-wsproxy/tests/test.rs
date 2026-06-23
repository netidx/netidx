use anyhow::{anyhow, bail, Result};
use futures::{SinkExt, StreamExt};
use netidx::{
    path::Path,
    protocol::value::Value,
    publisher::{PublisherBuilder, Val},
    subscriber::SubscriberBuilder,
    InternalOnly,
};
use serde_json::{json, Value as Json};
use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};
use tokio::{net::TcpSocket, task::JoinHandle, time};
use tokio_tungstenite::{client_async, tungstenite::Message, WebSocketStream};

const TEST_TIMEOUT: Duration = Duration::from_secs(10);
const READ_TIMEOUT: Duration = Duration::from_secs(3);
const RECV_BUF: u32 = 1 << 20;

struct TestEnv {
    netidx: InternalOnly,
    path: Path,
    val: Val,
    ws_addr: SocketAddr,
    wsproxy: JoinHandle<()>,
}

impl TestEnv {
    async fn new(name: &str) -> Result<Self> {
        let netidx = InternalOnly::new().await?;
        let path = Path::from(format!("/wsproxy/{name}"));
        let val = netidx.publisher().publish(path.clone(), Value::U64(0))?;
        netidx.publisher().flushed().await;

        // every websocket client gets its own publisher and subscriber, so a
        // slow client is isolated to its own netidx session
        let cfg = netidx.cfg();
        let routes = netidx_wsproxy::filter_with(
            move || {
                let cfg = cfg.clone();
                async move {
                    let subscriber = SubscriberBuilder::new(cfg.clone()).build()?;
                    let publisher = PublisherBuilder::new(cfg).build().await?;
                    Ok((publisher, subscriber))
                }
            },
            "ws",
            Some(TEST_TIMEOUT),
        );
        let (ws_addr, server) = warp::serve(routes).bind_ephemeral(([127, 0, 0, 1], 0));
        let wsproxy = tokio::spawn(server);

        Ok(Self { netidx, path, val, ws_addr, wsproxy })
    }

    async fn publish_u64(&self, seq: u64) -> Result<()> {
        let mut batch = self.netidx.publisher().start_batch();
        self.val.update(&mut batch, Value::U64(seq));
        batch.commit(None).await;
        Ok(())
    }
}

impl Drop for TestEnv {
    fn drop(&mut self) {
        self.wsproxy.abort();
    }
}

type Ws = WebSocketStream<tokio::net::TcpStream>;

struct Client {
    ws: Ws,
    id: u64,
}

impl Client {
    async fn connect(env: &TestEnv) -> Result<Self> {
        let socket = if env.ws_addr.is_ipv4() {
            TcpSocket::new_v4()?
        } else {
            TcpSocket::new_v6()?
        };
        socket.set_recv_buffer_size(RECV_BUF)?;
        let stream = socket.connect(env.ws_addr).await?;
        let url = format!("ws://{}/ws", env.ws_addr);
        let (mut ws, _) = client_async(url, stream).await?;
        ws.send(Message::Text(
            json!({"type": "Subscribe", "path": env.path}).to_string(),
        ))
        .await?;

        let id = loop {
            let msg = next_json(&mut ws, READ_TIMEOUT).await?;
            if msg.get("type").and_then(Json::as_str) == Some("Subscribed") {
                break msg.get("id").and_then(Json::as_u64).ok_or_else(|| {
                    anyhow!("Subscribed response has no numeric id: {msg}")
                })?;
            }
        };

        wait_for_seq(&mut ws, id, 0, READ_TIMEOUT).await?;
        Ok(Self { ws, id })
    }

    async fn wait_for_seq(&mut self, min_seq: u64) -> Result<u64> {
        wait_for_seq(&mut self.ws, self.id, min_seq, READ_TIMEOUT).await
    }
}

async fn next_json(ws: &mut Ws, timeout: Duration) -> Result<Json> {
    loop {
        let msg = time::timeout(timeout, ws.next())
            .await
            .map_err(|_| anyhow!("timed out waiting for websocket message"))?
            .ok_or_else(|| anyhow!("websocket closed"))??;
        match msg {
            Message::Text(txt) => return Ok(serde_json::from_str(&txt)?),
            Message::Close(frame) => bail!("websocket closed: {frame:?}"),
            Message::Binary(_)
            | Message::Ping(_)
            | Message::Pong(_)
            | Message::Frame(_) => {}
        }
    }
}

async fn wait_for_seq(
    ws: &mut Ws,
    id: u64,
    min_seq: u64,
    timeout: Duration,
) -> Result<u64> {
    let deadline = Instant::now() + timeout;
    let mut last = None;
    loop {
        let now = Instant::now();
        if now >= deadline {
            bail!("timed out waiting for seq >= {min_seq}, last seen {last:?}");
        }
        let msg = next_json(ws, deadline - now).await?;
        if let Some(seq) = response_seq(&msg, id) {
            last = Some(seq);
            if seq >= min_seq {
                return Ok(seq);
            }
        }
    }
}

fn response_seq(msg: &Json, id: u64) -> Option<u64> {
    if msg.get("type").and_then(Json::as_str) != Some("Update") {
        return None;
    }
    for update in msg.get("updates")?.as_array()? {
        if update.get("id").and_then(Json::as_u64)? != id {
            continue;
        }
        let event = update.get("event")?;
        if event.get("type").and_then(Json::as_str) != Some("Update") {
            continue;
        }
        let value = event.get("value")?;
        if value.get("type").and_then(Json::as_str)? == "U64" {
            return value.get("value")?.as_u64();
        }
    }
    None
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn each_websocket_client_gets_its_own_session() -> Result<()> {
    let env = TestEnv::new("independent").await?;
    let mut clients = Vec::new();
    for _ in 0..8 {
        clients.push(Client::connect(&env).await?);
    }

    // a shared subscriber would show one client; per client subscribers show
    // one netidx client connection per websocket client
    assert_eq!(env.netidx.publisher().clients(), 8);
    env.publish_u64(1).await?;
    for client in &mut clients {
        client.wait_for_seq(1).await?;
    }
    assert_eq!(env.netidx.publisher().clients(), 8);
    Ok(())
}
