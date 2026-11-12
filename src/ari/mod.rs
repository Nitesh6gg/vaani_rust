//! Asterisk REST Interface: ONE events WebSocket for the whole process
//! (invariant #6, never per call) with reconnect and backoff, plus the few REST
//! calls a call needs.

pub mod manager;
pub mod outbound;
pub mod sweep;

use crate::ai::urlenc;
use anyhow::{Context, Result, bail};
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::json;
use std::{sync::OnceLock, time::Duration};
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::Message as Ws;
use tokio_util::sync::CancellationToken;

const INITIAL_BACKOFF: Duration = Duration::from_millis(500);
const MAX_BACKOFF: Duration = Duration::from_secs(15);

#[derive(Debug, Clone, Deserialize, Default)]
pub struct Channel {
    pub id: String,
    /// "tech/name-id": an externalMedia channel over AudioSocket is "AudioSocket/host:port-uuid".
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub caller: Option<Number>,
    #[serde(default)]
    pub dialplan: Option<Dialplan>,
}
#[derive(Debug, Clone, Deserialize, Default)]
pub struct Number {
    #[serde(default)]
    pub number: String,
}
#[derive(Debug, Clone, Deserialize, Default)]
pub struct Dialplan {
    #[serde(default)]
    pub exten: String,
}

impl Channel {
    pub fn caller_number(&self) -> String {
        self.caller.as_ref().map(|c| c.number.clone()).unwrap_or_default()
    }
    /// The extension that routed the call into this Stasis app.
    pub fn called_number(&self) -> String {
        self.dialplan.as_ref().map(|d| d.exten.clone()).unwrap_or_default()
    }
}

#[derive(Debug, Clone)]
pub enum Event {
    /// `args` are the Stasis arguments: a transfer destination's first one is "transfer".
    StasisStart {
        channel: Channel,
        args: Vec<String>,
    },
    StasisEnd {
        channel: Channel,
    },
    ChannelDestroyed {
        channel: Channel,
        cause: i64,
        cause_txt: String,
    },
    /// The events WebSocket came back after a drop: anything that happened in
    /// the gap -- StasisEnd above all -- is lost for good.
    Reconnected,
}

#[derive(Deserialize)]
struct RawEvent {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    channel: Option<Channel>,
    #[serde(default)]
    cause: i64,
    #[serde(default)]
    cause_txt: String,
}

pub fn parse_event(text: &str) -> Option<Event> {
    let e: RawEvent = serde_json::from_str(text).ok()?;
    Some(match (e.kind.as_str(), e.channel) {
        ("StasisStart", Some(channel)) => Event::StasisStart { channel, args: e.args },
        ("StasisEnd", Some(channel)) => Event::StasisEnd { channel },
        ("ChannelDestroyed", Some(channel)) => Event::ChannelDestroyed { channel, cause: e.cause, cause_txt: e.cause_txt },
        _ => return None,
    })
}

/// ARI base URL -> events WebSocket URL: http://h:8088/ari -> ws://h:8088/ari/events.
pub fn events_url(base: &str, app: &str, user: &str, pass: &str) -> Result<String> {
    let (scheme, rest) = match base.split_once("://") {
        Some(("https", r)) => ("wss", r),
        Some((_, r)) => ("ws", r),
        None => bail!("invalid ARI_URL {base:?}: no scheme"),
    };
    Ok(format!("{scheme}://{}/events?app={}&api_key={}:{}", rest.trim_end_matches('/'), urlenc(app), urlenc(user), urlenc(pass)))
}

/// Idle time before the first keepalive probe on an ARI REST connection.
const ARI_TCP_KEEPALIVE: Duration = Duration::from_secs(30);

/// Dedicated to the ARI REST calls, with a bounded timeout so a hung Asterisk
/// can't block call setup forever. This isn't the "shared client for hot-path
/// provider calls" of invariant #5: it fires a handful of times per call.
///
/// TCP keepalive: a pooled connection to Asterisk that its host lost without a FIN (reboot, firewall state
/// dropped) is noticed within about `ARI_TCP_KEEPALIVE` + 3 probes, instead of failing the next hangup.
pub(crate) fn http() -> &'static reqwest::Client {
    static C: OnceLock<reqwest::Client> = OnceLock::new();
    C.get_or_init(|| {
        reqwest::Client::builder().timeout(Duration::from_secs(10)).tcp_keepalive(ARI_TCP_KEEPALIVE).build().expect("ari http client")
    })
}

#[derive(Clone)]
pub struct Ari {
    base: String,
    user: String,
    pass: String,
    app: String,
}

impl Ari {
    pub fn new(base: &str, user: &str, pass: &str, app: &str) -> Self {
        Self { base: base.trim_end_matches('/').into(), user: user.into(), pass: pass.into(), app: app.into() }
    }

    async fn send(&self, rb: reqwest::RequestBuilder, what: &str) -> Result<()> {
        let resp = rb.basic_auth(&self.user, Some(&self.pass)).send().await.with_context(|| what.to_string())?;
        let status = resp.status();
        if status.is_success() {
            return Ok(());
        }
        // Asterisk's own message ({"message":"..."}) says what it rejected.
        let body = resp.text().await.unwrap_or_default();
        bail!("{what}: {status}: {}", body.trim())
    }

    pub async fn answer(&self, channel: &str) -> Result<()> {
        self.send(http().post(format!("{}/channels/{channel}/answer", self.base)), "answer").await
    }

    pub async fn hangup(&self, channel: &str) -> Result<()> {
        self.send(http().delete(format!("{}/channels/{channel}", self.base)), "hangup").await
    }

    /// Hangs a channel up with a SIP-visible reason ("congestion" is a 503, "busy" a 486).
    pub async fn hangup_reason(&self, channel: &str, reason: &str) -> Result<()> {
        self.send(http().delete(format!("{}/channels/{channel}?reason={}", self.base, urlenc(reason))), "hangup").await
    }

    pub async fn create_bridge(&self, id: &str) -> Result<()> {
        self.send(http().post(format!("{}/bridges?type=mixing&bridgeId={}&name={}", self.base, urlenc(id), urlenc(id))), "create bridge")
            .await
    }

    pub async fn add_to_bridge(&self, bridge: &str, channels: &[&str]) -> Result<()> {
        self.send(http().post(format!("{}/bridges/{bridge}/addChannel?channel={}", self.base, urlenc(&channels.join(",")))), "add channel")
            .await
    }

    pub async fn remove_from_bridge(&self, bridge: &str, channel: &str) -> Result<()> {
        self.send(http().post(format!("{}/bridges/{bridge}/removeChannel?channel={}", self.base, urlenc(channel))), "remove channel").await
    }

    /// Dials `endpoint` into this Stasis app with `app_args` (a transfer destination:
    /// "transfer,<call id>"); it enters Stasis once answered. Asterisk hangs it up
    /// after `timeout_secs` of ringing.
    pub async fn originate(&self, endpoint: &str, app_args: &str, timeout_secs: u64, channel_id: &str, caller_id: &str) -> Result<()> {
        let url = format!(
            "{}/channels?endpoint={}&app={}&appArgs={}&timeout={timeout_secs}&channelId={}&callerId={}",
            self.base,
            urlenc(endpoint),
            urlenc(&self.app),
            urlenc(app_args),
            urlenc(channel_id),
            urlenc(caller_id)
        );
        // No callerId when there is none: an empty one would blank the callee's caller ID.
        let url = if caller_id.is_empty() { url.replace("&callerId=", "") } else { url };
        self.send(http().post(url), "originate").await
    }

    pub async fn delete_bridge(&self, id: &str) -> Result<()> {
        self.send(http().delete(format!("{}/bridges/{id}", self.base)), "delete bridge").await
    }

    /// An externalMedia channel over AudioSocket: Asterisk connects to
    /// `external_host` (we listen) and echoes `uuid` as the first frame. The
    /// `data` field is what chan_audiosocket rejects the request without
    /// ("400 Bad Request: data can not be empty").
    pub async fn external_media_audiosocket(&self, channel_id: &str, external_host: &str, uuid: &str) -> Result<()> {
        let body = json!({
            "channelId": channel_id,
            "app": self.app,
            "external_host": external_host,
            "encapsulation": "audiosocket",
            "transport": "tcp",
            "connection_type": "client",
            "format": "slin16",
            "direction": "both",
            "data": uuid,
        });
        self.send(http().post(format!("{}/channels/externalMedia", self.base)).json(&body), "externalMedia").await
    }
}

/// A random RFC 4122 version-4 UUID string.
pub fn uuid_v4() -> String {
    let mut b: [u8; 16] = rand::random();
    b[6] = (b[6] & 0x0f) | 0x40; // version 4
    b[8] = (b[8] & 0x3f) | 0x80; // variant 10
    let h = |r: std::ops::Range<usize>| b[r].iter().map(|x| format!("{x:02x}")).collect::<String>();
    format!("{}-{}-{}-{}-{}", h(0..4), h(4..6), h(6..8), h(8..10), h(10..16))
}

/// Runs the events WebSocket for the life of the process: connects with
/// exponential backoff, forwards parsed events, and reports connectivity.
pub async fn run_events(url: String, tx: mpsc::UnboundedSender<Event>, connected: watch::Sender<bool>, cancel: CancellationToken) {
    let mut backoff = INITIAL_BACKOFF;
    let mut ever_connected = false;
    loop {
        let conn = tokio::select! { _ = cancel.cancelled() => return, c = tokio_tungstenite::connect_async(&url) => c };
        match conn {
            Ok((mut ws, _)) => {
                backoff = INITIAL_BACKOFF;
                let _ = connected.send(true);
                if ever_connected {
                    tracing::info!("ari websocket reconnected");
                    crate::metrics::ARI_RECONNECTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let _ = tx.send(Event::Reconnected);
                }
                ever_connected = true;
                loop {
                    let msg = tokio::select! { _ = cancel.cancelled() => return, m = ws.next() => m };
                    match msg {
                        Some(Ok(Ws::Text(t))) => {
                            if let Some(ev) = parse_event(t.as_str())
                                && tx.send(ev).is_err()
                            {
                                return;
                            }
                        }
                        Some(Ok(_)) => {}
                        Some(Err(e)) => {
                            tracing::warn!(error = %e, "ari websocket error");
                            break;
                        }
                        None => break,
                    }
                }
                let _ = connected.send(false);
                tracing::warn!("ari websocket disconnected");
            }
            Err(e) => tracing::warn!(error = %e, ?backoff, "ari connect failed, retrying"),
        }
        tokio::select! { _ = cancel.cancelled() => return, _ = tokio::time::sleep(backoff) => {} }
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn events_url_derivation() {
        assert_eq!(events_url("http://h:8088/ari", "vaani", "u", "p@ss").unwrap(), "ws://h:8088/ari/events?app=vaani&api_key=u:p%40ss");
        assert!(events_url("https://h/ari/", "a", "u", "p").unwrap().starts_with("wss://h/ari/events?"));
        assert!(events_url("h:8088", "a", "u", "p").is_err());
    }

    #[test]
    fn parses_stasis_events() {
        let start = r#"{"type":"StasisStart","args":["transfer","c0"],"channel":{"id":"c1","caller":{"name":"","number":"1001"},"dialplan":{"context":"c","exten":"2000","priority":1}}}"#;
        let Some(Event::StasisStart { channel, args }) = parse_event(start) else { panic!() };
        assert_eq!(args, ["transfer", "c0"]);
        assert_eq!((channel.id.as_str(), channel.caller_number().as_str(), channel.called_number().as_str()), ("c1", "1001", "2000"));
        assert!(matches!(parse_event(r#"{"type":"StasisEnd","channel":{"id":"c1"}}"#), Some(Event::StasisEnd { .. })));
        let Some(Event::ChannelDestroyed { cause, cause_txt, .. }) =
            parse_event(r#"{"type":"ChannelDestroyed","cause":16,"cause_txt":"Normal Clearing","channel":{"id":"c1"}}"#)
        else {
            panic!()
        };
        assert_eq!((cause, cause_txt.as_str()), (16, "Normal Clearing"));
        assert!(parse_event(r#"{"type":"ChannelVarset"}"#).is_none());
        assert!(parse_event("garbage").is_none());
    }

    #[test]
    fn uuid_is_v4_shaped() {
        let u = uuid_v4();
        assert_eq!(u.len(), 36);
        assert_eq!(&u[14..15], "4");
        assert!(matches!(&u[19..20], "8" | "9" | "a" | "b"));
    }

    /// The ARI REST connections carry TCP keepalive (every 30 s), so a connection whose peer vanished without a
    /// goodbye (Asterisk's host restarted or was frozen, a firewall dropped an idle flow) is noticed by the
    /// kernel instead of at the next request, ten seconds late. Linux shows it in /proc/net/tcp: the timer
    /// field of an established connection is 02 while its keepalive timer runs.
    #[tokio::test]
    async fn ari_connections_have_tcp_keepalive() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let Ok(table) = std::fs::read_to_string("/proc/net/tcp") else { return eprintln!("skipped: no /proc/net/tcp") };
        drop(table);
        let lis = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = lis.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (mut s, _) = lis.accept().await.unwrap();
            let mut buf = [0u8; 4096];
            let _ = s.read(&mut buf).await;
            let _ = s.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}").await;
            tokio::time::sleep(Duration::from_secs(5)).await; // keeps the connection open, as Asterisk does
        });

        let resp = http().get(format!("http://127.0.0.1:{port}/ari/channels")).send().await.unwrap();
        assert_eq!(resp.text().await.unwrap(), "{}");

        // The client's end: remote 127.0.0.1:port (little-endian hex), ESTABLISHED (01).
        let remote = format!("0100007F:{port:04X}");
        let table = std::fs::read_to_string("/proc/net/tcp").unwrap();
        let timer = table
            .lines()
            .skip(1)
            .map(|l| l.split_whitespace().collect::<Vec<_>>())
            .find(|f| f.get(2) == Some(&remote.as_str()) && f.get(3) == Some(&"01"))
            .map(|f| f[5].to_string())
            .expect("the connection to the listener is established and pooled");
        // "02:<hex>" = keepalive timer, time left in USER_HZ (100) ticks: 30 s is 3000, reqwest's own default (15 s) is 1500.
        let (kind, left) = timer.split_once(':').unwrap();
        let left = u64::from_str_radix(left, 16).unwrap();
        assert_eq!(kind, "02", "keepalive timer running on the connection (tr:tm->when = {timer})");
        assert!((2000..=3000).contains(&left), "first probe due in about 30 s, not {left} ticks");
    }
}
