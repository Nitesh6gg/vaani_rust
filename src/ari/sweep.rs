//! The orphan sweep: Asterisk's channels of Vaani's own Stasis app that Vaani lost track of.
//!
//! A call whose events were lost, or whose setup died half way, leaves its channels Up in Stasis on
//! Asterisk for ever (billable, and counted against the trunk's channel limit), with nothing left to ever
//! hang them up. Found on Nov 9 in the Go server: 40 `AudioSocket/...` channels, 50 minutes old.

use super::{Ari, http};
use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use std::time::Duration;

/// How old an untracked channel must be before the sweep hangs it up. Setup is given 15 seconds, so
/// anything this old that Vaani doesn't know about is lost, not in progress.
const ORPHAN_MIN_AGE: Duration = Duration::from_secs(90);

#[derive(Debug, Deserialize, Default)]
pub struct ChannelInfo {
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub creationtime: String,
    #[serde(default)]
    pub dialplan: DialplanApp,
}

#[derive(Debug, Deserialize, Default)]
pub struct DialplanApp {
    #[serde(default)]
    pub app_name: String,
    #[serde(default)]
    pub app_data: String,
}

impl ChannelInfo {
    /// When the channel was created ("2026-11-09T11:29:50.123+0000"); `None` if unreadable.
    fn created(&self) -> Option<DateTime<Utc>> {
        ["%Y-%m-%dT%H:%M:%S%.f%z", "%Y-%m-%dT%H:%M:%S%z"]
            .iter()
            .find_map(|f| DateTime::parse_from_str(&self.creationtime, f).ok())
            .or_else(|| DateTime::parse_from_rfc3339(&self.creationtime).ok())
            .map(|t| t.with_timezone(&Utc))
    }
}

impl Ari {
    /// GET /channels: every channel Asterisk has.
    pub async fn list_channels(&self) -> Result<Vec<ChannelInfo>> {
        let resp =
            http().get(format!("{}/channels", self.base)).basic_auth(&self.user, Some(&self.pass)).send().await.context("list channels")?;
        let status = resp.status();
        if !status.is_success() {
            bail!("list channels: {status}: {}", resp.text().await.unwrap_or_default().trim());
        }
        resp.json().await.context("list channels")
    }
}

/// Hangs up the channels of Stasis app `app` that are older than 90 s and that `tracked` doesn't know;
/// returns their ids.
pub async fn sweep_once(ari: &Ari, app: &str, now: DateTime<Utc>, tracked: impl Fn(&str) -> bool) -> Result<Vec<String>> {
    let mut hung = vec![];
    for ch in ari.list_channels().await? {
        let ours =
            ch.dialplan.app_name == "Stasis" && (ch.dialplan.app_data == app || ch.dialplan.app_data.starts_with(&format!("{app},")));
        let Some(created) = ch.created() else { continue };
        let age = (now - created).to_std().unwrap_or_default();
        if !ours || age < ORPHAN_MIN_AGE || tracked(&ch.id) {
            continue;
        }
        tracing::warn!(call_id = %ch.id, event = "channel.orphan", name = %ch.name, age_s = age.as_secs(), "hanging up a channel Vaani lost track of");
        crate::metrics::ORPHAN_CHANNELS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if let Err(e) = ari.hangup(&ch.id).await {
            tracing::debug!(call_id = %ch.id, error = %e, "hangup failed");
        }
        hung.push(ch.id);
    }
    Ok(hung)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::HashSet,
        sync::{Arc, Mutex},
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    /// A fake Asterisk: GET /channels answers `channels`; every other request is logged and answered 200.
    async fn asterisk(channels: String, status: u16) -> (Ari, Arc<Mutex<Vec<String>>>) {
        let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/ari", lis.local_addr().unwrap());
        let log = Arc::new(Mutex::new(vec![]));
        let l = log.clone();
        tokio::spawn(async move {
            loop {
                let (mut s, _) = lis.accept().await.unwrap();
                let (l, channels) = (l.clone(), channels.clone());
                tokio::spawn(async move {
                    let mut c = [0u8; 4096];
                    let n = s.read(&mut c).await.unwrap_or(0);
                    let req = String::from_utf8_lossy(&c[..n]).to_string();
                    let line = req.lines().next().unwrap_or_default().to_string();
                    let body = if line.starts_with("GET /ari/channels ") { channels } else { "{}".to_string() };
                    l.lock().unwrap().push(line);
                    let resp = format!("HTTP/1.1 {status} X\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                    let _ = s.write_all(resp.as_bytes()).await;
                });
            }
        });
        (Ari::new(&url, "u", "p", "vaani"), log)
    }

    fn json(now: DateTime<Utc>, rows: &[(&str, &str, &str, i64)]) -> String {
        let items: Vec<String> = rows
            .iter()
            .map(|(id, app, data, age)| {
                format!(
                    r#"{{"id":"{id}","name":"AudioSocket/x","creationtime":"{}","dialplan":{{"app_name":"{app}","app_data":"{data}"}}}}"#,
                    (now - chrono::Duration::seconds(*age)).format("%Y-%m-%dT%H:%M:%S%.3f%z")
                )
            })
            .collect();
        format!("[{}]", items.join(","))
    }

    #[tokio::test]
    async fn hangs_up_only_channels_vaani_lost_track_of() {
        let now = Utc::now();
        let (ari, log) = asterisk(
            json(
                now,
                &[
                    ("orphan-media", "Stasis", "vaani,e763f874", 3000),  // lost: hung up
                    ("orphan-caller", "Stasis", "vaani", 200),           // a caller whose StasisStart was lost: hung up
                    ("orphan-out", "Stasis", "vaani,outbound,931", 100), // a dialed call that was lost: hung up
                    ("known-ext", "Stasis", "vaani,aaaa", 3000),         // Vaani tracks it
                    ("young", "Stasis", "vaani,bbbb", 30),               // setup may still be running
                    ("other-app", "Stasis", "otherapp,cccc", 3000),      // not ours
                    ("vaanilike", "Stasis", "vaani2,dddd", 3000),        // another app whose name starts the same
                    ("in-dialplan", "AudioSocket", "uuid", 3000),        // not in Stasis
                ],
            ),
            200,
        )
        .await;

        let mut hung = sweep_once(&ari, "vaani", now, |id| id == "known-ext").await.unwrap();
        hung.sort();
        assert_eq!(hung, ["orphan-caller", "orphan-media", "orphan-out"]);

        let l = log.lock().unwrap().clone();
        let deleted: HashSet<&str> = l.iter().filter_map(|x| x.strip_prefix("DELETE /ari/channels/")?.split_whitespace().next()).collect();
        assert_eq!(deleted, HashSet::from(["orphan-caller", "orphan-media", "orphan-out"]), "{l:?}");
    }

    #[tokio::test]
    async fn an_unreadable_list_hangs_up_nothing() {
        let (ari, log) = asterisk("oops".into(), 500).await;
        assert!(sweep_once(&ari, "vaani", Utc::now(), |_| false).await.is_err());
        assert!(!log.lock().unwrap().iter().any(|l| l.starts_with("DELETE")));
    }

    #[test]
    fn reads_asterisks_creation_times() {
        let ch = |t: &str| ChannelInfo { creationtime: t.into(), ..Default::default() };
        assert!(ch("2026-11-09T11:29:50.123+0000").created().is_some());
        assert!(ch("2026-11-09T11:29:50+0530").created().is_some());
        assert!(ch("2026-11-09T11:29:50Z").created().is_some());
        assert!(ch("garbage").created().is_none());
    }
}
