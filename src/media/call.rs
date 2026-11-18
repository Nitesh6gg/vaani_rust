//! One call's media plane over an accepted AudioSocket TCP connection:
//!
//! ```text
//! TCP read -> reader task -> inbound (cap 50) -> [agent's 20ms tick takes all that wait]
//! [agent] -> outbound (cap 5) -> writer task (own 20ms tick) -> TCP write
//! ```
//!
//! Exactly two tasks per call (reader, writer) joined by bounded channels --
//! never a task per frame (invariant #4). The writer keeps ticking and sends
//! silence when nothing is queued, so a slow agent never starves the far end
//! (invariants #1/#7). Ticks use a coalescing interval: after a stall the
//! missed ticks are skipped, not replayed in a burst.

use super::{FRAME_INTERVAL, FRAME_SIZE, Frame, SILENCE, audiosocket};
use crate::metrics;
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    time::Duration,
};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpStream, tcp::OwnedReadHalf},
    sync::mpsc,
    time::{self, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;

/// Agent audio waiting for the writer (~100ms).
const QUEUE: usize = 5;
/// Caller audio waiting for the agent (1 s). The agent takes everything waiting on each tick, so this
/// only fills while it can't: as the call starts (the workflow loading, the STT connecting) or when
/// Asterisk delivers a clump of frames after a stall. With 5 slots here and one frame taken per tick,
/// 40% of the calls of a real campaign lost caller audio, mostly their first words.
pub const INBOUND_QUEUE: usize = 50;

pub struct MediaStats {
    pub packets_in: AtomicU64,
    pub packets_out: AtomicU64,
    pub silence_out: AtomicU64,
    pub malformed: AtomicU64,
    pub inbound_dropped: AtomicU64,
    /// When the media connection was set up: the offsets below count from here.
    origin: std::time::Instant,
    /// Offsets (ms) of the first and the latest caller frame; `NEVER` until one arrives.
    first_in_ms: AtomicU64,
    last_in_ms: AtomicU64,
    /// The longest wait (ms) between two caller frames.
    max_gap_ms: AtomicU64,
    /// When the media connection ended (offset, ms); `NEVER` while it is up. The gap at the end is measured to here, not to
    /// when the summary happens to be written (which can wait behind other post-call work).
    ended_ms: AtomicU64,
}

const NEVER: u64 = u64::MAX;

impl Default for MediaStats {
    fn default() -> Self {
        Self {
            packets_in: AtomicU64::new(0),
            packets_out: AtomicU64::new(0),
            silence_out: AtomicU64::new(0),
            malformed: AtomicU64::new(0),
            inbound_dropped: AtomicU64::new(0),
            origin: std::time::Instant::now(),
            first_in_ms: AtomicU64::new(NEVER),
            last_in_ms: AtomicU64::new(NEVER),
            max_gap_ms: AtomicU64::new(0),
            ended_ms: AtomicU64::new(NEVER),
        }
    }
}

/// When the caller's audio arrived, over a call's media: what tells "no audio at all" from "it started late", "it had holes"
/// and "it stopped before the end". Offsets are ms from the media connection.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CallerAudio {
    pub first_ms: Option<u64>,
    /// The longest wait between two caller frames (a clump of frames after a stall shows here).
    pub max_gap_ms: u64,
    /// How long before now the latest caller frame came (the whole age when there was none).
    pub end_gap_ms: u64,
}

impl CallerAudio {
    /// One word for the call's caller audio: `none` (no frame ever), `late_start` (first after 2 s), `gaps` (a wait of 1 s or
    /// more inside the call), `stopped` (nothing for the last 5 s) or `ok`.
    pub fn shape(&self) -> &'static str {
        match self.first_ms {
            None => "none",
            Some(f) if f >= 2000 => "late_start",
            _ if self.max_gap_ms >= 1000 => "gaps",
            _ if self.end_gap_ms >= 5000 => "stopped",
            _ => "ok",
        }
    }
}

impl MediaStats {
    /// A caller frame has been read.
    pub fn caller_frame(&self) {
        let now = self.origin.elapsed().as_millis() as u64;
        let _ = self.first_in_ms.compare_exchange(NEVER, now, Relaxed, Relaxed);
        let prev = self.last_in_ms.swap(now, Relaxed);
        if prev != NEVER {
            self.max_gap_ms.fetch_max(now.saturating_sub(prev), Relaxed);
        }
    }

    /// The media connection ended: the end gap stops growing.
    pub fn media_ended(&self) {
        let _ = self.ended_ms.compare_exchange(NEVER, self.origin.elapsed().as_millis() as u64, Relaxed, Relaxed);
    }

    pub fn caller_audio(&self) -> CallerAudio {
        let ended = self.ended_ms.load(Relaxed);
        let now = if ended == NEVER { self.origin.elapsed().as_millis() as u64 } else { ended };
        let (first, last) = (self.first_in_ms.load(Relaxed), self.last_in_ms.load(Relaxed));
        CallerAudio {
            first_ms: (first != NEVER).then_some(first),
            max_gap_ms: self.max_gap_ms.load(Relaxed),
            end_gap_ms: if last == NEVER { now } else { now.saturating_sub(last) },
        }
    }
}

pub struct MediaIo {
    /// Caller audio, 20ms frames. Empty between arrivals: the consumer
    /// substitutes silence on its own tick, and takes every frame waiting when several are.
    pub inbound: mpsc::Receiver<Frame>,
    /// Agent audio to play. `try_send`: a full queue drops (never blocks the tick).
    pub outbound: mpsc::Sender<Frame>,
    /// Cancelled when the connection ends: far-end hangup, error, or dead media.
    pub closed: CancellationToken,
    pub stats: Arc<MediaStats>,
}

pub fn new_ticker() -> time::Interval {
    let mut t = time::interval(FRAME_INTERVAL);
    t.set_missed_tick_behavior(MissedTickBehavior::Skip);
    t
}

/// Starts the reader and writer tasks. `dead_after` (0 = never) is how long
/// inbound may be completely silent before the call's media is declared dead.
/// `cancel` is the call's lifetime: cancelling it stops both tasks.
pub fn spawn(call_id: String, stream: TcpStream, dead_after: Duration, cancel: CancellationToken) -> MediaIo {
    let _ = stream.set_nodelay(true);
    let (rd, mut wr) = stream.into_split();
    let (in_tx, in_rx) = mpsc::channel::<Frame>(INBOUND_QUEUE);
    let (out_tx, mut out_rx) = mpsc::channel::<Frame>(QUEUE);
    let closed = CancellationToken::new();
    let stats = Arc::new(MediaStats::default());

    tokio::spawn(read_loop(call_id.clone(), rd, in_tx, dead_after, cancel.clone(), closed.clone(), stats.clone()));

    let (closed_w, stats_w) = (closed.clone(), stats.clone());
    tokio::spawn(async move {
        let mut tick = new_ticker();
        let mut scratch = [0u8; audiosocket::HEADER_SIZE + FRAME_SIZE]; // reused: no per-frame alloc
        let mut last_tick: Option<time::Instant> = None;
        loop {
            let at = tokio::select! {
                _ = cancel.cancelled() => break,
                _ = closed_w.cancelled() => break,
                at = tick.tick() => at,
            };
            // How far this tick's interval is from the nominal 20ms (a stall shows as one big miss).
            if let Some(prev) = last_tick {
                metrics::PACER_DRIFT_MS.observe((at - prev).abs_diff(FRAME_INTERVAL).as_secs_f64() * 1000.0);
            }
            last_tick = Some(at);
            let (frame, filler) = match out_rx.try_recv() {
                Ok(f) => (f, false),
                Err(_) => (SILENCE, true),
            };
            // One write per frame (invariant #11). A failed write leaves the
            // framed stream misaligned, so it ends the call's media.
            if let Err(e) = audiosocket::write_frame(&mut wr, &mut scratch, audiosocket::KIND_SLIN16, &frame).await {
                if !closed_w.is_cancelled() {
                    tracing::error!(call_id = %call_id, error = %e, "media write failed; closing (framed stream can no longer be trusted)");
                }
                metrics::AUDIOSOCKET_SEND_ERRORS.fetch_add(1, Relaxed);
                closed_w.cancel();
                break;
            }
            stats_w.packets_out.fetch_add(1, Relaxed);
            metrics::AUDIOSOCKET_FRAMES_OUT.fetch_add(1, Relaxed);
            if filler {
                stats_w.silence_out.fetch_add(1, Relaxed);
                metrics::AUDIOSOCKET_SILENCE_SENT.fetch_add(1, Relaxed);
            }
        }
        let _ = wr.shutdown().await;
    });

    MediaIo { inbound: in_rx, outbound: out_tx, closed, stats }
}

async fn read_loop(
    call_id: String,
    mut rd: OwnedReadHalf,
    tx: mpsc::Sender<Frame>,
    dead_after: Duration,
    cancel: CancellationToken,
    closed: CancellationToken,
    stats: Arc<MediaStats>,
) {
    loop {
        let read = audiosocket::read_frame(&mut rd);
        let res = tokio::select! {
            _ = cancel.cancelled() => break,
            r = async {
                if dead_after.is_zero() { Ok(read.await) } else { time::timeout(dead_after, read).await }
            } => r,
        };
        let frame = match res {
            Err(_) => {
                metrics::MEDIA_WATCHDOG_TIMEOUTS.fetch_add(1, Relaxed);
                tracing::warn!(call_id = %call_id, event = "media.dead", secs = dead_after.as_secs(), "no inbound media; ending call media");
                break;
            }
            Ok(Err(e)) => {
                if e.kind() != std::io::ErrorKind::UnexpectedEof {
                    tracing::warn!(call_id = %call_id, error = %e, "media read error");
                }
                break;
            }
            Ok(Ok(f)) => f,
        };
        match frame {
            audiosocket::Inbound::Audio(f) => {
                stats.packets_in.fetch_add(1, Relaxed);
                stats.caller_frame();
                metrics::AUDIOSOCKET_FRAMES_IN.fetch_add(1, Relaxed);
                metrics::AUDIOSOCKET_BYTES_IN.fetch_add(FRAME_SIZE as u64, Relaxed);
                if tx.try_send(f).is_err() {
                    metrics::MEDIA_INBOUND_DROPPED.fetch_add(1, Relaxed);
                    stats.inbound_dropped.fetch_add(1, Relaxed); // consumer behind: drop, never block the reader
                }
            }
            audiosocket::Inbound::BadAudio(n) => {
                stats.malformed.fetch_add(1, Relaxed);
                metrics::AUDIOSOCKET_MALFORMED.fetch_add(1, Relaxed);
                tracing::debug!(call_id = %call_id, size = n, "audiosocket frame wrong size");
            }
            audiosocket::Inbound::Hangup => break,
            audiosocket::Inbound::Control(kind, _) => {
                tracing::debug!(call_id = %call_id, kind, "audiosocket control frame");
            }
        }
    }
    stats.media_ended();
    closed.cancel();
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{io::AsyncReadExt, net::TcpListener};

    #[test]
    fn caller_audio_shapes() {
        let a = |first_ms, max_gap_ms, end_gap_ms| CallerAudio { first_ms, max_gap_ms, end_gap_ms }.shape();
        assert_eq!(a(None, 0, 9000), "none", "a silent caller, or one never heard");
        assert_eq!(a(Some(3500), 20, 100), "late_start");
        assert_eq!(a(Some(200), 1400, 100), "gaps");
        assert_eq!(a(Some(200), 40, 7000), "stopped");
        assert_eq!(a(Some(200), 40, 100), "ok");
        assert_eq!(a(Some(1900), 999, 4999), "ok", "just inside every limit");
    }

    #[tokio::test]
    async fn caller_frames_are_timed_from_the_connection() {
        let s = MediaStats::default();
        assert_eq!(s.caller_audio().first_ms, None);
        assert!(s.caller_audio().end_gap_ms < 1000);
        s.caller_frame();
        tokio::time::sleep(Duration::from_millis(120)).await;
        s.caller_frame();
        s.caller_frame();
        let c = s.caller_audio();
        assert!(c.first_ms.unwrap() < 100, "{c:?}");
        assert!((100..400).contains(&c.max_gap_ms), "the 120 ms wait is the longest gap: {c:?}");
        assert!(c.end_gap_ms < 100, "{c:?}");
        s.media_ended();
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(s.caller_audio().end_gap_ms < 100, "frozen when the media ended, not growing with the wait for the summary");
    }

    #[tokio::test]
    async fn echo_frames_and_silence_filler() {
        let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = lis.local_addr().unwrap();
        let client = tokio::spawn(async move {
            let mut s = TcpStream::connect(addr).await.unwrap();
            let mut scratch = [0u8; 643];
            let mut f = [0u8; FRAME_SIZE];
            f[0] = 42;
            audiosocket::write_frame(&mut s, &mut scratch, audiosocket::KIND_SLIN16, &f).await.unwrap();
            // read three frames back: we expect filler silence (nothing was queued)
            let mut got = vec![];
            for _ in 0..3 {
                let mut buf = [0u8; 643];
                s.read_exact(&mut buf).await.unwrap();
                got.push(buf);
            }
            got
        });
        let (srv, _) = lis.accept().await.unwrap();
        let cancel = CancellationToken::new();
        let mut io = spawn("t".into(), srv, Duration::from_secs(5), cancel.clone());

        let f = io.inbound.recv().await.unwrap();
        assert_eq!(f[0], 42);

        let got = client.await.unwrap();
        for g in got {
            assert_eq!(&g[..3], &[0x12, 0x02, 0x80]);
            assert!(g[3..].iter().all(|b| *b == 0));
        }
        assert!(io.stats.silence_out.load(Relaxed) >= 3);
        let m = crate::metrics::render();
        let count = |name: &str| m.lines().find_map(|l| l.strip_prefix(name)?.trim().parse::<f64>().ok()).unwrap_or(0.0);
        assert!(count("vaani_pacer_drift_ms_count ") >= 2.0, "each tick after the first is timed against the nominal 20ms");
        assert!(count("vaani_audiosocket_frames_out_total ") >= 3.0 && count("vaani_audiosocket_silence_sent_total ") >= 3.0);
        cancel.cancel();
    }

    #[tokio::test]
    async fn far_end_close_cancels_closed_token() {
        let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = lis.local_addr().unwrap();
        let c = TcpStream::connect(addr).await.unwrap();
        let (srv, _) = lis.accept().await.unwrap();
        let io = spawn("t".into(), srv, Duration::ZERO, CancellationToken::new());
        drop(c);
        tokio::time::timeout(Duration::from_secs(2), io.closed.cancelled()).await.unwrap();
    }
}
