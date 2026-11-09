//! One call's media plane over an accepted AudioSocket TCP connection:
//!
//! ```text
//! TCP read -> reader task -> inbound (cap 5) -> [agent's 20ms release tick]
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

/// Queue depth between the tasks (~100ms of audio).
const QUEUE: usize = 5;

#[derive(Default)]
pub struct MediaStats {
    pub packets_in: AtomicU64,
    pub packets_out: AtomicU64,
    pub silence_out: AtomicU64,
    pub malformed: AtomicU64,
    pub inbound_dropped: AtomicU64,
}

pub struct MediaIo {
    /// Caller audio, 20ms frames. Empty between arrivals: the consumer
    /// substitutes silence on its own tick.
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
    let (in_tx, in_rx) = mpsc::channel::<Frame>(QUEUE);
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
    closed.cancel();
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{io::AsyncReadExt, net::TcpListener};

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
