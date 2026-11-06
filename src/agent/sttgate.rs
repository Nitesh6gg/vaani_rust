//! Speech-only sending to the STT (`STT_SPEECH_ONLY`). Sarvam bills the audio it
//! is sent (verified in Go, 2026-10-13: 100s connection, 10s sent, 10s billed),
//! and sending every frame of the call costs about twice what the caller's
//! speech does. Every frame goes into a ring buffer first, and when speech is
//! noticed the buffered pre-roll goes out ahead of the live frames, so a word's
//! first syllable still arrives.
//!
//! Closed: frames are kept (the newest pre-roll's worth), nothing is sent, but a
//! short silent chunk goes out every `KEEPALIVE` so the connection lives (Sarvam
//! closes an idle socket after 60s and rejects a ping).
//! Open: every frame is sent until `hangover` after the last speech.

use crate::media::{FRAME_INTERVAL, Frame, SILENCE, rms};
use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

/// While the gate is closed Sarvam gets one silent chunk this often.
const KEEPALIVE: Duration = Duration::from_secs(20);
/// The silence sent as that keepalive: 100ms, as pipecat's.
const KEEPALIVE_FRAMES: usize = 5;
/// The VAD heard speech this recently counts as speech now. `last_speech_at` is
/// the last 16ms hop that was speech, not a smoothed verdict.
const VOICED_WINDOW: Duration = Duration::from_millis(100);

#[derive(Debug, Clone, Copy)]
pub struct GateConfig {
    /// Audio from before speech was noticed that goes out when the gate opens, so
    /// the VAD's lag doesn't lose a first syllable (pipecat keeps 1s too).
    pub preroll: Duration,
    /// How long the gate stays open after the last speech: it has to outlast the
    /// flush wait plus the STT's reply to the flush.
    pub hangover: Duration,
    /// If above 0, a frame at least this loud also opens the gate: a net under a
    /// VAD that misses a quiet word. 0 = off.
    pub rms_floor: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Why {
    Vad,
    Rms,
}

impl GateConfig {
    /// Why `pcm` counts as speech, if it does -- the VAD wins when both would.
    pub fn voiced(&self, last_speech: Option<Instant>, now: Instant, pcm: &Frame) -> Option<Why> {
        if last_speech.is_some_and(|t| now.saturating_duration_since(t) < VOICED_WINDOW) {
            return Some(Why::Vad);
        }
        (self.rms_floor > 0.0 && rms(pcm) >= self.rms_floor).then_some(Why::Rms)
    }
}

pub struct SttGate {
    cfg: GateConfig,
    preroll_frames: usize,
    ring: VecDeque<Frame>,
    open: bool,
    last_voiced: Option<Instant>,
    last_sent: Instant,
    /// Frames handed to the STT, keepalive silence included (what Sarvam bills).
    pub sent_frames: u64,
}

impl SttGate {
    pub fn new(cfg: GateConfig, now: Instant) -> Self {
        let preroll_frames = ((cfg.preroll.as_millis() / FRAME_INTERVAL.as_millis()) as usize).max(1);
        Self {
            cfg,
            preroll_frames,
            ring: VecDeque::with_capacity(preroll_frames),
            open: false,
            last_voiced: None,
            last_sent: now,
            sent_frames: 0,
        }
    }

    /// Takes one 20ms frame and whether it counts as speech, and sends what the
    /// STT should get through `feed`: nothing, the frame, or the buffered
    /// pre-roll and the frame. Returns whether the gate opened on this frame.
    pub fn frame(&mut self, pcm: &Frame, why: Option<Why>, now: Instant, feed: &mut dyn FnMut(&Frame)) -> bool {
        if why.is_some() {
            self.last_voiced = Some(now);
        }
        if self.open && self.last_voiced.is_none_or(|t| now.saturating_duration_since(t) > self.cfg.hangover) {
            self.open = false;
        }
        if self.open {
            self.send(pcm, now, feed);
            return false;
        }
        if why.is_some() {
            self.open = true;
            while let Some(old) = self.ring.pop_front() {
                self.send(&old, now, feed);
            }
            self.send(pcm, now, feed);
            return true;
        }
        if self.ring.len() == self.preroll_frames {
            self.ring.pop_front();
        }
        self.ring.push_back(*pcm);
        if now.saturating_duration_since(self.last_sent) >= KEEPALIVE {
            for _ in 0..KEEPALIVE_FRAMES {
                self.send(&SILENCE, now, feed);
            }
        }
        false
    }

    fn send(&mut self, pcm: &Frame, now: Instant, feed: &mut dyn FnMut(&Frame)) {
        self.sent_frames += 1;
        self.last_sent = now;
        feed(pcm);
    }

    /// The audio handed to the STT so far, in seconds.
    pub fn sent_seconds(&self) -> f64 {
        self.sent_frames as f64 * FRAME_INTERVAL.as_secs_f64()
    }

    pub fn cfg(&self) -> GateConfig {
        self.cfg
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn marked(n: u8) -> Frame {
        let mut f = SILENCE;
        f[0] = n;
        f
    }

    fn gate() -> (SttGate, Instant) {
        let t0 = Instant::now();
        (SttGate::new(GateConfig { preroll: Duration::from_millis(100), hangover: Duration::from_millis(200), rms_floor: 400.0 }, t0), t0)
    }

    fn run(g: &mut SttGate, f: Frame, why: Option<Why>, at: Instant) -> Vec<u8> {
        let mut sent = vec![];
        g.frame(&f, why, at, &mut |p| sent.push(p[0]));
        sent
    }

    #[test]
    fn closed_gate_sends_nothing_and_keeps_only_the_newest_preroll() {
        let (mut g, t0) = gate();
        for i in 0..20u32 {
            assert!(run(&mut g, marked(i as u8), None, t0 + FRAME_INTERVAL * i).is_empty());
        }
        assert_eq!((g.sent_frames, g.ring.len()), (0, 5), "100ms of pre-roll = 5 frames");
    }

    #[test]
    fn speech_opens_it_with_the_preroll_first_then_live_frames_until_the_hangover() {
        let (mut g, t0) = gate();
        for i in 0..8u32 {
            run(&mut g, marked(i as u8), None, t0 + FRAME_INTERVAL * i);
        }
        let at = |i: u32| t0 + FRAME_INTERVAL * i;
        // Frames 3..8 are the newest 5; the speech frame (9) follows them.
        assert_eq!(run(&mut g, marked(9), Some(Why::Vad), at(8)), [3, 4, 5, 6, 7, 9]);
        assert_eq!(run(&mut g, marked(10), None, at(9)), [10], "open: every frame goes out");
        assert_eq!(run(&mut g, marked(11), None, at(8 + 10)), [11], "still inside the 200ms hangover");
        assert!(run(&mut g, marked(12), None, at(8 + 11)).is_empty(), "past the hangover the gate closes again");
        assert_eq!(g.sent_frames, 8);
    }

    #[test]
    fn a_silent_keepalive_goes_out_while_closed() {
        let (mut g, t0) = gate();
        assert!(run(&mut g, marked(1), None, t0 + KEEPALIVE - Duration::from_millis(20)).is_empty());
        assert_eq!(run(&mut g, marked(2), None, t0 + KEEPALIVE), [0; KEEPALIVE_FRAMES]);
        assert!(run(&mut g, marked(3), None, t0 + KEEPALIVE + Duration::from_millis(20)).is_empty(), "the clock restarts");
    }

    #[test]
    fn voiced_prefers_the_vad_and_falls_back_to_loudness() {
        let (g, t0) = gate();
        let mut loud = SILENCE;
        for c in loud.chunks_exact_mut(2) {
            c.copy_from_slice(&3000i16.to_le_bytes());
        }
        let now = t0 + Duration::from_secs(1);
        assert_eq!(g.cfg.voiced(Some(now - Duration::from_millis(50)), now, &loud), Some(Why::Vad));
        assert_eq!(g.cfg.voiced(Some(now - Duration::from_millis(500)), now, &loud), Some(Why::Rms));
        assert_eq!(g.cfg.voiced(None, now, &SILENCE), None);
    }
}
