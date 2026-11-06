//! Barge-in detection: the Earshot VAD (pure Rust, 16ms hops) with the same
//! vote window as the Go server's TEN VAD detector. Runs locally on the inbound
//! audio because it must react within a fraction of a second; the STT only
//! produces a transcript once the caller pauses, and that transcript then
//! confirms or dismisses the interruption.

use crate::media::Frame;
use std::time::Instant;

/// Earshot's analysis hop: 256 samples = 16ms at 16kHz. It does not divide the
/// 20ms media frame (320 samples), so samples carry over between frames.
pub const HOP: usize = 256;
/// A hop votes; `VOTES` of the last `HISTORY` hops must say "speech".
const HISTORY: usize = 5;
const VOTES: usize = 3;

pub trait BargeInDetector: Send {
    /// One 20ms LE PCM16 frame -> does the caller count as speaking right now?
    fn detect(&mut self, pcm: &Frame) -> bool;
    /// Clears the vote window (on pausing a reply, so stale votes don't re-trigger).
    fn reset(&mut self);
    /// When a hop last came back as speech (any single hop, not the window).
    fn last_speech_at(&self) -> Option<Instant>;
}

/// Verdict for one hop of samples; injectable so the voting is testable.
pub type HopFn = Box<dyn FnMut(&[i16]) -> bool + Send>;

pub struct VoteDetector {
    hop: HopFn,
    carry: Vec<i16>,
    history: [bool; HISTORY],
    idx: usize,
    last_speech: Option<Instant>,
}

impl VoteDetector {
    pub fn new(hop: HopFn) -> Self {
        Self { hop, carry: Vec::with_capacity(HOP + 320), history: [false; HISTORY], idx: 0, last_speech: None }
    }

    /// The production detector: Earshot, `threshold` in 0..1 (0.5 is its default).
    pub fn earshot(threshold: f32) -> Self {
        let mut det = earshot::Detector::default_boxed();
        Self::new(Box::new(move |hop| det.predict_i16(hop) >= threshold))
    }
}

impl BargeInDetector for VoteDetector {
    fn detect(&mut self, pcm: &Frame) -> bool {
        self.carry.extend(pcm.chunks_exact(2).map(|b| i16::from_le_bytes([b[0], b[1]])));
        let mut start = 0;
        while self.carry.len() - start >= HOP {
            let speech = (self.hop)(&self.carry[start..start + HOP]);
            self.history[self.idx] = speech;
            self.idx = (self.idx + 1) % HISTORY;
            if speech {
                self.last_speech = Some(Instant::now());
            }
            start += HOP;
        }
        self.carry.drain(..start);
        self.history.iter().filter(|s| **s).count() >= VOTES
    }

    fn reset(&mut self) {
        self.history = [false; HISTORY];
        self.idx = 0;
    }

    fn last_speech_at(&self) -> Option<Instant> {
        self.last_speech
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::media::SILENCE;

    fn loud() -> Frame {
        let mut f = SILENCE;
        for c in f.chunks_exact_mut(2) {
            c.copy_from_slice(&8000i16.to_le_bytes());
        }
        f
    }

    /// Fake hop verdict: loud samples are speech.
    fn energy() -> VoteDetector {
        VoteDetector::new(Box::new(|h| h.iter().all(|s| s.abs() > 1000)))
    }

    #[test]
    fn votes_need_three_speech_hops_and_hops_straddle_frames() {
        let mut d = energy();
        // 320 samples/frame, 256/hop: frame 1 -> 1 hop, frame 2 -> 1 hop (64 carried), ...
        assert!(!d.detect(&loud())); // 1 vote
        assert!(!d.detect(&loud())); // 2 votes
        assert!(d.detect(&loud())); // 3-4 votes -> speech
        assert!(d.last_speech_at().is_some());
        d.reset();
        assert!(!d.detect(&SILENCE));
    }

    #[test]
    fn single_click_is_not_speech() {
        let mut d = energy();
        assert!(!d.detect(&loud()));
        for _ in 0..10 {
            assert!(!d.detect(&SILENCE));
        }
    }

    #[test]
    fn earshot_runs_and_calls_silence_silence() {
        let mut d = VoteDetector::earshot(0.5);
        for _ in 0..50 {
            assert!(!d.detect(&SILENCE));
        }
    }
}
