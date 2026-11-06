//! Media plane: AudioSocket over TCP (Asterisk externalMedia), a 20ms pacer,
//! and a managed port pool. slin16 (16kHz s16le mono) end to end.

pub mod audiosocket;
pub mod call;
pub mod ports;

use std::time::Duration;

/// 16kHz x 2 bytes x 20ms: the one frame shape of the whole pipeline.
pub const FRAME_SIZE: usize = 640;
pub const FRAME_INTERVAL: Duration = Duration::from_millis(20);
/// One frame by value: 640 bytes on the stack/in the channel slot, so the hot
/// path never touches the allocator (invariant #3, without a pool).
pub type Frame = [u8; FRAME_SIZE];

pub const SILENCE: Frame = [0u8; FRAME_SIZE];

/// RMS of one LE PCM16 frame, in sample units (0..32768).
pub fn rms(pcm: &[u8]) -> f64 {
    let n = pcm.len() / 2;
    if n == 0 {
        return 0.0;
    }
    let sum: f64 = pcm
        .chunks_exact(2)
        .map(|b| {
            let s = f64::from(i16::from_le_bytes([b[0], b[1]]));
            s * s
        })
        .sum();
    (sum / n as f64).sqrt()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rms_of_silence_and_a_flat_signal() {
        assert_eq!(rms(&SILENCE), 0.0);
        let mut f = SILENCE;
        for c in f.chunks_exact_mut(2) {
            c.copy_from_slice(&1000i16.to_le_bytes());
        }
        assert!((rms(&f) - 1000.0).abs() < 1e-9);
    }
}
