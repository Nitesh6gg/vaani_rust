//! The call as one mono track -- caller and agent mixed, like Dograh's call
//! recording (pipecat's AudioBufferProcessor, mono). Both directions meet once
//! per 20ms tick, so they stay in step without timestamps.

use crate::media::Frame;

/// Dograh's own in-memory recording limit: 100MB, ~54 minutes of 16kHz mono.
const MAX_BYTES: usize = 100 << 20;

#[derive(Default)]
pub struct MixRecorder {
    pcm: Vec<u8>,
    pub truncated: bool,
}

impl MixRecorder {
    /// Records the caller's frame plus what goes out this tick. Past the cap the
    /// rest of the call isn't kept. ponytail: the buffer grows on demand, so a
    /// typical call costs about its own length (32 KB/s) and nothing is reserved up front.
    pub fn frame(&mut self, inbound: &Frame, outbound: Option<&Frame>) {
        if self.pcm.len() + inbound.len() > MAX_BYTES {
            self.truncated = true;
            return;
        }
        let at = self.pcm.len();
        self.pcm.extend_from_slice(inbound);
        if let Some(out) = outbound {
            mix_into(&mut self.pcm[at..], out);
        }
    }

    pub fn into_pcm(self) -> Vec<u8> {
        self.pcm
    }
}

/// Adds src's PCM16 LE samples into dst's, clipping at i16's range.
fn mix_into(dst: &mut [u8], src: &[u8]) {
    for (d, s) in dst.chunks_exact_mut(2).zip(src.chunks_exact(2)) {
        let sum = i32::from(i16::from_le_bytes([d[0], d[1]])) + i32::from(i16::from_le_bytes([s[0], s[1]]));
        d.copy_from_slice(&(sum.clamp(i32::from(i16::MIN), i32::from(i16::MAX)) as i16).to_le_bytes());
    }
}

/// A 16kHz mono PCM16 WAV file around `pcm`.
pub fn wav(pcm: &[u8]) -> Vec<u8> {
    let mut b = Vec::with_capacity(44 + pcm.len());
    b.extend_from_slice(b"RIFF");
    b.extend_from_slice(&(36 + pcm.len() as u32).to_le_bytes());
    b.extend_from_slice(b"WAVEfmt ");
    b.extend_from_slice(&16u32.to_le_bytes()); // fmt chunk size
    b.extend_from_slice(&1u16.to_le_bytes()); // PCM
    b.extend_from_slice(&1u16.to_le_bytes()); // mono
    b.extend_from_slice(&16000u32.to_le_bytes());
    b.extend_from_slice(&32000u32.to_le_bytes()); // byte rate
    b.extend_from_slice(&2u16.to_le_bytes()); // block align
    b.extend_from_slice(&16u16.to_le_bytes()); // bits per sample
    b.extend_from_slice(b"data");
    b.extend_from_slice(&(pcm.len() as u32).to_le_bytes());
    b.extend_from_slice(pcm);
    b
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::media::{FRAME_SIZE, SILENCE};

    fn tone(v: i16) -> Frame {
        let mut f = SILENCE;
        for c in f.chunks_exact_mut(2) {
            c.copy_from_slice(&v.to_le_bytes());
        }
        f
    }

    #[test]
    fn mixes_both_directions_into_one_slot_and_clips() {
        let mut r = MixRecorder::default();
        r.frame(&tone(1000), None);
        r.frame(&tone(1000), Some(&tone(234)));
        r.frame(&tone(30000), Some(&tone(30000)));
        let pcm = r.into_pcm();
        assert_eq!(pcm.len(), 3 * FRAME_SIZE);
        let s = |i: usize| i16::from_le_bytes([pcm[i * FRAME_SIZE], pcm[i * FRAME_SIZE + 1]]);
        assert_eq!((s(0), s(1), s(2)), (1000, 1234, i16::MAX));
    }

    #[test]
    fn wav_header_describes_the_data() {
        let w = wav(&[1, 2, 3, 4]);
        assert_eq!(&w[..4], b"RIFF");
        assert_eq!(u32::from_le_bytes(w[4..8].try_into().unwrap()), 40);
        assert_eq!(&w[8..16], b"WAVEfmt ");
        assert_eq!(u32::from_le_bytes(w[24..28].try_into().unwrap()), 16000);
        assert_eq!(u32::from_le_bytes(w[40..44].try_into().unwrap()), 4);
        assert_eq!(&w[44..], &[1, 2, 3, 4]);
    }
}
