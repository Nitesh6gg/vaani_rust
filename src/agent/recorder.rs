//! The call as one mono track -- caller and agent mixed, like Dograh's call
//! recording (pipecat's AudioBufferProcessor, mono). Both directions meet once
//! per 20ms tick, so they stay in step without timestamps.

use crate::{
    media::{FRAME_SIZE, Frame},
    metrics,
};
use std::sync::atomic::Ordering::Relaxed;

/// Dograh's own in-memory recording limit: 100MB, ~54 minutes of 16kHz mono.
const MAX_BYTES: usize = 100 << 20;

const WAV_HEADER: usize = 44;

/// A finished (or growing) recording: a WAV file, counted in `vaani_recording_bytes` for as long as the
/// engine holds it. Empty when nothing was recorded.
#[derive(Default)]
pub struct Recording(Vec<u8>);

impl Recording {
    /// The audio's size, without the WAV header.
    pub fn pcm_len(&self) -> usize {
        self.0.len().saturating_sub(WAV_HEADER)
    }

    /// The WAV file, handed over (to the upload): no longer counted as held.
    pub fn into_wav(mut self) -> Vec<u8> {
        let v = std::mem::take(&mut self.0);
        metrics::RECORDING_BYTES.fetch_sub(v.len() as u64, Relaxed);
        v
    }
}

impl Drop for Recording {
    fn drop(&mut self) {
        metrics::RECORDING_BYTES.fetch_sub(self.0.len() as u64, Relaxed);
    }
}

#[derive(Default)]
pub struct MixRecorder {
    rec: Recording,
    pub truncated: bool,
}

impl MixRecorder {
    /// Records the caller's frame plus what goes out this tick. Past the cap the
    /// rest of the call isn't kept. ponytail: the buffer grows on demand, so a
    /// typical call costs about its own length (32 KB/s) and nothing is reserved up front;
    /// chunked upload during the call (S3 multipart) if `vaani_recording_bytes` ever matters.
    pub fn frame(&mut self, inbound: &Frame, outbound: Option<&Frame>) {
        let pcm = &mut self.rec.0;
        if pcm.len() + inbound.len() > MAX_BYTES {
            self.truncated = true;
            return;
        }
        if pcm.is_empty() {
            // The WAV header's place, filled in by `finish`: the file is built where the audio already
            // is, never copied into a second buffer at the end of the call.
            pcm.extend_from_slice(&[0; WAV_HEADER]);
            metrics::RECORDING_BYTES.fetch_add(WAV_HEADER as u64, Relaxed);
        }
        let at = pcm.len();
        pcm.extend_from_slice(inbound);
        metrics::RECORDING_BYTES.fetch_add(inbound.len() as u64, Relaxed);
        if let Some(out) = outbound {
            mix_into(&mut pcm[at..], out);
        }
    }

    /// Caller frames that arrived late, in one clump just before the current tick's frame (oldest first).
    /// They belong to the ticks that were recorded as silence while they were on their way: each is
    /// mixed back into its own slot, so the recording keeps its length and the two sides stay in step.
    /// A clump longer than what is recorded so far (the call's start-up) is simply added.
    pub fn late(&mut self, frames: &[Frame]) {
        let slots = self.rec.pcm_len() / FRAME_SIZE;
        if frames.len() > slots {
            for f in frames {
                self.frame(f, None);
            }
            return;
        }
        let pcm = &mut self.rec.0;
        let mut at = pcm.len() - frames.len() * FRAME_SIZE;
        for f in frames {
            mix_into(&mut pcm[at..at + FRAME_SIZE], f);
            at += FRAME_SIZE;
        }
    }

    /// The recording as a WAV file (empty if no frame was recorded).
    pub fn finish(mut self) -> Recording {
        let pcm = self.rec.pcm_len();
        if pcm > 0 {
            self.rec.0[..WAV_HEADER].copy_from_slice(&header(pcm));
        }
        self.rec
    }
}

/// Adds src's PCM16 LE samples into dst's, clipping at i16's range.
fn mix_into(dst: &mut [u8], src: &[u8]) {
    for (d, s) in dst.chunks_exact_mut(2).zip(src.chunks_exact(2)) {
        let sum = i32::from(i16::from_le_bytes([d[0], d[1]])) + i32::from(i16::from_le_bytes([s[0], s[1]]));
        d.copy_from_slice(&(sum.clamp(i32::from(i16::MIN), i32::from(i16::MAX)) as i16).to_le_bytes());
    }
}

/// The header of a 16kHz mono PCM16 WAV file holding `pcm_len` bytes of audio.
fn header(pcm_len: usize) -> [u8; WAV_HEADER] {
    let mut b = Vec::with_capacity(WAV_HEADER);
    b.extend_from_slice(b"RIFF");
    b.extend_from_slice(&(36 + pcm_len as u32).to_le_bytes());
    b.extend_from_slice(b"WAVEfmt ");
    b.extend_from_slice(&16u32.to_le_bytes()); // fmt chunk size
    b.extend_from_slice(&1u16.to_le_bytes()); // PCM
    b.extend_from_slice(&1u16.to_le_bytes()); // mono
    b.extend_from_slice(&16000u32.to_le_bytes());
    b.extend_from_slice(&32000u32.to_le_bytes()); // byte rate
    b.extend_from_slice(&2u16.to_le_bytes()); // block align
    b.extend_from_slice(&16u16.to_le_bytes()); // bits per sample
    b.extend_from_slice(b"data");
    b.extend_from_slice(&(pcm_len as u32).to_le_bytes());
    b.try_into().expect("a WAV header is 44 bytes")
}

/// A 16kHz mono PCM16 WAV file around `pcm` (tests build their fixtures with it).
#[cfg(test)]
pub fn wav(pcm: &[u8]) -> Vec<u8> {
    [&header(pcm.len())[..], pcm].concat()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::media::SILENCE;

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
        let rec = r.finish();
        assert_eq!(rec.pcm_len(), 3 * FRAME_SIZE);
        let file = rec.into_wav();
        assert_eq!(file[..WAV_HEADER], header(3 * FRAME_SIZE), "the header is filled in where it was reserved");
        assert_eq!(file, wav(&file[WAV_HEADER..]), "the same file `wav` builds, without the copy");
        let pcm = &file[WAV_HEADER..];
        let s = |i: usize| i16::from_le_bytes([pcm[i * FRAME_SIZE], pcm[i * FRAME_SIZE + 1]]);
        assert_eq!((s(0), s(1), s(2)), (1000, 1234, i16::MAX));
    }

    #[test]
    fn late_caller_frames_go_back_into_the_ticks_they_missed() {
        let s = |pcm: &[u8], i: usize| i16::from_le_bytes([pcm[i * FRAME_SIZE], pcm[i * FRAME_SIZE + 1]]);
        // Three ticks recorded with no caller audio (the agent was speaking 100), then the caller's
        // frames for the last two of them arrive together.
        let mut r = MixRecorder::default();
        for _ in 0..3 {
            r.frame(&SILENCE, Some(&tone(100)));
        }
        r.late(&[tone(1000), tone(2000)]);
        r.frame(&tone(3000), None);
        let file = r.finish().into_wav();
        let pcm = &file[WAV_HEADER..];
        assert_eq!(pcm.len(), 4 * FRAME_SIZE, "the recording keeps its length: one slot per tick");
        assert_eq!((s(pcm, 0), s(pcm, 1), s(pcm, 2), s(pcm, 3)), (100, 1100, 2100, 3000), "each late frame is mixed into its own tick");

        // Before anything was recorded (the call's start-up), late frames open the recording.
        let mut r = MixRecorder::default();
        r.late(&[tone(7), tone(8)]);
        r.frame(&tone(9), None);
        let file = r.finish().into_wav();
        let pcm = &file[WAV_HEADER..];
        assert_eq!((pcm.len(), s(pcm, 0), s(pcm, 1), s(pcm, 2)), (3 * FRAME_SIZE, 7, 8, 9));

        // A clump longer than the recording so far is added whole.
        let mut r = MixRecorder::default();
        r.frame(&SILENCE, None);
        r.late(&[tone(1), tone(2), tone(3)]);
        let file = r.finish().into_wav();
        let pcm = &file[WAV_HEADER..];
        assert_eq!((pcm.len(), s(pcm, 1), s(pcm, 2), s(pcm, 3)), (4 * FRAME_SIZE, 1, 2, 3));
    }

    #[test]
    fn nothing_recorded_is_an_empty_recording() {
        let rec = MixRecorder::default().finish();
        assert_eq!(rec.pcm_len(), 0);
        assert!(rec.into_wav().is_empty());
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
