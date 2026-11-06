//! The call-control sounds, embedded so they work whichever directory the server
//! starts from: the loop that plays to the caller while a transfer rings, and the
//! beep that plays when the destination answers. Both match the self-hosted
//! Dograh's api/assets files. Served as 16kHz LE PCM16, the pipeline's format.

use anyhow::{Result, bail};
use std::sync::{Arc, LazyLock};

static HOLD_RING_WAV: &[u8] = include_bytes!("../assets/transfer_hold_ring_8000.wav");
static BEEP_WAV: &[u8] = include_bytes!("../assets/beep.wav");

static HOLD_RING: LazyLock<Arc<Vec<u8>>> = LazyLock::new(|| Arc::new(decode(HOLD_RING_WAV).expect("embedded hold ring")));
static BEEP: LazyLock<Arc<Vec<u8>>> = LazyLock::new(|| Arc::new(decode(BEEP_WAV).expect("embedded beep")));

/// The transfer hold/ring loop.
pub fn transfer_hold_ring() -> Arc<Vec<u8>> {
    HOLD_RING.clone()
}

/// The transfer-connected beep.
pub fn beep() -> Arc<Vec<u8>> {
    BEEP.clone()
}

/// Reads a mono PCM16 WAV at 8kHz or 16kHz and returns 16kHz LE PCM16.
fn decode(wav: &[u8]) -> Result<Vec<u8>> {
    let (rate, pcm) = parse_wav(wav)?;
    match rate {
        16000 => Ok(pcm.to_vec()),
        8000 => Ok(upsample2x(pcm)),
        r => bail!("assets: unsupported sample rate {r} (want 8000 or 16000)"),
    }
}

/// Walks the RIFF chunks (files may carry LIST/metadata chunks before "data")
/// and returns the sample rate and raw samples of a mono PCM16 WAV.
fn parse_wav(b: &[u8]) -> Result<(u32, &[u8])> {
    if b.len() < 12 || &b[0..4] != b"RIFF" || &b[8..12] != b"WAVE" {
        bail!("assets: not a RIFF/WAVE file");
    }
    let (mut off, mut rate) = (12, None);
    while off + 8 <= b.len() {
        let id = &b[off..off + 4];
        let mut size = u32::from_le_bytes(b[off + 4..off + 8].try_into()?) as usize;
        let body = off + 8;
        size = size.min(b.len() - body); // tolerate a truncated final chunk
        match id {
            b"fmt " => {
                if size < 16 {
                    bail!("assets: short fmt chunk");
                }
                let u16at = |i: usize| u16::from_le_bytes([b[body + i], b[body + i + 1]]);
                let (format, channels, bits) = (u16at(0), u16at(2), u16at(14));
                if format != 1 || channels != 1 || bits != 16 {
                    bail!("assets: want mono 16-bit PCM, got format={format} channels={channels} bits={bits}");
                }
                rate = Some(u32::from_le_bytes(b[body + 4..body + 8].try_into()?));
            }
            b"data" => {
                let Some(rate) = rate else { bail!("assets: data chunk before fmt chunk") };
                return Ok((rate, &b[body..body + (size & !1)]));
            }
            _ => {}
        }
        off = body + size + (size & 1); // chunks are word-aligned
    }
    bail!("assets: no data chunk")
}

/// Doubles the sample rate by linear interpolation. Plenty for a ring tone and a
/// beep; a real resampling filter would be overkill.
fn upsample2x(pcm: &[u8]) -> Vec<u8> {
    let s: Vec<i16> = pcm.chunks_exact(2).map(|b| i16::from_le_bytes([b[0], b[1]])).collect();
    let mut out = Vec::with_capacity(s.len() * 4);
    for (i, &cur) in s.iter().enumerate() {
        let next = s.get(i + 1).copied().unwrap_or(cur);
        out.extend_from_slice(&cur.to_le_bytes());
        out.extend_from_slice(&(((i32::from(cur) + i32::from(next)) / 2) as i16).to_le_bytes());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_sounds_decode_to_16khz() {
        let (rate, raw) = parse_wav(HOLD_RING_WAV).unwrap();
        assert_eq!(rate, 8000);
        assert_eq!(transfer_hold_ring().len(), raw.len() * 2, "8kHz doubled to 16kHz");
        assert!(beep().len() >= 640, "the beep is at least one frame");
        assert!(transfer_hold_ring().len() >= 640 * 50, "the ring loops for seconds");
    }

    #[test]
    fn upsampling_interpolates_and_bad_files_are_rejected() {
        let pcm: Vec<u8> = [100i16, 300, -200].iter().flat_map(|s| s.to_le_bytes()).collect();
        let out: Vec<i16> = upsample2x(&pcm).chunks_exact(2).map(|b| i16::from_le_bytes([b[0], b[1]])).collect();
        assert_eq!(out, [100, 200, 300, 50, -200, -200]);
        assert!(parse_wav(b"nope").is_err());
        assert!(decode(&crate::agent::recorder::wav(&[0, 0])).is_ok(), "16kHz passes through");
    }
}
