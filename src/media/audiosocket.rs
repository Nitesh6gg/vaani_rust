//! Asterisk res_audiosocket wire protocol: `kind(1) | len(2, big-endian) | payload`.
//! Audio payload (kind 0x12, slin16) is little-endian by definition.
//!
//! INVARIANT #11: a frame goes out in ONE write. Asterisk allows only 5ms
//! between reading a frame's header and its payload before it hangs the
//! channel up, so header and payload must never be separate writes.

use super::{FRAME_SIZE, Frame};
use std::io;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const KIND_HANGUP: u8 = 0x00;
pub const KIND_SLIN16: u8 = 0x12;

pub const HEADER_SIZE: usize = 3;

// One frame per 20ms tick, moved by value: boxing it would put an allocation on the hot path.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, PartialEq, Eq)]
pub enum Inbound {
    /// A full 640-byte slin16 frame.
    Audio(Frame),
    /// Audio of any other size: counted and dropped by the caller.
    BadAudio(usize),
    Hangup,
    /// UUID/DTMF/error/unknown: control plane, not the media pipeline.
    Control(u8, Vec<u8>),
}

/// Reads exactly one frame; blocks until header and payload have arrived.
pub async fn read_frame<R: AsyncRead + Unpin>(r: &mut R) -> io::Result<Inbound> {
    let mut hdr = [0u8; HEADER_SIZE];
    r.read_exact(&mut hdr).await?;
    let (kind, len) = (hdr[0], usize::from(u16::from_be_bytes([hdr[1], hdr[2]])));

    if kind == KIND_SLIN16 && len == FRAME_SIZE {
        let mut f = [0u8; FRAME_SIZE];
        r.read_exact(&mut f).await?;
        return Ok(Inbound::Audio(f));
    }

    let mut payload = vec![0u8; len];
    r.read_exact(&mut payload).await?;
    Ok(match kind {
        KIND_HANGUP => Inbound::Hangup,
        KIND_SLIN16 => Inbound::BadAudio(len),
        _ => Inbound::Control(kind, payload),
    })
}

/// Builds `kind | len | payload` into `buf` and returns the used prefix.
pub fn encode_into<'a>(buf: &'a mut [u8], kind: u8, payload: &[u8]) -> io::Result<&'a [u8]> {
    let n = HEADER_SIZE + payload.len();
    if payload.len() > usize::from(u16::MAX) || buf.len() < n {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "audiosocket payload too large"));
    }
    buf[0] = kind;
    buf[1..3].copy_from_slice(&(payload.len() as u16).to_be_bytes());
    buf[HEADER_SIZE..n].copy_from_slice(payload);
    Ok(&buf[..n])
}

/// One `write_all` of the whole frame. `buf` is caller-owned scratch so the
/// 20ms loop reuses it instead of allocating.
pub async fn write_frame<W: AsyncWrite + Unpin>(w: &mut W, buf: &mut [u8], kind: u8, payload: &[u8]) -> io::Result<()> {
    let framed = encode_into(buf, kind, payload)?;
    w.write_all(framed).await
}

#[cfg(test)]
mod tests {
    use super::*;
    const KIND_UUID: u8 = 0x01;

    #[tokio::test]
    async fn roundtrip_audio_and_control() {
        let mut scratch = [0u8; HEADER_SIZE + FRAME_SIZE];
        let mut wire = Vec::new();
        let mut f = [0u8; FRAME_SIZE];
        f[0] = 7;
        f[639] = 9;
        write_frame(&mut wire, &mut scratch, KIND_SLIN16, &f).await.unwrap();
        write_frame(&mut wire, &mut scratch, KIND_UUID, b"abc").await.unwrap();
        write_frame(&mut wire, &mut scratch, KIND_SLIN16, &[1, 2]).await.unwrap();
        write_frame(&mut wire, &mut scratch, KIND_HANGUP, &[]).await.unwrap();

        let mut r = &wire[..];
        assert_eq!(read_frame(&mut r).await.unwrap(), Inbound::Audio(f));
        assert_eq!(read_frame(&mut r).await.unwrap(), Inbound::Control(KIND_UUID, b"abc".to_vec()));
        assert_eq!(read_frame(&mut r).await.unwrap(), Inbound::BadAudio(2));
        assert_eq!(read_frame(&mut r).await.unwrap(), Inbound::Hangup);
        assert!(read_frame(&mut r).await.is_err()); // EOF
    }

    #[test]
    fn header_is_big_endian_length() {
        let mut b = [0u8; 8];
        let out = encode_into(&mut b, KIND_SLIN16, &[0u8; 640]).unwrap_err();
        assert_eq!(out.kind(), io::ErrorKind::InvalidInput); // scratch too small
        let mut b = [0u8; 643];
        let out = encode_into(&mut b, KIND_SLIN16, &[0u8; 640]).unwrap();
        assert_eq!(&out[..3], &[0x12, 0x02, 0x80]);
    }
}
