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
