//! Dograh's workflow recordings (workflow_recordings, api/services/pipecat/
//! recording_audio_cache.py): an organization's uploaded audio files, played
//! instead of TTS for a greeting, a transition, a tool's message, or an LLM
//! reply that asks for one. Each is downloaded from MinIO, converted by ffmpeg
//! to 16kHz mono PCM, trimmed of leading/trailing silence, and cached in memory
//! and on disk -- as Dograh does.

use super::{storage::Storage, store::Cached};
use crate::agent::workflow::Clip;
use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Deserializer};
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::OnceCell;

/// Dograh's RECORDING_RESPONSE_MODE_INSTRUCTIONS (pipecat_engine_context_composer.py),
/// word for word (typos included): appended to a node's prompt when it mentions
/// "RECORDING_ID:" and the organization has recordings.
pub const RECORDING_INSTRUCTIONS: &str = "RESPONSE MODE INSTRUCTIONS - MANDATORY FORMAT:
Every response you generate MUST begin with excatcly one response mode indicator.
You have two modes for responding:

1. DYNAMIC SPEECH (▸): Generate text that will be converted to speech by TTS.
   Format: ▸ followed by a space and your full spoken response. Nothing else.
   Example: ▸ Hello! How can I help you today?

2. PRE-RECORDED AUDIO (●): Play a pre-recorded audio message.
   Format: ● followed by a space followed by recording_id followed by provided transcript. Nothing else.
   Example: ● rec_greeting_01 [ Provided Transcript ]

RULES:
- Your response MUST start with either ▸ or ● as the very first character.
- For ▸ (dynamic speech): Follow with a space and your response to be generated using TTS engine. Dont mix with ●
- For ● (pre-recorded audio): Follow with a space and recording_id of the audio clip with its transcript. Dont mix with ▸
- Use ● when a pre-recorded message matches the situation well.
- Use ▸ when you need to generate a dynamic, contextual response.
- *NEVER* mix modes in a single response, since we rely on the markers to decide whether to play using TTS or Pre-recorded audio.";

/// What makes a node prompt use recording response mode.
pub const PROMPT_RECORDING_REF: &str = "RECORDING_ID:";

/// One recording's download + conversion is bounded by this. Detached from the
/// call: a load the call can't wait for still fills the cache.
const CLIP_LOAD_TIMEOUT: Duration = Duration::from_secs(30);
/// Caps a downloaded recording file.
const MAX_RECORDING_BYTES: usize = 50 << 20;
/// pipecat's SPEAKING_THRESHOLD, the amplitude Dograh's _trim_silence treats as sound.
const SPEAKING_THRESHOLD: i32 = 20;

/// A recording's primary key as the workflow stores it: Dograh writes a number
/// or a numeric string (and reads it with int()). 0 = none.
pub fn recording_ref<'de, D: Deserializer<'de>>(d: D) -> Result<i64, D::Error> {
    Ok(match Value::deserialize(d)? {
        Value::Number(n) => n.as_f64().unwrap_or(0.0) as i64,
        Value::String(s) if !s.is_empty() => s.parse().map_err(|_| serde::de::Error::custom(format!("recording id {s:?}")))?,
        _ => 0,
    })
}

#[derive(Debug, Clone)]
pub struct RecordingRow {
    pub id: i64,
    pub recording_id: String,
    pub transcript: String,
    pub storage_key: String,
    pub backend: String,
}

/// The call's recordings, ready to play: by primary key (the greeting,
/// transitions and tools pick one from a dropdown) and by recording_id (what an
/// LLM reply names). `has_any` is Dograh's has_active_recordings, whether or not
/// every clip could be loaded.
#[derive(Default, Clone)]
pub struct ClipSet {
    pub by_pk: HashMap<i64, Arc<Clip>>,
    pub by_id: Arc<HashMap<String, Arc<Clip>>>,
    pub has_any: bool,
}

/// Converted recordings by storage key (a new upload gets a new key, so an
/// edited recording is never served stale), in memory and on disk across
/// restarts. One per process; loads are shared by concurrent calls.
/// One recording's load: shared by every call that asks for it.
type Cell = Arc<OnceCell<Arc<Vec<u8>>>>;

#[derive(Clone)]
pub struct ClipCache {
    storage: Storage,
    dir: PathBuf,
    cells: Arc<Mutex<HashMap<String, Cell>>>,
}

impl ClipCache {
    pub fn new(storage: Storage) -> Self {
        Self { storage, dir: std::env::temp_dir().join("vaani-recordings"), cells: Default::default() }
    }

    /// `r`'s converted PCM: from memory, else disk, else downloaded and
    /// converted -- once per storage key however many calls ask at the same time.
    /// The load runs on its own task, so a call that gives up still leaves the
    /// cache filled for the next.
    pub async fn get(&self, r: &RecordingRow) -> Result<Arc<Vec<u8>>> {
        if r.backend != "minio" {
            bail!("stored in {:?}; only minio is supported", r.backend);
        }
        let cell = self.cells.lock().unwrap().entry(r.storage_key.clone()).or_default().clone();
        if let Some(v) = cell.get() {
            return Ok(v.clone());
        }
        let (this, row) = (self.clone(), r.clone());
        tokio::spawn(async move {
            cell.get_or_try_init(|| async {
                match tokio::time::timeout(CLIP_LOAD_TIMEOUT, this.fetch(&row)).await {
                    Ok(Ok(pcm)) => Ok(Arc::new(pcm)),
                    Ok(Err(e)) => Err(e),
                    Err(_) => Err(anyhow!("timed out")),
                }
                .inspect_err(|e| {
                    tracing::warn!(component = "dograh", event = "recording.load_failed", recording_id = %row.recording_id, storage_key = %row.storage_key, error = %format!("{e:#}"), "dograh: loading a recording failed")
                })
            })
            .await
            .cloned()
        })
        .await
        .context("load task")?
    }

    async fn fetch(&self, r: &RecordingRow) -> Result<Vec<u8>> {
        use sha2::{Digest, Sha256};
        let digest = Sha256::digest(r.storage_key.as_bytes());
        let name: String = digest[..16].iter().map(|b| format!("{b:02x}")).collect();
        let cached = self.dir.join(format!("{name}.pcm"));
        if let Ok(pcm) = tokio::fs::read(&cached).await
            && !pcm.is_empty()
        {
            return Ok(pcm);
        }
        let file = self.storage.get(&r.storage_key, MAX_RECORDING_BYTES).await?;
        let ext = std::path::Path::new(&r.storage_key).extension().map(|e| format!(".{}", e.to_string_lossy())).unwrap_or_default();
        let pcm = trim_silence(convert_audio(&file, &ext).await?);
        if tokio::fs::create_dir_all(&self.dir).await.is_ok()
            && let Err(e) = tokio::fs::write(&cached, &pcm).await
        {
            tracing::warn!(component = "dograh", error = %e, "dograh: caching a recording on disk failed");
        }
        Ok(pcm)
    }
}

/// Dograh's convert_audio_file: ffmpeg turns any audio file into raw 16kHz mono
/// s16le. The file goes through a temp file with its own extension, not a pipe,
/// as some containers need a seekable input.
async fn convert_audio(file: &[u8], ext: &str) -> Result<Vec<u8>> {
    let tmp = std::env::temp_dir().join(format!("vaani-rec-{:016x}{ext}", rand::random::<u64>()));
    tokio::fs::write(&tmp, file).await.context("temp file")?;
    let out = tokio::process::Command::new("ffmpeg")
        .args(["-i"])
        .arg(&tmp)
        .args(["-f", "s16le", "-acodec", "pcm_s16le", "-ac", "1", "-ar", "16000", "-loglevel", "error", "pipe:1"])
        .kill_on_drop(true)
        .output()
        .await;
    let _ = tokio::fs::remove_file(&tmp).await;
    let out = out
        .map_err(|e| if e.kind() == std::io::ErrorKind::NotFound { anyhow!("ffmpeg not found on PATH") } else { anyhow!("ffmpeg: {e}") })?;
    if !out.status.success() {
        bail!("ffmpeg: {}: {}", out.status, String::from_utf8_lossy(&out.stderr).trim());
    }
    if out.stdout.is_empty() {
        bail!("ffmpeg produced no audio");
    }
    Ok(out.stdout)
}

/// Dograh's _trim_silence: drops leading and trailing 10ms frames whose loudest
/// sample doesn't exceed `SPEAKING_THRESHOLD`. All-silent audio is returned as-is.
pub fn trim_silence(pcm: Vec<u8>) -> Vec<u8> {
    const FRAME: usize = 16000 / 100 * 2; // 10ms of s16le
    let frames = pcm.len() / FRAME;
    let loud = |i: usize| {
        pcm[i * FRAME..(i + 1) * FRAME].chunks_exact(2).any(|b| i32::from(i16::from_le_bytes([b[0], b[1]])).abs() > SPEAKING_THRESHOLD)
    };
    let Some(first) = (0..frames).find(|&i| loud(i)) else { return pcm };
    let last = (first + 1..frames).rev().find(|&i| loud(i)).unwrap_or(first);
    pcm[first * FRAME..(last + 1) * FRAME].to_vec()
}

impl super::Store {
    /// Organization `org`'s active recordings.
    async fn recordings(&self, org: i64) -> Result<Vec<RecordingRow>> {
        let db = self.db().await?;
        let rows = db
            .query(
                "SELECT id::bigint, recording_id, COALESCE(transcript, ''), storage_key, storage_backend::text
                 FROM workflow_recordings WHERE organization_id = $1::bigint AND is_active",
                &[&org],
            )
            .await
            .context("query recordings")?;
        Ok(rows
            .iter()
            .map(|r| RecordingRow { id: r.get(0), recording_id: r.get(1), transcript: r.get(2), storage_key: r.get(3), backend: r.get(4) })
            .collect())
    }

    /// Loads the organization's recordings this workflow needs: `pks`
    /// (referenced by the greeting, a transition or a tool), and every one if
    /// `all_by_id` (a node prompt names recordings). Clips that can't be loaded
    /// in time are left out, with a warning.
    pub(crate) async fn load_clips(&self, org: Option<i64>, pks: &HashSet<i64>, all_by_id: bool) -> (ClipSet, Vec<String>) {
        let mut set = ClipSet::default();
        let Some(org) = org.filter(|_| !pks.is_empty() || all_by_id) else { return (set, vec![]) };
        let rows = match self.recordings(org).await {
            Ok(r) => r,
            Err(e) => return (set, vec![format!("recordings unavailable: {e:#}")]),
        };
        set.has_any = !rows.is_empty();
        let Some(cache) = &self.clips else {
            let warn = (!pks.is_empty() || (all_by_id && set.has_any))
                .then(|| "recordings unavailable: no MinIO configured (MINIO_ENDPOINT)".to_string());
            return (set, warn.into_iter().collect());
        };

        let (mut warnings, mut found, mut by_id) = (vec![], HashSet::new(), HashMap::new());
        let wanted: Vec<&RecordingRow> = rows.iter().filter(|r| all_by_id || pks.contains(&r.id)).collect();
        let loads = futures_util::future::join_all(wanted.iter().map(|r| cache.get(r))).await;
        for (r, loaded) in wanted.into_iter().zip(loads) {
            found.insert(r.id);
            match loaded {
                Ok(pcm) => {
                    let clip = Arc::new(Clip { id: r.recording_id.clone(), pcm, transcript: r.transcript.clone() });
                    set.by_pk.insert(r.id, clip.clone());
                    by_id.insert(r.recording_id.clone(), clip);
                }
                Err(e) => warnings.push(format!("recording {} ({}) unavailable: {e:#}", r.recording_id, r.id)),
            }
        }
        set.by_id = Arc::new(by_id);
        for pk in pks {
            if !found.contains(pk) {
                warnings.push(format!("recording {pk} not found or not active"));
            }
        }
        (set, warnings)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pcm(samples: &[i16]) -> Vec<u8> {
        samples.iter().flat_map(|s| s.to_le_bytes()).collect()
    }

    #[test]
    fn trim_drops_silent_10ms_frames_at_both_ends() {
        let mut s = vec![0i16; 160 * 2]; // 20ms of silence
        s.extend(vec![500i16; 160]); // 10ms of sound
        s.extend(vec![10i16; 160]); // quiet (under the threshold) in the middle...
        s.extend(vec![500i16; 160]); // ...sound again
        s.extend(vec![0i16; 160 * 3]); // trailing silence
        let out = trim_silence(pcm(&s));
        assert_eq!(out.len(), 160 * 3 * 2, "sound, quiet, sound survive; silence either side goes");
        assert_eq!(trim_silence(pcm(&vec![0; 320])), pcm(&vec![0; 320]), "all-silent audio is returned as-is");
        assert!(trim_silence(vec![]).is_empty());
    }

    #[test]
    fn recording_refs_accept_numbers_and_numeric_strings() {
        #[derive(Deserialize)]
        struct R {
            #[serde(default, deserialize_with = "recording_ref")]
            id: i64,
        }
        let id = |j: &str| serde_json::from_str::<R>(j).unwrap().id;
        assert_eq!((id(r#"{"id": 7}"#), id(r#"{"id": "12"}"#), id(r#"{"id": ""}"#), id(r#"{"id": null}"#), id("{}")), (7, 12, 0, 0, 0));
        assert!(serde_json::from_str::<R>(r#"{"id": "x"}"#).is_err());
    }

    fn row(key: &str) -> RecordingRow {
        RecordingRow { id: 1, recording_id: "rec".into(), transcript: "hi".into(), storage_key: key.into(), backend: "minio".into() }
    }

    #[tokio::test]
    async fn a_cached_clip_is_served_without_touching_minio() {
        // The disk cache is keyed by the storage key's hash: a hit means no download, no ffmpeg.
        let (dir, key) = (std::env::temp_dir().join(format!("vaani-test-{:x}", rand::random::<u64>())), "recordings/x.mp3");
        let cache = ClipCache {
            storage: Storage {
                endpoint: "127.0.0.1:1".into(),
                access_key: String::new(),
                secret_key: String::new(),
                bucket: "b".into(),
                secure: false,
            },
            dir: dir.clone(),
            cells: Default::default(),
        };
        use sha2::{Digest, Sha256};
        let name: String = Sha256::digest(key.as_bytes())[..16].iter().map(|b| format!("{b:02x}")).collect();
        tokio::fs::create_dir_all(&dir).await.unwrap();
        tokio::fs::write(dir.join(format!("{name}.pcm")), [1u8, 2, 3, 4]).await.unwrap();
        assert_eq!(*cache.get(&row(key)).await.unwrap(), vec![1, 2, 3, 4]);
        // Second call: from memory.
        assert_eq!(*cache.get(&row(key)).await.unwrap(), vec![1, 2, 3, 4]);
        // Unreachable MinIO and no cached file: a clear error, not a hang.
        assert!(cache.get(&row("recordings/missing.mp3")).await.is_err());
        let mut other = row("k");
        other.backend = "s3".into();
        assert!(cache.get(&other).await.unwrap_err().to_string().contains("only minio"));
        let _ = tokio::fs::remove_dir_all(dir).await;
    }
}
