//! Dograh's recording response mode (pipecat_engine_context_composer.py): a
//! reply starting with `●` plays a recording, one starting with `▸` is spoken.

use regex::Regex;
use std::sync::LazyLock;

pub const RECORDING_MARKER: &str = "●";
pub const TTS_MARKER: &str = "▸";

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Detecting,
    Tts,
    Recording,
}

/// Dograh's RecordingRouterProcessor for one LLM reply (one stream round): it
/// holds the reply back until its marker arrives, then either passes the text
/// after `▸` on to TTS or, after `●`, picks out the recording_id that follows --
/// as soon as a space ends it, so playback starts without waiting for the
/// transcript the LLM writes after it. Modes never mix: whatever follows a second
/// marker is dropped. A reply with no marker at all is spoken as-is.
pub struct ClipRouter {
    mode: Mode,
    /// Detecting: the reply so far; recording: the text after `●`.
    buf: String,
    /// Tts: a `●` came after; recording: the id was already picked.
    stopped: bool,
}

impl Default for ClipRouter {
    fn default() -> Self {
        Self { mode: Mode::Detecting, buf: String::new(), stopped: false }
    }
}

const WS: [char; 3] = [' ', '\t', '\n'];

impl ClipRouter {
    /// One LLM token -> the text to send to TTS, and the recording to play (at
    /// most once per reply).
    pub fn feed(&mut self, tok: &str) -> (String, Option<String>) {
        match self.mode {
            Mode::Tts => {
                if self.stopped {
                    return (String::new(), None);
                }
                if let Some(i) = tok.find(RECORDING_MARKER) {
                    self.stopped = true;
                    return (tok[..i].to_string(), None);
                }
                (tok.to_string(), None)
            }
            Mode::Recording => {
                if self.stopped {
                    return (String::new(), None);
                }
                let tok = tok.find(TTS_MARKER).map_or(tok, |i| &tok[..i]);
                self.buf.push_str(tok);
                // The id is complete once whitespace follows it.
                let rest = self.buf.trim_start_matches(WS);
                if rest.contains(WS) {
                    self.stopped = true;
                    return (String::new(), rest.split_whitespace().next().map(String::from));
                }
                (String::new(), None)
            }
            Mode::Detecting => {
                self.buf.push_str(tok);
                // Whichever marker comes first decides (Dograh checks `●` first
                // even when `▸` precedes it in the same chunk).
                let (rec, say) = (self.buf.find(RECORDING_MARKER), self.buf.find(TTS_MARKER));
                match (rec, say) {
                    (Some(r), s) if s.is_none_or(|s| r < s) => {
                        let rest = self.buf[r + RECORDING_MARKER.len()..].to_string();
                        (self.mode, self.buf) = (Mode::Recording, String::new());
                        self.feed(&rest)
                    }
                    (_, Some(s)) => {
                        let rest =
                            self.buf[s + TTS_MARKER.len()..].strip_prefix(' ').unwrap_or(&self.buf[s + TTS_MARKER.len()..]).to_string();
                        (self.mode, self.buf) = (Mode::Tts, String::new());
                        self.feed(&rest)
                    }
                    _ => (String::new(), None),
                }
            }
        }
    }

    /// Turns a `●` reply whose recording isn't available into a spoken one: what
    /// the LLM had written after the id when it was picked is returned for TTS,
    /// and the rest of the reply follows as ordinary speech. The LLM writes the
    /// recording's transcript there, so the caller hears what the recording would
    /// have said instead of silence (Dograh plays nothing).
    pub fn speak_rest(&mut self) -> String {
        let mut rest = self.buf.trim_start_matches(WS);
        if let Some(id) = rest.split_whitespace().next() {
            rest = rest[id.len()..].trim_start_matches(WS);
        }
        // Modes never mix: a second `●` ends the speech (and anything after it
        // that's still to come).
        let (rest, stopped) = match rest.find(RECORDING_MARKER) {
            Some(i) => (&rest[..i], true),
            None => (rest, false),
        };
        let rest = rest.to_string();
        (self.mode, self.buf, self.stopped) = (Mode::Tts, String::new(), stopped);
        rest
    }

    /// Ends the reply: a recording whose id was never followed by a space plays
    /// now; a reply that never had a marker is spoken as-is.
    pub fn flush(&mut self) -> (String, Option<String>) {
        match self.mode {
            Mode::Recording if !self.stopped => {
                if let Some(id) = self.buf.split_whitespace().next() {
                    self.stopped = true;
                    return (String::new(), Some(id.to_string()));
                }
            }
            Mode::Detecting => return (std::mem::take(&mut self.buf), None),
            _ => {}
        }
        (String::new(), None)
    }
}

static REF: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"●\s*(\S+)\s*").unwrap());

/// `text` without the "● <recording_id> " references of recorded replies, and
/// those ids in order. The LLM's own history keeps them (it needs the format it's
/// asked to follow); the call's log and transcript show only what was said.
pub fn strip_recording_refs(text: &str) -> (String, Vec<String>) {
    let ids = REF.captures_iter(text).map(|c| c[1].to_string()).collect();
    (REF.replace_all(text, "").trim().to_string(), ids)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(tokens: &[&str]) -> (String, Vec<String>) {
        let (mut r, mut speak, mut ids) = (ClipRouter::default(), String::new(), vec![]);
        for t in tokens {
            let (s, id) = r.feed(t);
            speak += &s;
            ids.extend(id);
        }
        let (s, id) = r.flush();
        ids.extend(id);
        (speak + &s, ids)
    }

    #[test]
    fn router_follows_dograhs_markers() {
        let cases: &[(&[&str], &str, &[&str])] = &[
            (&["▸ Hello", " there."], "Hello there.", &[]),
            (&["●", " offer_01", " Our offer is…"], "", &["offer_01"]),
            (&["● offer_01"], "", &["offer_01"]),
            (&["Hello ", "there."], "Hello there.", &[]),
            (&["▸ Yes. ● rec_01 x"], "Yes. ", &[]),
            (&["\n", "▸", "Hi"], "Hi", &[]),
            (&["● rec_01", "▸ more"], "", &["rec_01"]),
        ];
        for (tokens, speak, ids) in cases {
            assert_eq!(run(tokens), (speak.to_string(), ids.iter().map(|s| s.to_string()).collect::<Vec<_>>()), "{tokens:?}");
        }
    }

    #[test]
    fn speak_rest_turns_a_missing_recording_into_speech() {
        let cases: &[(&[&str], &str)] = &[
            (&["● miss", "ing_01", " Hi", " there."], "Hi there."),
            (&["● missing_01 Hi there."], "Hi there."),
            (&["● missing_01 "], ""),
            (&["● missing_01 Hi ● other"], "Hi "),
        ];
        for (tokens, want) in cases {
            let (mut r, mut spoke) = (ClipRouter::default(), String::new());
            for t in *tokens {
                let (s, id) = r.feed(t);
                spoke += &s;
                if let Some(id) = id {
                    assert_eq!(id, "missing_01");
                    spoke += &r.speak_rest();
                }
            }
            let (s, id) = r.flush();
            assert!(id.is_none());
            assert_eq!(spoke + &s, *want, "{tokens:?}");
        }
    }

    #[test]
    fn strips_recording_references() {
        let cases: &[(&str, &str, &[&str])] = &[
            ("ठीक है, noted.", "ठीक है, noted.", &[]),
            ("● vbdplx3x ठीक है, वर्तमान राज्य सरकार?", "ठीक है, वर्तमान राज्य सरकार?", &["vbdplx3x"]),
            ("चलिए बात करते हैं. ● x62otbiy हम राय ले रहे हैं.", "चलिए बात करते हैं. हम राय ले रहे हैं.", &["x62otbiy"]),
            ("● a1 One. ● b2 Two.", "One. Two.", &["a1", "b2"]),
            ("● a1 ", "", &["a1"]),
        ];
        for (input, want, ids) in cases {
            let (got, got_ids) = strip_recording_refs(input);
            assert_eq!((got.as_str(), got_ids), (*want, ids.iter().map(|s| s.to_string()).collect::<Vec<_>>()), "{input:?}");
        }
    }
}
