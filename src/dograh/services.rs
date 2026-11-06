//! A call's LLM, STT and TTS as configured in Dograh (run_pipeline.py +
//! configuration/resolve.py): the workflow owner's `user_configurations`, with
//! the workflow's `model_overrides` merged on top -- per service, a different
//! provider replaces the section, the same provider only changes given fields.
//! Anything Vaani can't run is an error: STT/TTS must be Sarvam, the LLM an
//! OpenAI-compatible provider.

use anyhow::{Result, bail};
use serde_json::{Map, Value};

#[derive(Debug, Clone, Default)]
pub struct LlmConfig {
    pub base_url: String,
    pub api_key: String,
    pub model: String,
}
#[derive(Debug, Clone, Default)]
pub struct SttConf {
    pub api_key: String,
    pub model: String,
    pub language: String,
}
#[derive(Debug, Clone, Default)]
pub struct TtsConf {
    pub api_key: String,
    pub model: String,
    pub voice: String,
    pub language: String,
}
#[derive(Debug, Clone, Default)]
pub struct Services {
    pub llm: LlmConfig,
    pub stt: SttConf,
    pub tts: TtsConf,
}

/// OpenAI-compatible endpoints of providers whose Dograh config carries no
/// base_url; the others (bifrost, openrouter, sarvam, speaches) store theirs and
/// `default_base_url` is Dograh's field default, used only when it's missing.
fn fixed_base_url(provider: &str) -> Option<&'static str> {
    Some(match provider {
        "openai" => "https://api.openai.com/v1",
        "groq" => "https://api.groq.com/openai/v1",
        // Dograh uses pipecat's native Google client; Vaani uses Google's
        // OpenAI-compatible endpoint with the same key.
        "google" => "https://generativelanguage.googleapis.com/v1beta/openai",
        _ => return None,
    })
}
fn default_base_url(provider: &str) -> Option<&'static str> {
    Some(match provider {
        "bifrost" => "http://192.168.26.130:8080/v1",
        "openrouter" => "https://openrouter.ai/api/v1",
        "sarvam" => "https://api.sarvam.ai/v1",
        "speaches" => "http://localhost:11434/v1",
        _ => return None,
    })
}

fn s(sec: &Map<String, Value>, key: &str) -> String {
    sec.get(key).and_then(Value::as_str).unwrap_or_default().to_string()
}
fn s_or(sec: &Map<String, Value>, key: &str, def: &str) -> String {
    Some(s(sec, key)).filter(|v| !v.is_empty()).unwrap_or_else(|| def.to_string())
}
/// The section's api_key; a list picks one at random per call (as Dograh does
/// to spread load across keys).
fn api_key(sec: &Map<String, Value>) -> String {
    match sec.get("api_key") {
        Some(Value::String(k)) => k.clone(),
        Some(Value::Array(a)) if !a.is_empty() => a[rand::random_range(0..a.len())].as_str().unwrap_or_default().to_string(),
        _ => String::new(),
    }
}

pub fn resolve(user_cfg: &str, workflow_cfg: &str) -> Result<(Services, Vec<String>)> {
    if user_cfg.is_empty() {
        bail!("the workflow's owner has no model configuration in Dograh");
    }
    let user: Map<String, Value> = serde_json::from_str(user_cfg).map_err(|e| anyhow::anyhow!("unreadable user configuration: {e}"))?;
    let overrides: Map<String, Value> = if workflow_cfg.is_empty() {
        Map::new()
    } else {
        let v: Value = serde_json::from_str(workflow_cfg).map_err(|e| anyhow::anyhow!("unreadable workflow_configurations: {e}"))?;
        v.get("model_overrides").and_then(Value::as_object).cloned().unwrap_or_default()
    };

    let realtime = |m: &Map<String, Value>| m.get("is_realtime").and_then(Value::as_bool).unwrap_or(false);
    if realtime(&user) || realtime(&overrides) {
        bail!("realtime (speech-to-speech) mode isn't supported");
    }

    let effective = |name: &str| -> Result<Map<String, Value>> {
        let base = user.get(name).and_then(Value::as_object).cloned();
        let over = match overrides.get(name) {
            Some(v) => Some(v.as_object().cloned().ok_or_else(|| anyhow::anyhow!("unreadable {name} override"))?),
            None => None,
        };
        let merged = match (base, over) {
            (base, None) => base,
            (None, Some(o)) => Some(o),
            (Some(b), Some(o)) if o.get("provider").is_some_and(|p| !p.is_null()) && o.get("provider") != b.get("provider") => Some(o),
            (Some(mut b), Some(o)) => {
                b.extend(o);
                Some(b)
            }
        };
        merged.ok_or_else(|| anyhow::anyhow!("no {name} configured in Dograh"))
    };

    let mut warnings = vec![];

    let l = effective("llm")?;
    let provider = s(&l, "provider");
    let base_url = fixed_base_url(&provider)
        .map(String::from)
        .or_else(|| Some(s(&l, "base_url")).filter(|u| !u.is_empty()))
        .or_else(|| default_base_url(&provider).map(String::from));
    let Some(base_url) = base_url else {
        bail!("llm provider {provider:?} isn't supported (Vaani needs an OpenAI-compatible one: openai, google, groq, openrouter, sarvam, bifrost, speaches)");
    };
    let llm = LlmConfig { base_url, api_key: api_key(&l), model: s(&l, "model") };
    if llm.model.is_empty() {
        bail!("llm has no model configured");
    }

    let st = effective("stt")?;
    if s(&st, "provider") != "sarvam" {
        bail!("stt provider {:?} isn't supported (Vaani supports sarvam)", s(&st, "provider"));
    }
    // Dograh's Sarvam defaults, for fields a stored config lacks.
    let stt = SttConf { api_key: api_key(&st), model: s_or(&st, "model", "saaras:v3"), language: s_or(&st, "language", "hi-IN") };
    if st.get("segmented").and_then(Value::as_bool).unwrap_or(false) {
        warnings.push("sarvam stt 'segmented' mode isn't supported; streaming instead".to_string());
    }

    let tt = effective("tts")?;
    if s(&tt, "provider") != "sarvam" {
        bail!("tts provider {:?} isn't supported (Vaani supports sarvam)", s(&tt, "provider"));
    }
    let tts = TtsConf {
        api_key: api_key(&tt),
        model: s_or(&tt, "model", "bulbul:v2"),
        voice: s_or(&tt, "voice", "suhani"),
        language: s_or(&tt, "language", "hi-IN"),
    };
    if stt.api_key.is_empty() || tts.api_key.is_empty() {
        bail!("sarvam api_key missing in Dograh's configuration");
    }
    Ok((Services { llm, stt, tts }, warnings))
}

#[cfg(test)]
mod tests {
    use super::*;

    const USER: &str = r#"{
        "llm": {"provider": "openai", "api_key": "sk-1", "model": "gpt-4.1"},
        "stt": {"provider": "sarvam", "api_key": "s-key", "model": "saaras:v4"},
        "tts": {"provider": "sarvam", "api_key": ["t-key"], "model": "bulbul:v3", "voice": "shubh"}
    }"#;

    #[test]
    fn user_config_with_defaults() {
        let (s, w) = resolve(USER, "").unwrap();
        assert!(w.is_empty());
        assert_eq!((s.llm.base_url.as_str(), s.llm.model.as_str()), ("https://api.openai.com/v1", "gpt-4.1"));
        assert_eq!((s.stt.model.as_str(), s.stt.language.as_str()), ("saaras:v4", "hi-IN"));
        assert_eq!((s.tts.api_key.as_str(), s.tts.voice.as_str(), s.tts.language.as_str()), ("t-key", "shubh", "hi-IN"));
    }

    #[test]
    fn overrides_merge_same_provider_and_replace_a_different_one() {
        let wf = r#"{"model_overrides": {
            "llm": {"model": "gpt-4.1-mini"},
            "tts": {"provider": "sarvam", "voice": "anushka"},
            "stt": {"provider": "deepgram", "api_key": "x"}}}"#;
        let e = resolve(USER, wf).unwrap_err().to_string();
        assert!(e.contains("stt provider \"deepgram\""), "{e}");

        let wf = r#"{"model_overrides": {"llm": {"model": "gpt-4.1-mini"}, "tts": {"voice": "anushka"}}}"#;
        let (s, _) = resolve(USER, wf).unwrap();
        assert_eq!(s.llm.model, "gpt-4.1-mini");
        assert_eq!(s.llm.api_key, "sk-1", "same provider: only the given fields change");
        assert_eq!(s.tts.voice, "anushka");

        let wf = r#"{"model_overrides": {"llm": {"provider": "openrouter", "model": "m", "api_key": "or"}}}"#;
        let (s, _) = resolve(USER, wf).unwrap();
        assert_eq!(s.llm.base_url, "https://openrouter.ai/api/v1", "a different provider replaces the section");
    }

    #[test]
    fn unsupported_setups_are_errors() {
        assert!(resolve("", "").is_err());
        assert!(resolve(r#"{"is_realtime": true}"#, "").unwrap_err().to_string().contains("realtime"));
        let no_key = USER.replace(r#""api_key": "s-key", "#, "");
        assert!(resolve(&no_key, "").unwrap_err().to_string().contains("api_key missing"));
    }
}
