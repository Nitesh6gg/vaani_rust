//! Dograh's cost model (api/services/pricing/{llm,tts,stt}.py and
//! cost_calculator.py): a call's usage priced into a cost breakdown, so Dograh's
//! run pages and its organization's usage cycle show what the call cost. Dograh's
//! own cost job never runs for calls written straight to its database, so Vaani
//! runs it.

use serde_json::Value;

/// Dograh's conversion rate for Sarvam's INR prices.
const INR_PER_USD: f64 = 96.5;

/// Dograh's TokenPricingModel: USD per token, cached input billed at
/// `(1 - cache_read_discount)` of the input price.
#[derive(Clone, Copy)]
pub struct TokenPrice {
    prompt: f64,
    completion: f64,
    cache_read_discount: f64,
}

const fn per_m(prompt: f64, completion: f64) -> TokenPrice {
    TokenPrice { prompt: prompt / 1e6, completion: completion / 1e6, cache_read_discount: 0.5 }
}

const fn per_k_tokens(prompt: f64, completion: f64) -> TokenPrice {
    TokenPrice { prompt: prompt / 1000.0, completion: completion / 1000.0, cache_read_discount: 0.5 }
}

/// A Sarvam rate card entry, INR per 1M tokens.
const fn sarvam_inr(input: f64, cached: f64, output: f64) -> TokenPrice {
    TokenPrice { prompt: input / INR_PER_USD / 1e6, completion: output / INR_PER_USD / 1e6, cache_read_discount: 1.0 - cached / input }
}

type Table<P> = &'static [(&'static str, &'static [(&'static str, P)])];

/// Dograh's LLM_PRICING, except Sarvam's entries, which follow Sarvam's own rate
/// card (Oct 2026: 105B and 105B Conversations Rs15/5/60 per 1M input/cached/output,
/// Gemma 4 31B Rs35/10/90, GLM 5.3 Rs126/20/390).
const LLM: Table<TokenPrice> = &[
    (
        "openai",
        &[
            ("gpt-3.5-turbo", per_k_tokens(0.0015, 0.002)),
            ("gpt-4", per_k_tokens(0.03, 0.06)),
            ("gpt-4.1", per_m(2.00, 8.00)),
            ("gpt-4.1-mini", per_m(0.40, 1.60)),
            ("gpt-4.1-nano", per_m(0.10, 0.40)),
            ("gpt-4.5-preview", per_m(75.00, 150.00)),
            ("gpt-4o", per_m(2.50, 10.00)),
            ("gpt-4o-audio-preview", per_m(2.50, 10.00)),
            ("gpt-4o-realtime-preview", per_m(5.00, 20.00)),
            ("gpt-4o-mini", per_m(0.15, 0.60)),
            ("gpt-4o-mini-audio-preview", per_m(0.15, 0.60)),
            ("gpt-4o-mini-realtime-preview", per_m(0.60, 2.40)),
            ("gpt-4o-search-preview", per_m(2.50, 10.00)),
            ("gpt-4o-mini-search-preview", per_m(0.15, 0.60)),
            ("o1", per_m(15.00, 60.00)),
            ("o1-pro", per_m(150.00, 600.00)),
            ("o1-mini", per_m(1.10, 4.40)),
            ("o3", per_m(10.00, 40.00)),
            ("o3-mini", per_m(1.10, 4.40)),
            ("o4-mini", per_m(1.10, 4.40)),
            ("computer-use-preview", per_m(3.00, 12.00)),
            ("gpt-image-1", per_m(5.00, 0.0)),
            ("codex-mini-latest", per_m(1.50, 6.00)),
            ("gpt-4o-transcribe", per_m(2.50, 10.00)),
            ("gpt-4o-mini-transcribe", per_m(1.25, 5.00)),
            ("gpt-4o-mini-tts", per_m(0.60, 0.0)),
        ],
    ),
    (
        "google",
        &[
            ("gemini-3.5-flash", per_m(1.50, 9.00)),
            ("gemini-3.1-pro", per_m(2.00, 12.00)),
            ("gemini-3.1-flash-lite", per_m(0.25, 1.50)),
            ("gemini-2.5-flash", per_m(0.30, 2.50)),
            ("gemini-2.5-flash-lite", per_m(0.10, 0.40)),
            ("default", per_m(0.25, 1.50)),
        ],
    ),
    (
        "groq",
        &[
            ("llama-3.3-70b-versatile", per_m(0.59, 0.79)),
            ("llama-3.1-8b-instant", per_m(0.05, 0.08)),
            ("openai/gpt-oss-120b", per_m(0.15, 0.60)),
            ("openai/gpt-oss-20b", per_m(0.075, 0.30)),
            ("deepseek-r1-distill-llama-70b", per_m(0.59, 0.79)),
            ("default", per_m(0.59, 0.79)),
        ],
    ),
    (
        "sarvam",
        &[
            ("sarvam-105b", sarvam_inr(15.0, 5.0, 60.0)),
            ("sarvam-105b-conversations", sarvam_inr(15.0, 5.0, 60.0)),
            ("gemma4", sarvam_inr(35.0, 10.0, 90.0)),
            ("glm5.3", sarvam_inr(126.0, 20.0, 390.0)),
            ("default", sarvam_inr(15.0, 5.0, 60.0)),
        ],
    ),
    ("azure", &[("gpt-4.1-mini", per_m(0.44, 8.80))]),
];

/// Dograh's TTS_PRICING: USD per character. Sarvam's rate card (Oct 2026): Bulbul
/// v3 Rs2.7 and v4 Flash Rs1.7 per 1K characters, whichever API is used. Dograh's
/// table still has v3 at Rs3 per 1K and no v4 Flash, which would then be charged
/// the v3 rate.
const TTS: Table<f64> = &[
    ("openai", &[("gpt-4o-mini-tts", 0.6 / 1e6), ("default", 0.6 / 1e6)]),
    (
        "sarvam",
        &[
            ("bulbul:v3", 2.7 / 1000.0 / INR_PER_USD),
            ("bulbul:v4-flash", 1.7 / 1000.0 / INR_PER_USD),
            ("bulbul:v2", 15.0 / INR_PER_USD / 10_000.0),
            ("default", 2.7 / 1000.0 / INR_PER_USD),
        ],
    ),
    ("deepgram", &[("aura-2", 0.030 / 1000.0), ("aura-1", 0.015 / 1000.0), ("default", 0.030 / 1000.0)]),
    ("elevenlabs", &[("default", 0.0256 / 1000.0)]),
    ("default", &[("default", 0.030 / 1000.0)]),
];

/// Dograh's STT_PRICING: USD per second.
const STT: Table<f64> = &[
    ("deepgram", &[("nova-3-general", 0.0048 / 60.0), ("nova-2", 0.0058 / 60.0), ("default", 0.0048 / 60.0)]),
    (
        "sarvam",
        &[
            ("saaras:v3", 30.0 / INR_PER_USD / 3600.0),
            ("saaras:v4", 30.0 / INR_PER_USD / 3600.0),
            ("saarika:v2.5", 30.0 / INR_PER_USD / 3600.0),
            ("saaras:v2.5", 30.0 / INR_PER_USD / 3600.0),
            ("default", 30.0 / INR_PER_USD / 3600.0),
        ],
    ),
    ("openai", &[("gpt-4o-transcribe", 0.015 / 60.0), ("default", 0.015 / 60.0)]),
    ("default", &[("default", 0.0077 / 60.0)]),
];

/// cost_calculator.py's _infer_provider_from_model; an unmatched model falls
/// back to Dograh's first provider per service.
fn infer_provider(model: &str, service: &str) -> &'static str {
    let m = model.to_lowercase();
    let has = |words: &[&str]| words.iter().any(|w| m.contains(w));
    if m.is_empty() {
        "unknown"
    } else if has(&["gemini"]) {
        "google"
    } else if has(&["saaras", "saarika", "bulbul", "sarvam"]) {
        "sarvam"
    } else if has(&["groq", "gpt-oss", "llama"]) {
        "groq"
    } else if has(&["gpt", "whisper", "openai"]) {
        "openai"
    } else if has(&["eleven"]) {
        "elevenlabs"
    } else if has(&["deepgram", "nova", "phonecall", "general"]) {
        "deepgram"
    } else {
        match service {
            "stt" => "deepgram",
            _ => "openai",
        }
    }
}

/// cost_calculator.py's get_pricing_model: the provider's model, else its
/// "default", else the "default" provider's. One addition: a model not found as
/// written is retried without a gateway prefix ("sarvam/sarvam-105b-conversations"
/// -> "sarvam-105b-conversations"). Dograh misses those and falls back to the
/// provider default, which for Sarvam's conversation model was the wrong rate.
fn lookup<P: Copy>(table: Table<P>, provider: &str, model: &str) -> Option<P> {
    let get = |prov: &str, mo: &str| table.iter().find(|(p, _)| *p == prov)?.1.iter().find(|(m, _)| *m == mo).map(|(_, v)| *v);
    get(provider, model)
        .or_else(|| model.split_once('/').and_then(|(_, rest)| get(provider, rest)))
        .or_else(|| get(provider, "default"))
        .or_else(|| get("default", model))
        .or_else(|| get("default", "default"))
}

/// A usage key's model part (cost_calculator.py's _parse_key).
fn model_of(key: &str) -> &str {
    key.split_once("|||").map_or("unknown", |(_, m)| m)
}

/// cost_calculator.py's calculate_total_cost result.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Cost {
    pub llm: f64,
    pub tts: f64,
    pub stt: f64,
    pub total: f64,
}

/// Prices a run's `usage_info` as Dograh's calculate_total_cost does.
pub fn calculate_cost(usage: &Value) -> Cost {
    let mut c = Cost::default();
    let n = |v: &Value, k: &str| v.get(k).and_then(Value::as_f64).unwrap_or(0.0);
    for (key, t) in usage["llm"].as_object().into_iter().flatten() {
        let model = model_of(key);
        if let Some(p) = lookup(LLM, infer_provider(model, "llm"), model) {
            let cost = n(t, "prompt_tokens") * p.prompt + n(t, "completion_tokens") * p.completion
                - n(t, "cache_read_input_tokens") * p.prompt * p.cache_read_discount;
            c.llm += cost.max(0.0);
        }
    }
    for (key, chars) in usage["tts"].as_object().into_iter().flatten() {
        let model = model_of(key);
        if let Some(p) = lookup(TTS, infer_provider(model, "tts"), model) {
            c.tts += chars.as_f64().unwrap_or(0.0) * p;
        }
    }
    for (key, secs) in usage["stt"].as_object().into_iter().flatten() {
        let model = model_of(key);
        if let Some(p) = lookup(STT, infer_provider(model, "stt"), model) {
            c.stt += secs.as_f64().unwrap_or(0.0) * p;
        }
    }
    c.total = c.llm + c.tts + c.stt;
    c
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn close(a: f64, b: f64) -> bool {
        (a - b).abs() < 1e-12
    }

    #[test]
    fn llm_tokens_are_priced_per_model_with_a_cache_discount() {
        let u = json!({"llm": {"LLM|||gpt-4.1": {"prompt_tokens": 1_000_000, "completion_tokens": 100_000, "cache_read_input_tokens": 400_000}}});
        // 1M * $2 + 100k * $8 - 400k * $2 * 0.5 = 2 + 0.8 - 0.4
        assert!(close(calculate_cost(&u).llm, 2.4), "{:?}", calculate_cost(&u));
    }

    #[test]
    fn sarvam_models_use_the_inr_rate_card_even_behind_a_gateway_prefix() {
        let u = json!({"llm": {"LLM|||sarvam/sarvam-105b-conversations": {"prompt_tokens": 1_000_000, "completion_tokens": 1_000_000, "cache_read_input_tokens": 1_000_000}}});
        // input Rs15 + output Rs60 - cached 1M * Rs15 * (1 - 5/15) = Rs65 per...: (15 + 60 - 10) / 96.5
        assert!(close(calculate_cost(&u).llm, 65.0 / 96.5), "{:?}", calculate_cost(&u));
        assert_eq!(infer_provider("sarvam/x", "llm"), "sarvam");
    }

    #[test]
    fn tts_and_stt_follow_the_sarvam_rate_card() {
        let u = json!({"tts": {"SarvamTTSService|||bulbul:v3": 1000}, "stt": {"SarvamSTTService|||saaras:v4": 3600}});
        let c = calculate_cost(&u);
        assert!(close(c.tts, 2.7 / 96.5) && close(c.stt, 30.0 / 96.5) && close(c.total, 32.7 / 96.5), "{c:?}");
    }

    #[test]
    fn unknown_models_fall_back_to_the_providers_default_then_to_nothing() {
        assert!(close(calculate_cost(&json!({"llm": {"LLM|||gemini-9": {"prompt_tokens": 1_000_000}}})).llm, 0.25));
        // an OpenAI model that isn't listed has no "default": priced at zero, not guessed.
        assert_eq!(calculate_cost(&json!({"llm": {"LLM|||gpt-99": {"prompt_tokens": 1_000_000}}})).llm, 0.0);
        assert_eq!(calculate_cost(&json!({})), Cost::default());
        assert_eq!(infer_provider("", "llm"), "unknown");
        assert_eq!(infer_provider("nova-3", "stt"), "deepgram");
        assert_eq!(model_of("no-separator"), "unknown");
    }
}
