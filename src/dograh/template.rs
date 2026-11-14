//! Dograh's `render_template` (api/utils/template_renderer.py): `{{path}}`,
//! `{{path | default}}`, `{{path | fallback:default}}`, plus the built-ins
//! `current_time[_<Zone>]` and `current_weekday[_<Zone>]`.

use jiff::{Timestamp, tz::TimeZone};
use regex::{Captures, Regex};
use serde_json::{Map, Value};
use std::sync::LazyLock;

static VAR: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\{\{\s*([^|\s}]+)(?:\s*\|\s*([^:}]+)(?::([^}]+))?)?\s*\}\}").unwrap());
/// The zone of a `{{current_time_<Zone>}}`/`{{current_weekday_<Zone>}}`, which a
/// bare `{{current_weekday}}`/`{{current_time}}` in the same template inherits.
static BUILTIN_TZ: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\{\{\s*(?:current_time|current_weekday)_([^|\s}]+)").unwrap());

pub fn render(s: &str, vars: &Map<String, Value>, now: Timestamp) -> String {
    if s.is_empty() {
        return String::new();
    }
    let default_tz = BUILTIN_TZ.captures(s).map(|m| m[1].trim().to_string()).unwrap_or_default();
    let out = VAR.replace_all(s, |m: &Captures| {
        let path = m[1].trim();
        if let Some(v) = builtin(path, &default_tz, now) {
            return v;
        }
        let mut value = lookup(vars, path);
        let empty = matches!(&value, None | Some(Value::Null)) || value == Some(&Value::String(String::new()));
        if let Some(filter) = m.get(2).map(|f| f.as_str().trim()).filter(|f| !f.is_empty())
            && empty
        {
            let fallback = if filter == "fallback" {
                match m.get(3).map(|f| f.as_str().trim()).filter(|f| !f.is_empty()) {
                    Some(fb) => fb.to_string(),
                    None => py_title(path),
                }
            } else {
                filter.to_string()
            };
            return fallback;
        }
        value.take().map(format_value).unwrap_or_default()
    });
    out.replace("\\n", "\n")
}

fn builtin(path: &str, default_tz: &str, now: Timestamp) -> Option<String> {
    let (zone, weekday) = if path == "current_time" {
        (default_tz, false)
    } else if let Some(z) = path.strip_prefix("current_time_") {
        (z, false)
    } else if path == "current_weekday" {
        (default_tz, true)
    } else {
        let z = path.strip_prefix("current_weekday_")?;
        (z, true)
    };
    // An unknown zone isn't a built-in (Dograh logs it and falls through to vars).
    let tz = if zone.is_empty() { TimeZone::UTC } else { TimeZone::get(zone).ok()? };
    let t = now.to_zoned(tz);
    Some(if weekday { t.strftime("%A").to_string() } else { t.strftime("%Y-%m-%d %H:%M:%S %Z").to_string() })
}

/// Follows a dotted path ("customer.address.city") into vars.
fn lookup<'a>(vars: &'a Map<String, Value>, path: &str) -> Option<&'a Value> {
    let mut parts = path.split('.');
    let mut cur = vars.get(parts.next()?)?;
    for key in parts {
        cur = cur.as_object()?.get(key)?;
    }
    (!cur.is_null()).then_some(cur)
}

/// Python's `str()` for JSON-decoded values.
fn format_value(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::String(s) => s.clone(),
        Value::Bool(true) => "True".into(),
        Value::Bool(false) => "False".into(),
        other => other.to_string(), // numbers as written; objects/arrays as JSON, as Dograh does
    }
}

/// Python's `str.title()`: upper-case each letter that follows a non-letter.
fn py_title(s: &str) -> String {
    let mut prev_letter = false;
    s.chars()
        .map(|c| {
            let out = if c.is_alphabetic() {
                if prev_letter { c.to_lowercase().next().unwrap_or(c) } else { c.to_uppercase().next().unwrap_or(c) }
            } else {
                c
            };
            prev_letter = c.is_alphabetic();
            out
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn vars(v: Value) -> Map<String, Value> {
        v.as_object().unwrap().clone()
    }
    fn now() -> Timestamp {
        "2026-03-04T20:30:00Z".parse().unwrap() // a Wednesday, 02:00 next day in IST
    }

    #[test]
    fn variables_paths_defaults_and_fallbacks() {
        let v = vars(json!({"name": "Asha", "n": 5, "ok": true, "c": {"city": "Pune"}, "empty": ""}));
        let r = |s| render(s, &v, now());
        assert_eq!(r("Hi {{name}}, {{ n }} {{ok}}"), "Hi Asha, 5 True");
        assert_eq!(r("{{c.city}} {{c.zip}}|"), "Pune |");
        assert_eq!(r("{{missing | ji}}"), "ji");
        assert_eq!(r("{{empty | there}}"), "there");
        assert_eq!(r("{{caller_name | fallback}}"), "Caller_Name");
        assert_eq!(r("{{caller_name | fallback:friend}}"), "friend");
        assert_eq!(r("a\\nb"), "a\nb");
        assert_eq!(r("{{c}}"), r#"{"city":"Pune"}"#);
    }

    #[test]
    fn builtin_time_and_weekday_inherit_the_zone() {
        let v = vars(json!({}));
        let r = |s| render(s, &v, now());
        assert_eq!(r("{{current_time_UTC}}"), "2026-03-04 20:30:00 UTC");
        assert_eq!(r("{{current_weekday_Asia/Kolkata}} {{current_weekday}}"), "Thursday Thursday");
        assert_eq!(r("{{current_time_Asia/Kolkata}}"), "2026-03-05 02:00:00 IST");
        assert_eq!(r("{{current_weekday}}"), "Wednesday");
        assert_eq!(r("{{current_time_Nowhere/Land}}"), ""); // unknown zone falls through to (missing) vars
    }
}
