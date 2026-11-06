//! Acknowledgement words a caller says while listening ("जी", "हाँ", "ok",
//! "hmm"): said over a paused reply they mean "go on", not "stop".
//! Deliberately absent: anything that refuses, questions or stops (नहीं, ना,
//! no, wait, क्या, रुको...), so one word outside this list is enough to interrupt.

use crate::ai::chunker::is_letter;

pub const WORDS: &[&str] = &[
    // English, and Latin-script Hindi/Hinglish as Sarvam writes it.
    "yes", "yeah", "yea", "yep", "yup", "ok", "okay", "okk", "k", "alright", "right", "sure",
    "hmm", "hm", "mm", "mhm", "mmhmm", "uh", "huh", "uhhuh", "aha", "ah", "oh",
    "haan", "han", "haa", "ha", "haanji", "hanji", "ji", "jee",
    "achha", "acha", "accha", "achchha", "theek", "thik", "theekhai", "hai", "sahi", "bilkul",
    // Hindi / Urdu (Devanagari).
    "जी", "हाँ", "हां", "हा", "हाँजी", "हांजी", "जीहाँ", "हम्म", "हम्म्म", "हम", "हं", "हूँ", "हुँ", "ऊँ", "उँ",
    "अच्छा", "अच्छाजी", "ठीक", "है", "सही", "बिल्कुल", "बिलकुल", "ओके", "ओह", "आह",
    // Urdu (Arabic script): جی ہاں اچھا ٹھیک ہے
    "جی", "ہاں", "اچھا", "ٹھیک", "ہے",
    // Marathi.
    "हो", "होय", "बरं", "बर", "आहे",
    // Gujarati.
    "હા", "હાં", "જી", "હમ્મ", "બરાબર", "સારું", "ઓકે", "અચ્છા", "ઠીક", "છે",
    // Bengali.
    "হ্যাঁ", "হ্যা", "হুম", "আচ্ছা", "ঠিক", "আছে", "ওকে", "জি",
    // Punjabi (Gurmukhi).
    "ਹਾਂ", "ਹਾਂਜੀ", "ਜੀ", "ਹਮ", "ਅੱਛਾ", "ਠੀਕ", "ਹੈ", "ਓਕੇ",
    // Odia.
    "ହଁ", "ହଉ", "ଆଚ୍ଛା", "ଠିକ୍", "ଠିକ", "ଅଛି", "ଓକେ",
    // Tamil.
    "ஆமா", "ஆமாம்", "சரி", "ம்ம்", "ம்", "ஹ்ம்", "ஓகே", "ஆ",
    // Telugu.
    "అవును", "సరే", "అలాగే", "హా", "ఊ", "హ్మ్", "ఓకే",
    // Kannada.
    "ಹೌದು", "ಸರಿ", "ಹಾ", "ಹ್ಮ್", "ಓಕೆ", "ಆಯ್ತು",
    // Malayalam.
    "അതെ", "ശരി", "ഉം", "ഹാ", "ഓക്കെ", "ഓകെ",
];

/// True if `text` is nothing but acknowledgement words, in any mix of the
/// languages above. Punctuation (the danda included) separates words, Latin is
/// case-insensitive, and a letter held long ("hmmmm") counts as its short form.
pub fn backchannel(text: &str) -> bool {
    let lower = text.to_lowercase();
    let mut any = false;
    for w in lower.split(|c: char| c.is_whitespace() || !(is_letter(c) || crate::ai::chunker::is_mark(c) || crate::ai::chunker::is_digit(c))) {
        if w.is_empty() {
            continue;
        }
        any = true;
        if !WORDS.contains(&squeeze_repeats(w).as_str()) {
            return false;
        }
    }
    any
}

/// Cuts any run of one letter to at most two ("hmmmm" -> "hmm").
fn squeeze_repeats(w: &str) -> String {
    let (mut out, mut prev, mut run) = (String::with_capacity(w.len()), None, 0);
    for c in w.chars() {
        if Some(c) == prev {
            run += 1;
        } else {
            (prev, run) = (Some(c), 1);
        }
        if run <= 2 {
            out.push(c);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acknowledgements_in_many_scripts_resume_but_real_words_interrupt() {
        for ok in ["जी", "हाँ।", "ठीक है", "Haan ji", "OK!", "hmmmm", "okkkk", "अच्छा, ठीक है।", "ஆமாம்", "جی"] {
            assert!(backchannel(ok), "{ok:?} should be an acknowledgement");
        }
        for no in ["जी नहीं", "हाँ लेकिन मुझे रुकिए", "wait", "no", "क्या", "", "...", "ok but why"] {
            assert!(!backchannel(no), "{no:?} should interrupt");
        }
    }
}
