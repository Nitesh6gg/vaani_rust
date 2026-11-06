//! Buffers streamed LLM tokens and releases chunks for TTS at a sentence
//! delimiter or after `RUNE_THRESHOLD` characters, whichever comes first, so
//! playback can start before the reply is complete. One instance per turn.

use unicode_properties::{GeneralCategory, GeneralCategoryGroup, UnicodeGeneralCategory};

/// Counted in chars, not bytes: 80 bytes is ~27 Devanagari characters.
const RUNE_THRESHOLD: usize = 80;
/// A chunk with no word break at all is cut here, never before a combining
/// mark or right after a virama (that would split a written syllable).
const HARD_LIMIT: usize = 2 * RUNE_THRESHOLD;
const VIRAMA: char = '्';

fn is_delimiter(c: char) -> bool {
    matches!(c, '.' | '!' | '?' | '\n' | '।' | '॥')
}
pub fn is_letter(c: char) -> bool {
    c.general_category_group() == GeneralCategoryGroup::Letter
}
pub fn is_mark(c: char) -> bool {
    c.general_category_group() == GeneralCategoryGroup::Mark
}
pub fn is_digit(c: char) -> bool {
    c.general_category() == GeneralCategory::DecimalNumber
}
pub fn is_content(c: char) -> bool {
    is_letter(c) || is_digit(c)
}
pub fn has_letter_or_digit(s: &str) -> bool {
    s.chars().any(is_content)
}

#[derive(Default)]
pub struct SentenceChunker {
    buf: String,
}

impl SentenceChunker {
    /// Appends `token`; returns the chunks that are ready, in order.
    pub fn feed(&mut self, token: &str) -> Vec<String> {
        self.buf.push_str(token);
        let mut out = vec![];
        while let Some(cut) = self.next_cut() {
            let rest = self.buf.split_off(cut);
            out.push(std::mem::replace(&mut self.buf, rest));
        }
        out
    }

    /// Byte offset to flush at, if a trigger has fired.
    ///
    /// A delimiter only cuts once the segment has a letter/digit before it
    /// (bare "..." would otherwise flush as its own request, which Sarvam
    /// rejects). A '.' right after a digit never cuts: list marker or decimal.
    /// Past the threshold the cut goes after the last space following a
    /// comma/semicolon/colon in the chunk's second half, else after the last
    /// space, so chunks end on whole words.
    fn next_cut(&self) -> Option<usize> {
        let (mut count, mut has_content) = (0usize, false);
        let (mut last_space, mut last_pause) = (0usize, 0usize);
        let mut prev = '\0';
        let mut iter = self.buf.char_indices().peekable();

        while let Some((i, r)) = iter.next() {
            let end = i + r.len_utf8();
            count += 1;

            if r.is_whitespace() && has_content {
                last_space = end;
                if matches!(prev, ',' | ';' | ':') && count >= RUNE_THRESHOLD / 2 {
                    last_pause = end;
                }
            }

            if count >= RUNE_THRESHOLD && !(is_delimiter(r) && has_content) {
                if last_pause > 0 {
                    return Some(last_pause);
                }
                if last_space > 0 {
                    return Some(last_space);
                }
                if count >= HARD_LIMIT
                    && let Some(&(_, next)) = iter.peek()
                    && !is_mark(next)
                    && r != VIRAMA
                {
                    return Some(end);
                }
            }

            if is_delimiter(r) && has_content {
                if r == '.' && is_digit(prev) {
                    prev = r;
                    continue;
                }
                return Some(end);
            }

            if is_content(r) {
                has_content = true;
            }
            prev = r;
        }
        None
    }

    /// Remaining text at the end of a turn; "" if it has no letter/digit.
    pub fn flush(&mut self) -> String {
        let s = std::mem::take(&mut self.buf);
        if has_letter_or_digit(&s) { s } else { String::new() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(tokens: &[&str]) -> (Vec<String>, String) {
        let mut c = SentenceChunker::default();
        let mut out = vec![];
        for t in tokens {
            out.extend(c.feed(t));
        }
        (out, c.flush())
    }

    #[test]
    fn splits_on_sentence_delimiters_incl_danda() {
        let (o, rest) = run(&["Hello there. How ", "are you? नमस्ते। आप कैसे हैं", "॥ tail"]);
        assert_eq!(o, ["Hello there.", " How are you?", " नमस्ते।", " आप कैसे हैं॥"]);
        assert_eq!(rest, " tail");
    }

    #[test]
    fn bare_punctuation_and_numbered_lists_do_not_cut() {
        let (o, rest) = run(&["...", " ok", ".", "\n1.", " first\n2.", " second."]);
        assert_eq!(o, ["... ok.", "\n1. first\n", "2. second."]);
        assert_eq!(rest, "");
        let (o, rest) = run(&["Pi is 3.", "14 roughly."]);
        assert_eq!(o, ["Pi is 3.14 roughly."]);
        assert_eq!(rest, "");
        assert_eq!(run(&["..."]).1, ""); // trailing bare punctuation dropped
    }

    #[test]
    fn long_run_cuts_on_a_word_boundary() {
        let words = "word ".repeat(40); // 200 chars, no delimiter
        let (o, rest) = run(&[&words]);
        assert!(!o.is_empty());
        for chunk in &o {
            assert!(chunk.ends_with(' '), "chunk must end on a word break: {chunk:?}");
            assert!(chunk.chars().count() <= RUNE_THRESHOLD + 5);
        }
        assert_eq!(o.concat() + &rest, words);
    }

    #[test]
    fn never_splits_a_devanagari_syllable() {
        let long: String = "बिजली".repeat(60); // one enormous "word", 300 chars
        let (o, rest) = run(&[&long]);
        assert!(!o.is_empty());
        for chunk in &o {
            assert!(!chunk.ends_with(VIRAMA));
        }
        // the next chunk never starts with a combining mark
        for chunk in o.iter().skip(1).chain(Some(&rest)) {
            if let Some(c) = chunk.chars().next() {
                assert!(!is_mark(c), "chunk starts with a combining mark: {chunk:?}");
            }
        }
        assert_eq!(o.concat() + &rest, long);
    }
}
