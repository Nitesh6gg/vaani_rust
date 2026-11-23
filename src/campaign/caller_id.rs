//! Choosing the caller ID (DID) of a campaign call, as Dograh's backend does
//! (api/services/campaign/caller_id.py): the same rules, so a campaign behaves the same whichever
//! engine dials it.
//!
//! A campaign may select some of the organization's numbers (`orchestrator_metadata.caller_ids`) and
//! a strategy (`caller_id_strategy`). A retry carries the numbers its earlier attempts used
//! (`tried_caller_ids`, oldest first, in the queued run's variables) and goes out on a different one
//! when it can.

use serde_json::{Map, Value};

/// How a campaign picks among its selected numbers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Strategy {
    /// In turn (a counter shared through Redis).
    #[default]
    RoundRobin,
    Random,
    /// The same number for the same contact.
    Sticky,
}

impl Strategy {
    /// An unknown name is the default, as in Dograh.
    pub fn parse(s: &str) -> Self {
        match s {
            "random" => Self::Random,
            "sticky" => Self::Sticky,
            _ => Self::RoundRobin,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::RoundRobin => "round_robin",
            Self::Random => "random",
            Self::Sticky => "sticky",
        }
    }
}

/// The selected numbers, trimmed, in order, without blanks or duplicates.
pub fn clean(raw: Vec<String>) -> Vec<String> {
    let mut out: Vec<String> = Vec::with_capacity(raw.len());
    for n in raw {
        let n = n.trim().to_string();
        if !n.is_empty() && !out.contains(&n) {
            out.push(n);
        }
    }
    out
}

/// The selected numbers that are still configured on the organization.
pub fn candidates(selected: &[String], available: &[String]) -> Vec<String> {
    selected.iter().filter(|n| available.contains(n)).cloned().collect()
}

/// FNV-1a, 64 bits: the same function in Dograh's backend, so a contact keeps its number across engines.
pub fn stable_hash(text: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in text.bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h
}

/// One of `cands`, or `None` when there are none. Numbers in `tried` (oldest first) are skipped while
/// others remain; when every candidate was tried, the one tried longest ago is used again. `counter`
/// drives round robin, `contact` drives sticky and `entropy` (any random number) drives random.
pub fn pick(cands: &[String], strategy: Strategy, tried: &[String], contact: &str, counter: u64, entropy: u64) -> Option<String> {
    if cands.is_empty() {
        return None;
    }
    let fresh: Vec<&String> = cands.iter().filter(|n| !tried.contains(n)).collect();
    if fresh.is_empty() {
        return cands.iter().min_by_key(|n| tried.iter().position(|t| t == *n).unwrap_or(usize::MAX)).cloned();
    }
    let len = fresh.len() as u64;
    let i = match strategy {
        Strategy::RoundRobin => counter % len,
        Strategy::Random => entropy % len,
        Strategy::Sticky => stable_hash(contact) % len,
    };
    Some(fresh[i as usize].clone())
}

/// The variable a queued run carries its tried numbers in (the dialer's bookkeeping, not part of a call's variables).
pub const TRIED_KEY: &str = "tried_caller_ids";

/// The numbers a queued run's earlier attempts used, oldest try first.
pub fn tried_of(vars: &Map<String, Value>) -> Vec<String> {
    vars.get(TRIED_KEY)
        .and_then(Value::as_array)
        .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
        .unwrap_or_default()
}

/// `tried` plus `last`, the number the attempt that is starting uses. A number used again moves to the end (the
/// most recent try), so when every number was tried the one tried longest ago is the next, and they take turns.
pub fn with_tried(mut tried: Vec<String>, last: &str) -> Vec<String> {
    if !last.is_empty() {
        tried.retain(|t| t != last);
        tried.push(last.to_string());
    }
    tried
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    const ABC: [&str; 3] = ["0731111", "0732222", "0733333"];

    #[test]
    fn clean_trims_and_drops_blanks_and_duplicates() {
        assert_eq!(clean(v(&["0731111", " ", "0732222", "0731111", " 0733333 "])), v(&ABC));
    }

    #[test]
    fn strategy_names_default_to_round_robin() {
        assert_eq!(Strategy::parse("sticky"), Strategy::Sticky);
        assert_eq!(Strategy::parse("random"), Strategy::Random);
        assert_eq!(Strategy::parse("round_robin"), Strategy::RoundRobin);
        assert_eq!(Strategy::parse("nonsense"), Strategy::RoundRobin);
        assert_eq!(Strategy::parse(Strategy::Sticky.as_str()), Strategy::Sticky);
    }

    #[test]
    fn candidates_are_the_selected_numbers_still_configured() {
        assert_eq!(candidates(&v(&ABC), &v(&["0733333", "0731111"])), v(&["0731111", "0733333"]));
        assert!(candidates(&v(&["0731111"]), &[]).is_empty());
    }

    #[test]
    fn no_candidates_gives_none() {
        assert_eq!(pick(&[], Strategy::RoundRobin, &[], "", 0, 0), None);
    }

    #[test]
    fn round_robin_cycles_in_selection_order() {
        let c = v(&ABC);
        let got: Vec<String> = (1..=6).map(|n| pick(&c, Strategy::RoundRobin, &[], "", n, 0).unwrap()).collect();
        assert_eq!(got, v(&["0732222", "0733333", "0731111", "0732222", "0733333", "0731111"]));
    }

    #[test]
    fn random_uses_every_number() {
        let c = v(&ABC);
        let seen: std::collections::HashSet<String> = (0..30).map(|e| pick(&c, Strategy::Random, &[], "", 0, e).unwrap()).collect();
        assert_eq!(seen.len(), 3);
    }

    #[test]
    fn stable_hash_is_fnv1a_64_like_dograhs_backend() {
        // The same values Dograh's tests check.
        assert_eq!(stable_hash(""), 14695981039346656037);
        assert_eq!(stable_hash("a"), 12638187200555641996);
        assert_eq!(stable_hash("+919876543210"), 5333354722377341857);
    }

    #[test]
    fn sticky_gives_a_contact_the_same_number_and_spreads_contacts() {
        let c = v(&ABC);
        let first = pick(&c, Strategy::Sticky, &[], "+919876543210", 0, 0).unwrap();
        assert!((0..5).all(|n| pick(&c, Strategy::Sticky, &[], "+919876543210", n, n).unwrap() == first));
        let spread: std::collections::HashSet<String> =
            (0..40).map(|n| pick(&c, Strategy::Sticky, &[], &format!("+9198765432{n:02}"), 0, 0).unwrap()).collect();
        assert_eq!(spread.len(), 3);
    }

    #[test]
    fn a_retry_avoids_the_numbers_already_tried() {
        let c = v(&ABC);
        for strategy in [Strategy::RoundRobin, Strategy::Random, Strategy::Sticky] {
            for n in 0..6 {
                let got = pick(&c, strategy, &v(&["0731111"]), "x", n, n).unwrap();
                assert!(got == "0732222" || got == "0733333", "{strategy:?} {n}: {got}");
                assert_eq!(pick(&c, strategy, &v(&["0731111", "0732222"]), "x", n, n).unwrap(), "0733333");
            }
        }
    }

    #[test]
    fn when_every_number_was_tried_the_oldest_try_is_reused() {
        // `tried` is oldest first: 0732222 was tried longest ago.
        assert_eq!(pick(&v(&ABC), Strategy::RoundRobin, &v(&["0732222", "0733333", "0731111"]), "", 0, 0).unwrap(), "0732222");
        assert_eq!(pick(&v(&["0731111"]), Strategy::Sticky, &v(&["0731111"]), "x", 0, 0).unwrap(), "0731111");
    }

    #[test]
    fn tried_numbers_round_trip_through_the_variables() {
        let mut vars = Map::new();
        assert!(tried_of(&vars).is_empty());
        let t = with_tried(with_tried(Vec::new(), "0731111"), "0732222");
        vars.insert(TRIED_KEY.into(), Value::from(t.clone()));
        assert_eq!(tried_of(&vars), t);
        assert_eq!(with_tried(t.clone(), "0731111"), v(&["0732222", "0731111"]), "no duplicates: a number used again moves to the end");
        assert_eq!(with_tried(t.clone(), ""), t, "a blank number adds nothing");
    }

    /// Retries rotate: a number used again moves to the end of the tried list, so with two numbers the attempts alternate for as
    /// long as it goes on (a list that kept its first order would reuse the same number every time once both were tried).
    #[test]
    fn retries_alternate_between_two_numbers_for_as_long_as_they_go_on() {
        let cands = v(&["0731111", "0732222"]);
        let (mut tried, mut used) = (Vec::new(), Vec::new());
        for attempt in 0..6 {
            let n = pick(&cands, Strategy::RoundRobin, &tried, "", attempt, 0).unwrap();
            tried = with_tried(tried, &n);
            used.push(n);
        }
        assert_eq!(used, v(&["0731111", "0732222", "0731111", "0732222", "0731111", "0732222"]));
        assert_eq!(tried.len(), 2, "the list never grows past the selected numbers");
    }
}
