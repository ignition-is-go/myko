use std::cmp::Ordering;

/// Tiered match quality. Tier ordering is strict:
/// every `Exact` > every `Prefix` > every `Subsequence(_)` > every `Typo(_)`.
/// Within a tier, the inner number breaks ties.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Score {
    Exact,
    Prefix,
    Subsequence(u16), // nucleo score; higher is better
    Typo(u8),         // Levenshtein distance; lower is better
}

impl PartialOrd for Score {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Score {
    fn cmp(&self, other: &Self) -> Ordering {
        use Score::{Exact, Prefix, Subsequence, Typo};
        const fn tier(score: Score) -> u8 {
            match score {
                Exact => 3,
                Prefix => 2,
                Subsequence(_) => 1,
                Typo(_) => 0,
            }
        }

        let tier_order = tier(*self).cmp(&tier(*other));
        if tier_order != Ordering::Equal {
            return tier_order;
        }

        match (self, other) {
            (Subsequence(a), Subsequence(b)) => a.cmp(b),
            (Typo(a), Typo(b)) => b.cmp(a),
            _ => Ordering::Equal,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Score::*;

    #[test]
    fn exact_beats_everything() {
        assert!(Exact > Prefix);
        assert!(Exact > Subsequence(u16::MAX));
        assert!(Exact > Typo(0));
    }

    #[test]
    fn prefix_beats_subsequence_and_typo() {
        assert!(Prefix > Subsequence(u16::MAX));
        assert!(Prefix > Typo(0));
    }

    #[test]
    fn subsequence_beats_typo() {
        assert!(Subsequence(0) > Typo(0));
    }

    #[test]
    fn within_subsequence_higher_is_better() {
        assert!(Subsequence(100) > Subsequence(50));
    }

    #[test]
    fn within_typo_lower_is_better() {
        assert!(Typo(0) > Typo(1));
        assert!(Typo(1) > Typo(2));
    }
}
