use std::path::{Path, PathBuf};

use ignore::gitignore::{Gitignore, GitignoreBuilder};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ExclusionError {
    #[error("invalid exclude rule at index {index}: {message}")]
    InvalidRule { index: usize, message: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExclusionReason {
    Absolute,
    Pattern,
    IncludedByNegation,
    NotMatched,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExclusionDecision {
    excluded: bool,
    reason: ExclusionReason,
}

impl ExclusionDecision {
    #[must_use]
    pub const fn excluded(reason: ExclusionReason) -> Self {
        Self {
            excluded: true,
            reason,
        }
    }

    #[must_use]
    pub const fn included(reason: ExclusionReason) -> Self {
        Self {
            excluded: false,
            reason,
        }
    }

    #[must_use]
    pub const fn is_excluded(self) -> bool {
        self.excluded
    }

    #[must_use]
    pub const fn reason(self) -> ExclusionReason {
        self.reason
    }
}

#[derive(Debug)]
pub struct ExclusionMatcher {
    patterns: Gitignore,
    absolute_excludes: Vec<String>,
}

impl ExclusionMatcher {
    pub fn new(
        global_rules: &[String],
        root_rules: &[String],
        absolute_excludes: &[String],
    ) -> Result<Self, ExclusionError> {
        let mut builder = GitignoreBuilder::new(Path::new(""));

        for (index, rule) in global_rules.iter().chain(root_rules.iter()).enumerate() {
            validate_rule(rule, index)?;
            builder
                .add_line(None, rule)
                .map_err(|error| ExclusionError::InvalidRule {
                    index,
                    message: error.to_string(),
                })?;
        }

        let patterns = builder
            .build()
            .map_err(|error| ExclusionError::InvalidRule {
                index: global_rules.len() + root_rules.len(),
                message: error.to_string(),
            })?;

        Ok(Self {
            patterns,
            absolute_excludes: absolute_excludes
                .iter()
                .map(|path| normalize_absolute_path(path))
                .collect(),
        })
    }

    #[must_use]
    pub fn decision_for_path<P: AsRef<Path>>(&self, path: P, is_dir: bool) -> ExclusionDecision {
        let path = path.as_ref();
        if self.matches_absolute_exclude(path) {
            return ExclusionDecision::excluded(ExclusionReason::Absolute);
        }

        let matched = self.patterns.matched_path_or_any_parents(path, is_dir);
        if matched.is_ignore() {
            return ExclusionDecision::excluded(ExclusionReason::Pattern);
        }
        if matched.is_whitelist() {
            return ExclusionDecision::included(ExclusionReason::IncludedByNegation);
        }

        ExclusionDecision::included(ExclusionReason::NotMatched)
    }

    fn matches_absolute_exclude(&self, path: &Path) -> bool {
        let candidate = normalize_absolute_path(&path_to_stable_string(path));
        self.absolute_excludes.iter().any(|excluded| {
            candidate == *excluded
                || candidate
                    .strip_prefix(excluded)
                    .is_some_and(|rest| rest.starts_with('/'))
        })
    }
}

fn path_to_stable_string(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn normalize_absolute_path(path: &str) -> String {
    let normalized = path.replace('\\', "/").trim_end_matches('/').to_string();
    if looks_like_windows_path(&normalized) {
        normalized.to_lowercase()
    } else {
        normalized
    }
}

fn looks_like_windows_path(path: &str) -> bool {
    let bytes = path.as_bytes();
    bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':'
}

fn validate_rule(rule: &str, index: usize) -> Result<(), ExclusionError> {
    if rule.trim().is_empty() {
        return Err(ExclusionError::InvalidRule {
            index,
            message: "rule must not be empty".to_string(),
        });
    }
    if rule.chars().any(char::is_control) {
        return Err(ExclusionError::InvalidRule {
            index,
            message: "rule must not contain control characters".to_string(),
        });
    }

    Ok(())
}

#[must_use]
pub fn stable_path(path: &str) -> PathBuf {
    PathBuf::from(path.replace('\\', "/"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn global_and_root_rules_are_applied_in_order() {
        let matcher = ExclusionMatcher::new(
            &["**/*.tmp".to_string()],
            &["!important.tmp".to_string()],
            &[],
        )
        .unwrap();

        assert!(matcher
            .decision_for_path(stable_path("cache/file.tmp"), false)
            .is_excluded());
        assert!(!matcher
            .decision_for_path(stable_path("important.tmp"), false)
            .is_excluded());
    }

    #[test]
    fn last_matching_rule_wins() {
        let matcher = ExclusionMatcher::new(
            &["**/*.tmp".to_string()],
            &["!keep.tmp".to_string(), "keep.tmp".to_string()],
            &[],
        )
        .unwrap();

        assert!(matcher
            .decision_for_path(stable_path("keep.tmp"), false)
            .is_excluded());
    }

    #[test]
    fn absolute_excludes_override_negation() {
        let matcher = ExclusionMatcher::new(
            &["**/*.tmp".to_string()],
            &["!secret.tmp".to_string()],
            &["D:/Root/secret.tmp".to_string()],
        )
        .unwrap();

        let decision = matcher.decision_for_path(stable_path("D:/Root/secret.tmp"), false);

        assert!(decision.is_excluded());
        assert_eq!(decision.reason(), ExclusionReason::Absolute);
    }

    #[test]
    fn absolute_excludes_match_descendants() {
        let matcher = ExclusionMatcher::new(&[], &[], &["/var/lib/private".to_string()]).unwrap();

        assert!(matcher
            .decision_for_path(stable_path("/var/lib/private/data.bin"), false)
            .is_excluded());
    }

    #[test]
    fn invalid_rule_returns_error() {
        let error = ExclusionMatcher::new(&["bad\0rule".to_string()], &[], &[]).unwrap_err();

        assert!(matches!(error, ExclusionError::InvalidRule { .. }));
    }
}
