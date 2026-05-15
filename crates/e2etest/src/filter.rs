/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use crate::group::RunGroup;
use itertools::Itertools;
use std::collections::HashMap;
use std::collections::HashSet;
use std::iter;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FilterMatcher<'a> {
    Any,
    Partial(&'a str),
    Exact(&'a str),
}

impl<'a> FilterMatcher<'a> {
    fn new(filter: &'a str) -> Self {
        if filter.is_empty() {
            Self::Any
        } else if let Some(filter) = filter
            .strip_prefix('"')
            .and_then(|filter| filter.strip_suffix('"'))
        {
            Self::Exact(filter)
        } else {
            Self::Partial(filter)
        }
    }

    fn matches(self, candidate: &str) -> bool {
        match self {
            Self::Any => true,
            Self::Partial(filter) => candidate.contains(filter),
            Self::Exact(filter) => candidate == filter,
        }
    }
}

/// Represents the filter configuration for test execution.
#[derive(Clone, Debug)]
pub(crate) struct Filter {
    /// - Key: test file name (e.g., "crud", "full_scan")
    /// - Value: HashSet of specific test names within that file (empty means run all tests in file)
    tests: Arc<HashMap<String, HashSet<String>>>,
    groups: Arc<HashSet<String>>,
}

impl Filter {
    pub(crate) fn empty() -> Self {
        Self {
            tests: Arc::new(HashMap::new()),
            groups: Arc::new(HashSet::new()),
        }
    }

    /// Parse command line filters into the expected filter format for test execution.
    /// Returns a HashMap where:
    pub(crate) fn new(filters: &[String], group: &dyn RunGroup) -> Self {
        let mut filter_map = HashMap::new();
        let mut group_set = HashSet::new();

        if filters.is_empty() {
            // Run all tests
            return Self {
                tests: Arc::new(filter_map),
                groups: Arc::new(group_set),
            };
        }

        let parent_name = group.name().to_string();
        let mut update_group_set = |group_name: &str| {
            group_name
                .split("::")
                .fold(parent_name.clone(), |acc, name| {
                    let acc = format!("{acc}::{name}");
                    group_set.insert(acc.clone());
                    acc
                })
        };
        for filter in filters {
            // Check for <group>::<test> syntax
            if let Some((group_part, test_part)) = filter.rsplit_once("::") {
                let group_filter = FilterMatcher::new(group_part);
                let test_filter = FilterMatcher::new(test_part);

                for (group_name, test_name) in group
                    .test_names()
                    .iter()
                    .filter_map(|name| name.rsplit_once("::"))
                {
                    if !group_filter.matches(group_name) {
                        continue;
                    }

                    let parent_group_name = format!("{parent_name}::{group_name}");
                    if matches!(test_filter, FilterMatcher::Any) {
                        filter_map.entry(parent_group_name).or_default();
                        update_group_set(group_name);
                        continue;
                    }
                    if test_filter.matches(test_name) {
                        filter_map
                            .entry(parent_group_name)
                            .or_default()
                            .insert(test_name.to_string());
                        update_group_set(group_name);
                    }
                }
            } else {
                // Not found `::`, check for matching both file and test case name
                let filter = FilterMatcher::new(filter);

                for (group_name, test_name) in group
                    .test_names()
                    .iter()
                    .filter_map(|name| name.rsplit_once("::"))
                {
                    let parent_group_name = format!("{parent_name}::{group_name}");
                    if filter.matches(group_name) {
                        filter_map.entry(parent_group_name).or_default();
                        update_group_set(group_name);
                        continue;
                    }
                    if filter.matches(test_name) {
                        filter_map
                            .entry(parent_group_name)
                            .or_default()
                            .insert(test_name.to_string());
                        update_group_set(group_name);
                    }
                }
            }
        }

        Self {
            tests: Arc::new(filter_map),
            groups: Arc::new(group_set),
        }
    }

    pub(crate) fn consider_group(
        &self,
        group_names: impl IntoIterator<Item = impl AsRef<str>>,
        group_name: &str,
    ) -> bool {
        let group_name = group_names
            .into_iter()
            .map(|v| v.as_ref().to_string())
            .chain(iter::once(group_name.to_string()))
            .join("::");
        self.groups.is_empty() || self.groups.contains(&group_name)
    }

    pub(crate) fn consider_test(
        &self,
        group_names: impl IntoIterator<Item = impl AsRef<str>>,
        test_name: &str,
    ) -> bool {
        if self.tests.is_empty() {
            return true;
        }
        let group_name = group_names
            .into_iter()
            .map(|v| v.as_ref().to_string())
            .join("::");
        if let Some(tests) = self.tests.get(&group_name) {
            tests.is_empty() || tests.contains(test_name)
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::Fixture;
    use crate::fixture::Setup;
    use crate::group::Group;
    use crate::test::RunTest;
    use crate::test::Test;

    #[derive(Clone)]
    struct GroupFixture;

    impl From<&()> for GroupFixture {
        fn from(_: &()) -> Self {
            Self
        }
    }

    impl From<&GroupFixture> for GroupFixture {
        fn from(_: &GroupFixture) -> Self {
            Self
        }
    }

    impl Fixture for GroupFixture {
        async fn setup(_: &mut impl Setup) -> Self {
            Self
        }
        async fn teardown(self) {}
    }

    #[derive(Clone)]
    struct TestFixture;

    impl From<&GroupFixture> for TestFixture {
        fn from(_: &GroupFixture) -> Self {
            Self
        }
    }

    impl Fixture for TestFixture {
        async fn setup(_: &mut impl Setup) -> Self {
            Self
        }
        async fn teardown(self) {}
    }

    struct TestImpl(String);

    impl Test for TestImpl {
        type Fixture = TestFixture;

        fn name(&self) -> &str {
            &self.0
        }

        #[allow(clippy::manual_async_fn)]
        fn run(&self, _: Arc<Self::Fixture>) -> impl Future<Output = ()> + Send + 'static {
            async {}
        }
    }

    struct GroupImpl {
        name: String,
        tests: Vec<Box<dyn RunTest>>,
        groups: Vec<Box<dyn RunGroup>>,
    }

    impl Group for GroupImpl {
        type Fixture = GroupFixture;

        fn name(&self) -> &str {
            &self.name
        }

        fn tests(&self) -> &[Box<dyn RunTest>] {
            &self.tests
        }

        fn groups(&self) -> &[Box<dyn RunGroup>] {
            &self.groups
        }
    }

    fn make_dummy_group(group_name: &str, test_names: &[&str]) -> Box<dyn RunGroup> {
        Box::new(GroupImpl {
            name: group_name.to_string(),
            tests: test_names
                .iter()
                .map(|&name| Box::new(TestImpl(name.to_string())) as Box<dyn RunTest>)
                .collect(),
            groups: vec![],
        })
    }

    fn make_test_cases() -> Box<dyn RunGroup> {
        Box::new(GroupImpl {
            name: "root".to_string(),
            tests: vec![],
            groups: vec![
                make_dummy_group("crud", &["simple_create", "drop_index"]),
                make_dummy_group("full_scan", &["scan_index", "scan_all"]),
                make_dummy_group("other", &["misc", "simple_misc"]),
            ],
        })
    }

    fn make_overlapping_test_cases() -> Box<dyn RunGroup> {
        Box::new(GroupImpl {
            name: "root".to_string(),
            tests: vec![],
            groups: vec![
                make_dummy_group("crud", &["simple_create", "simple_create_extra"]),
                make_dummy_group("crud_extra", &["simple_create", "simple_create_additional"]),
            ],
        })
    }

    #[test]
    fn test_no_filters_runs_all() {
        let test_cases = make_test_cases();
        let filters: Vec<String> = vec![];
        let result = Filter::new(&filters, test_cases.as_ref());
        assert!(result.groups.is_empty());
        assert!(result.tests.is_empty());
    }

    #[test]
    fn test_empty_filters_runs_all() {
        let test_cases = make_test_cases();
        let filters: Vec<String> = vec!["::".to_string()];
        let result = Filter::new(&filters, test_cases.as_ref());
        // It should contain all available test files with empty test cases (running all)
        assert_eq!(result.groups.len(), 3);
        assert_eq!(result.tests.len(), 3);
        assert!(result.groups.contains("root::crud"));
        assert!(result.tests["root::crud"].is_empty());
        assert!(result.groups.contains("root::full_scan"));
        assert!(result.tests["root::full_scan"].is_empty());
        assert!(result.groups.contains("root::other"));
        assert!(result.tests["root::other"].is_empty());
    }

    #[test]
    fn test_file_partial_match() {
        let test_cases = make_test_cases();
        let filters = vec!["crud".to_string()];
        let result = Filter::new(&filters, test_cases.as_ref());
        assert!(result.groups.contains("root::crud"));
        assert!(result.tests.contains_key("root::crud"));
        assert!(result.tests["root::crud"].is_empty());
        assert_eq!(result.groups.len(), 1);
        assert_eq!(result.tests.len(), 1);
    }

    #[test]
    fn test_test_case_partial_match() {
        let test_cases = make_test_cases();
        let filters = vec!["simple".to_string()];
        let result = Filter::new(&filters, test_cases.as_ref());
        assert!(result.groups.contains("root::crud"));
        assert!(result.tests["root::crud"].contains("simple_create"));
        assert!(result.groups.contains("root::other"));
        assert!(result.tests["root::other"].contains("simple_misc"));
        assert_eq!(result.groups.len(), 2);
        assert_eq!(result.tests.len(), 2);
    }

    #[test]
    fn test_file_and_test_case_syntax() {
        let test_cases = make_test_cases();
        let filters = vec!["crud::simple".to_string()];
        let result = Filter::new(&filters, test_cases.as_ref());
        assert!(result.groups.contains("root::crud"));
        assert!(result.tests["root::crud"].contains("simple_create"));
        assert_eq!(result.groups.len(), 1);
        assert_eq!(result.tests.len(), 1);
    }

    #[test]
    fn test_file_and_empty_test_case_syntax() {
        let test_cases = make_test_cases();
        let filters = vec!["crud::".to_string()];
        let result = Filter::new(&filters, test_cases.as_ref());
        assert!(result.groups.contains("root::crud"));
        assert!(result.tests.contains_key("root::crud"));
        assert!(result.tests["root::crud"].is_empty());
        assert_eq!(result.groups.len(), 1);
        assert_eq!(result.tests.len(), 1);
    }

    #[test]
    fn test_empty_file_and_test_case_syntax() {
        let test_cases = make_test_cases();
        let filters = vec!["::simple".to_string()];
        let result = Filter::new(&filters, test_cases.as_ref());
        assert!(result.groups.contains("root::crud"));
        assert!(result.tests["root::crud"].contains("simple_create"));
        assert!(result.groups.contains("root::other"));
        assert!(result.tests["root::other"].contains("simple_misc"));
        assert_eq!(result.groups.len(), 2);
        assert_eq!(result.tests.len(), 2);
    }

    #[test]
    fn test_exact_file_match_syntax() {
        let test_cases = make_overlapping_test_cases();
        let filters = vec!["\"crud\"::".to_string()];
        let result = Filter::new(&filters, test_cases.as_ref());
        assert!(result.groups.contains("root::crud"));
        assert!(result.tests.contains_key("root::crud"));
        assert!(result.tests["root::crud"].is_empty());
        assert_eq!(result.groups.len(), 1);
        assert_eq!(result.tests.len(), 1);
    }

    #[test]
    fn test_exact_test_case_match_syntax() {
        let test_cases = make_overlapping_test_cases();
        let filters = vec!["::\"simple_create\"".to_string()];
        let result = Filter::new(&filters, test_cases.as_ref());
        assert!(result.groups.contains("root::crud"));
        assert!(result.tests["root::crud"].contains("simple_create"));
        assert!(!result.tests["root::crud"].contains("simple_create_extra"));
        assert!(result.groups.contains("root::crud_extra"));
        assert!(result.tests["root::crud_extra"].contains("simple_create"));
        assert!(!result.tests["root::crud_extra"].contains("simple_create_additional"));
        assert_eq!(result.groups.len(), 2);
        assert_eq!(result.tests.len(), 2);
    }

    #[test]
    fn test_exact_file_and_test_case_syntax() {
        let test_cases = make_overlapping_test_cases();
        let filters = vec!["\"crud\"::\"simple_create\"".to_string()];
        let result = Filter::new(&filters, test_cases.as_ref());
        assert!(result.groups.contains("root::crud"));
        assert!(result.tests["root::crud"].contains("simple_create"));
        assert!(!result.tests["root::crud"].contains("simple_create_extra"));
        assert_eq!(result.groups.len(), 1);
        assert_eq!(result.tests.len(), 1);
    }
}
