/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use crate::group::RunGroup;
use itertools::Itertools;
use std::collections::BTreeSet;
use std::iter;
use std::sync::Arc;
use wildmatch::WildMatch;

/// Represents the filter configuration for test execution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Filter {
    All,
    Some {
        tests: Arc<BTreeSet<String>>,
        groups: Arc<BTreeSet<String>>,
    },
}

impl Filter {
    pub(crate) fn empty() -> Self {
        Self::All
    }

    /// Parse command line filters into the expected filter format for test execution.
    pub(crate) fn new(filters: &[String], group: &dyn RunGroup) -> Self {
        if filters.is_empty() {
            return Self::All;
        }

        let mut filtered_tests = BTreeSet::new();
        let mut filtered_groups = BTreeSet::new();

        let parent_name = group.name().to_string();
        filters
            .iter()
            .map(|filter| WildMatch::new(filter))
            .for_each(|filter| {
                group
                    .test_names()
                    .iter()
                    .filter_map(|name| name.rsplit_once("::"))
                    .map(|(group_name, test_name)| {
                        (
                            format!("{parent_name}::{group_name}"),
                            format!("{parent_name}::{group_name}::{test_name}"),
                        )
                    })
                    .filter(|(_, test_name)| filter.matches(test_name))
                    .for_each(|(group_name, test_name)| {
                        filtered_groups.insert(group_name);
                        filtered_tests.insert(test_name);
                    });
            });

        Self::Some {
            tests: Arc::new(filtered_tests),
            groups: Arc::new(filtered_groups),
        }
    }

    pub(crate) fn consider_group(
        &self,
        group_names: impl IntoIterator<Item = impl AsRef<str>>,
        group_name: &str,
    ) -> bool {
        match self {
            Self::All => true,
            Self::Some { groups, .. } => {
                let group_name = group_names
                    .into_iter()
                    .map(|v| v.as_ref().to_string())
                    .chain(iter::once(group_name.to_string()))
                    .join("::");
                groups.contains(&group_name)
            }
        }
    }

    pub(crate) fn consider_test(
        &self,
        group_names: impl IntoIterator<Item = impl AsRef<str>>,
        test_name: &str,
    ) -> bool {
        match self {
            Self::All => true,
            Self::Some { tests, .. } => {
                let test_name = group_names
                    .into_iter()
                    .map(|v| v.as_ref().to_string())
                    .chain(iter::once(test_name.to_string()))
                    .join("::");
                tests.contains(&test_name)
            }
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

    impl From<&GroupFixture> for GroupFixture {
        fn from(_: &GroupFixture) -> Self {
            Self
        }
    }

    impl Fixture for GroupFixture {
        async fn setup(_: &mut impl Setup) -> Option<Self> {
            Some(Self)
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
        async fn setup(_: &mut impl Setup) -> Option<Self> {
            Some(Self)
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
                make_dummy_group("full_scan", &["scan_index", "scan_undex", "scan_all"]),
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
        assert_eq!(result, Filter::All);
    }

    #[test]
    fn test_empty_filters_runs_all() {
        let test_cases = make_test_cases();
        let filters: Vec<String> = vec!["*".into()];
        let result = Filter::new(&filters, test_cases.as_ref());

        // It should contain all available test cases

        assert_eq!(
            result,
            Filter::Some {
                groups: Arc::new(
                    ["root::crud", "root::full_scan", "root::other"]
                        .into_iter()
                        .map(String::from)
                        .collect()
                ),
                tests: Arc::new(
                    [
                        "root::crud::drop_index",
                        "root::crud::simple_create",
                        "root::full_scan::scan_all",
                        "root::full_scan::scan_index",
                        "root::full_scan::scan_undex",
                        "root::other::misc",
                        "root::other::simple_misc",
                    ]
                    .into_iter()
                    .map(String::from)
                    .collect()
                ),
            }
        );
    }

    #[test]
    fn test_wrong_filters_runs_nothing() {
        let test_cases = make_test_cases();
        let filters: Vec<String> = vec!["bad_name".into()];
        let result = Filter::new(&filters, test_cases.as_ref());

        assert_eq!(
            result,
            Filter::Some {
                groups: Arc::new(BTreeSet::new()),
                tests: Arc::new(BTreeSet::new()),
            }
        );
    }

    #[test]
    fn test_module_partial_match() {
        let test_cases = make_test_cases();
        let filters = vec!["*crud*".into()];
        let result = Filter::new(&filters, test_cases.as_ref());

        assert_eq!(
            result,
            Filter::Some {
                groups: Arc::new(["root::crud"].into_iter().map(String::from).collect()),
                tests: Arc::new(
                    ["root::crud::drop_index", "root::crud::simple_create",]
                        .into_iter()
                        .map(String::from)
                        .collect()
                ),
            }
        );
    }

    #[test]
    fn test_test_case_partial_match() {
        let test_cases = make_test_cases();
        let filters = vec!["*simple*".into()];
        let result = Filter::new(&filters, test_cases.as_ref());

        assert_eq!(
            result,
            Filter::Some {
                groups: Arc::new(
                    ["root::crud", "root::other"]
                        .into_iter()
                        .map(String::from)
                        .collect()
                ),
                tests: Arc::new(
                    ["root::crud::simple_create", "root::other::simple_misc",]
                        .into_iter()
                        .map(String::from)
                        .collect()
                ),
            }
        );
    }

    #[test]
    fn test_test_case_question_sign() {
        let test_cases = make_test_cases();
        let filters = vec!["*_?ndex*".into()];
        let result = Filter::new(&filters, test_cases.as_ref());

        assert_eq!(
            result,
            Filter::Some {
                groups: Arc::new(
                    ["root::crud", "root::full_scan"]
                        .into_iter()
                        .map(String::from)
                        .collect()
                ),
                tests: Arc::new(
                    [
                        "root::crud::drop_index",
                        "root::full_scan::scan_index",
                        "root::full_scan::scan_undex",
                    ]
                    .into_iter()
                    .map(String::from)
                    .collect()
                ),
            }
        );
    }

    #[test]
    fn test_module_and_test_case_syntax() {
        let test_cases = make_test_cases();
        let filters = vec!["*crud*::*simple*".into()];
        let result = Filter::new(&filters, test_cases.as_ref());

        assert_eq!(
            result,
            Filter::Some {
                groups: Arc::new(["root::crud"].into_iter().map(String::from).collect()),
                tests: Arc::new(
                    ["root::crud::simple_create",]
                        .into_iter()
                        .map(String::from)
                        .collect()
                ),
            }
        );
    }

    #[test]
    fn test_module_and_empty_test_case_syntax() {
        let test_cases = make_test_cases();
        let filters = vec!["*crud*::*".into()];
        let result = Filter::new(&filters, test_cases.as_ref());

        assert_eq!(
            result,
            Filter::Some {
                groups: Arc::new(["root::crud"].into_iter().map(String::from).collect()),
                tests: Arc::new(
                    ["root::crud::drop_index", "root::crud::simple_create",]
                        .into_iter()
                        .map(String::from)
                        .collect()
                ),
            }
        );
    }

    #[test]
    fn test_empty_module_and_test_case_syntax() {
        let test_cases = make_test_cases();
        let filters = vec!["*::*simple*".into()];
        let result = Filter::new(&filters, test_cases.as_ref());

        assert_eq!(
            result,
            Filter::Some {
                groups: Arc::new(
                    ["root::crud", "root::other"]
                        .into_iter()
                        .map(String::from)
                        .collect()
                ),
                tests: Arc::new(
                    ["root::crud::simple_create", "root::other::simple_misc",]
                        .into_iter()
                        .map(String::from)
                        .collect()
                ),
            }
        );
    }

    #[test]
    fn test_exact_module_match_syntax() {
        let test_cases = make_overlapping_test_cases();
        let filters = vec!["root::crud::*".into()];
        let result = Filter::new(&filters, test_cases.as_ref());

        assert_eq!(
            result,
            Filter::Some {
                groups: Arc::new(["root::crud"].into_iter().map(String::from).collect()),
                tests: Arc::new(
                    [
                        "root::crud::simple_create",
                        "root::crud::simple_create_extra",
                    ]
                    .into_iter()
                    .map(String::from)
                    .collect()
                ),
            }
        );
    }

    #[test]
    fn test_exact_test_case_match_syntax() {
        let test_cases = make_overlapping_test_cases();
        let filters = vec!["*::simple_create".into()];
        let result = Filter::new(&filters, test_cases.as_ref());

        assert_eq!(
            result,
            Filter::Some {
                groups: Arc::new(
                    ["root::crud", "root::crud_extra"]
                        .into_iter()
                        .map(String::from)
                        .collect()
                ),
                tests: Arc::new(
                    [
                        "root::crud::simple_create",
                        "root::crud_extra::simple_create",
                    ]
                    .into_iter()
                    .map(String::from)
                    .collect()
                ),
            }
        );
    }

    #[test]
    fn test_exact_module_and_test_case_syntax() {
        let test_cases = make_overlapping_test_cases();
        let filters = vec!["root::crud::simple_create".into()];
        let result = Filter::new(&filters, test_cases.as_ref());

        assert_eq!(
            result,
            Filter::Some {
                groups: Arc::new(["root::crud"].into_iter().map(String::from).collect()),
                tests: Arc::new(
                    ["root::crud::simple_create",]
                        .into_iter()
                        .map(String::from)
                        .collect()
                ),
            }
        );
    }
}
