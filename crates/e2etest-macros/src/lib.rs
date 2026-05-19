/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! This crate provides a macros for e2etest framework

use convert_case::ccase;
use itertools::Itertools;
use proc_macro::TokenStream;
use quote::quote;
use syn::Expr;
use syn::FieldsUnnamed;
use syn::FnArg;
use syn::GenericArgument;
use syn::Ident;
use syn::Index;
use syn::ItemFn;
use syn::Path;
use syn::PathArguments;
use syn::ReturnType;
use syn::Token;
use syn::Type;
use syn::TypePath;
use syn::parse::Parse;
use syn::parse::ParseStream;
use syn::parse_macro_input;
use syn::spanned::Spanned;

fn group_groups_name(name: &str) -> String {
    format!("_E2ETEST_{name}_GROUPS", name = ccase!(constant, name))
}

fn group_tests_name(name: &str) -> String {
    format!("_E2ETEST_{name}_TESTS", name = ccase!(constant, name))
}

fn group_fixture_name(name: &str) -> String {
    format!("_E2etestGroupFixture{name}", name = ccase!(pascal, name))
}

fn group_type_name(name: &str) -> String {
    format!("_E2etestGroup{name}", name = ccase!(pascal, name))
}

fn test_fixture_name(name: &str) -> String {
    format!("_E2etestTestFixture{name}", name = ccase!(pascal, name))
}

fn test_type_name(name: &str) -> String {
    format!("_E2etestTest{name}", name = ccase!(pascal, name))
}

fn test_register_name(name: &str) -> String {
    format!("_e2etest_register_{name}", name = ccase!(snake, name))
}

struct GroupParams {
    name: Ident,
    fixtures: Vec<Ident>,
    parent: Option<Path>,
}

impl Parse for GroupParams {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let _: Ident = input.parse().and_then(|v: Ident| {
            let span = v.span();
            (v == "name")
                .then_some(v)
                .ok_or(syn::Error::new(span, "expected 'name' token"))
        })?;
        let _: Token![=] = input.parse()?;
        let name: Ident = input.parse()?;
        let name = Ident::new(&name.to_string().to_lowercase(), name.span());

        let mut fixtures = Vec::new();
        let mut parent = None;

        while !input.is_empty() {
            let _: Token![,] = input.parse()?;

            let name: Ident = input.parse()?;
            if name == "fixtures" {
                let _: Token![=] = input.parse()?;
                let fields: FieldsUnnamed = input.parse()?;
                fixtures = fields
                    .unnamed
                    .into_iter()
                    .map(|elem| {
                        let Type::Path(type_path) = elem.ty else {
                            return Err(syn::Error::new(
                                elem.span(),
                                "Expected a type implementing Fixture",
                            ));
                        };
                        Ok(type_path.path.segments)
                    })
                    .map(|segments| {
                        segments.and_then(|segments| {
                            let span = segments.span();
                            (segments.len() == 1)
                            .then_some(segments)
                            .and_then(|segments| segments.first().cloned())
                            .ok_or(syn::Error::new(
                                span,
                                "Expected a single pathtuple of Fixture with at least one element",
                            ))
                        })
                    })
                    .map_ok(|path_segment| path_segment.ident)
                    .collect::<syn::Result<_>>()?;
            } else if name == "parent" {
                let _: Token![=] = input.parse()?;
                parent = Some(input.parse()?);
            } else {
                return Err(syn::Error::new(
                    name.span(),
                    "unexpected parameter, expected 'fixtures' or 'parent'",
                ));
            }
        }
        Ok(Self {
            name,
            fixtures,
            parent,
        })
    }
}

/// Macro for defining a test group.
///
/// It generates a struct implementing `e2etest::Group` trait and registers it in the framework.
/// The macro takes the following parameters:
/// - `name`: the name of the group (required)
/// - `fixtures`: a tuple of fixture types that will be set up for each test in the group (optional)
/// - `parent`: a path to the parent group, if this group is a subgroup (optional)
///
/// If you use this macro you should add `linkme` as a dependency in your crate.
#[proc_macro]
pub fn group(item: TokenStream) -> TokenStream {
    let params = parse_macro_input!(item as GroupParams);
    generate_group(params)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

fn generate_group(params: GroupParams) -> syn::Result<proc_macro2::TokenStream> {
    let name = params.name;
    let name_string = name.to_string();
    let fixtures = params.fixtures;
    let group_tests = Ident::new(&group_tests_name(&name_string), name.span());
    let group_groups = Ident::new(&group_groups_name(&name_string), name.span());
    let group_fixture = Ident::new(&group_fixture_name(&name_string), name.span());
    let group_type = Ident::new(&group_type_name(&name_string), name.span());
    let register_group = if let Some(parent) = &params.parent {
        let Some(last) = parent.segments.last() else {
            return Err(syn::Error::new(
                parent.segments.span(),
                "Expected parent path to have at least one segment",
            ));
        };
        let parent_name = last.ident.to_string();
        let mut parent_groups = parent.clone();
        let Some(last) = parent_groups.segments.last_mut() else {
            return Err(syn::Error::new(
                parent_groups.segments.span(),
                "Expected parent path to have at least one segment",
            ));
        };
        last.ident = Ident::new(&group_groups_name(&parent_name), name.span());
        quote! {
            #[linkme::distributed_slice(#parent_groups)]
        }
    } else {
        quote! {}
    };

    let expanded = quote! {
        #[linkme::distributed_slice]
        pub static #group_tests: [fn() -> Box<dyn e2etest::RunTest>];

        #[linkme::distributed_slice]
        pub static #group_groups: [fn() -> Box<dyn e2etest::RunGroup>];

        struct #group_fixture(#(std::sync::Arc<#fixtures>),*);
        impl e2etest::Fixture for #group_fixture {
            async fn setup(setup: &mut impl e2etest::Setup) -> Self {
                Self(#(setup.setup::<#fixtures>().await),*)
            }
            async fn teardown(self) { }
        }

        struct #group_type;

        impl e2etest::Group for #group_type {
            type Fixture = #group_fixture;

            fn name(&self) -> &str {
                #name_string
            }

            fn tests(&self) -> &[Box<dyn e2etest::RunTest>] {
                use std::sync::LazyLock;
                use e2etest::RunTest;

                static TESTS: LazyLock<Vec<Box<dyn e2etest::RunTest>>> = LazyLock::new(|| {
                    #group_tests.iter().map(|test_fn| test_fn()).collect()
                });
                TESTS.as_slice()
            }

            fn groups(&self) -> &[Box<dyn e2etest::RunGroup>] {
                use std::sync::LazyLock;
                use e2etest::RunGroup;

                static GROUPS: LazyLock<Vec<Box<dyn RunGroup>>> = LazyLock::new(|| {
                    #group_groups.iter().map(|group_fn| group_fn()).collect()
                });
                GROUPS.as_slice()
            }
        }

        #register_group
        pub fn #name() -> Box<dyn e2etest::RunGroup> {
            Box::new(#group_type)
        }
    };

    Ok(expanded)
}

struct TestParams {
    group: Path,
    timeout: Option<Expr>,
    skip: Option<Expr>,
}

impl Parse for TestParams {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let _: Ident = input.parse().and_then(|v: Ident| {
            let span = v.span();
            (v == "group")
                .then_some(v)
                .ok_or(syn::Error::new(span, "expected 'group' token"))
        })?;
        let _: Token![=] = input.parse()?;
        let group: Path = input.parse()?;
        let mut timeout = None;
        let mut skip = None;

        while !input.is_empty() {
            let _: Token![,] = input.parse()?;

            let name: Ident = input.parse()?;
            if name == "timeout" {
                let _: Token![=] = input.parse()?;
                timeout = Some(input.parse()?);
            } else if name == "skip" {
                let _: Token![=] = input.parse()?;
                skip = Some(input.parse()?);
            } else {
                return Err(syn::Error::new(
                    name.span(),
                    "unexpected parameter, expected 'timeout' or 'skip'",
                ));
            }
        }

        Ok(Self {
            group,
            timeout,
            skip,
        })
    }
}

fn take_fixtures(run: &ItemFn) -> syn::Result<Vec<TypePath>> {
    let fixtures: Vec<_> = run
        .sig
        .inputs
        .iter()
        .map(|arg| {
            if let FnArg::Typed(pat_type) = arg
                && let Type::Path(type_path) = &*pat_type.ty
                && let Some(path_segment) = type_path.path.segments.first()
                && let PathArguments::AngleBracketed(angle_bracketed) = &path_segment.arguments
                && let Some(generic_arg) = angle_bracketed.args.first()
                && let GenericArgument::Type(Type::Path(fixture_type)) = generic_arg
            {
                Ok(fixture_type.clone())
            } else {
                Err(syn::Error::new(
                    arg.span(),
                    "Expected arguments of type Arc<Fixture>",
                ))
            }
        })
        .collect::<syn::Result<_>>()?;
    if fixtures.is_empty() {
        Err(syn::Error::new(
            run.sig.inputs.span(),
            "Expected the test function to have at least one argument of type Arc<Fixture>",
        ))
    } else {
        Ok(fixtures)
    }
}

/// Macro for defining a test.
///
/// It generates a struct implementing `e2etest::Test` trait and registers it in the framework.
/// The macro takes the following parameters:
/// - `group`: the path to the group this test belongs to (required)
/// - `timeout`: an expression resolved to `Duration` as the timeout for the test (optional)
/// - `skip`: a boolean expression indicating whether the test should be skipped (optional)
///
/// The test function must be async, return `()`, and take as arguments a list of `Arc<Fixture>`
/// as a list of fixtures used inside the test.
///
/// If you use this macro you should add `linkme` and `async-backtrace` as a dependency in your crate.
#[proc_macro_attribute]
pub fn test(attr: TokenStream, item: TokenStream) -> TokenStream {
    let params = parse_macro_input!(attr as TestParams);
    let run = parse_macro_input!(item as ItemFn);
    generate_test(params, run)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

fn generate_test(params: TestParams, run: ItemFn) -> syn::Result<proc_macro2::TokenStream> {
    let Some(last) = params.group.segments.last() else {
        return Err(syn::Error::new(
            params.group.segments.span(),
            "Expected group path to have at least one segment",
        ));
    };
    let group_name = last.ident.to_string();
    let mut group_tests = params.group.clone();
    let Some(last) = group_tests.segments.last_mut() else {
        return Err(syn::Error::new(
            group_tests.segments.span(),
            "Expected group path to have at least one segment",
        ));
    };
    last.ident = Ident::new(&group_tests_name(&group_name), last.ident.span());

    let name = run.sig.ident.clone();
    let name_string = name.to_string();
    let test_fixture = Ident::new(&test_fixture_name(&name_string), name.span());
    let test_type = Ident::new(&test_type_name(&name_string), name.span());
    let test_register = Ident::new(&test_register_name(&name_string), name.span());
    assert!(
        run.sig.asyncness.is_some(),
        "Expected the test function to be async"
    );
    assert!(
        matches!(run.sig.output, ReturnType::Default),
        "Expected the test function to return ()"
    );

    let timeout = if let Some(timeout) = &params.timeout {
        quote! {
            fn timeout(&self) -> Option<std::time::Duration> {
                Some(#timeout)
            }
        }
    } else {
        quote! {}
    };
    let skip = if let Some(skip) = &params.skip {
        quote! {
            fn skip(&self) -> bool {
                #skip
            }
        }
    } else {
        quote! {}
    };

    let fixtures = take_fixtures(&run)?;
    let fixtures_range = (0..fixtures.len()).map(Index::from);

    let expanded = quote! {
        struct #test_fixture(#(std::sync::Arc<#fixtures>),*);
        impl e2etest::Fixture for #test_fixture {
            async fn setup(setup: &mut impl e2etest::Setup) -> Self {
                Self(#(setup.setup::<#fixtures>().await),*)
            }
            async fn teardown(self) { }
        }

        struct #test_type;

        impl e2etest::Test for #test_type {
            type Fixture = #test_fixture;

            fn name(&self) -> &str {
                #name_string
            }

            #timeout

            #skip

            fn run(&self, fixture: std::sync::Arc<#test_fixture>) -> impl std::future::Future<Output = ()> + Send + 'static {
                async move {
                    #name(#(std::sync::Arc::clone(&fixture.#fixtures_range)),*).await;
                }
            }
        }

        #[linkme::distributed_slice(#group_tests)]
        fn #test_register() -> Box<dyn e2etest::RunTest> {
            Box::new(#test_type)
        }

        #[async_backtrace::framed]
        #run
    };

    Ok(expanded)
}
