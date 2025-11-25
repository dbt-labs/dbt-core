/// fsinfo! constructs an FsInfo struct with optional data and desc fields
#[macro_export]
macro_rules! fsinfo {
    // Basic version with just event and target
    ($event:expr, $target:expr) => {
        $crate::logging::FsInfo {
            event: $event,
            target: $target,
            data: None,
            desc: None,
        }
    };
    // Version with desc
    ($event:expr, $target:expr, desc = $desc:expr) => {
        $crate::logging::FsInfo {
            event: $event,
            target: $target,
            data: None,
            desc: Some($desc),
        }
    };
    // Version with data
    ($event:expr, $target:expr, data = $data:expr) => {
        $crate::logging::FsInfo {
            event: $event,
            target: $target,
            data: Some($data),
            desc: None,
        }
    };
    // Version with both data and desc
    ($event:expr, $target:expr, data = $data:expr, desc = $desc:expr) => {
        $crate::logging::FsInfo {
            event: $event,
            target: $target,
            data: Some($data),
            desc: Some($desc),
        }
    };
}

// ------------------------------------------------------------------------------------------------
// The following macros are logging related. They assume that the io args has the function:
// should_show(option: ShowOptions) -> bool
// logger is initialized by init_logger and will specify the output destination and format
// ------------------------------------------------------------------------------------------------

#[macro_export]
macro_rules! show_result_with_default_title {
    ( $io:expr, $option:expr, $artifact:expr) => {{
        use $crate::io_args::ShowOptions;
        if $io.should_show($option) {
            let output = format!("\n{}\n{}", $option.title(), $artifact);
            $crate::_log!(
                $crate::macros::log_adapter::log::Level::Info,
                _INVOCATION_ID_ = $io.invocation_id.as_u128();
                "{}",
                 output
            );
        }
    }};
}

#[macro_export]
macro_rules! show_result_with_title {
    ( $io:expr, $option:expr, $title: expr, $artifact:expr) => {{
        use $crate::io_args::ShowOptions;
        use $crate::pretty_string::BLUE;
            use dbt_common::constants::INLINE_NODE;
        use serde_json::json;
        if $io.should_show($option) {
            let output = format!("\n{}\n{}", $title, $artifact);
            $crate::_log!(
                $crate::macros::log_adapter::log::Level::Info,
                _INVOCATION_ID_ = $io.invocation_id.as_u128(),
                name= "ShowNode",
                data:serde = json!({ "preview": $artifact.to_string(), "unique_id": INLINE_NODE });
                "{}", output
            );
        }
    }};
}

#[macro_export]
macro_rules! show_progress {
    ( $io:expr, $info:expr) => {{
        use $crate::io_args::ShowOptions;
        use $crate::pretty_string::pretty_green;
        use $crate::logging::{FsInfo, LogEvent, LogFormat};

        if !$info.is_phase_completed() {

            if let Some(reporter) = &$io.status_reporter {
                reporter.show_progress($info.event.action().as_str(), &$info.target, $info.desc.as_deref());
            }

            // This whole macro became entirely unweldy, the following condition is a VERY
            // temporary bandaid fix for a regression where JSON output was not being emitted
            // for certain progress events. The entire macro is expected to be removed
            // after migration to new tracing-based logging is complete.
            let should_emit_json_event = $io.log_format == LogFormat::Json && ($info.is_phase_render()
                || $info.is_phase_run());

            // TODO: these filtering conditions should be moved to the logger side
            if (
                ($io.should_show(ShowOptions::Progress) && $info.is_phase_unknown())
                || ($io.should_show(ShowOptions::ProgressHydrate) && $info.is_phase_hydrate())
                || ($io.should_show(ShowOptions::ProgressParse) && $info.is_phase_parse())
                || ($io.should_show(ShowOptions::ProgressRender) && $info.is_phase_render())
                || ($io.should_show(ShowOptions::ProgressAnalyze) && $info.is_phase_analyze())
                || ($io.should_show(ShowOptions::ProgressRun) && $info.is_phase_run())
                || should_emit_json_event
            )
                // Do not show parse/compile generic tests
                && !($info.target.contains(dbt_common::constants::DBT_GENERIC_TESTS_DIR_NAME)
                    && ($info.event.action().as_str().contains(dbt_common::constants::PARSING)
                        || $info.event.action().as_str().contains(dbt_common::constants::RENDERING)
                        || $info.event.action().as_str().contains(dbt_common::constants::ANALYZING)))
            {
                let output = pretty_green($info.event.action().as_str(), &$info.target, $info.desc.as_deref());
                let event = $info.event;
                if let Some(data_json) = $info.data {
                    $crate::_log!(event.level(),
                        _INVOCATION_ID_ = $io.invocation_id.as_u128(),
                        _TRACING_HANDLED_ = true,
                        name = event.name(), data:serde = data_json;
                        "{}", output
                    );
                } else {
                    $crate::_log!(event.level(),
                        _INVOCATION_ID_ = $io.invocation_id.as_u128(),
                        _TRACING_HANDLED_ = true,
                        name = event.name();
                        "{}", output
                    );
                }
            }
        }
    }};
}

#[macro_export]
macro_rules! show_info {
    ( $io:expr, $info:expr) => {{
        use $crate::io_args::ShowOptions;
        use $crate::pretty_string::pretty_green;
        use $crate::logging::{FsInfo, LogEvent};


        if let Some(reporter) = &$io.status_reporter {
            reporter.show_progress($info.event.action().as_str(), &$info.target, $info.desc.as_deref());
        }

        // TODO: these filtering conditions should be moved to the logger side
        if (
            ($io.should_show(ShowOptions::Progress) && $info.is_phase_unknown())
            || ($io.should_show(ShowOptions::ProgressParse) && $info.is_phase_parse())
            || ($io.should_show(ShowOptions::ProgressRender) && $info.is_phase_render())
            || ($io.should_show(ShowOptions::ProgressAnalyze) && $info.is_phase_analyze())
            || ($io.should_show(ShowOptions::ProgressRun) && $info.is_phase_run())
        )
            // Do not show parse/compile generic tests
            && !($info.target.contains(dbt_common::constants::DBT_GENERIC_TESTS_DIR_NAME)
                && ($info.event.action().as_str().contains(dbt_common::constants::PARSING)
                    || $info.event.action().as_str().contains(dbt_common::constants::RENDERING)
                    || $info.event.action().as_str().contains(dbt_common::constants::ANALYZING)))
        {
            let output = pretty_green($info.event.action().as_str(), &$info.target, $info.desc.as_deref());
            let event = $info.event;
            if let Some(data_json) = $info.data {
                $crate::_log!(event.level(),
                    _INVOCATION_ID_ = $io.invocation_id.as_u128(),
                    name = event.name(), data:serde = data_json;
                     "{}", output
                );
            } else {
                $crate::_log!(event.level(),
                    _INVOCATION_ID_ = $io.invocation_id.as_u128(),
                    name = event.name();
                     "{}", output
                );
            }
        }
    }};
}

#[macro_export]
/// Display a progress bar or spinner with optional context items.
///
/// Each progress bar or spinner must have a unique identifier (`uid`), which is
/// a string that is displayed as a prefix to the left of the progress bar or
/// spinner. It is the caller's responsibility to ensure that the `uid` is
/// unique -- only a single progress bar or spinner with a given `uid` will be
/// displayed at a time, if a bar or spinner with a given `uid` is already
/// displayed, subsequent calls to this macro with the same `uid` will be
/// silently ignored.
///
/// When a progress bar or spinner is active, it can be associated with context
/// items that provide additional information about the progress being made.
/// Context items will be displayed as a list of items on the right side of the
/// progress bar or spinner, as much as space allows.
///
/// All variants of this macro returns a scope guard that will automatically
/// remove the progress bar or item when it goes out of scope.
macro_rules! with_progress {

    // Start a new spinner
    ($io:expr, spinner => $uid:expr ) => {{
        use $crate::logging::ProgressBarGuard;
        use $crate::logging::TermEvent;

        $crate::_log!(
            $crate::macros::log_adapter::log::Level::Info,
            _INVOCATION_ID_ = $io.invocation_id.as_u128(),
            _TERM_ONLY_ = true,
            _TERM_EVENT_:serde = TermEvent::start_spinner($uid.into());

            "Starting spinner with uid: {}",
            $uid
        );
        ProgressBarGuard::new(
            $io.invocation_id.as_u128(),
            TermEvent::remove_spinner($uid.into())
        )
    }};

    // Add a context item to the spinner
    ($io:expr, spinner => $uid:expr, item => $item:expr ) => {{
        use $crate::logging::ProgressBarGuard;
        use $crate::logging::TermEvent;

        $crate::_log!(
            $crate::macros::log_adapter::log::Level::Info,
            _INVOCATION_ID_ = $io.invocation_id.as_u128(),
            _TERM_ONLY_ = true,
            _TERM_EVENT_:serde = TermEvent::add_spinner_context_item($uid.into(), $item.into());

            "Starting item: {} on spinner: {}",
            $item, $uid
        );
        ProgressBarGuard::new(
            $io.invocation_id.as_u128(),
            TermEvent::finish_spinner_context_item($uid.into(), $item.into())
        )
    }};

    // Start a new progress bar with a total length
    ($io:expr, bar => $uid:expr, length => $total:expr ) => {{
        use $crate::logging::ProgressBarGuard;
        use $crate::logging::TermEvent;

        $crate::_log!(
            $crate::macros::log_adapter::log::Level::Info,
            _INVOCATION_ID_ = $io.invocation_id.as_u128(),
            _TERM_ONLY_ = true,
            _TERM_EVENT_:serde = TermEvent::start_bar($uid.into(), $total as u64);

            "Starting progress bar with uid: {}, total: {}",
            $uid, $total
        );
        ProgressBarGuard::new(
            $io.invocation_id.as_u128(),
            TermEvent::remove_bar($uid.into())
        )
    }};

    // Add a context item to the progress bar and increment the progress bar by
    // one
    ($io:expr, bar => $uid:expr, item => $item:expr ) => {{
        use $crate::logging::ProgressBarGuard;
        use $crate::logging::TermEvent;

        $crate::_log!(
            $crate::macros::log_adapter::log::Level::Info,
            _INVOCATION_ID_ = $io.invocation_id.as_u128(),
            _TERM_ONLY_ = true,
            _TERM_EVENT_:serde = TermEvent::add_bar_context_item($uid.into(), $item.into());

            "Starting item: {} on progress bar: {}",
            $item, $uid
        );
        ProgressBarGuard::new(
            $io.invocation_id.as_u128(),
            TermEvent::finish_bar_context_item($uid.into(), $item.into(), None)
        )
    }};
}

#[macro_export]
/// Show a new progress bar or spinner, or add an in-progress item to an
/// existing one
macro_rules! start_progress {

    ($io:expr, spinner => $uid:expr) => {{
        use $crate::logging::TermEvent;

        $crate::_log!(
            $crate::macros::log_adapter::log::Level::Info,
            _INVOCATION_ID_ = $io.invocation_id.as_u128(),
            _TERM_ONLY_ = true,
            _TERM_EVENT_:serde = TermEvent::start_spinner($uid.into());

            "Starting spinner with uid: {}",
            $uid
        );
    }};

    ($io:expr, bar => $uid:expr, length => $total:expr) => {{
        use $crate::logging::TermEvent;

        $crate::_log!(
            $crate::macros::log_adapter::log::Level::Info,
            _INVOCATION_ID_ = $io.invocation_id.as_u128(),
            _TERM_ONLY_ = true,
            _TERM_EVENT_:serde = TermEvent::start_bar($uid.into(), $total.into());

            "Starting progress bar with uid: {}, total: {}",
            $uid, $total
        );
    }};

    ($io:expr, spinner => $uid:expr, item => $item:expr) => {{
        use $crate::logging::TermEvent;

        $crate::_log!(
            $crate::macros::log_adapter::log::Level::Info,
            _INVOCATION_ID_ = $io.invocation_id.as_u128(),
            _TERM_ONLY_ = true,
            _TERM_EVENT_:serde = TermEvent::add_spinner_context_item($uid.into(), $item.into());

            "Updating progress for uid: {}, item: {}",
            $uid, $item
        );
    }};

    ($io:expr, bar => $uid:expr, item => $item:expr) => {{
        use $crate::logging::TermEvent;

        $crate::_log!(
            $crate::macros::log_adapter::log::Level::Info,
            _INVOCATION_ID_ = $io.invocation_id.as_u128(),
            _TERM_ONLY_ = true,
            _TERM_EVENT_:serde = TermEvent::add_bar_context_item($uid.into(), $item.into());

            "Updating progress for uid: {}, item: {}",
            $uid, $item
        );
    }};
}

#[macro_export]
macro_rules! finish_progress {
    ($io:expr, spinner => $uid:expr) => {{
        use $crate::logging::TermEvent;

        $crate::_log!(
            $crate::macros::log_adapter::log::Level::Info,
            _INVOCATION_ID_ = $io.invocation_id.as_u128(),
            _TERM_ONLY_ = true,
            _TERM_EVENT_:serde = TermEvent::remove_spinner($uid.into());

            "Finishing spinner with uid: {}",
            $uid
        );
    }};

    ($io:expr, bar => $uid:expr) => {{
        use $crate::logging::TermEvent;

        $crate::_log!(
            $crate::macros::log_adapter::log::Level::Info,
            _INVOCATION_ID_ = $io.invocation_id.as_u128(),
            _TERM_ONLY_ = true,
            _TERM_EVENT_:serde = TermEvent::remove_bar($uid.into());

            "Finishing progress bar with uid: {}",
            $uid
        );
    }};

    ($io:expr, bar => $uid:expr, item => $item:expr, outcome => $outcome:expr) => {{
        use $crate::logging::TermEvent;
        use $crate::logging::StatEvent;

        $crate::_log!(
            $crate::macros::log_adapter::log::Level::Info,
            _INVOCATION_ID_ = $io.invocation_id.as_u128(),
            _TERM_ONLY_ = true,
            _STAT_EVENT_:serde = $crate::logging::StatEvent::counter(
                $outcome,
                1
            ),
            _TERM_EVENT_:serde = TermEvent::finish_bar_context_item($uid.into(), $item.into());

            "Finishing item: {} on progress bar: {}",
            $item, $uid
        );
    }};

    ($io:expr, spinner => $uid:expr, outcome => $outcome:expr) => {{
        use $crate::logging::TermEvent;
        use $crate::logging::StatEvent;

        $crate::_log!(
            $crate::macros::log_adapter::log::Level::Info,
            _INVOCATION_ID_ = $io.invocation_id.as_u128(),
            _TERM_ONLY_ = true,
            _STAT_EVENT_:serde = StatEvent::counter(
                $outcome.into(),
                1
            ),
            _TERM_EVENT_:serde = TermEvent::finish_spinner_context_item($uid.into(), "".into());

            "Finishing spinner with uid: {}, outcome: {}",
            $uid, $outcome
        );
    }};

}

// --------------------------------------------------------------------------------------------------

/// Returns the fully qualified name of the current function.
#[macro_export]
macro_rules! current_function_name {
    () => {{
        fn f() {}
        fn type_name_of_val<T>(_: T) -> &'static str {
            ::std::any::type_name::<T>()
        }
        let mut name = type_name_of_val(f).strip_suffix("::f").unwrap_or("");
        while let Some(rest) = name.strip_suffix("::{{closure}}") {
            name = rest;
        }
        name
    }};
}

/// Returns just the name of the current function without the module path.
#[macro_export]
macro_rules! current_function_short_name {
    () => {{
        fn f() {}
        fn type_name_of_val<T>(_: T) -> &'static str {
            ::std::any::type_name::<T>()
        }
        let mut name = type_name_of_val(f).strip_suffix("::f").unwrap_or("");
        // If this macro is used in a closure, the last path segment will be {{closure}}
        // but we want to ignore it
        // Caveat: for example, this is the case if you use this macro in a a async test function annotated with #[tokio::test]
        while let Some(rest) = name.strip_suffix("::{{closure}}") {
            name = rest;
        }
        name.split("::").last().unwrap_or("")
    }};
}

/// Returns the path to the crate of the caller
#[macro_export]
macro_rules! this_crate_path {
    () => {
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    };
}

#[cfg(test)]
mod tests {
    // top-level function test
    fn test_function_1() -> &'static str {
        current_function_short_name!()
    }

    mod nested {
        pub fn test_nested_function() -> &'static str {
            current_function_short_name!()
        }
    }

    #[test]
    fn test_current_function_short_name() {
        assert_eq!(test_function_1(), "test_function_1");
        assert_eq!(nested::test_nested_function(), "test_nested_function");

        let closure = || current_function_short_name!();
        assert_eq!(closure(), "test_current_function_short_name");
    }

    // top-level function test
    fn test_function_2() -> &'static str {
        current_function_name!()
    }

    #[test]
    fn test_current_function_name() {
        assert_eq!(
            test_function_2(),
            "dbt_common::macros::tests::test_function_2"
        );

        // test closure
        let closure: fn() -> &'static str = || current_function_name!();
        let closure_name = closure();
        assert_eq!(
            closure_name,
            "dbt_common::macros::tests::test_current_function_name"
        );
    }
}

/// This module contains a workaround for
///
///     non-primitive cast: `&[(&str, Value<'_>); 1]` as `&[(&str, Value<'_>)]`rust-analyzer(E0605)
///
/// TODO: remove this once the issue is fixed in upstream (either by 'rust-analyzer', or by 'log' crate)
#[macro_use]
pub mod log_adapter {
    pub use log;

    #[macro_export]
    #[clippy::format_args]
    macro_rules! _log {
        // log!(logger: my_logger, target: "my_target", Level::Info, "a {} event", "log");
        (logger: $logger:expr, target: $target:expr, $lvl:expr, $($arg:tt)+) => ({
            $crate::__log!(
                logger: $crate::macros::log_adapter::log::__log_logger!($logger),
                target: $target,
                $lvl,
                $($arg)+
            )
        });

        // log!(logger: my_logger, Level::Info, "a log event")
        (logger: $logger:expr, $lvl:expr, $($arg:tt)+) => ({
            $crate::__log!(
                logger: $crate::macros::log_adapter::log::__log_logger!($logger),
                target: $crate::macros::log_adapter::log::__private_api::module_path!(),
                $lvl,
                $($arg)+
            )
        });

        // log!(target: "my_target", Level::Info, "a log event")
        (target: $target:expr, $lvl:expr, $($arg:tt)+) => ({
            $crate::__log!(
                logger: $crate::macros::log_adapter::log::__log_logger!(__log_global_logger),
                target: $target,
                $lvl,
                $($arg)+
            )
        });

        // log!(Level::Info, "a log event")
        ($lvl:expr, $($arg:tt)+) => ({
            $crate::__log!(
                logger: $crate::macros::log_adapter::log::__log_logger!(__log_global_logger),
                target: $crate::macros::log_adapter::log::__private_api::module_path!(),
                $lvl,
                $($arg)+
            )
        });
    }

    #[doc(hidden)]
    #[macro_export]
    macro_rules! __log {
        // log!(logger: my_logger, target: "my_target", Level::Info, key1:? = 42, key2 = true; "a {} event", "log");
        (logger: $logger:expr, target: $target:expr, $lvl:expr, $($key:tt $(:$capture:tt)? $(= $value:expr)?),+; $($arg:tt)+) => ({
            let lvl = $lvl;
            if lvl <= $crate::macros::log_adapter::log::STATIC_MAX_LEVEL && lvl <= $crate::macros::log_adapter::log::max_level() {
                $crate::macros::log_adapter::log::__private_api::log(
                    $logger,
                    format_args!($($arg)+),
                    lvl,
                    &($target, $crate::macros::log_adapter::log::__private_api::module_path!(), $crate::macros::log_adapter::log::__private_api::loc()),
                    [$(($crate::macros::log_adapter::log::__log_key!($key), $crate::macros::log_adapter::log::__log_value!($key $(:$capture)* = $($value)*))),+].as_slice(),
                );
            }
        });

        // log!(logger: my_logger, target: "my_target", Level::Info, "a {} event", "log");
        (logger: $logger:expr, target: $target:expr, $lvl:expr, $($arg:tt)+) => ({
            let lvl = $lvl;
            if lvl <= $crate::macros::log_adapter::log::STATIC_MAX_LEVEL && lvl <= $crate::macros::log_adapter::log::max_level() {
                $crate::macros::log_adapter::log::__private_api::log(
                    $logger,
                    format_args!($($arg)+),
                    lvl,
                    &($target, $crate::macros::log_adapter::log::__private_api::module_path!(), $crate::macros::log_adapter::log::__private_api::loc()),
                    (),
                );
            }
        });
    }
}
