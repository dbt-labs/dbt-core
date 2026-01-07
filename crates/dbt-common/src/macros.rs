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
