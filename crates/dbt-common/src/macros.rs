pub use humantime as _vendored_human_time;

/// fs_err!(code,msg) construct a user-facing [FsError], to be used for further
/// processing, e.g. typically used in `.map_err(|_| fs_err!(..))`, etc
#[macro_export]
macro_rules! fs_err {
    ($code:expr, $($arg:tt)*) => {
        Box::new($crate::FsError::new(
            $code,
            format!($($arg)*),
        ))
    };
    (code => $code:expr, loc => $location:expr, $($arg:tt)*) => {
        Box::new($crate::FsError::new(
            $code,
            format!($($arg)*),
        ).with_location($location))
    };
    (code => $code:expr, hacky_yml_loc => $location:expr, $($arg:tt)*) => {
        Box::new($crate::FsError::new(
            $code,
            format!($($arg)*),
        ).with_hacky_yml_location($location))
    }
}

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

/// err! constructs a user-facing [FsError] and immediately wrap it in an `Err`
/// variant of a `Result`, typically used in `return err!(...)`, etc
#[macro_export]
macro_rules! err {
    ($code:expr, $($arg:tt)*) => {
        Err($crate::fs_err!($code, $($arg)*))
    };
    (code => $code:expr, loc => $location:expr, $($arg:tt)*) => {
        Err($crate::fs_err!(code => $code, loc => $location, $($arg)*))
    };
    (code => $code:expr, hacky_yml_loc => $location:expr, $($arg:tt)*) => {
        Err($crate::fs_err!(code => $code, hacky_yml_loc => $location, $($arg)*))
    }
}

#[macro_export]
macro_rules! unexpected_err {
    ($($arg:tt)*) => {
        Err($crate::unexpected_fs_err!($($arg)*))
    }
}

#[macro_export]
macro_rules! unexpected_fs_err {
    ($($arg:tt)*) => {
        Box::new($crate::FsError::new_with_forced_backtrace(
            $crate::ErrorCode::Unexpected,
            format!($($arg)*),
        ))
    }
}

#[macro_export]
macro_rules! not_implemented_err {
    ($($arg:tt)*) => {
        Err($crate::not_implemented_fs_err!($($arg)*))
    }
}

#[macro_export]
macro_rules! not_implemented_fs_err {
    ($($arg:tt)*) => {
        Box::new($crate::FsError::new(
            $crate::ErrorCode::NotImplemented,
            format!($($arg)*),
        ))
    }
}

#[macro_export]
macro_rules! ectx {
    (code => $code:expr, loc => $location:expr $(,)? ) => {
        || $crate::ErrContext {
            code: Some($code),
            location: Some($location),
            context: None,
        }
    };
    (code => $code:expr, loc => $location:expr, $($arg:tt)*) => {
        || $crate::ErrContext {
            code: Some($code),
            location: Some($location),
            context: Some(format!($($arg)*)),
        }
    };
    (code => $code:expr, $($arg:tt)*) => {
        || $crate::ErrContext {
            code: Some($code),
            location: None,
            context: Some(format!($($arg)*)),
        }
    };
    (loc => $location:expr) => {
        || $crate::ErrContext {
            code: None,
            location: Some($location),
            context: None,
        }
    };
    (loc => $location:expr, $($arg:tt)*) => {
        || $crate::ErrContext {
            code: None,
            location: Some($location),
            context: Some(format!($($arg)*)),
        }
    };
    (code => $code:expr) => {
        || $crate::ErrContext {
            code: Some($code),
            location: None,
            context: None,
        }
    };
    ($($arg:tt)*) => {
        || $crate::ErrContext {
            code: None,
            location: None,
            context: Some(format!($($arg)*)),
        }
    };
}

// ------------------------------------------------------------------------------------------------
// The following macros are logging related. They assume that the io args has the function:
// should_show(option: ShowOptions) -> bool
// logger is initialized by init_logger and will specify the output destination and format
// ------------------------------------------------------------------------------------------------

#[macro_export]
macro_rules! show_result {
    ( $io:expr, $option:expr, $artifact:expr) => {{
        use $crate::io_args::ShowOptions;
        use dbt_common::constants::INLINE_NODE;
        use serde_json::json;
        if $io.should_show($option) {
            let output = format!("\n{}", $artifact);
            // this preview field and name is used by the dbt-cloud CLI to display the result
            $crate::_log!(
                $crate::macros::log_adapter::log::Level::Info,
                name= "ShowNode",
                data:serde = json!({ "preview": $artifact.to_string(), "unique_id": INLINE_NODE });
                "{}", output
            );
        }
    }};
}

#[macro_export]
macro_rules! show_error_result {
    ( $io:expr, $artifact:expr) => {{
        use $crate::io_args::ShowOptions;
        use dbt_common::constants::INLINE_NODE;
        use serde_json::json;
        let output = format!("\n{}", $artifact);
        // this preview field and name is used by the dbt-cloud CLI to display the result
        $crate::_log!(
            $crate::macros::log_adapter::log::Level::Info,
            name= "ShowNode",
            data:serde = json!({ "preview": $artifact.to_string(), "unique_id": INLINE_NODE });
            "{}", output
        );

    }};
}

#[macro_export]
macro_rules! show_result_with_default_title {
    ( $io:expr, $option:expr, $artifact:expr) => {{
        use $crate::io_args::ShowOptions;
        if $io.should_show($option) {
            let output = format!("\n{}\n{}", $option.title(), $artifact);
            $crate::_log!($crate::macros::log_adapter::log::Level::Info, "{}", output);
        }
    }};
}

#[macro_export]
macro_rules! show_list_result_with_default_title {
    ( $io:expr, $option:expr, $list_result:expr) => {{
        use $crate::io_args::ShowOptions;
        use $crate::pretty_string::BLUE;
        if $io.should_show($option) {
            $crate::_log!(
                $crate::macros::log_adapter::log::Level::Info,
                "\n{}",
                BLUE.apply_to($option.title())
            );

            for item in $list_result {
                $crate::_log!(
                    $crate::macros::log_adapter::log::Level::Info,
                    name = "PrintEvent",
                    code = "Z052";
                    "{}",
                    item
                );
            }
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
        use $crate::logging::{FsInfo, LogEvent};


        if let Some(reporter) = &$io.status_reporter {
            reporter.show_progress($info.event.action().as_str(), &$info.target, $info.desc.as_deref());
        }

        if (
            ($io.should_show(ShowOptions::Progress) && $info.is_phase_unknown())
            || ($io.should_show(ShowOptions::ProgressRun) && $info.is_phase_run())
            || ($io.should_show(ShowOptions::ProgressCompile) && $info.is_phase_compile())
            || ($io.should_show(ShowOptions::ProgressParse) && $info.is_phase_parse())
        )
            // Do not show parse/compile generic tests
            && !($info.target.contains(dbt_common::constants::DBT_GENERIC_TESTS_DIR_NAME)
                && ($info.event.action().as_str().contains(dbt_common::constants::PARSING)
                    || $info.event.action().as_str().contains(dbt_common::constants::COMPILING)))
        {
            let output = pretty_green($info.event.action().as_str(), &$info.target, $info.desc.as_deref());
            let log_config = $info.event;
            if let Some(data_json) = $info.data {
                $crate::_log!(log_config.level(), name = log_config.name(), data:serde = data_json; "{}", output);
            } else {
                $crate::_log!(log_config.level(), name = log_config.name(); "{}", output);
            }
        }
    }};
}

#[macro_export]
macro_rules! show_warning {
    ($io:expr, $err:expr) => {{
        use $crate::constants::WARNING;
        use $crate::error_counter::increment_warning_counter;
        use $crate::pretty_string::{color_quotes, YELLOW};
        increment_warning_counter(&$io.invocation_id.to_string());

        let err = $err;
        if let Some(status_reporter) = &$io.status_reporter {
            status_reporter.collect_warning(&err);
        }

        $crate::_log!(
            $crate::macros::log_adapter::log::Level::Warn,
            code = err.code.to_string();
            "{} {}",
            YELLOW.apply_to(WARNING),
            color_quotes(err.pretty().as_str())
        );
    }};

    ( $io:expr, info => $info:expr) => {{
        use $crate::io_args::ShowOptions;
        use $crate::pretty_string::pretty_yellow;
        use $crate::logging::{FsInfo, LogEvent};

        if $io.should_show(ShowOptions::Progress)
            // Do not show parse generic tests
        {
            let output = pretty_yellow($info.event.action().as_str(), &$info.target, $info.desc.as_deref());
            let log_config = $info.event;
            if let Some(data_json) = $info.data {
                $crate::_log!(log_config.level(), name = log_config.name(), data:serde = data_json; "{}", output);
            } else {
                $crate::_log!(log_config.level(), name = log_config.name(); "{}", output);
            }
        }
    }};
}

#[macro_export]
macro_rules! show_warning_soon_to_be_error {
    ($io:expr, $err:expr) => {{
        use $crate::constants::WARNING;
        use $crate::error_counter::{increment_warning_counter, increment_autofix_suggestion_counter};
        use $crate::pretty_string::{color_quotes, YELLOW, RED};
        increment_warning_counter(&$io.invocation_id.to_string());
        increment_autofix_suggestion_counter(&$io.invocation_id.to_string());

        let err = $err;
        if let Some(status_reporter) = &$io.status_reporter {
            status_reporter.collect_warning(&err);
        }

        $crate::_log!(
            $crate::macros::log_adapter::log::Level::Warn,
            code = err.code.to_string();
            "{} {} {}",
            YELLOW.apply_to(WARNING),
            "(will error post beta)",
            color_quotes(err.pretty().as_str())
        );
    }};

    ( $io:expr, info => $info:expr) => {{
        use $crate::io_args::ShowOptions;
        use $crate::pretty_string::pretty_yellow;
        use $crate::logging::{FsInfo, LogEvent};
        use $crate::error_counter::increment_autofix_suggestion_counter;
        increment_autofix_suggestion_counter(&$io.invocation_id.to_string());

        if $io.should_show(ShowOptions::Progress)
            // Do not show parse generic tests
        {
            let output = pretty_yellow($info.event.action().as_str(), &$info.target, $info.desc.as_deref());
            let log_config = $info.event;
            if let Some(data_json) = $info.data {
                $crate::_log!(log_config.level(), name = log_config.name(), data:serde = data_json; "{}", output);
            } else {
                $crate::_log!(log_config.level(), name = log_config.name(); "{}", output);
            }
        }
    }};
}

#[macro_export]
macro_rules! show_error {
    ($io:expr,$err:expr) => {{
        use std::path::Path;
        use $crate::constants::ERROR;
        use $crate::error_counter::increment_error_counter;
        use $crate::pretty_string::color_quotes;
        use $crate::pretty_string::RED;
        use $crate::FsError;
        increment_error_counter(&$io.invocation_id.to_string());
        // clean up the path before showing the error
        let mut err = $err;
        if let Some(status_reporter) = &$io.status_reporter {
            status_reporter.collect_error(&err);
        }

        $crate::_log!(
            $crate::macros::log_adapter::log::Level::Error,
            code = err.code.to_string();
            "{} {}",
            RED.apply_to(ERROR),
            color_quotes(err.pretty().as_str())
        );
    }};

    ( $io:expr, info => $info:expr) => {{
        use $crate::io_args::ShowOptions;
        use $crate::pretty_string::pretty_red;
        use $crate::logging::{FsInfo, LogEvent};

        if $io.should_show(ShowOptions::ProgressRun)
            // Do not show parse generic tests
        {
            let output = pretty_red($info.event.action().as_str(), &$info.target, $info.desc.as_deref());
            let log_config = $info.event;
            if let Some(data_json) = $info.data {
                $crate::_log!(log_config.level(), name = log_config.name(), data:serde = data_json; "{}", output);
            } else {
                $crate::_log!(log_config.level(), name = log_config.name(); "{}", output);
            }
        }
    }};
}

#[macro_export]
macro_rules! show_fail {
    ($io:expr,$fmt:expr) => {{
        use $crate::error_counter::increment_error_counter;
        increment_error_counter(&$io.invocation_id.to_string());

        $crate::_log!(
            $crate::macros::log_adapter::log::Level::Error,
            code = $crate::ErrorCode::Generic.to_string();
            "{}",
            $fmt
        );
    }};
}

#[macro_export]
macro_rules! show_autofix_suggestion {
    ($io:expr) => {{
        use dbt_common::show_warning;
        use $crate::pretty_string::BLUE;
        use $crate::FsError;

        show_warning!(
            $io,
            $crate::FsError::new(
                $crate::ErrorCode::Generic,
                "Warnings marked (will error post beta) will turn into errors before leaving beta. Please fix them."
            )
        );
        $crate::_log!(
            $crate::macros::log_adapter::log::Level::Info,
            "{} Try the autofix script: {}",
            BLUE.apply_to("suggestion:"),
            BLUE.apply_to("https://github.com/dbt-labs/dbt-autofix")
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

// ------------------------------------------------------------------------------------------------
// The following macros depend on this type
// pub struct Args {
//     pub io: IoArgs,
//     pub target: String
//     pub command: String,
//     pub from_main: bool
// }
// ------------------------------------------------------------------------------------------------
#[macro_export]
macro_rules! show_progress_exit {
    ( $arg:expr, $start_time:expr) => {{
        use $crate::io_args::ShowOptions;
        use $crate::constants::FINISHED;
        use $crate::error_counter::{get_autofix_suggestion_counter, get_error_counter, get_warning_counter};
        use $crate::macros::_vendored_human_time::{format_duration, FormattedDuration};
        use $crate::pretty_string::color_quotes;
        use $crate::pretty_string::{GREEN, RED, YELLOW};
        use $crate::FsError;
        use serde_json::json;
        let e_ct = get_error_counter(&$arg.io.invocation_id.to_string());
        let w_ct = get_warning_counter(&$arg.io.invocation_id.to_string());
        let a_ct = get_autofix_suggestion_counter(&$arg.io.invocation_id.to_string());
        let e_msg = RED.apply_to(if e_ct == 1 { "error" } else { "errors" });
        let w_msg = YELLOW.apply_to(if w_ct == 1 { "warning" } else { "warnings" });

        // Show autofix suggestion if there are any autofix suggestions
        if a_ct > 0 {
            $crate::show_autofix_suggestion!($arg.io);
        }

        let (action, msg) = if e_ct > 0 && w_ct > 0 {
            (
                RED.apply_to(FINISHED),
                format!(" with {} {} and {} {}", e_ct, e_msg, w_ct, w_msg),
            )
        } else if e_ct > 0 {
            (RED.apply_to(FINISHED), format!(" with {} {}", e_ct, e_msg))
        } else if w_ct > 0 {
            (
                YELLOW.apply_to(FINISHED),
                format!(" with {} {}", w_ct, w_msg),
            )
        } else {
            (GREEN.apply_to(FINISHED), "".to_owned())
        };
        let duration = if $arg.from_main {
            let duration = format_duration($start_time.elapsed().unwrap()).to_string();

            format!(" in {}", duration)
        } else {
            "".to_owned()
        };
        let target = match &$arg.target {
            Some(target) => color_quotes(&format!("target '{}'", target)),
            None => "".to_owned(),
        };

        let output = format!(
            "{} '{}' {}{}{}",
            action, &$arg.command, target, msg, duration
        );
        if $arg.io.show.contains(&ShowOptions::ProgressRun) || e_ct > 0 {
            let elapsed = $start_time.elapsed().unwrap().as_secs_f32();
            let completed_at = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.6f").to_string();
            log::info!(elapsed = elapsed, name = "CommandCompleted", data:serde = json!({"completed_at": completed_at, "elapsed": elapsed, "success": e_ct == 0}); "{}", output);
        }

        Ok::<i32, Box<FsError>>(if e_ct == 0 { 0 } else { 1 })
    }};
}

#[macro_export]
macro_rules! maybe_interactive_or_exit {
    ( $arg:expr, $start_time:expr, $resolver_state:expr, $db:expr, $jinja_env:expr, $instructions:expr) => {
        if !$arg.interactive {
            show_progress_exit!($arg, $start_time)
        } else {
            repl::run($resolver_state, &$arg, $db, $jinja_env, $instructions).await
        }
    };
}

#[macro_export]
macro_rules! checkpoint_maybe_exit {
    ( $phase:expr, $arg:expr, $start_time:expr ) => {
        if $arg.phase <= $phase
            || $crate::error_counter::get_error_counter($arg.io.invocation_id.to_string().as_str())
                > 0
        {
            return show_progress_exit!($arg, $start_time);
        }
    };
}

#[macro_export]
macro_rules! checkpoint_maybe_interactive_or_exit {
    ( $phase:expr, $arg:expr, $start_time:expr, $resolver_state:expr, $db:expr, $jinja_env:expr, $instructions:expr ) => {
        if $arg.phase <= $phase
            || $crate::error_counter::get_error_counter($arg.io.invocation_id.to_string().as_str())
                > 0
        {
            return maybe_interactive_or_exit!(
                $arg,
                $start_time,
                $resolver_state,
                $db,
                $jinja_env,
                $instructions
            );
        }
    };
}

#[macro_export]
macro_rules! check_cancellation {
    ($cancel_flag:expr) => {{
        if let Some(flag) = &$cancel_flag {
            if flag.load(std::sync::atomic::Ordering::Relaxed) {
                Err($crate::fs_err!(
                    $crate::ErrorCode::OperationCanceled,
                    "Operation cancelled"
                ))
            } else {
                Ok(())
            }
        } else {
            Ok(())
        }
    }};
    (flag: $cancel_flag:expr) => {{
        if $cancel_flag.load(std::sync::atomic::Ordering::Relaxed) {
            Err($crate::fs_err!(
                $crate::ErrorCode::OperationCanceled,
                "Operation cancelled"
            ))
        } else {
            Ok(())
        }
    }};
    ($cancel_flag:expr, $message:expr) => {{
        if let Some(flag) = &$cancel_flag {
            if flag.load(std::sync::atomic::Ordering::Relaxed) {
                Err($crate::fs_err!(
                    $crate::ErrorCode::OperationCanceled,
                    $message
                ))
            } else {
                Ok(())
            }
        } else {
            Ok(())
        }
    }};
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
