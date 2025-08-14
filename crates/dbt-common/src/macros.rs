pub use humantime as _vendored_human_time;

// Re-export dbt_error here such that downstream crates could use
// macros using dbt_error without having to add it as a dependency explicitly.
pub use dbt_error as _dbt_error;

// Re-export dbt-telemetry to allow using it in macros without requiring
// the call-site crate to declare it as a dependency explicitly
pub use dbt_telemetry as _dbt_telemetry;

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
macro_rules! show_result {
    ( $io:expr, $option:expr, $artifact:expr) => {
        $crate::show_result!($io, $option, $artifact, columns = Option::<&[String]>::None)
    };

    ( $io:expr, $option:expr, $artifact:expr, columns = $columns:expr) => {
        $crate::show_result!($io, $option, $artifact, columns = $columns, unique_id = Option::<&str>::None)
    };

    ( $io:expr, $option:expr, $artifact:expr, columns = $columns:expr, unique_id = $unique_id:expr) => {{
        use $crate::io_args::ShowOptions;
        use dbt_common::constants::INLINE_NODE;
        use serde_json::json;
        if $io.should_show($option) {
            let output = format!("\n{}", $artifact);
            // this preview field and name is used by the dbt-cloud CLI to display the result
            let node_id = $unique_id.unwrap_or(INLINE_NODE);
            let mut data = json!({
                "preview": $artifact.to_string(),
                "unique_id": node_id
            });

            // columns can be used to show column names when the resultset is empty, eg.
            //   { "preview": "[]", "columns": ["column1", "column2"] }
            if let Some(cols) = $columns {
                data["columns"] = json!(cols);
            }

            $crate::_log!(
                $crate::macros::log_adapter::log::Level::Info,
                _INVOCATION_ID_ = $io.invocation_id.as_u128(),
                name= "ShowNode",
                data:serde = data;
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
            _INVOCATION_ID_ = $io.invocation_id.as_u128(),
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
                    _INVOCATION_ID_ = $io.invocation_id.as_u128(),
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

#[macro_export]
macro_rules! show_warning {
    ($io:expr, $err:expr) => {{
        use $crate::constants::WARNING;
        use $crate::error_counter::increment_warning_counter;
        use $crate::pretty_string::{color_quotes, YELLOW};
        increment_warning_counter(&$io.invocation_id.to_string());

        let err = $err;

        // New tracing based logic
        use $crate::tracing::{ToTracingValue, log_level_to_severity};
        use $crate::tracing::emit::_tracing::Level as TracingLevel;
        use $crate::tracing::metrics::{increment_metric, MetricKey};
        use $crate::tracing::constants::TRACING_ATTR_FIELD;
        use $crate::macros::_dbt_telemetry::{TelemetryAttributes, RecordCodeLocation};
        increment_metric(MetricKey::TotalWarnings, 1);

        let (original_severity_number, original_severity_text) = log_level_to_severity(&$crate::macros::log_adapter::log::Level::Warn);

        $crate::emit_tracing_event!(
            level: TracingLevel::WARN,
            TelemetryAttributes::Log {
                code: Some(err.code as u16 as u32),
                dbt_core_code: None,
                original_severity_number,
                original_severity_text: original_severity_text.to_string(),
                location: RecordCodeLocation::none(), // Will be auto injected
            },
            "{}",
            err.pretty().as_str()
        );

        if let Some(status_reporter) = &$io.status_reporter {
            status_reporter.collect_warning(&err);
        }

        $crate::_log!(
            $crate::macros::log_adapter::log::Level::Warn,
            _INVOCATION_ID_ = $io.invocation_id.as_u128(),
            _TRACING_HANDLED_ = true,
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
            _INVOCATION_ID_ = $io.invocation_id.as_u128(),
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
macro_rules! show_package_error {
    ($io:expr, $pkg:expr) => {{
        use $crate::constants::{WARNING, ERROR};
        use $crate::error_counter::{increment_error_counter, increment_warning_counter, has_package_with_error_or_warning, mark_package_with_error_or_warning, increment_autofix_suggestion_counter};
        use $crate::pretty_string::{color_quotes, YELLOW, RED};

        if !has_package_with_error_or_warning(&$io.invocation_id.to_string(), $pkg) {
            // Mark the package with an error or warning
            mark_package_with_error_or_warning(&$io.invocation_id.to_string(), $pkg);

            let err = $crate::fs_err!(
                $crate::error::ErrorCode::DependencyWarning,
                "Package `{}` issued one or more compatibility warnings. To display all warnings associated with this package, run with `--show-all-deprecations`.",
                $pkg
            );

            if let Some(status_reporter) = &$io.status_reporter {
                status_reporter.collect_warning(&err);
            }

            if std::env::var("_DBT_FUSION_STRICT_MODE").is_ok() {
                // Increment once per invocation
                increment_error_counter(&$io.invocation_id.to_string());

                $crate::_log!(
                    $crate::macros::log_adapter::log::Level::Error,
                    _INVOCATION_ID_ = $io.invocation_id.as_u128(),
                    code = err.code.to_string();
                    "{} {}",
                    RED.apply_to(ERROR),
                    color_quotes(err.pretty().as_str())
                );
            } else {
                // Increment once per invocation
                increment_warning_counter(&$io.invocation_id.to_string());
                increment_autofix_suggestion_counter(&$io.invocation_id.to_string());

                $crate::_log!(
                    $crate::macros::log_adapter::log::Level::Warn,
                    _INVOCATION_ID_ = $io.invocation_id.as_u128(),
                    code = err.code.to_string();
                    "{} {} {}",
                    YELLOW.apply_to(WARNING),
                    "(will error post beta)",
                    color_quotes(err.pretty().as_str())
                );
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
        use $crate::macros::_dbt_error::FsError;
        increment_error_counter(&$io.invocation_id.to_string());
        // clean up the path before showing the error
        let mut err = $err;
        if let Some(status_reporter) = &$io.status_reporter {
            status_reporter.collect_error(&err);
        }

        $crate::_log!(
            $crate::macros::log_adapter::log::Level::Error,
            _INVOCATION_ID_ = $io.invocation_id.as_u128(),
            code = err.code.to_string();
            "{} {}",
            RED.apply_to(ERROR),
            color_quotes(err.pretty().as_str())
        );
    }};

    ( $io:expr, info => $info:expr, increment_counter: $increment:expr) => {{
        use $crate::io_args::ShowOptions;
        use $crate::pretty_string::pretty_red;
        use $crate::error_counter::increment_error_counter;
        use $crate::logging::{FsInfo, LogEvent};
        if $increment {
            increment_error_counter(&$io.invocation_id.to_string());
        }
        if $io.should_show(ShowOptions::ProgressRun)
            // Do not show parse generic tests
        {
            let output = pretty_red($info.event.action().as_str(), &$info.target, $info.desc.as_deref());
            let log_config = $info.event;
            if let Some(data_json) = $info.data {
                $crate::_log!(
                    log_config.level(),
                    _INVOCATION_ID_ = $io.invocation_id.as_u128(),
                    name = log_config.name(),
                    data:serde = data_json; "{}", output
                );
            } else {
                $crate::_log!(
                    log_config.level(),
                    _INVOCATION_ID_ = $io.invocation_id.as_u128(),
                    name = log_config.name();
                     "{}", output
                );
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
            _INVOCATION_ID_ = $io.invocation_id.as_u128(),
            code = $crate::macros::_dbt_error::ErrorCode::Generic.to_string();
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
        use $crate::macros::_dbt_error::FsError;

        show_warning!(
            $io,
            $crate::macros::_dbt_error::FsError::new(
                $crate::macros::_dbt_error::ErrorCode::Generic,
                "Warnings marked (will error post beta) will turn into errors before leaving beta. Please fix them."
            )
        );
        $crate::_log!(
            $crate::macros::log_adapter::log::Level::Info,
            _INVOCATION_ID_ = $io.invocation_id.as_u128();
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
        use $crate::macros::_dbt_error::FsError;
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
    ( $arg:expr, $start_time:expr, $resolver_state:expr, $db:expr, $map_compiled_sql:expr, $jinja_env:expr, $token:expr) => {
        if !$arg.interactive {
            show_progress_exit!($arg, $start_time)
        } else {
            repl::run(
                $resolver_state,
                &$arg,
                $db,
                $map_compiled_sql,
                $jinja_env,
                $token,
            )
            .await
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
    ( $phase:expr, $arg:expr, $start_time:expr, $resolver_state:expr, $db:expr, $map_compiled_sql:expr, $jinja_env:expr, $cancel_token:expr) => {
        if $arg.phase <= $phase
            || $crate::error_counter::get_error_counter($arg.io.invocation_id.to_string().as_str())
                > 0
        {
            return maybe_interactive_or_exit!(
                $arg,
                $start_time,
                $resolver_state,
                $db,
                $map_compiled_sql,
                $jinja_env,
                $cancel_token
            );
        }
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

#[macro_export]
macro_rules! show_selected_nodes_summary {
    ($io:expr, $schedule:expr, $nodes:expr) => {{
        use $crate::io_args::ShowOptions;
        use std::collections::BTreeMap;

        if $io.should_show(ShowOptions::Progress) {
            // Count nodes by resource type
            let mut counts: BTreeMap<&str, usize> = BTreeMap::new();

            for node_id in &$schedule.selected_nodes {
                if let Some(node) = $nodes.get_node(node_id) {
                    let resource_type = node.resource_type();
                    *counts.entry(resource_type).or_insert(0) += 1;
                }
            }

            // Build the summary string
            let mut parts = Vec::new();
            for (resource_type, count) in counts {
                let resource_name = match resource_type {
                    "model" => "models",
                    "test" => "data tests",
                    "unit_test" => "unit tests",
                    "source" => "sources",
                    "seed" => "seeds",
                    "snapshot" => "snapshots",
                    "analysis" => "analyses",
                    "operation" => "operations",
                    "exposure" => "exposures",
                    "metric" => "metrics",
                    "macro" => "macros",
                    "group" => "groups",
                    "semantic_model" => "semantic models",
                    "saved_query" => "saved queries",
                    _ => resource_type,
                };
                parts.push(format!("{} {}", count, resource_name));
            }

            if !parts.is_empty() {
                let summary = format!("Found {}", parts.join(", "));
                $crate::_log!(
                    $crate::macros::log_adapter::log::Level::Info,
                    _INVOCATION_ID_ = $io.invocation_id.as_u128();
                    "{}",
                    summary
                );
            }
        }
    }};
}
