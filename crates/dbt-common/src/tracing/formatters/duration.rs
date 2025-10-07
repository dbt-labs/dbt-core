use std::time::Duration;

/// Format elapsed duration for final invocation summaries.
///
/// The formatting targets a compact single token suitable for `[<value>]` blocks,
/// matching the desired UX in the run result output.
pub fn format_duration_for_summary(duration: Duration) -> String {
    let total_secs = duration.as_secs_f64();

    if total_secs >= 3600.0 {
        let hours = (total_secs / 3600.0).floor() as u64;
        let minutes = ((total_secs % 3600.0) / 60.0).floor() as u64;
        let seconds = total_secs % 60.0;
        if seconds >= 1.0 {
            format!("{hours}h {minutes}m {seconds:.0}s")
        } else if minutes > 0 {
            format!("{hours}h {minutes}m")
        } else {
            format!("{hours}h")
        }
    } else if total_secs >= 60.0 {
        let minutes = (total_secs / 60.0).floor() as u64;
        let seconds = total_secs % 60.0;
        if seconds >= 1.0 {
            format!("{minutes}m {seconds:.0}s")
        } else {
            format!("{minutes}m")
        }
    } else if total_secs >= 1.0 {
        format!("{total_secs:.1}s")
    } else {
        let millis = duration.as_millis();
        if millis >= 1 {
            format!("{millis}ms")
        } else {
            let micros = duration.as_micros();
            if micros >= 1 {
                format!("{micros}us")
            } else {
                format!("{}ns", duration.as_nanos())
            }
        }
    }
}
