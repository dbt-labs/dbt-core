//! Runs external commands with a bounded wait → graceful terminate →
//! forceful kill escalation. Discovery probes arbitrary `dbt` installs found
//! on `PATH`, so a probe must never be allowed to hang the caller forever
//! just because the program it invoked is badly behaved.

use std::{
    io::Read,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

/// Outcome of running an external command. The runner itself returns `None`
/// when the program could not be found or spawned at all.
#[derive(Debug, Clone)]
pub(crate) struct ProcessOutput {
    pub(crate) success: bool,
    pub(crate) stdout: String,
    pub(crate) stderr: String,
}

/// How long a probed `dbt` gets to exit on its own before we intervene.
pub(crate) const NORMAL_WAIT: Duration = Duration::from_secs(8);
/// How much longer it gets to exit after a graceful termination request
/// before we escalate to a forceful kill.
pub(crate) const GRACE_WAIT: Duration = Duration::from_secs(2);
const POLL_INTERVAL: Duration = Duration::from_millis(50);

pub(crate) fn real_run(program: &str, args: &[&str]) -> Option<ProcessOutput> {
    run_with_timeouts(program, args, NORMAL_WAIT, GRACE_WAIT)
}

/// Runs `program` and waits up to `normal_wait` for it to exit on its own;
/// if it hasn't, requests a graceful termination and waits up to
/// `grace_wait` more before forcibly killing it. Split out from `real_run`
/// so tests can exercise the full wait/terminate/kill escalation without
/// sleeping through the real (multi-second) production durations.
fn run_with_timeouts(
    program: &str,
    args: &[&str],
    normal_wait: Duration,
    grace_wait: Duration,
) -> Option<ProcessOutput> {
    let mut child = Command::new(program)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .ok()?;

    // Drain stdout/stderr on their own threads while we poll for exit below —
    // otherwise a chatty child could fill a pipe buffer and block on writing
    // to it, hanging forever even though we're not blocked on `wait()`.
    let mut stdout_pipe = child.stdout.take().expect("stdout was piped");
    let mut stderr_pipe = child.stderr.take().expect("stderr was piped");
    let stdout_handle = std::thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = stdout_pipe.read_to_end(&mut buf);
        buf
    });
    let stderr_handle = std::thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = stderr_pipe.read_to_end(&mut buf);
        buf
    });

    // Give it normal_wait to exit on its own; if not, ask it nicely
    // (SIGTERM, where that concept exists) and give it grace_wait more before
    // giving up and forcing the issue with a kill.
    let status = match wait_with_deadline(&mut child, normal_wait) {
        Some(status) => status,
        None => {
            terminate_gracefully(&child);
            match wait_with_deadline(&mut child, grace_wait) {
                Some(status) => status,
                None => {
                    let _ = child.kill();
                    child.wait().ok()?
                }
            }
        }
    };

    let stdout = stdout_handle.join().unwrap_or_default();
    let stderr = stderr_handle.join().unwrap_or_default();

    Some(ProcessOutput {
        success: status.success(),
        stdout: String::from_utf8_lossy(&stdout).into_owned(),
        stderr: String::from_utf8_lossy(&stderr).into_owned(),
    })
}

/// Polls `child` for exit until it's done or `timeout` elapses.
fn wait_with_deadline(child: &mut Child, timeout: Duration) -> Option<std::process::ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return Some(status),
            Ok(None) => {}
            Err(_) => return None,
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return None;
        }
        std::thread::sleep(POLL_INTERVAL.min(remaining));
    }
}

#[cfg(unix)]
fn terminate_gracefully(child: &Child) {
    // SAFETY: `child.id()` is a valid, still-owned PID for the duration of
    // this call (the child hasn't been `wait()`-reaped yet), and `kill(2)`
    // with a PID and no side channels is safe to call with any PID/signal
    // combination — worst case it fails (e.g. ESRCH if the process already
    // exited between our last poll and this call), which we ignore.
    unsafe {
        libc::kill(child.id() as libc::pid_t, libc::SIGTERM);
    }
}

#[cfg(not(unix))]
fn terminate_gracefully(_child: &Child) {
    // No portable graceful-termination signal on this platform; the
    // subsequent forceful kill (after GRACE_WAIT) is the only option.
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(unix)]
    fn real_run_captures_output_and_exit_status() {
        let output = real_run("/bin/sh", &["-c", "echo hello; echo err >&2; exit 3"]).unwrap();
        assert!(!output.success);
        assert_eq!(output.stdout, "hello\n");
        assert_eq!(output.stderr, "err\n");
    }

    #[test]
    #[cfg(unix)]
    fn real_run_kills_process_that_ignores_sigterm() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let script = dir.path().join("stubborn.sh");
        // Prints its own PID to stdout (read back through the same
        // pipe-draining path `real_run` uses) rather than redirecting a
        // command's output to a file — redirecting to a file from inside a
        // script that then spins in a tight loop was observed to make the
        // shell's `trap` on this platform unreliable, which is a shell/test
        // quirk unrelated to what's under test here.
        std::fs::write(
            &script,
            "#!/bin/sh\ntrap '' TERM\necho $$\nwhile true; do :; done\n",
        )
        .unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();

        // Exercise the same wait/terminate/kill escalation as production,
        // but with millisecond-scale durations instead of NORMAL_WAIT/
        // GRACE_WAIT so this test doesn't spend ~10s asleep in every run.
        // normal_wait needs enough headroom over shell-startup latency
        // (fork/exec/dynamic-link) under load that the child reliably gets
        // to install its trap before normal_wait elapses and we send TERM —
        // 100ms was observed to be flaky for this under CI-like contention.
        let normal_wait = Duration::from_millis(500);
        let grace_wait = Duration::from_millis(300);

        let started = Instant::now();
        let output =
            run_with_timeouts(script.to_str().unwrap(), &[], normal_wait, grace_wait).unwrap();
        let elapsed = started.elapsed();

        // Should have gone through the full wait cycle (the script ignores
        // SIGTERM, so only the forceful kill at the end ends it) but not
        // hung well past it.
        assert!(elapsed >= normal_wait + grace_wait);
        assert!(elapsed < normal_wait + grace_wait + Duration::from_secs(5));
        assert!(!output.success);

        let pid = output.stdout.trim();
        assert!(!pid.is_empty(), "script should have printed its PID");
        let still_alive = Command::new("kill")
            .args(["-0", pid])
            .status()
            .unwrap()
            .success();
        assert!(!still_alive, "process should have been killed");
    }
}
