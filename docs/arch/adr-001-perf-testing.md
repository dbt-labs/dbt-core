# Performance Regression Framework

## Context
We want the ability to benchmark our perfomance overtime with new changes going forward.

### Options
- Static Window: Compare the develop branch to fastest version and ensure it doesn't exceed a static window (i.e. time parse on develop and time parse on 0.20.latest and make sure it's not more than 5% slower)
    - Pro: quick to run
    - Pro: simple to implement
    - Con: rerunning a failing test could get it to pass in a large number of changes.
    - Con: several small regressions could press us up against the threshold requiring us to do unexpected additional performance work, or lower the threshold to get a release out.
- Variance-aware Testing: Run both the develop branch and our fastest version *many times* to collect a set of timing data. We can fail on a static window based on medians, confidence interval midpoints, and even variance magnitude.
    - Pro: would catch more small performance regressions
    - Con: would take much longer to run
    - Con: Need to be very careful about making sure caching doesn't wreck the curve (or if it does, it wrecks the curve equally for all tests)
- Stateful Tracking: For example, the rust compiler team does some [bananas performance tracking](https://perf.rust-lang.org/). This option could be done in tandem with the above options, however it would require results be stored somewhere.
    - Pro: we can graph our performance history and look really cool.
    - Pro: Variance-aware testing would run in half the time since you can just reference old runs for comparison
    - Con: state in tests sucks
    - Con: longer to build
- Performance Profiling: Running a sampling-based profiler through a series of standardized test runs (test designed to hit as many/all of the code paths in the codebase) to determine if any particular function/class/other code has regressed in performance.
    - Pro: easy to find the cause of the perf. regression
    - Pro: should be able to run on a fairly small project size without losing much test resolution (a 5% change in a function should be evident with even a single case that runs that code path)
    - Con: complex to build
    - Con: compute intensive
    - Con: requires stored results to compare against

## Decision
We decided to start with variance-aware testing with the ability to add stateful tracking by leveraging `hyperfine` which does all the variance work for us, and outputs clear json artifacts. Since we're running perfornace testing on a schedule it doesn't matter that as we add more tests it may take hours to run. The artifacts are all stored in the github action runs today, but could easily be changed to be sent somewhere in the action to track over time.

## Status
Completed

## Consequences
We now have the ability to more rigorously detect performance regressions, but we do not have a solid way to identify where that regression is coming from. Adding Performance Profiling cababilities will help with this, but for now just running it nightly should help us narrow it down to specific commits. As we add more performance tests, the testing matrix may take hours to run which consumes resources on GitHub Actions. Because performance testing is asynchronous, failures are easier to miss or ignore, and because it is non-deterministic it adds a non-trivial amount of complexity to our development process.
