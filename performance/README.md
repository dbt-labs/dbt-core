# Performance Regression Testing
This directory includes dbt project setups to test on and a test runner written in Rust which runs specific dbt commands on each of the projects. Orchestration is done via the GitHub Action workflow in `/.github/workflows/performance.yml`. The workflow is scheduled to run every night, but it can also be triggered manually.

The github workflow hardcodes our baseline branch for performance metrics as `0.20.latest`. As future versions become faster, this branch will be updated to hold us to those new standards.

## Adding a new dbt project
Just make a new directory under `performance/projects/`. It will automatically be picked up by the tests.

## Adding a new dbt command
In `runner/src/measure.rs::measure` add a metric to the `metrics` Vec. The Github Action will handle recompilation if you don't have the rust toolchain installed.

## Future work
- add more projects to test different configurations that have been known bottlenecks
- add more dbt commands to measure
- possibly using the uploaded json artifacts to store these results so they can be graphed over time
- reading new metrics from a file so no one has to edit rust source to add them to the suite
- instead of building the rust every time, we could publish and pull down the latest version.
- instead of manually setting the baseline version of dbt to test, pull down the latest stable version as the baseline.
