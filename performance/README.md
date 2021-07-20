# Performance Testing
This directory includes dbt project setups to test on and a test runner written in Rust which runs specific dbt commands on each of the projects. Orchestration is done via the GitHub Action workflow in `/.github/workflows/performance.yml`. The workflow is scheduled to run every night, but it can also be triggered manually.

## Adding a new project
Just make a new directory under projects. It will automatically be picked up by the tests.

## Adding a new dbt command
In `runner/src/main.rs` add a metric to the `metrics` Vec in the main function. The Github Action will handle recompilation.

## Future work
- add more projects to test different configurations that have been known bottlenecks
- add more dbt commands to measure
- consider storing these results so they can be graphed over time
