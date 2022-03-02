# Performance Regression Testing
This test suite samples the performance characteristics of individual commits against performance models for prior releases. Performance is measured in project-command pairs which are assumed to conform to a normal distribution. The sampling and comparison is effecient enough to run against PRs.

This directory includes dbt project setups that are known performance bottlenecks, and a runner written in Rust that runs specific dbt commands on each of the projects. Orchestration is done via the GitHub Action workflow in `/.github/workflows/performance.yml`.

Performance baselines measured during our release process are committed to this directory via github action. (TODO make the file and name it here).

## Investigating Regressions

If your commit has failed one of the performance regression tests, it does not necessarily mean your commit has a performance regression. However, the observed runtime value was so much slower than the expected value that it was unlikely to be random noise. This means that any commit after the release it is being compared against through this failing commit might contain the cause. Start by investigating the failing commit and working your way backwards.

Here's an example of how this could happen:

```
Commit
A      <- last release
B
C      <- perf regression
D
E
F      <- the only failing commit
```
- Commit A is measured to have an expected value for one performance metric of 30 seconds with a standard deviation of 0.5 seconds.
- Commit B doesn't introduce a performance regression and passes the performance regression tests.
- Commit C introduces a performance regression such that the new expected value of the metric is 32 seconds with a standard deviation still at 0.5 seconds, but we don't know this because we don't estimate the whole performance distribution on every commit for performance reasons. It passes the performance regression test because we happened to sample a value of 31 seconds which is within our threshold for the original model. It's also only 2 standard deviations away from the actual performance model of commit C so this situation isn't even particularly unlikely.
- Commit D samples a value of 31.4 seconds and passes
- Commit E samples a value of 31.2 seconds and passes
- Commit F samples a value of 32.9 seconds and fails

Because these performance regression tests are non-deterministic, it is frequently going to be possible to rerun the test on a failing commit and get it to pass. The more often we do this, the farther down the commit history we will be punting detection that can't be easily passed rerun.

## The Statistics
Particle physicists need to be confident in declaring new discoveries, snack manufacturers need to be sure each snack is within the regulated margin of error for nutrition facts, and weight-rated climbing gear needs to be produced so you can trust your life to every unit that comes off the line. All of these use cases use the same kind of math to meet their needs: sigma-based p-values. This section will peel apart that math with the help of a physicist and walk through how we apply this approach to performance regression testing in this test suite.

You are likely familiar with forming a hypothesis of the form "A and B are correlated" which is known as _the research hypothesis_. Additionally, it follows that the hypothesis "A and B are not correlated" is relevant and is known as _the null hypothesis_. When looking at data, we commonly use a _p-value_ to determine the significance of the data. Formally, a _p-value_ is the probability of obtaining data at least as extreme as the ones observed, if the null hypothesis is true. To refine this definition, The experimental partical physicist [Dr. Tommaso Dorigo](https://userswww.pd.infn.it/~dorigo/#about) has an excellent [glossary](https://www.science20.com/quantum_diaries_survivor/fundamental_glossary_higgs_broadcast-85365) of these terms that helps clarify: "'Extreme' is quite tricky instead: it depends on what is your 'alternate hypothesis' of reference, and what kind of departure it would produce on the studied statistic derived from the data. So 'extreme' will mean 'departing from the typical values expected for the null hypothesis, toward the values expected from the alternate hypothesis.'" In the context of performance regression testing, our research hypothesis is that "after commit A, the codebase includes a performance regression" which means we expect the runtime of our measured processes to be _slower_, not faster than the expected value.

Given this definition of p-value, we need to explicitly call out the common tendancy to apply _probability inversion_ to our observations. To quote [Dr. Tommaso Dorigo](https://www.science20.com/quantum_diaries_survivor/fundamental_glossary_higgs_broadcast-85365) again, "If your ability on the long jump puts you in the 99.99% percentile, that does not mean that you are a kangaroo, and neither can one infer that the probability that you belong to the human race is 0.01%." Using our previously defined terms, the p-value is _not_ the probability that the null hypothesis _is true_.

This brings us to calculating sigma values. Sigma refers to the standard deviation of a statistical model, which is used as a measurement of how far away an observed value is from the expected value. When we say that we have a "3 sigma result" we are saying that if the null hypothesis is true, this is a particularly unlikely observation. Not that the null hypothesis is false. Exactly how unlikely depends on what the expected values from our research hypothesis are. In the context of performance regression testing, if the null hypothesis is false, we are expecting the results to be _slower_ than the expected value not _slower or faster_. Looking at a normal distrubiton below, we can see that we only care about one _half_ of the distribution: the half where the values are slower than the expected value. This means that when we're calculating the p-value we are not including both sides of the normal distribution.

![normal distibution](./images/normal.svg)

Because of this, the following table describes the significance of each sigma level for our _one-sided_ hypothesis:

| σ   | p-value        | scientific significance |
| --- | -------------- | ----------------------- |
| 1 σ | 1 in 6         |                         |
| 2 σ | 1 in 44        |                         |
| 3 σ | 1 in 741       |  evidence               |
| 4 σ | 1 in 31,574    |                         |
| 5 σ | 1 in 3,486,914 |  discovery              |

When detecting performance regressions that trigger alerts, block PRs, or delay releases we want to be conservative enough that detections are infrequently triggered by noise, but not so conservative as to miss most actual regressions. This test suite uses a 3 sigma standard so that only about 1 in every 700 runs is expected to fail the performance regression test suite due to expected variance in our measurements.

### Concrete Example

The following example data was collected by running the code in this repository in Github Actions.

In dbt v1.0.3, we have the following mean and standard deviation when parsing a dbt project with 2000 models:

μ (mean):   55.06<br/>
σ (stddev): 2.188<br/>

The 2-sided 3 sigma range can be calculated with these two values via:

x < μ - 3 σ or x > μ + 3 σ<br/>
x < 55.06 - 3 * 2.188 or x > 55.06 + 3 * 2.188 <br/>
x < 48.496 or x > 61.624<br/>

It follows that the 1-sided 3 sigma range for performance regressions is just:<br/>
x > 61.624

If when we sample a single `dbt parse` of the same project with a commit slated to go into dbt v1.0.4, we observe a 62s parse time, then this observation is so unlikely if there were no code-induced performance regressions, that we should investigate if there is a performance regression in any of the commits between this failure and the commit where the initial distribution was measured.

Observations with 3 sigma significance that are _not_ performance regressions could be due to observing unlikely values (roughly 1 in every 750 observations), or variations in the instruments we use to take these measurements such as github actions. At this time we do not measure the variation in the instruments we use to account for these in our calculations.

## Expanding the Tests
Regression tests run pre-defined dbt commands accross a set of source-committed dbt projects that are known to cause performance bottlenecks. This collection of projects and commands should expand over time to reflect user feedback about poorly performing projects to protect against poor performance in these scenarios in future versions.

### Adding a new dbt project
Just make a new directory under `performance/projects/`. It will automatically be picked up by the tests.

### Adding a new dbt command
TODO

## Future work
- add more projects to test different configurations that have been known bottlenecks
- add more dbt commands to measure
- reading new metrics from a file so no one has to edit rust source to add them to the suite
- build in a git-bisect run to automatically identify the commits that caused a performance regression by modeling each commit's expected value for the failing metric. Output the command to run this in each performance regression failure message.
