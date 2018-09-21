### Release Procedure :shipit:

#### Branching Strategy

dbt has three types of branches:

- **Trunks** track the latest release of a minor version of dbt. Historically, we used the `master` branch as the trunk. Each minor version release has a corresponding trunk. For example, the `0.11.x` series of releases has a branch called `0.11.latest`. This allows us to release new patch versions under `0.11` without necessarily needing to pull them into the latest version of dbt.
- **Release Branches** track a specific, not yet complete release of dbt. These releases are codenamed since we don't always know what their semantic version will be. Example: `dev/lucretia-mott` became `0.11.1`.
- **Feature Branches** track individual features and fixes. On completion they should be merged into a release branch.

#### Git & PyPI

1. Update CHANGELOG.md with the most recent changes
2. If this is a release candidate, you want to create it off of your release branch. If it's an actual release, you must first merge to master. Open a Pull Request in Github to merge it.
3. Bump the version using `bumpversion`:
  - Dry run first by running `bumpversion --new-version <desired-version> <part>` and checking the diff. If it looks correct, clean up the chanages and move on:
  - Alpha releases: `bumpversion --commit --tag --new-version 0.10.2a1 num`
  - Patch releases: `bumpversion --commit --tag --new-version 0.10.2 patch`
  - Minor releases: `bumpversion --commit --tag --new-version 0.11.0 minor`
  - Major releases: `bumpversion --commit --tag --new-version 1.0.0 major`
4. Deploy to pypi
  - `python setup.py sdist upload -r pypi`
5. Deploy to homebrew (see below)
6. Deploy to conda-forge (see below)
7. Git release notes (points to changelog)
8. Post to slack (point to changelog)

After releasing a new version, it's important to merge the changes back into the other outstanding release branches. This avoids merge conflicts moving forward.

In some cases, where the branches have diverged wildly, it's ok to skip this step. But this means that the changes you just released won't be included in future releases.

#### Homebrew Release Process

1. Clone the `homebrew-dbt` repository:

```
git clone git@github.com:fishtown-analytics/homebrew-dbt.git
```

2. For ALL releases (prereleases and version releases), copy the relevant formula. To copy from the latest version release of dbt, do:

```bash
cp Formula/dbt.rb Formula/dbt@{NEW-VERSION}.rb
```

To copy from a different version, simply copy the corresponding file.

3. Open the file, and edit the following:
- the name of the ruby class: this is important, homebrew won't function properly if the class name is wrong. Check historical versions to figure out the right name.
- under the `bottle` section, remove all of the hashes (lines starting with `sha256`)

4. Create a **Python 3.7** virtualenv, activate it, and then install two packages: `homebrew-pypi-poet`, and the version of dbt you are preparing. I use:

```
pyenv virtualenv 3.7.0 homebrew-dbt-{VERSION}
pyenv activate homebrew-dbt-{VERSION}
pip install dbt=={VERSION} homebrew-pypi-poet
```

homebrew-pypi-poet is a program that generates a valid homebrew formula for an installed pip package. You want to use it to generate a diff against the existing formula. Then you want to apply the diff for the dependency packages only -- e.g. it will tell you that `google-api-core` has been updated and that you need to use the latest version.

5. reinstall, test, and audit dbt. if the test or audit fails, fix the formula with step 1.

```bash
brew uninstall --force Formula/{YOUR-FILE}.rb
brew install Formula/{YOUR-FILE}.rb
brew test dbt
brew audit --strict dbt
```

6. Ask Connor to bottle the change (only his laptop can do it!)

#### Conda Forge Release Process

1. Clone the fork of `conda-forge/dbt-feedstock` [here](https://github.com/fishtown-analytics/dbt-feedstock)
```bash
git clone git@github.com:fishtown-analytics/dbt-feedstock.git

```
2. Update the version and sha256 in `recipe/meta.yml`. To calculate the sha256, run:

```bash
wget https://github.com/fishtown-analytics/dbt/archive/v{version}.tar.gz
openssl sha256 v{version}.tar.gz
```

3. Push the changes and create a PR against `conda-forge/dbt-feedstock`

4. Confirm that all automated conda-forge tests are passing
