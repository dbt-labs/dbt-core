### Release Procedure :shipit:

1. Update CHANGELOG.md with the most recent changes
2. If this is a release candidate, you want to create it off of development. If it's an actual release, you must first merge to master.
  - `git checkout master`
  - `git pull origin development`
3. Bump the version using `bumpversion`:
  - Dry run first by running `bumpversion --new-version <desired-version> <part>` and checking the diff. If it looks correct, clean up the chanages and move on:
  - Alpha releases: `bumpversion --commit --tag --new-version 0.10.2a1 num`
  - Patch releases: `bumpversion --commit --tag --new-version 0.10.2 patch`
  - Minor releases: `bumpversion --commit --tag --new-version 0.11.0 minor`
  - Major releases: `bumpversion --commit --tag --new-version 1.0.0 major`
4. Deploy to pypi
  - `python setup.py sdist upload -r pypi`
5. Deploy to homebrew
  - Make a pull request against homebrew-core
6. Deploy to conda-forge
  - Make a pull request against dbt-feedstock
7. Git release notes (points to changelog)
8. Post to slack (point to changelog)

#### Homebrew Release Process

1. fork homebrew and add a remote:

```
cd $(brew --repo homebrew/core)
git remote add origin <your-github-username> <your-fork-url>
```

2. edit the formula.

```bash
brew update
mkvirtualenv --python="$(which python3)" brew
pip install homebrew-pypi-poet dbt
diff "$(brew --repo homebrew/core)"/Formula/dbt.rb <(poet -f dbt)
```

find any differences in resource stanzas, and incorporate them into the formula

```
brew edit dbt
...
diff "$(brew --repo homebrew/core)"/Formula/dbt.rb <(poet -f dbt)
```

3. reinstall, test, and audit dbt. if the test or audit fails, fix the formula with step 1.

```bash
brew uninstall --force dbt
brew install --build-from-source dbt
brew test dbt
brew audit --strict dbt
```

4. make a pull request for the change.

```bash
cd $(brew --repo homebrew/core)
git pull origin master
git checkout -b dbt-<version> origin/master
git add . -p
git commit -m 'dbt <version>'
git push -u <your-github-username> dbt-<version>
```

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
