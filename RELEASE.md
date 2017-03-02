### Release Procedure :shipit:

1. Update changelog
1. Bumpversion
1. Merge to master
  - (on master) git pull origin development
1. Deploy to pypi
  - python setup.py sdist upload -r pypi
1. Deploy to homebrew
  - Make a pull request against homebrew-core
1. Deploy to conda-forge
  - Make a pull request against dbt-feedstock
1. Git release notes (points to changelog)
1. Post to slack (point to changelog)

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
