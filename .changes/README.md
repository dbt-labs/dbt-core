# CHANGELOG Automation

We use [changie](https://changie.dev/) to automate `CHANGELOG` generation.  For installation and format/command specifics, see the documentation.

### Quick Tour

- All new change entries get generated under `/.changes/unreleased` as a yaml file
- `header.tpl.md` contains the contents of the entire CHANGELOG file
- `.changie.yaml` contains the fields in a change, the format of a single change, as well as the format of the Contributors section for each version.

### Workflow

#### Daily workflow
Almost every code change we make associated with an issue will require a `CHANGELOG` entry.  After you have created the PR in GitHub, run `changie new` and follow the command prompts to generate a yaml file with your change details.  This only needs to be done once per PR.

The `changie new` command will ensure correct file format and file name.  There is a one to one mapping of issues to changes.  Multiple issues cannot be lumped into a single entry. If you make a mistake, the yaml file may be directly modified and saved as long as the format is preserved.

Note: If your PR has been cleared by the team as not needing a changelog entry, the `Skip Changelog - dbt-fusion` label may be put on the PR to bypass the GitHub action that blacks PRs from being merged when they are missing a `CHANGELOG` entry.

#### Prerelease Workflow
These commands batch up changes in `/.changes/unreleased` to be included in this prerelease and move those files to a directory named for the release version.  The `--move-dir` will be created if it does not exist and is created in `/.changes`.

```
changie batch <version>  --move-dir '<version>' --prerelease 'rc1'
changie merge
```

Example
```
changie batch 1.0.5  --move-dir '1.0.5' --prerelease 'rc1'
changie merge
```

#### Final Release Workflow
These commands batch up changes in `/.changes/unreleased` as well as `/.changes/<version>` to be included in this final release and delete all prereleases.  This rolls all prereleases up into a single final release.  All `yaml` files in `/unreleased` and `<version>` will be deleted at this point.

```
changie batch <version>  --include '<version>' --remove-prereleases
changie merge
```

Example
```
changie batch 1.0.5  --include '1.0.5' --remove-prereleases
changie merge
```

### A Note on Manual Edits & Gotchas
- Changie generates markdown files in the `.changes` directory that are parsed together with the `changie merge` command.  Every time `changie merge` is run, it regenerates the entire file.  For this reason, any changes made directly to `CHANGELOG.md` will be overwritten on the next run of `changie merge`.
- If changes need to be made to the `CHANGELOG.md`, make the changes to the relevant `<version>.md` file located in the `/.changes` directory.  You will then run `changie merge` to regenerate the `CHANGELOG.MD`.
- Do not run `changie batch` again on released versions.  Our final release workflow deletes all of the yaml files associated with individual changes.  If for some reason modifications to the `CHANGELOG.md` are required after we've generated the final release `CHANGELOG.md`, the modifications need to be done manually to the `<version>.md` file in the `/.changes` directory.
- changie can modify, create and delete files depending on the command you run.  This is expected.  Be sure to commit everything that has been modified and deleted.
