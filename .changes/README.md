# CHANGELOG Automation

We use [changie](https://changie.dev/) to automate `CHANGLEOG` generation.  For installation and format/command specifics, see the documentation.

### Workflow

#### Daily workflow
Each code change gets a changlog file by running `changie new` and following the prompts.  This ensures correct file format and file name.

#### Prerelease Workflow
These commands batch up changes in `/.changes/unreleased` to be included in this prerelease and move those files to a directory named for the release version.  The `move-dir` will be created if it deos not exist and is created in `/.changes`.
`changie batch <version>  --move-dir '<version>' --prerelease 'rc1'`
`changie merge`

#### Final Release Workflow
These commands batch up changes in `/.changes/unreleased` as well as `/.changes/<version>` to be included in this final release and delete all prereleases.  This rolls all prereleases up into a single final release.
`changie batch <version>  --include '<version>' --remove-prereleases`
`changie merge`
