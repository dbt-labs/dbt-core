# CHANGELOG Automation

We use [changie](https://changie.dev/) to automate `CHANGLEOG` generation.  For installation and format/command specifics, see the documentation.

### Prereleases

The workflow we follow is to have prerelease versions before a final release.  There may be one to many prereleases.

`/dbt-core/scripts/generate-changelog.sh` automated the process of batching these changelogs per prerelease and then rolling all the prereleases up into a single final release when we are ready.

Running the script for a prerelease will move all yaml files under `.changes/unreleased` into a folder `.changes/$version` instead of deleting them.  When it's time for a final release, all prerelase `md` files will be deleted and a single final markdown file will take its place containing all changes for that version.  All of the yaml files will then be deleted as well.
