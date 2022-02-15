# Github package 'latest' tag wrangler for containers
## Usage

Plug in the necessary inputs to determine if the container being built should be tagged 'latest; at the package level, for example `dbt-redshift:latest`.

## Inputs
| Input | Description |
| - | - |
| `package` | Name of the GH package to check against |
| `new_version` | Semver of new container |
| `gh_token` | GH token with package read scope|
| `halt_on_missing` | Return non-zero exit code if requested package does not exist. (defaults to false)|


## Outputs
| Output | Description |
| - | - |
| `latest` | Wether or not the new container should be tagged 'latest'|
| `minor_latest` | Wether or not the new container should be tagged major.minor.latest |

## Example workflow
```yaml
name: Ship it!
on:
  workflow_dispatch:
    inputs:
      package:
       description: The package to publish
       required: true
      version_number:
       description: The version number
       required: true

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v1
    - name: Wrangle latest tag
      id: is_latest
      uses: ./.github/actions/latest-wrangler
      with:
        package: ${{ github.event.inputs.package }}
        new_version: ${{ github.event.inputs.new_version }}
        gh_token: ${{ secrets.GITHUB_TOKEN }}
    - name: Print the results
      run: |
        echo "Is it latest?  Survey says: ${{ steps.is_latest.outputs.latest }} !"
        echo "Is it minor.latest?  Survey says: ${{ steps.is_latest.outputs.minor_latest }} !"
```
