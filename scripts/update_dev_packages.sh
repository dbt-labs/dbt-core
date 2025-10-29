#!/bin/bash -e
set -e

# this is used in dbt-common for CI

repo=$1
ref=$2
target_file="pyproject.toml"

req_sed_pattern="s|${repo}.git@main|${repo}.git@${ref}|g"
if [[ "$OSTYPE" == darwin* ]]; then
  # mac ships with a different version of sed that requires a delimiter arg
  sed -i "" "$req_sed_pattern" "$target_file"
else
  sed -i "$req_sed_pattern" "$target_file"
fi
