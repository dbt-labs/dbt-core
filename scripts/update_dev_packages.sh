#!/bin/bash -e
set -e

repo=$1
ref=$2
target_req_file="dev-requirements.txt"

req_sed_pattern="s|${repo}.git@main|${repo}.git@${ref}|g"
if [[ "$OSTYPE" == darwin* ]]; then
 # mac ships with a different version of sed that requires a delimiter arg
 sed -i "" "$req_sed_pattern" $target_req_file
else
 sed -i "$req_sed_pattern" $target_req_file
fi
