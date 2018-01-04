#!/usr/bin/env bash

INITIAL_ARGS=""
PROFILES_FILE_SUBST="/root/.dbt-profiles/profiles.yml"

# So that passwords can be set via environment variables and not baked into docker images,
# run $PROFILES_FILE through envsubst.
if [ -f "$PROFILES_FILE" ]; then
  mkdir -p $(dirname $PROFILES_FILE_SUBST)
  envsubst < "$PROFILES_FILE" > "$PROFILES_FILE_SUBST"
  INITIAL_ARGS="--profiles-dir $(dirname $PROFILES_FILE_SUBST)"
fi

# Detect when dbt failed using regex. Pending https://github.com/analyst-collective/dbt/issues/297
set -o pipefail # also do fail when dbt does manage to exit non-zero.
stdbuf -oL dbt $@ $INITIAL_ARGS |
  while IFS= read -r line
  do
    echo "$line"
    if [[ "$line" =~ ^Done.*ERROR=[^0].*$ ]]; then
      exit 1
    fi
  done
