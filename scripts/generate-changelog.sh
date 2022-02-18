#!/bin/bash

read -p "version number: " version
read -p "Final release? (Y/N): " final

if [ "$final" == "N" ] || [ "$final" == "n" ]; then
    read -p "Enter pre-release (rc, etc): " prerelease
    full_version="$version-$prerelease"
    changie batch $full_version --keep
    if [ ! -d /.changes/$version ]; then
        mkdir ./.changes/$version
    fi

    mv ./.changes/unreleased/* ./.changes/$version/

else
    full_version=$version
    mv ./.changes/$version/* ./.changes/unreleased/
    rm -rf ./.changes/$version*
    changie batch $full_version
fi

changie merge
