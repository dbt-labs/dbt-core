#!/bin/sh
set -e

say() {
    echo "uninstall.sh: $1"
}

log() {
    say "$1" >&2
}

err() {
    if [ ! -z $td ]; then
        rm -rf $td
    fi

    log "ERROR $1"
    exit 1
}

need() {
    if ! command -v $1 >/dev/null 2>&1; then
        err "need $1 (command not found)"
    fi
}

help() {
    echo "Usage: uninstall.sh [options]"
    echo ""
    echo "Options:"
    echo "  --installLocation, -i     Install location of dbt"
    echo "  --help, -h                Show this help text"
}

update=false
while test $# -gt 0; do
    case $1 in
    --help | -h)
        help
        exit 0
        ;;
    --installLocation | -i)
        installLocation=$2
        shift
        ;;
    *) ;;

    esac
    shift
done

if [ -z "${installLocation:-}" ]; then
    installLocation="$HOME/.local/bin/dbt"
else
    case "$installLocation" in
        *"/dbt") ;;
        *) installLocation="$installLocation/dbt" ;;
    esac
fi

operating_system=$(uname -s | tr '[:upper:]' '[:lower:]')

if [ "$operating_system" = "linux" ]; then
    rm -rf $installLocation
    log "Uninstalled dbt from $installLocation"
elif [ "$operating_system" = "darwin" ]; then
    rm -rf $installLocation
    log "Uninstalled dbt from $installLocation"
else
    err "Unsupported OS: $operating_system"
fi
