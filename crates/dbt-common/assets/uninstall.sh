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
    echo "  --package PACKAGE         Uninstall package PACKAGE [dbt|dbt-lsp|all]"
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
    --package | -p)
        package=$2
        shift
        ;;
    *) ;;

    esac
    shift
done

package="${package:-dbt}"

if [ -z "${installLocation:-}" ]; then
    dbtInstallLocation="$HOME/.local/bin/dbt"
    lspInstallLocation="$HOME/.local/bin/dbt-lsp"
else
    dbtInstallLocation="$installLocation/dbt"
    lspInstallLocation="$installLocation/dbt-lsp"
fi

operating_system=$(uname -s | tr '[:upper:]' '[:lower:]')

if [ "$operating_system" = "linux" ]; then
    if [ "$package" = "all" ] || [ "$package" = "dbt" ]; then
        rm -rf $dbtInstallLocation
        log "Uninstalled dbt from $dbtInstallLocation"
    fi
    if [ "$package" = "all" ] || [ "$package" = "dbt-lsp" ]; then
        rm -rf $lspInstallLocation
        log "Uninstalled dbt-lsp from $lspInstallLocation"
    fi
elif [ "$operating_system" = "darwin" ]; then
    if [ "$package" = "all" ] || [ "$package" = "dbt" ]; then
        rm -rf $dbtInstallLocation
        log "Uninstalled dbt from $dbtInstallLocation"
    fi
    if [ "$package" = "all" ] || [ "$package" = "dbt-lsp" ]; then
        rm -rf $lspInstallLocation
        log "Uninstalled dbt-lsp from $lspInstallLocation"
    fi
else
    err "Unsupported OS: $operating_system"
fi
