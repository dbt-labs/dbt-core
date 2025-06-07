#!/bin/sh
set -e

# Source common functions
. "$(dirname "$0")/install_common.sh"

# Color codes
GRAY='\033[0;90m'
NC='\033[0m' # No Color

log() {
    printf "install.sh: %s\n" "$1"
}

log_grey() {
    printf "${GRAY}install.sh: %s${NC}\n" "$1" >&2
}

err() {
    if [ ! -z $td ]; then
        rm -rf $td
    fi

    log_grey "ERROR $1"
    exit 1
}

need() {
    if ! command -v $1 >/dev/null 2>&1; then
        err "need $1 (command not found)"
    fi
}

help() {
    echo "Usage: install.sh [options]"
    echo ""
    echo "Options:"
    echo "  --update, -u       Update to latest or specified version"
    echo "  --version VER      Install version VER"
    echo "  --target TARGET    Install for target platform TARGET"
    echo "  --to DEST          Install to DEST"
    echo "  --help, -h         Show this help text"
}

update=false
while test $# -gt 0; do
    case $1 in
    --update | -u)
        update=true
        ;;
    --help | -h)
        help
        exit 0
        ;;
    --version)
        version=$2
        shift
        ;;
    --package | -p)
        package=$2
        shift
        ;;
    --target)
        target=$2
        shift
        ;;
    --to)
        dest=$2
        shift
        ;;
    *) ;;

    esac
    shift
done

# Set default package if not specified
package="${package:-dbt}"

# Dependencies
need basename
need curl
need install
need mkdir
need mktemp
need tar

# Optional dependencies
if [ -z $version ] || [ -z $target ]; then
    need cut
fi

if [ -z $version ]; then
    need rev
fi

if [ -z $target ]; then
    need grep
fi

if [ -z "${dest:-}" ]; then
    dest="$HOME/.local/bin"
else
    # Convert relative path to absolute
    case "$dest" in
        /*) ;; # Already absolute path
        *) dest="$PWD/$dest" ;; # Convert relative to absolute
    esac
fi

# Check if it is already installed and get current versions
current_dbt_version=$(check_binary_version "$dest/dbt" "dbt")
current_lsp_version=$(check_binary_version "$dest/dbt-lsp" "dbt-lsp")

# Determine version to install
target_version=$(determine_version "$version")

target="${target:-$(detect_target)}"

install_packages "$package" "$target_version" "$target" "$dest" "$update"

validate_versions "$dest"
