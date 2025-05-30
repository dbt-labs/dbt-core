#!/bin/sh
set -e

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
    echo "  --update, -u     Update to latest or specified version"
    echo "  --version VER    Install version VER"
    echo "  --target TARGET  Install for target platform TARGET"
    echo "  --to DEST        Install to DEST"
    echo "  --help, -h       Show this help text"
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

# Check if it is already installed and get current version
current_version=""
if [ -f "$dest/dbt" ] && [ -x "$dest/dbt" ]; then
    current_version=$($dest/dbt --version 2>/dev/null | cut -d ' ' -f 2 || echo "")
    if [ ! -z "$current_version" ]; then
        log_grey "Current installed version: $current_version"
    fi
fi

fetch_release="https://public.cdn.getdbt.com/fs/latest.json"

if [ -z "$version" ]; then
    log_grey "Checking for latest version at $fetch_release"
    version=$(curl -s "$fetch_release" | sed -n 's/.*"tag": *"v\([^"]*\)".*/\1/p')
    log_grey "Latest available version: $version"

    # If current version matches latest, exit
    if [ "$current_version" = "$version" ]; then
        log "Latest version $version is already installed at $dest/dbt"
        exit 0
    fi
else
    log_grey "Requested version: $version"

    # If current version matches requested version, exit
    if [ "$current_version" = "$version" ]; then
        log "Version $version is already installed at $dest/dbt"
        exit 0
    fi
fi

cpu_arch_target=$(uname -m)
operating_system=$(uname -s | tr '[:upper:]' '[:lower:]')

if [ -z $target ]; then
    if [ "$operating_system" = "linux" ]; then
        if [ -n "$(ldd --version 2>/dev/null)" ]; then
            if [ "$cpu_arch_target" = "arm64" ] || [ "$cpu_arch_target" = "aarch64" ]; then
                target="aarch64-unknown-linux-gnu"
            elif [ "$cpu_arch_target" = "x86_64" ]; then
                target="x86_64-unknown-linux-gnu"
            else
                err "Unsupported CPU Architecture: $cpu_arch_target"
            fi
        else
            if [ "$cpu_arch_target" = "x86_64" ]; then
                target="x86_64-unknown-linux-gnu"
            else
                err "Unsupported CPU Architecture: $cpu_arch_target"
            fi
        fi
    elif [ "$operating_system" = "darwin" ]; then
        if [ "$cpu_arch_target" = "arm64" ]; then
            target="aarch64-apple-darwin"
        else
            target="x86_64-apple-darwin"
        fi
    else
        err "Unsupported OS: $operating_system"
    fi
fi

log_grey "Target: $target"

log "Installing dbt to: $dest"
# Create the directory if it doesn't exist
mkdir -p "$dest"

url="https://public.cdn.getdbt.com/fs/cli/fs-v$version-$target.tar.gz"

log_grey "Downloading: $url"
td=$(mktemp -d || mktemp -d -t tmp)
curl -sL $url | tar -C $td -xz

# Function to format install output in grey
format_install_output() {
    while IFS= read -r line; do
        printf "${GRAY}%s${NC}\n" "$line" >&2
    done
}

for f in $(cd $td && find . -type f); do
    test -x $td/$f || {
        log_grey "File $f is not executable, skipping"
        continue
    }

    if [ -e "$dest/dbt" ] && [ $update = false ]; then
        err "dbt already exists in $dest, use the --update flag to reinstall"
    elif [ -e "$dest/dbt" ] && [ $update = true ]; then
        # Remove file - no sudo needed for home directory
        rm -f "$dest/dbt" || {
            err "Error: Failed to remove existing dbt binary."
        }
    fi

    log_grey "Moving $f to $dest"
    # No sudo needed for home directory
    mkdir -p "$dest" && install -v -m 755 "$td/$f" "$dest" 2>&1 | format_install_output || {
        err "Error: Failed to install dbt binary."
    }
done

cat<<EOF

 =====              =====    ┓┓  
=========        =========  ┏┫┣┓╋
 ===========    >========   ┗┻┗┛┗
  ======================    ███████╗██╗   ██╗███████╗██╗ ██████╗ ███╗   ██╗
   ====================     ██╔════╝██║   ██║██╔════╝██║██╔═══██╗████╗  ██║
    ========--========      █████╗  ██║   ██║███████╗██║██║   ██║██╔██╗ ██║
     =====-    -=====       ██╔══╝  ██║   ██║╚════██║██║██║   ██║██║╚██╗██║
    ========--========      ██╔══╝  ██║   ██║╚════██║██║██║   ██║██║╚██╗██║
   ====================     ██║     ╚██████╔╝███████║██║╚██████╔╝██║ ╚████║
  ======================    ╚═╝      ╚═════╝ ╚══════╝╚═╝ ╚═════╝ ╚═╝  ╚═══╝
 ========<   ============                        ┌─┐┌┐┌┌─┐┬┌┐┌┌─┐
=========      ==========                        ├┤ ││││ ┬││││├┤ 
 =====             =====                         └─┘┘└┘└─┘┴┘└┘└─┘ $version

EOF

rm -rf $td

show_path_instructions() {
    log_grey ""
    log_grey "NOTE: $dest may not be in your PATH."
    log_grey "To add it permanently, run one of these commands depending on your shell:"
    log_grey "  For bash/zsh: echo 'export PATH=\"\$PATH:$dest\"' >> ~/.bashrc  # or ~/.zshrc"
    log_grey ""
    log_grey "To use dbt in this session immediately, run:"
    log_grey "    export PATH=\"\$PATH:$dest\""
    log_grey ""
    log_grey "Then restart your terminal or run 'source ~/.bashrc' (or equivalent) for permanent changes"
}

# Detect shell and config file early
config_file=""
if [ -n "$SHELL" ]; then
    shell_name=$(basename "$SHELL")
else
    if [ -f "$HOME/.bashrc" ]; then
        shell_name="bash"
    elif [ -f "$HOME/.profile" ]; then
        shell_name="sh"
    else
        shell_name=$(ps -p $PPID -o comm= | sed 's/.*\///')
    fi
fi

# Set config file based on shell
if [ "$shell_name" = "zsh" ]; then
    config_file="$HOME/.zshrc"
elif [ "$shell_name" = "bash" ]; then
    config_file="$HOME/.bashrc"
elif [ "$shell_name" = "bash" ]; then
    config_file="$HOME/.bash_profile"
elif [ "$shell_name" = "fish" ]; then
    config_file="$HOME/.config/fish/config.fish"
fi

if [ -z "$config_file" ]; then
    log_grey "NOTE: Failed to identify config file."
    show_path_instructions
    exit 0
fi

# check if the config file exists or not and create it if it doesn't
if [ ! -f "$config_file" ]; then
    if touch "$config_file"; then
        log_grey "Created config file $config_file"
    else
        log_grey "Note: Failed to create config file $config_file.  You will need to manually update your PATH."
    fi
fi

needs_config_path_update=false
if ! grep -q "export PATH=\"\$PATH:$dest\"" "$config_file" 2>/dev/null; then
    # If the path is not in the config file, then we need to update the config file
    needs_config_path_update=true
fi

needs_path_update=false
if ! echo "$PATH" | grep -q "$dest"; then
    # If the dest is not in the PATH, then we need to notify to update the PATH.  When the config file needs updates this is already handled.
    needs_path_update=true
fi

# Check if aliases need to be updated
needs_alias_update=false
if ! grep -q "alias dbtf=$dest/dbt" "$config_file" 2>/dev/null; then
    needs_alias_update=true
fi

if [ "$shell_name" != "fish" ]; then

    if [ "$needs_config_path_update" = true ]; then
        {
            echo "" >> "$config_file" && \
            echo "# Added by dbt installer" >> "$config_file" && \
            echo "export PATH=\"\$PATH:$dest\"" >> "$config_file" && \
            log_grey "Added $dest to PATH in $config_file"
        } || {
            # Fall back to instructions if modification failed
            log "NOTE: Failed to modify $config_file."
            show_path_instructions
        }
    fi
else
    if [ "$needs_config_path_update" = true ]; then
        {
            echo "fish_add_path $dest" >> "$config_file"
            log_grey "Added $dest to PATH in $config_file"
        } || {
            # Fall back to instructions if modification failed
            log_grey "NOTE: Failed to modify $config_file."
            show_path_instructions
        }

    fi
fi

if [ "$needs_path_update" = true ]; then
    log "To use dbt in this session, run:" && \
    log "    source $config_file" && \
    log "" && \
    log "The PATH change will be permanent for new terminal sessions.";
fi


# Handle alias updates separately
if [ "$needs_alias_update" = true ]; then
        {
            echo "" >> "$config_file" && \
            echo "# dbt aliases" >> "$config_file" && \
            echo "alias dbtf=$dest/dbt" >> "$config_file" && \
            log "Added alias dbtf to $config_file" && \
            log "To run with dbtf in this session, run: source $config_file"
        } || {
            log_grey "NOTE: Failed to add aliases to $config_file."
        }
fi
