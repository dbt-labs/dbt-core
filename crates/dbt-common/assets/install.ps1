<#
.SYNOPSIS
Install the FS CLI Binary for a target platform.

.DESCRIPTION
This script installs the FS CLI Binary. It allows specifying the version, target platform, and installation location.

.PARAMETER Update
Updates to latest or specified version.

.PARAMETER Version
Version of the CLI to install. Default is the latest release.

.PARAMETER Target
Install the release compiled for the specified target OS.

.PARAMETER To
Location where to install the binary. Default is C:\Program Files.

.EXAMPLE
.\install.ps1 -Update

.EXAMPLE
.\install.ps1 -Version "1.2.3" -Target "Windows" -To "C:\MyFolder"
#>

param(
    [switch]$Update,
    [string]$Version,
    [string]$Target,
    [string]$To = "$env:USERPROFILE\.local\bin"
)

# Color support
$Host.UI.RawUI.WindowTitle = "dbt Installer"

function Write-Log {
    param($Message)
    Write-Host "install.ps1: $Message"
}

function Write-ErrorLog {
    param($Message)
    Write-Error "install.ps1: ERROR $Message"
}

function Write-GrayLog {
    param($Message)
    $prevColor = $Host.UI.RawUI.ForegroundColor
    $Host.UI.RawUI.ForegroundColor = "DarkGray"
    Write-Host $Message
    $Host.UI.RawUI.ForegroundColor = $prevColor
}

# Check for required commands
function Test-Need {
    param($Command)
    if (-not (Get-Command $Command -ErrorAction SilentlyContinue)) {
        Write-ErrorLog "need $Command (command not found)"
        exit 1
    }
}

# Process arguments
if ($PSBoundParameters.ContainsKey('Help')) {
    Get-Help $MyInvocation.MyCommand.Name
    exit 0
}

# Main script logic starts here

# Set strict error handling
$ErrorActionPreference = 'Stop'

# Define constants
# use an environment variable to allow for testing
$HOSTNAME = if ($env:_FS_HOSTNAME) { $env:_FS_HOSTNAME } else { "public.cdn.getdbt.com" }
$fetchLatest = "https://$HOSTNAME/fs/latest.json"

# Function to handle errors and exit
function Handle-Error {
    param(
        [string]$ErrorMessage,
        [string]$AdditionalInfo = ""
    )
    Write-ErrorLog $ErrorMessage
    if ($AdditionalInfo) {
        Write-Log $AdditionalInfo
    }
    exit 1
}

# Check for current installed version
$currentVersion = $null
$dbtPath = Join-Path -Path $To -ChildPath "dbt.exe"
if (Test-Path $dbtPath) {
    try {
        $versionOutput = & $dbtPath --version 2>$null
        if ($versionOutput -match '\s+v?(\d+\.\d+\.\d+)') {
            $currentVersion = $Matches[1]
            Write-Log "Current installed version: $currentVersion"
        }
    } catch {
        Write-Log "Could not determine current version"
    }
}

# Check if a specific version is provided
if ([string]::IsNullOrEmpty($Version)) {
    Write-Log "Attempting to fetch latest version from $fetchLatest"

    # Check proxy settings
    $proxy = [System.Net.WebRequest]::DefaultWebProxy
    if ($proxy.Address) {
        Write-Log "Proxy is configured: $($proxy.Address)"
    }

    # Attempt to fetch version with detailed error handling
    try {
        Write-Log "Downloading $fetchLatest"
        $versionInfo = Invoke-RestMethod -Uri $fetchLatest -ErrorAction Stop
    } catch {
        $errorMessage = if ($_.Exception.Message -like "*Unable to connect*") {
            "Connection failed. Error: $_"
        } else {
            "Failed to fetch version information. Error: $_"
        }
        Handle-Error $errorMessage "Troubleshooting steps:`n1. Check if you can access the URL in a web browser`n2. Verify your PowerShell execution policy (Get-ExecutionPolicy)`n3. Check if you're behind a proxy that needs configuration`n4. Check your firewall settings"
    }

    # Extract version from the JSON response
    if ($versionInfo -is [PSCustomObject] -and $versionInfo.tag) {
        $version = $versionInfo.tag -replace '^v', ''
    } else {
        Handle-Error "Unexpected response format from version endpoint" "Response received: $($versionInfo | ConvertTo-Json)"
    }

    Write-Log "Version: latest ($version)"

    # If current version matches latest exit
    if (($currentVersion -eq $version)) {
        Write-Host "`nLatest version $version is already installed at $dest\dbt.exe`n"
        exit 0
    }
} else {
    $version = $Version -replace '^v', ''
    Write-Log "Version: $version"

    # If current version matches requested version, exit
    if (($currentVersion -eq $version)) {
        Write-Host "`nVersion $version is already installed at $dest\dbt.exe`n"
        exit 0
    }
}


# Determine CPU architecture and operating system
$cpuArchTarget = switch -Wildcard ((Get-WmiObject Win32_Processor).Architecture) {
    0 { "x86" }
    9 { "x64" }
    # Add more cases if needed for different architectures
    Default { "unknown" }
}

$operatingSystem = "windows" # Since this script is intended for Windows

# Log the information
Write-Log "CPU Architecture: $cpuArchTarget"
Write-Log "Operating System: $operatingSystem"

if ([string]::IsNullOrEmpty($Target)) {
    # Check CPU architecture and set target for supported architecture
    if ($cpuArchTarget -eq "x64") {
        $target = "x86_64-pc-windows-msvc"
    } else {
        Write-ErrorLog "Unsupported CPU Architecture: $cpuArchTarget"
        exit 1
    }
}

# Log the target
Write-Log "Target: $target"

# Setting the default installation destination if not specified
if ([string]::IsNullOrEmpty($To)) {
    # Install to user's AppData folder which doesn't require admin privileges
    $dest = Join-Path -Path $env:USERPROFILE -ChildPath ".local\bin"
} else {
    $dest = $To
}

Write-Log "Installing dbt to: $dest"

# Construct the download URL for the zip file
$url = "https://public.cdn.getdbt.com/fs/cli/fs-v$version-$target.zip"

Write-Log "Downloading: $url"

# Create a temporary directory for the download
$td = New-Item -ItemType Directory -Force -Path ([System.IO.Path]::GetTempPath() + [System.Guid]::NewGuid().ToString())

# Download the zip file
try {
    Write-GrayLog "Downloading $url"
    Invoke-WebRequest -Uri $url -OutFile "$td\fs.zip"
} catch {
    Handle-Error "Failed to download" "Please check your network connection and try again"
}

# Extract the contents of the zip file
try {
    Write-GrayLog "Extracting files..."
    Expand-Archive -Path "$td\fs.zip" -DestinationPath $td -Force
} catch {
    Handle-Error "Failed to extract the zip file: $_" "Please ensure you have sufficient disk space and permissions"
}

# Create destination directory if it doesn't exist
if (-not (Test-Path -Path $dest)) {
    New-Item -Path $dest -ItemType Directory -Force
}

# Iterate over files in the temporary directory
$found = $false
Get-ChildItem -Path $td -File -Recurse | ForEach-Object {
    $filePath = $_.FullName

    # Check if the file is executable
    if ($_.Extension -eq ".exe") {
        $found = $true
        $destFilePath = Join-Path -Path $dest -ChildPath $_.Name

        # Check for existing installation
        if (Test-Path -Path $destFilePath) {
            # Remove the existing file if updating
            Remove-Item -Path $destFilePath -Force
        }

        # Copy the file to the destination
        Write-GrayLog "Moving to: $destFilePath"
        Copy-Item -Path $filePath -Destination $destFilePath -Force
        Write-GrayLog "Copied $($_.Name) to $destFilePath"
    }
}

if (-not $found) {
    Handle-Error "No executable files found in the downloaded package" "The downloaded package appears to be corrupted or incomplete"
}

# Add the installation destination to the user PATH if it's not already there
$userPath = [Environment]::GetEnvironmentVariable('Path', [EnvironmentVariableTarget]::User)
if (-not ($userPath -split ';' -contains $dest)) {
    $newUserPath = $userPath + ';' + $dest
    [Environment]::SetEnvironmentVariable('Path', $newUserPath, [EnvironmentVariableTarget]::User)
    Write-Log "Added $dest to user PATH."
} else {
    Write-Host "$dest already in PATH"
}

# Create a persistent alias
$psProfilePath = $PROFILE
if (-not (Test-Path -Path $psProfilePath)) {
    New-Item -ItemType File -Path $psProfilePath -Force
}

$aliasCommand = "Set-Alias -Name dbtf -Value '$dest\dbt.exe'"
if (-not (Select-String -Path $psProfilePath -Pattern "Set-Alias.*dbtf.*dbt\.exe" -Quiet)) {
    Add-Content -Path $psProfilePath -Value "`n# dbt CLI alias" -Force
    Add-Content -Path $psProfilePath -Value $aliasCommand -Force
    Write-Log "Added dbtf alias to PowerShell profile"
} else {
    Write-Host "dbtf alias already exists in PowerShell profile"
}

Write-Host @"

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
 =====             =====                         └─┘┘└┘└─┘┴┘└┘└─┘ $Version

"@

Write-Host "`nSuccessfully installed dbt v$Version to $dest\dbt.exe"
Write-Host "`nNote: You may need to restart your machine to refresh your PATH:"
Write-Host "`nRun 'dbt --help' to get started`n"

# Clean up the temporary directory
Remove-Item -Path $td -Recurse -Force
