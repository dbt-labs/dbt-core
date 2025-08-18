# Uninstall script for dbt and dbt-lsp
param(
    [string]$installLocation = "$env:USERPROFILE\.local\bin",
    [string]$package
)

# Color support
$Host.UI.RawUI.WindowTitle = "dbt Uninstaller"

# Get script path for cleanup
$scriptPath = $MyInvocation.MyCommand.Path

function Write-Log {
    param($Message)
    Write-Host "uninstall.ps1: $Message"
}

function Write-GrayLog {
    param($Message)
    $prevColor = $Host.UI.RawUI.ForegroundColor
    $Host.UI.RawUI.ForegroundColor = "DarkGray"
    [Console]::Error.WriteLine("uninstall.ps1: $Message")
    $Host.UI.RawUI.ForegroundColor = $prevColor
}

function Write-ErrorLog {
    param($Message)
    Write-GrayLog "ERROR $Message"
    if ($scriptPath -and (Test-Path $scriptPath)) {
        Remove-Item -Path $scriptPath -Force -ErrorAction SilentlyContinue
    }
    exit 1
}

# Function to check required commands
function Test-Need {
    param($Command)
    if (-not (Get-Command $Command -ErrorAction SilentlyContinue)) {
        Write-ErrorLog "need $Command (command not found)"
        exit 1
    }
}

# Function to format package name for display
function Format-PackageName {
    param([string]$PackageName)
    if ($PackageName -eq "all") {
        return "dbt and dbt-lsp"
    }
    return $PackageName
}

# Show help if requested
if ($PSBoundParameters.ContainsKey('Help')) {
    Write-Host @"
Usage: uninstall.ps1 [options]

Options:
  -installLocation PATH  Install location of dbt (default: $env:USERPROFILE\.local\bin)
  -package PACKAGE      Uninstall package PACKAGE [dbt|dbt-lsp|all] (default: dbt)
  -Help                 Show this help text
"@
    if ($scriptPath -and (Test-Path $scriptPath)) {
        Remove-Item -Path $scriptPath -Force -ErrorAction SilentlyContinue
    }
    exit 0
}

# Check required commands
Test-Need "Remove-Item"
Test-Need "Test-Path"
Test-Need "Join-Path"

# Set default package if not specified
if (-not $package) {
    $package = "dbt"
}

# Convert relative path to absolute if needed
if (-not [System.IO.Path]::IsPathRooted($installLocation)) {
    $installLocation = Join-Path $PWD $installLocation
    Write-GrayLog ("Converting to absolute path: " + $installLocation)
}

# Function to uninstall a package
function Uninstall-Package {
    param(
        [string]$PackageName,
        [string]$Location
    )

    $binaryPath = "$Location\$PackageName.exe"
    
    if (Test-Path $binaryPath) {
        try {
            Remove-Item -Path $binaryPath -Force -ErrorAction Stop
            Write-Log ("Uninstalled " + (Format-PackageName $PackageName) + " from " + $binaryPath)
            return $true
        } catch {
            Write-GrayLog ("Failed to uninstall " + (Format-PackageName $PackageName) + ": " + $_)
            return $false
        }
    } else {
        Write-GrayLog ((Format-PackageName $PackageName) + " not found at: " + $binaryPath)
        return $false
    }
}

# Determine which packages to uninstall
$packagesToUninstall = switch ($package) {
    "all" { @("dbt", "dbt-lsp") }
    "dbt" { @("dbt") }
    "dbt-lsp" { @("dbt-lsp") }
    default { 
        Write-ErrorLog ("Invalid package: " + $package + ". Must be one of: dbt, dbt-lsp, all")
        exit 1
    }
}

# Uninstall selected packages
foreach ($pkg in $packagesToUninstall) {
    if (-not (Uninstall-Package -PackageName $pkg -Location $installLocation)) {
        Write-ErrorLog ("Failed to uninstall " + $pkg)
    }
}

# Clean up the script itself
if ($scriptPath -and (Test-Path $scriptPath)) {
    Remove-Item -Path $scriptPath -Force -ErrorAction SilentlyContinue
}