<#
.SYNOPSIS
Install the dbt CLI Binary for Windows.

.DESCRIPTION
This script installs the dbt CLI Binary. It allows specifying the version, target platform, and installation location.

.PARAMETER Update
Updates to latest or specified version.

.PARAMETER Version
Version of dbt to install. Default is the latest release.

.PARAMETER Target
Install the release compiled for the specified target OS.

.PARAMETER To
Location to install the binary. Default is $env:USERPROFILE\.local\bin.

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

# Function to write logs
function Write-Log {
    param($Message)
    Write-Host ("install.ps1: " + $Message.Replace("\\", "\"))
    [Console]::Out.Flush()
}

function Write-GrayLog {
    param($Message)
    $prevColor = $Host.UI.RawUI.ForegroundColor
    $Host.UI.RawUI.ForegroundColor = "DarkGray"
    [Console]::Error.WriteLine("install.ps1: " + $Message.Replace("\\", "\"))
    $Host.UI.RawUI.ForegroundColor = $prevColor
}

function Write-ErrorLog {
    param($Message)
    Write-GrayLog ("ERROR " + $Message.Replace("\\", "\"))
    if ($td -and (Test-Path $td)) {
        Remove-Item -Path $td -Recurse -Force -ErrorAction SilentlyContinue
    }
    exit 1
}

function Write-Debug {
    param($Message)
    Write-GrayLog ("DEBUG " + $Message)
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

# Check PowerShell version
$requiredVersion = [Version]"5.1"
$currentVersion = $PSVersionTable.PSVersion
if ($currentVersion -lt $requiredVersion) {
    Write-ErrorLog "PowerShell version $requiredVersion or higher is required. Current version: $currentVersion"
    exit 1
}

# Function to check required commands
function Test-Requirement {
    param(
        [string]$Command,
        [string]$ModuleName = "",
        [string]$MinimumVersion = ""
    )
    
    $cmdlet = Get-Command $Command -ErrorAction SilentlyContinue
    if (-not $cmdlet) {
        if ($ModuleName) {
            Write-ErrorLog "Required command '$Command' not found. You may need to install the $ModuleName module"
        } else {
            Write-ErrorLog "Required command '$Command' not found"
        }
        return $false
    }

    if ($MinimumVersion -and $cmdlet.Version -lt [Version]$MinimumVersion) {
        Write-ErrorLog "Command '$Command' version $MinimumVersion or higher is required. Current version: $($cmdlet.Version)"
        return $false
    }

    return $true
}

# Function to check and install Visual C++ Redistributable
function Test-VCRedist {
    $registryPath = "HKLM:\SOFTWARE\Microsoft\VisualStudio\14.0\VC\Runtimes\x64"
    if (-not (Test-Path $registryPath)) {
        Write-GrayLog "Microsoft Visual C++ Redistributable not found. Installing..."
        $url = "https://aka.ms/vs/17/release/vc_redist.x64.exe"
        $outpath = Join-Path ([System.IO.Path]::GetTempPath()) "vc_redist.x64.exe"

        try {
            Invoke-WebRequest -Uri $url -OutFile $outpath -ErrorAction Stop
            $process = Start-Process -FilePath $outpath -ArgumentList "/install", "/quiet", "/norestart" -Wait -PassThru
            if ($process.ExitCode -ne 0 -and $process.ExitCode -ne 3010) {  # 3010 means success but requires restart
                throw "Installation failed with exit code $($process.ExitCode)"
            }
            Remove-Item $outpath -ErrorAction SilentlyContinue
            Write-GrayLog "Microsoft Visual C++ Redistributable installed successfully"
        } catch {
            Write-GrayLog "Failed to install Microsoft Visual C++ Redistributable"
            Write-GrayLog "Please install it manually from: https://aka.ms/vs/17/release/vc_redist.x64.exe"
            Write-GrayLog ("Error: " + $_.Exception.Message)
            return $false
        }
    }
    return $true
}

# Check required commands
$requirements = @(
    @{Command="Invoke-WebRequest"; ModuleName="Microsoft.PowerShell.Utility"},
    @{Command="Expand-Archive"; ModuleName="Microsoft.PowerShell.Archive"},
    @{Command="Get-WmiObject"; ModuleName="Microsoft.PowerShell.Management"},
    @{Command="ConvertTo-Json"; ModuleName="Microsoft.PowerShell.Utility"}
)

foreach ($req in $requirements) {
    if (-not (Test-Requirement @req)) {
        exit 1
    }
}

# Check for Visual C++ Redistributable
if (-not (Test-VCRedist)) {
    exit 1
}

# Check write permissions to destination
function Test-WritePermission {
    param([string]$Path)
    
    try {
        # Try to create a test file
        $testFile = Join-Path $Path "test_write_permission"
        $null = New-Item -ItemType File -Path $testFile -Force -ErrorAction Stop
        Remove-Item -Path $testFile -Force -ErrorAction Stop
        return $true
    } catch {
        Write-ErrorLog "No write permission to $Path. You may need to run this script with elevated privileges"
        return $false
    }
}

# Function to show PATH update instructions
function Show-PathInstructions {
    param([string]$InstallPath)
    
    Write-GrayLog ""
    Write-GrayLog ("NOTE: " + $InstallPath + " may not be in your PATH.")
    Write-GrayLog "To add it permanently, you can:"
    Write-GrayLog "  1. Run in PowerShell as Administrator:"
    Write-GrayLog ("     [Environment]::SetEnvironmentVariable('Path', `$env:Path + ';$InstallPath', [EnvironmentVariableTarget]::User)")
    Write-GrayLog "  2. Or manually add to Path in System Properties -> Environment Variables"
    Write-GrayLog ""
    Write-GrayLog "To use dbt in this session immediately, run:"
    Write-GrayLog ("    `$env:Path += ';$InstallPath'")
    Write-GrayLog ""
    Write-GrayLog "Then restart your terminal for permanent changes to take effect"
}

# Register cleanup for script termination
trap {
    Remove-TempDirs
    exit 1
}

# Define constants
# use an environment variable to allow for testing
$HOSTNAME = if ($env:_FS_HOSTNAME) { $env:_FS_HOSTNAME } else { "public.cdn.getdbt.com" }
$versionsUrl = "https://$HOSTNAME/fs/versions.json"

# Global variables to track state
$script:TempDirs = @()
$script:UpdateScheduled = $false
$script:PathUpdated = $false

# Function to clean up temp directories
function Remove-TempDirs {
    foreach ($td in $script:TempDirs) {
        if ($td -and (Test-Path $td)) {
            try {
                Remove-Item -Path $td -Recurse -Force -ErrorAction SilentlyContinue
            } catch {
                Write-Debug "Failed to remove temp directory: $td - $($_.Exception.Message)"
            }
        }
    }
    $script:TempDirs = @()
}

# Function to create and track temp directory
function New-TrackedTempDir {
    $td = New-Item -ItemType Directory -Force -Path ([System.IO.Path]::GetTempPath() + [System.Guid]::NewGuid().ToString())
    $script:TempDirs += $td.FullName
    return $td.FullName
}

# Function to handle errors and exit
function Handle-Error {
    param(
        [string]$ErrorMessage,
        [string]$AdditionalInfo = "",
        [switch]$NoExit
    )

    # Clear any active progress bars
    Write-Progress -Activity "*" -Status "Failed" -Completed

    # Log additional info if provided
    if ($AdditionalInfo) {
        Write-GrayLog $AdditionalInfo
    }

    # Clean up any temp directories
    Remove-TempDirs

    # Log the error
    Write-ErrorLog $ErrorMessage

    # Exit if requested
    if (-not $NoExit) {
        exit 1
    }
}

# Function to get installed version
function Get-InstalledVersion {
    param(
        [string]$BinaryPath
    )

    if (Test-Path $BinaryPath) {
        try {
            $versionOutput = & $BinaryPath --version 2>$null
            # Split on space and take second part (e.g. "dbt 2.0.0-beta.63" -> "2.0.0-beta.63")
            $version = ($versionOutput -split ' ')[1]
            # Remove 'v' prefix if present
            $version = $version -replace '^v', ''
            if ($version) {
                Write-Log ("Current installed version: " + $version)
                return $version
            }
        } catch {
            if ($_.Exception.Message -match "0x[0-9a-fA-F]+") {
                Write-GrayLog "Exit code indicates possible missing dependencies. Try running 'dbt --version' directly to see the error."
            }
        }
    }
    return $null
}

# Check for current installed version
$currentVersion = Get-InstalledVersion -BinaryPath (Join-Path -Path $To -ChildPath "dbt.exe") -PackageName "dbt"

# Function to get version information from versions.json
function Get-VersionInfo {
    Write-GrayLog ("Attempting to fetch version information from " + $versionsUrl)

    try {
        $versionInfo = Invoke-RestMethod -Uri $versionsUrl -ErrorAction Stop
        return $versionInfo
    } catch {
        Handle-Error ("Failed to fetch version information. Error: " + $_)
    }
}

# Function to determine target version
function Get-TargetVersion {
    param(
        [string]$SpecificVersion,
        [PSCustomObject]$VersionInfo
    )

    if ([string]::IsNullOrEmpty($SpecificVersion)) {
        Write-GrayLog "Checking for latest version"
        # Convert VersionInfo to hashtable to use .ContainsKey()
        $versionHash = @{}
        $VersionInfo.PSObject.Properties | ForEach-Object { $versionHash[$_.Name] = $_.Value }

        if (-not $versionHash.ContainsKey('latest')) {
            Handle-Error "No latest version found in versions.json" ("Response received: " + ($VersionInfo | ConvertTo-Json))
        }

        if (-not $VersionInfo.latest.PSObject.Properties['tag']) {
            Handle-Error "No tag field found in latest version" ("Response received: " + ($VersionInfo | ConvertTo-Json))
        }

        $version = $VersionInfo.latest.tag -replace '^v', ''
        Write-Log ("Latest version: " + $version)
        return $version
    } else {
        Write-GrayLog ("Checking for " + $SpecificVersion + " version")

        # Check if version exists in versions.json
        # Convert VersionInfo to hashtable to use .ContainsKey()
        $versionHash = @{}
        $VersionInfo.PSObject.Properties | ForEach-Object { $versionHash[$_.Name] = $_.Value }
        
        if ($versionHash.ContainsKey($SpecificVersion)) {
            # Version exists in versions.json
            $version = $VersionInfo.$SpecificVersion.tag -replace '^v', ''
            Write-GrayLog ($SpecificVersion + " available version: " + $version)
            return $version
        } else {
            # Version not found in versions.json, use as-is
            Write-GrayLog ("Requested version: " + $SpecificVersion)
            return $SpecificVersion
        }
    }
}

# Function to compare versions
function Compare-Versions {
    param(
        [string]$CurrentVersion,
        [string]$TargetVersion,
        [bool]$IsLatest = $false
    )

    if ([string]::IsNullOrEmpty($CurrentVersion) -or [string]::IsNullOrEmpty($TargetVersion)) {
        return $false
    }

    if ($CurrentVersion -eq $TargetVersion) {
        if ($IsLatest) {
            Write-Log ("Latest version " + $TargetVersion + " is already installed at " + $dest + "\dbt.exe")
        } else {
            Write-Log ("Version " + $TargetVersion + " is already installed at " + $dest + "\dbt.exe")
        }
        return $true
    }
    return $false
}

# Function to update PATH
function Update-Path {
    param([string]$Path)
    
    $userPath = [Environment]::GetEnvironmentVariable('Path', [EnvironmentVariableTarget]::User)
    if (-not ($userPath -split ';' -contains $Path)) {
        try {
            $newUserPath = $userPath + ';' + $Path
            [Environment]::SetEnvironmentVariable('Path', $newUserPath, [EnvironmentVariableTarget]::User)
            return $true
        } catch {
            Show-PathInstructions -InstallPath $Path
            return $false
        }
    }
    return $false
}

# Function to install dbt
function Install-Dbt {
    param(
        [string]$Version,
        [string]$Target,
        [string]$Destination,
        [switch]$Update
    )

    $td = New-TrackedTempDir

    try {
        $url = "https://public.cdn.getdbt.com/fs/cli/fs-v" + $Version + "-" + $Target + ".zip"

        Write-Log ("Installing dbt to: " + ($Destination -replace "\\\\", "\"))
        Write-Log ("Downloading: " + $url)

        try {
            Invoke-WebRequest -Uri $url -OutFile "$td\fs.zip" -ErrorAction Stop
        }
        catch {
            Write-ErrorLog ("Failed to download package from " + $url + ". Verify you are requesting a valid version on a supported platform.")
            return $false
        }

        # Extract files
        try {
            Expand-Archive -Path "$td\fs.zip" -DestinationPath $td -Force
        }
        catch {
            Write-ErrorLog ("Failed to extract files: " + $_.Exception.Message)
            return $false
        }

        # Find the executable
        $sourceFile = $null
        Get-ChildItem -Path $td -File -Recurse | ForEach-Object {
            if ($_.Extension -eq ".exe") {
                $sourceFile = $_.FullName
            }
        }

        if (-not $sourceFile) {
            Write-ErrorLog "No executable found in package"
            return $false
        }

        if (-not (Test-Path -Path $Destination)) {
            New-Item -Path $Destination -ItemType Directory -Force | Out-Null
        }

        $destFilePath = Join-Path -Path $Destination -ChildPath "dbt.exe"

        if (Test-Path $destFilePath) {
            if (-not $Update) {
                Write-ErrorLog ("dbt already exists in " + ($Destination -replace "\\\\", "\") + ", use the --update flag to reinstall")
                return $false
            }

            # Get the parent process that launched us
            try {
                $parentProcess = Get-Process -Id (Get-CimInstance Win32_Process -Filter "ProcessId = $PID").ParentProcessId -ErrorAction SilentlyContinue

                if ($parentProcess -and $parentProcess.ProcessName.StartsWith("dbt")) {
                    # Wait for parent dbt process to exit
                    $parentProcess.WaitForExit()
                }

                # Now we can safely replace the file
                Move-Item -Path $sourceFile -Destination $destFilePath -Force
            }
            catch {
                Write-ErrorLog ("Failed to update dbt: " + $_.Exception.Message)
                return $false
            }
        }
        else {
            # For new installations, just copy the file
            try {
                Copy-Item -Path $sourceFile -Destination $destFilePath -Force
            }
            catch {
                Write-ErrorLog ("Failed to install dbt: " + $_.Exception.Message)
                return $false
            }
        }

        $script:PathUpdated = Update-Path -Path $Destination

        Write-Log ("Successfully installed dbt v" + $Version + " to " + ($destFilePath -replace "\\\\", "\"))
        return $true
    }
    finally {
        Remove-TempDirs
    }
}

# Clean version format
if (-not [string]::IsNullOrEmpty($Version)) {
    $Version = $Version -replace '^v', ''
}

# Get version information
$versionInfo = Get-VersionInfo

# Determine target version
$version = Get-TargetVersion -SpecificVersion $Version -VersionInfo $versionInfo

# Determine CPU architecture and operating system
$cpuArchTarget = switch -Wildcard ((Get-WmiObject Win32_Processor).Architecture) {
    0 { "x86" }
    9 { "x64" }
    # Add more cases if needed for different architectures
    Default { "unknown" }
}

$operatingSystem = "windows" # Since this script is intended for Windows

# Set target based on architecture
if ([string]::IsNullOrEmpty($Target)) {
    # Check CPU architecture and set target for supported architecture
    if ($cpuArchTarget -eq "x64") {
        $target = "x86_64-pc-windows-msvc"
    } else {
        Write-ErrorLog "Unsupported CPU Architecture: $cpuArchTarget"
        exit 1
    }
}

# System info logs
Write-Log "CPU Architecture: $cpuArchTarget"
Write-Log "Operating System: $operatingSystem"
Write-GrayLog "Target: $target"

# Log the information

# Setting the default installation destination if not specified
if ([string]::IsNullOrEmpty($To)) {
    # Install to user's AppData folder which doesn't require admin privileges
    $dest = Join-Path -Path $env:USERPROFILE -ChildPath ".local\bin"
} else {
    $dest = $To
}

# Check write permissions to destination
if (-not (Test-WritePermission -Path $dest)) {
    exit 1
}

# Install dbt
if (-not (Install-Dbt -Version $version -Target $target -Destination $dest -Update:$Update)) {
    exit 1
}

# Try to add to PATH, but don't fail if we can't
$userPath = [Environment]::GetEnvironmentVariable('Path', [EnvironmentVariableTarget]::User)
if (-not ($userPath -split ';' -contains $dest)) {
    try {
        $newUserPath = $userPath + ';' + $dest
        [Environment]::SetEnvironmentVariable('Path', $newUserPath, [EnvironmentVariableTarget]::User)
        Write-Log "Added $dest to user PATH"
        $script:PathUpdated = $true
    } catch {
        Show-PathInstructions -InstallPath $dest
    }
} else {
    Write-Log "$dest already in PATH"
}

# Create a persistent alias for dbt
$psProfilePath = $PROFILE
if (-not (Test-Path -Path $psProfilePath)) {
    New-Item -ItemType File -Path $psProfilePath -Force
}

# Update alias handling to be silent
$aliasCommand = "Set-Alias -Name dbtf -Value '$dest\dbt.exe'"
if (-not (Select-String -Path $psProfilePath -Pattern "Set-Alias.*dbtf.*dbt\.exe" -Quiet)) {
    Add-Content -Path $psProfilePath -Value "`n# dbt CLI alias" -Force
    Add-Content -Path $psProfilePath -Value $aliasCommand -Force
}

# Display ASCII art without install.ps1 prefix
# This differs from the original install.sh because windows doesn't support ANSI escape codes
Write-Host @"

 =====              =====    DBT  
=========        =========  FUSION
 ===========    >========   -----
  ======================    ********************************************
   ====================     *          FUSION ENGINE INSTALLED         *
    ========--========      *                                          *
     =====-    -=====                    Version: $Version
    ========--========      *                                          *
   ====================     *     Run 'dbt --help' to get started      *
  ======================    ********************************************
 ========<   ============   
=========      ==========   
 =====             =====    

"@

# Show appropriate final messages
if ($script:PathUpdated) {
    Write-Log "Note: You may need to restart your terminal to use dbt from any directory"
}
Write-Log "Run 'dbt --help' to get started"

# Clean up
Remove-TempDirs

# Exit
exit 0
