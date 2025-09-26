# Process script parameters
param(
    [switch]$Update,
    [string]$Version,
    [string]$Target,
    [string]$To = "$env:USERPROFILE\.local\bin"
)

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

# Define constants
# this HOSTNAME is used for CI testing
$script:HOSTNAME = if ($env:_FS_HOSTNAME) { $env:_FS_HOSTNAME } else { 'public.cdn.getdbt.com' }
$script:versionsUrl = "https://$script:HOSTNAME/fs/versions.json"

# Global variables to track state
$script:TempDirs = @()
$script:UpdateScheduled = $false
$script:PathUpdated = $false

# Color support
$Host.UI.RawUI.WindowTitle = 'dbt Installer'

#region Logging Functions
function Write-Log {
    param($Message)
    Write-Host ('install.ps1: ' + $Message.Replace("\\", "\"))
    [Console]::Out.Flush()
}

function Write-GrayLog {
    param($Message)
    $prevColor = $Host.UI.RawUI.ForegroundColor
    $Host.UI.RawUI.ForegroundColor = "DarkGray"
    [Console]::Error.WriteLine('install.ps1: ' + $Message.Replace("\\", "\"))
    $Host.UI.RawUI.ForegroundColor = $prevColor
}

function Write-Debug {
    param($Message)
    Write-GrayLog ('DEBUG ' + $Message)
}

#region Error Handling Functions
function Write-ErrorAndExit {
    param(
        [string]$ErrorMessage,
        [string]$AdditionalInfo = ''
    )

    # Clear any active progress bars
    Write-Progress -Activity '*' -Status 'Failed' -Completed

    # Log additional info if provided
    if ($AdditionalInfo) {
        Write-GrayLog $AdditionalInfo
    }

    # Clean up any temp directories
    Remove-TempDirs

    # Log the error in red (PowerShell standard for errors)
    Write-Host 'install.ps1: ERROR ' -ForegroundColor Red -NoNewline
    Write-Host ($ErrorMessage.Replace("\\", "\")) -ForegroundColor Red

    # Stop script execution
    throw
}

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

function New-TrackedTempDir {
    $td = New-Item -ItemType Directory -Force -Path ([System.IO.Path]::GetTempPath() + [System.Guid]::NewGuid().ToString())
    $script:TempDirs += $td.FullName
    return $td.FullName
}
#endregion

#region System Check Functions
function Test-Need {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)]
        [string]$Command
    )

    if (-not (Get-Command $Command -ErrorAction SilentlyContinue)) {
        Write-ErrorAndExit "Required command '$Command' not found"
    }
}

function Test-RequiredCommands {
    $commands = @(
        'Invoke-WebRequest',
        'Expand-Archive',
        'Get-WmiObject',
        'ConvertTo-Json'
    )

    foreach ($command in $commands) {
        Test-Need -Command $command
    }
}

function Test-VCRedist {
    $registryPath = "HKLM:\SOFTWARE\Microsoft\VisualStudio\14.0\VC\Runtimes\x64"
    if (-not (Test-Path $registryPath)) {
        Write-GrayLog 'Microsoft Visual C++ Redistributable not found. Installing...'
        $url = 'https://aka.ms/vs/17/release/vc_redist.x64.exe'
        $outpath = Join-Path ([System.IO.Path]::GetTempPath()) 'vc_redist.x64.exe'

        try {
            Invoke-WebRequest -Uri $url -OutFile $outpath -ErrorAction Stop
            $process = Start-Process -FilePath $outpath -ArgumentList '/install', '/quiet', '/norestart' -Wait -PassThru
            if ($process.ExitCode -ne 0 -and $process.ExitCode -ne 3010) {  # 3010 means success but requires restart
                Write-ErrorAndExit 'Failed to install Microsoft Visual C++ Redistributable' -AdditionalInfo "Installation failed with exit code: $($process.ExitCode)"
            }
            Remove-Item $outpath -ErrorAction SilentlyContinue
            Write-GrayLog 'Microsoft Visual C++ Redistributable installed successfully'
        } catch {
            Write-ErrorAndExit 'Failed to install Microsoft Visual C++ Redistributable' -AdditionalInfo "Please install it manually from: https://aka.ms/vs/17/release/vc_redist.x64.exe`nError: $($_.Exception.Message)"
        }
    }
}

function Test-WritePermission {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)]
        [string]$Path
    )

    $testFile = Join-Path $Path 'test_write_permission'
    $null = New-Item -ItemType File -Path $testFile -Force -ErrorAction Stop
    Remove-Item -Path $testFile -Force -ErrorAction Stop
}
#endregion

#region Version Management Functions

function Test-VersionExists {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)]
        [string]$Version,
        [Parameter(Mandatory=$true)]
        [string]$Target
    )

    $url = "https://$script:HOSTNAME/fs/cli/fs-v$Version-$Target.zip"
    try {
        Invoke-WebRequest -Uri $url -Method Head -UseBasicParsing -ErrorAction Stop
        return $true
    } catch {
        if ($_.Exception.Response.StatusCode -eq 404) {
            Write-ErrorAndExit "Version $Version does not exist" -AdditionalInfo "URL checked: $url"
        } else {
            Write-ErrorAndExit 'Failed to check version availability' -AdditionalInfo "Error: $($_.Exception.Message)`nURL: $url"
        }
    }
}
function Get-InstalledVersion {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [ValidateScript({
            if(-not (Test-Path -Path $_ -IsValid)) {
                throw "Path '$_' contains invalid characters"
            }
            return $true
        })]
        [string]$BinaryPath
    )

    try {
        if (-not (Test-Path $BinaryPath)) {
            Write-GrayLog "No existing installation found at $BinaryPath"
            return $null
        }

        try {
            $versionOutput = & $BinaryPath --version 2>&1
            if ($LASTEXITCODE -ne 0) {
                Write-ErrorAndExit 'Failed to get version' -AdditionalInfo "Exit code: $LASTEXITCODE"
            }

            # Split on space and take second part (e.g. "dbt 2.0.0-beta.63" -> "2.0.0-beta.63")
            $version = ($versionOutput -split ' ')[1]
            if (-not $version) {
                Write-ErrorAndExit 'Failed to get version' -AdditionalInfo 'Unexpected output format from --version command'
            }

            # Remove 'v' prefix if present
            $version = $version -replace '^v', ''
            Write-Log ('Current installed version: ' + $version)
            return $version
        } catch {
            $errorMsg = switch -Wildcard ($_.Exception.Message) {
                "*0x8007045A*" { "The binary appears to be corrupted or is missing dependencies" }
                "*0xC0000135*" { "Missing required DLL dependencies. Try installing Visual C++ Redistributable" }
                "*Command exited with code*" { "Failed to get version. Try running 'dbt --version' directly to see the error" }
                "*Unexpected version output*" { "Version command output was in unexpected format" }
                default { "Error checking version: $($_.Exception.Message)" }
            }
            Write-GrayLog $errorMsg
            return $null
        }
    } catch {
        Write-GrayLog "Error accessing binary at $BinaryPath`: $($_.Exception.Message)"
        return $null
    }
}

function Get-VersionInfo {
    [CmdletBinding()]
    param()

    Write-GrayLog ('Attempting to fetch version information from ' + $versionsUrl)

    try {
        $versionInfo = Invoke-RestMethod -Uri $versionsUrl -ErrorAction Stop

        # Validate version info structure
        if (-not $versionInfo -or $versionInfo -isnot [PSObject]) {
            Write-ErrorAndExit 'Invalid version information received from server'
        }

        # Validate latest version exists
        if (-not ($versionInfo.PSObject.Properties.Name -contains 'latest')) {
            Write-ErrorAndExit 'No latest version information available'
        }

        return $versionInfo
    } catch {
        $errorMsg = switch -Wildcard ($_.Exception.Message) {
            "*404*" { "Version information not found at $versionsUrl" }
            "*Could not establish trust relationship*" { "SSL/TLS connection failed. Check your internet security settings" }
            "*Unable to connect*" { "Connection failed. Check your internet connection" }
            "*Invalid version information*" { "Invalid version information received from server" }
            "*No latest version*" { "No latest version information available" }
            default { "Failed to fetch version information: $($_.Exception.Message)" }
        }
        Write-ErrorAndExit $errorMsg -AdditionalInfo "URL: $versionsUrl"
    }
}

function Get-TargetVersion {
    [CmdletBinding()]
    param(
        [Parameter()]
        [AllowNull()]
        [AllowEmptyString()]
        [string]$SpecificVersion,

        [Parameter(Mandatory=$true)]
        [ValidateNotNull()]
        [PSCustomObject]$VersionInfo
    )

    try {
        if ([string]::IsNullOrEmpty($SpecificVersion)) {
            Write-GrayLog 'Checking for latest version'

            if (-not $VersionInfo.PSObject.Properties['latest']) {
                Write-ErrorAndExit 'No latest version found in versions.json'
            }

            if (-not $VersionInfo.latest.PSObject.Properties['tag']) {
                Write-ErrorAndExit 'No tag field found in latest version'
            }

            $script:version = $VersionInfo.latest.tag -replace '^v', ''
            if (-not $version) {
                Write-ErrorAndExit 'Empty version tag in latest version'
            }

            Write-Log ('Latest version: ' + $version)
            return $version
        } else {
            Write-GrayLog ("Checking for " + $SpecificVersion + " version")

            # Check if version is a known tag in versions.json
            if (-not $VersionInfo.PSObject.Properties[$SpecificVersion]) {
                # If not a known tag, validate semantic version format
                if ($SpecificVersion -notmatch '^\d+\.\d+\.\d+(-[a-zA-Z0-9.]+)?$') {
                    Write-ErrorAndExit "Invalid version format: '$SpecificVersion'. Must be a semantic version (e.g., 1.2.3, 1.2.3-beta.1) or a known tag (e.g., latest, canary)" -AdditionalInfo "Available tags: $($VersionInfo.PSObject.Properties.Name -join ', ')"
                }

                # For direct version input, verify it exists on CDN
                $script:target = Get-Architecture
                $null = Test-VersionExists -Version $SpecificVersion -Target $target
            }

            if ($VersionInfo.PSObject.Properties[$SpecificVersion]) {
                if (-not $VersionInfo.$SpecificVersion.PSObject.Properties['tag']) {
                    Write-ErrorAndExit "No tag field found for version $SpecificVersion"
                }

                $version = $VersionInfo.$SpecificVersion.tag -replace '^v', ''
                if (-not $version) {
                    Write-ErrorAndExit "Empty version tag for version $SpecificVersion"
                }

                Write-GrayLog ($SpecificVersion + " available version: " + $version)
                return $version
            } else {
                Write-GrayLog ("Version $SpecificVersion not found in versions.json, using as-is")
                return $SpecificVersion
            }
        }
    } catch {
        Write-ErrorAndExit 'Failed to determine target version' -AdditionalInfo $_.Exception.Message
    }
}

function Compare-Versions {
    [CmdletBinding()]
    param(
        [Parameter()]
        [AllowNull()]
        [AllowEmptyString()]
        [string]$CurrentVersion,

        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [string]$TargetVersion,

        [Parameter()]
        [bool]$IsLatest = $false
    )

    if ($CurrentVersion -eq $TargetVersion) {
        $message = if ($IsLatest) { 'Latest' } else { 'Version' }
        Write-ErrorAndExit "$message version $TargetVersion is already installed at $dest\dbt.exe"
    }
    return $true
}
#endregion

#region Installation Helper Functions
function Get-Architecture {
    # We only support x64 Windows with MSVC
    $arch = (Get-WmiObject Win32_Processor).Architecture
    if ($arch -ne 9) { # 9 = x64
        Write-ErrorAndExit 'Only x64 architecture is supported'
    }

    if (-not [string]::IsNullOrEmpty($Target)) {
        return $Target
    }

    $script:target = 'x86_64-pc-windows-msvc'
    Write-GrayLog "Target: $target"
    return $target
}

function Set-InstallationDestination {
    # Setting the default installation destination if not specified
    if ([string]::IsNullOrEmpty($To)) {
        # Install to user's AppData folder which doesn't require admin privileges
        $script:dest = Join-Path -Path $env:USERPROFILE -ChildPath ".local\bin"
    } else {
        $script:dest = $To
    }

    # Check write permissions to destination
    try {
        Test-WritePermission -Path $dest
    } catch {
        Write-ErrorAndExit "Cannot write to $dest" -AdditionalInfo 'You may need to run this script with elevated privileges'
    }
    return $dest
}
#endregion

#region Path and Alias Functions
function Update-InstallPath {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [string]$Path
    )

    # Try to add to PATH, but don't fail if we can't
    $userPath = [Environment]::GetEnvironmentVariable('Path', [EnvironmentVariableTarget]::User)
    if (-not ($userPath -split ';' -contains $Path)) {
        try {
            $newUserPath = $userPath + ';' + $Path
            [Environment]::SetEnvironmentVariable('Path', $newUserPath, [EnvironmentVariableTarget]::User)
            Write-Log "Added $Path to user PATH"
            $script:PathUpdated = $true
        } catch {
            Show-PathInstructions -InstallPath $Path
        }
    } else {
        Write-Log "$Path already in PATH"
    }
}

function Set-DbtAlias {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)]
        [string]$InstallPath
    )

    try {
        $null = New-Item -ItemType File -Path $PROFILE -Force -ErrorAction SilentlyContinue
        $aliasCommand = "Set-Alias -Name dbtf -Value '$InstallPath\dbt.exe'"

        if (-not (Select-String -Path $PROFILE -Pattern "Set-Alias.*dbtf.*dbt\.exe" -Quiet)) {
            Add-Content -Path $PROFILE -Value "`n# dbt CLI alias`n$aliasCommand" -Force
        }
    } catch {
        Write-ErrorAndExit "Failed to set dbt alias in $PROFILE"
    }
}

function Show-PathInstructions {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)]
        [string]$InstallPath
    )

    Write-GrayLog ('NOTE: ' + $InstallPath + ' may not be in your PATH.')
    Write-GrayLog 'To add it permanently, you can:'
    Write-GrayLog '  1. Run in PowerShell as Administrator:'
    Write-GrayLog ("     [Environment]::SetEnvironmentVariable('Path', `$env:Path + ';$InstallPath', [EnvironmentVariableTarget]::User)")
    Write-GrayLog '  2. Or manually add to Path in System Properties -> Environment Variables'
    Write-GrayLog ''
    Write-GrayLog 'To use dbt in this session immediately, run:'
    Write-GrayLog ("    `$env:Path += ';$InstallPath'")
    Write-GrayLog ''
    Write-GrayLog 'Then restart your terminal for permanent changes to take effect'
}
#endregion

#region UI Functions
function Show-AsciiArt {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [string]$Version
    )
    # Display ASCII art without install.ps1 prefix
    # This differs from the original install.sh because windows doesn't support ANSI escape codes
    Write-Host @"

 =====              =====    DBT  
=========        =========  FUSION
 ===========    >========   ------
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
}
#endregion

#region Main Installation Functions
function Install-Dbt {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [string]$Version,

        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [ValidatePattern('^[a-z0-9_]+-[a-z]+-[a-z0-9]+-[a-z]+$')]
        [string]$Target,

        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [ValidateScript({
            if(-not (Test-Path -Path $_ -IsValid)) {
                throw "Path '$_' contains invalid characters"
            }
            return $true
        })]
        [string]$Destination,

        [switch]$Update
    )

    $td = New-TrackedTempDir

    try {
        $url = "https://$script:HOSTNAME/fs/cli/fs-v" + $Version + "-" + $Target + ".zip"

        Write-Log ('Installing dbt to: ' + ($Destination -replace "\\\\", "\"))
        Write-Log ('Downloading: ' + $url)

        try {
            Invoke-WebRequest -Uri $url -OutFile "$td\fs.zip" -ErrorAction Stop
        }
        catch {
            Write-ErrorAndExit "Failed to download package from $url. Verify you are requesting a valid version on a supported platform."
        }

        # Extract files
        try {
            Expand-Archive -Path "$td\fs.zip" -DestinationPath $td -Force
        }
        catch {
            Write-ErrorAndExit 'Failed to extract files' -AdditionalInfo $_.Exception.Message
        }

        # Find the executable
        $sourceFile = $null
        Get-ChildItem -Path $td -File -Recurse | ForEach-Object {
            if ($_.Extension -eq '.exe') {
                $sourceFile = $_.FullName
            }
        }

        if (-not $sourceFile) {
            Write-ErrorAndExit 'No executable found in package' -AdditionalInfo 'The downloaded package appears to be corrupted or empty'
        }

        if (-not (Test-Path -Path $Destination)) {
            New-Item -Path $Destination -ItemType Directory -Force | Out-Null
        }

        $destFilePath = Join-Path -Path $Destination -ChildPath 'dbt.exe'

        if (Test-Path $destFilePath) {
            if (-not $Update) {
                Write-ErrorAndExit "dbt already exists in $($Destination -replace '\\', ''). Use the -Update flag to reinstall"
            }

            # Get the parent process that launched us
            try {
                $parentProcess = Get-Process -Id (Get-CimInstance Win32_Process -Filter "ProcessId = $PID").ParentProcessId -ErrorAction SilentlyContinue

                if ($parentProcess -and $parentProcess.ProcessName.StartsWith('dbt')) {
                    # Wait for parent dbt process to exit
                    $parentProcess.WaitForExit()
                }

                # Now we can safely replace the file
                Move-Item -Path $sourceFile -Destination $destFilePath -Force
            }
            catch {
                Write-ErrorAndExit 'Failed to update dbt' -AdditionalInfo $_.Exception.Message
            }
        }
        else {
            # For new installations, just copy the file
            try {
                Copy-Item -Path $sourceFile -Destination $destFilePath -Force
            }
            catch {
                Write-ErrorAndExit 'Failed to install dbt' -AdditionalInfo $_.Exception.Message
            }
        }

        $script:PathUpdated = Update-InstallPath -Path $Destination

        Write-Log ('Successfully installed dbt v' + $Version + ' to ' + ($destFilePath -replace "\\\\", "\"))
    }
    finally {
        Remove-TempDirs
    }
}

function main {
    # Set strict error handling
    $ErrorActionPreference = 'Stop'

    # Check PowerShell version
    $requiredVersion = [Version]'5.1'
    $currentVersion = $PSVersionTable.PSVersion
    if ($currentVersion -lt $requiredVersion) {
        Write-ErrorAndExit "PowerShell version $requiredVersion or higher is required. Current version: $currentVersion"
    }

    # Check required commands
    Test-RequiredCommands

    # Check for Visual C++ Redistributable
    Test-VCRedist

    # Check for current installed version
    $currentVersion = Get-InstalledVersion -BinaryPath (Join-Path -Path $To -ChildPath 'dbt.exe')

    # Clean version format
    if (-not [string]::IsNullOrEmpty($Version)) {
        $Version = $Version -replace '^v', ''
    }

    # Get version information
    $versionInfo = Get-VersionInfo

    # Determine target version
    $version = Get-TargetVersion -SpecificVersion $Version -VersionInfo $versionInfo

    # Check if dbt is already installed
    $dbtPath = Join-Path -Path $To -ChildPath 'dbt.exe'
    if (Test-Path $dbtPath) {
        if (-not $Update) {
            Write-ErrorAndExit "dbt already exists in $To, use the -Update flag to reinstall"
        }
        # Compare versions and exit if no update needed
        if ($currentVersion -eq $version) {
            Write-Log "dbt version $version is already installed"
            return
        }
    }

    # Determine target architecture
    $script:target = Get-Architecture

    # Set installation destination
    $script:dest = Set-InstallationDestination

    # Install dbt
    Install-Dbt -Version $version -Target $target -Destination $dest -Update:$Update

    # Update PATH
    Update-InstallPath -Path $dest

    # Update alias
    Set-DbtAlias -InstallPath $dest

    # Display ASCII art
    Show-AsciiArt -Version $version

    if ($Update -and $currentVersion) {
        Write-GrayLog "Successfully updated dbt from $currentVersion to $Version"
    }

    # Show appropriate final messages
    if ($script:PathUpdated) {
        Write-Log 'Note: You may need to restart your terminal to use dbt from any directory'
    }
    Write-Log "Run 'dbt --help' to get started"

    # Clean up
    Remove-TempDirs

    return
}
#endregion

# Handle Ctrl+C
trap {
    Write-GrayLog 'Cancelling installation...'
    Remove-TempDirs
    return
}

try {
    main
} catch {
    # If this is a Write-ErrorAndExit error, it's already been logged
    if (-not $_.Exception.Message.StartsWith('ERROR ')) {
        Write-GrayLog 'Cancelling installation...'
        Write-GrayLog $_.Exception.Message
    }
    Remove-TempDirs
}
