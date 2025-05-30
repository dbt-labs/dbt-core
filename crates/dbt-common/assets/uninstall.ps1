param(
    [string]$installLocation = "$env:USERPROFILE\.local\bin"
)

# Binary name
$CLI_BINARY = "dbt"

function Write-ErrorLog {
    param($Message)
    Write-Error "ERROR: $Message"
}

# Ensure the path ends with the binary name
$binaryPath = if ($installLocation -match "\\$CLI_BINARY\.exe$") {
    $installLocation
} else {
    Join-Path $installLocation "$CLI_BINARY.exe"
}

# Check if the binary exists before attempting to remove it
if (Test-Path $binaryPath) {
    try {
        Remove-Item -Path $binaryPath -Force -ErrorAction Stop
        Write-Host "Uninstalled $CLI_BINARY.exe from $installLocation"
    } catch {
        Write-ErrorLog $_.Exception.Message
    }
} else {
    Write-ErrorLog "The binary does not exist at: $installLocation"
}