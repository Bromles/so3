param(
    [string]$Workload = "lin-kv",
    [int]$NodeCount = 1,
    [string]$TimeLimit = "20",
    [string]$Rate = "100",
    [string]$Concurrency = "2n",
    [string]$MaelstromBin = "",
    [string]$MaelstromJar = "",
    [string]$BinaryPath = "",
    [string]$OutputDir = "var/maelstrom",
    [switch]$LogStderr,
    [switch]$NoBuild
)

$ErrorActionPreference = "Stop"
$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$defaultJarPath = Join-Path $repoRoot ".tools/maelstrom/maelstrom/lib/maelstrom.jar"

function Resolve-MaelstromCommand {
    param(
        [string]$ExplicitBin,
        [string]$ExplicitJar
    )

    if ($ExplicitBin) {
        return @($ExplicitBin, "test")
    }

    if ($ExplicitJar) {
        return @("java", "-jar", $ExplicitJar, "test")
    }

    $command = Get-Command maelstrom -ErrorAction SilentlyContinue
    if ($command) {
        return @($command.Source, "test")
    }

    $envJar = $env:MAELSTROM_JAR
    if ($envJar) {
        return @("java", "-jar", $envJar, "test")
    }

    if (Test-Path $defaultJarPath) {
        return @("java", "-jar", $defaultJarPath, "test")
    }

    throw "Maelstrom executable not found. Set -MaelstromBin, -MaelstromJar, or MAELSTROM_JAR."
}

function Test-SymlinkSupport {
    $tempRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("so3-maelstrom-" + [guid]::NewGuid())
    $targetPath = Join-Path $tempRoot "target.txt"
    $linkPath = Join-Path $tempRoot "link.txt"

    New-Item -ItemType Directory -Force -Path $tempRoot | Out-Null
    Set-Content -Path $targetPath -Value "ok"

    try {
        New-Item -ItemType SymbolicLink -Path $linkPath -Target $targetPath | Out-Null
        return $true
    }
    catch {
        return $false
    }
    finally {
        Remove-Item -LiteralPath $tempRoot -Recurse -Force -ErrorAction SilentlyContinue
    }
}

function Resolve-AdapterBinary {
    param([string]$ExplicitBinaryPath)

    if ($ExplicitBinaryPath) {
        return (Resolve-Path $ExplicitBinaryPath).Path
    }

    $cargoMetadata = cargo metadata --no-deps --format-version 1 | ConvertFrom-Json
    $targetDirectory = $cargoMetadata.target_directory
    $binaryName = if ($IsWindows) { "so3-maelstrom.exe" } else { "so3-maelstrom" }

    return (Join-Path $targetDirectory "debug\$binaryName")
}

Push-Location $repoRoot
try {
    if ($IsWindows -and -not (Test-SymlinkSupport)) {
        throw "Maelstrom requires symbolic link creation for store/current on Windows. Run from an elevated shell or enable Windows Developer Mode."
    }

    if (-not $NoBuild) {
        cargo build -p so3-maelstrom
    }

    $adapterBinary = Resolve-AdapterBinary -ExplicitBinaryPath $BinaryPath
    if (-not (Test-Path $adapterBinary)) {
        throw "Maelstrom adapter binary not found at $adapterBinary"
    }

    $maelstromCommand = Resolve-MaelstromCommand -ExplicitBin $MaelstromBin -ExplicitJar $MaelstromJar
    $resultsDirectory = Join-Path (Get-Location) $OutputDir
    New-Item -ItemType Directory -Force -Path $resultsDirectory | Out-Null

    $command = @(
        $maelstromCommand +
        @(
            "--workload", $Workload,
            "--bin", $adapterBinary,
            "--node-count", $NodeCount,
            "--time-limit", $TimeLimit,
            "--rate", $Rate,
            "--concurrency", $Concurrency,
            "--no-ssh"
        )
    )

    if ($LogStderr) {
        $command += "--log-stderr"
    }

    Write-Host "Running:" ($command -join " ")
    & $command[0] $command[1..($command.Length - 1)]
    if ($LASTEXITCODE -ne 0) {
        throw "Maelstrom exited with code $LASTEXITCODE"
    }
}
finally {
    Pop-Location
}
