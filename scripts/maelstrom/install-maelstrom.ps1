param(
    [string]$Version = "0.2.4",
    [string]$InstallDir = ".tools/maelstrom"
)

$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$installRoot = Join-Path $repoRoot $InstallDir
$archivePath = Join-Path $installRoot "maelstrom.tar.bz2"
$extractDir = Join-Path $installRoot "maelstrom"
$jarPath = Join-Path $extractDir "lib\maelstrom.jar"
$downloadUrl = "https://github.com/jepsen-io/maelstrom/releases/download/v$Version/maelstrom.tar.bz2"

New-Item -ItemType Directory -Force -Path $installRoot | Out-Null
if (Test-Path $extractDir) {
    Remove-Item -LiteralPath $extractDir -Recurse -Force
}
Invoke-WebRequest -Uri $downloadUrl -OutFile $archivePath
tar -xjf $archivePath -C $installRoot

Write-Host "Installed Maelstrom under $extractDir"
Write-Host "Use jar path: $jarPath"
Write-Host "Set MAELSTROM_JAR to that jar or pass -MaelstromJar."
