param(
    [string]$MaelstromBin = "",
    [string]$MaelstromJar = ""
)

$ErrorActionPreference = "Stop"

& (Join-Path $PSScriptRoot "run-lin-kv.ps1") `
    -Workload "lin-kv" `
    -NodeCount 1 `
    -TimeLimit "10" `
    -Rate "20" `
    -Concurrency "1n" `
    -MaelstromBin $MaelstromBin `
    -MaelstromJar $MaelstromJar `
    -LogStderr
