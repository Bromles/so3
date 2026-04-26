param(
    [string]$MaelstromBin = "",
    [string]$MaelstromJar = ""
)

$ErrorActionPreference = "Stop"

& (Join-Path $PSScriptRoot "run-lin-kv.ps1") `
    -Workload "lin-kv" `
    -NodeCount 3 `
    -TimeLimit "10" `
    -Rate "10" `
    -Concurrency "2n" `
    -MaelstromBin $MaelstromBin `
    -MaelstromJar $MaelstromJar `
    -LogStderr
