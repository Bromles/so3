param(
    [string]$MaelstromBin = "",
    [string]$MaelstromJar = ""
)

$ErrorActionPreference = "Stop"

& (Join-Path $PSScriptRoot "run-lin-kv.ps1") `
    -Workload "lin-kv" `
    -NodeCount 3 `
    -TimeLimit "30" `
    -Rate "20" `
    -Concurrency "2n" `
    -Nemesis "partition" `
    -NemesisInterval "5" `
    -MaelstromBin $MaelstromBin `
    -MaelstromJar $MaelstromJar `
    -LogStderr
