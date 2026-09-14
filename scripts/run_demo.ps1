param(
    [ValidateSet("demo", "full")]
    [string]$Mode = "demo",
    [ValidateSet("all", "data", "parquet", "lazy", "execution", "cache", "mllib", "streaming")]
    [string]$Step = "all",
    [int]$HoldSeconds = 120
)

$projectRoot = Split-Path -Parent $PSScriptRoot
$python = Join-Path $projectRoot ".venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $python)) {
    throw "Project virtual environment not found. Run .\scripts\bootstrap.ps1 first."
}

& $python -m src.spark_demo --mode $Mode --step $Step --hold-seconds $HoldSeconds
exit $LASTEXITCODE
