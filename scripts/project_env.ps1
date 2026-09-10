$projectRoot = Split-Path -Parent $PSScriptRoot
$localJdk = Get-ChildItem -LiteralPath (Join-Path $projectRoot ".jdk") -Directory -Filter "jdk-*" |
    Where-Object { Test-Path -LiteralPath (Join-Path $_.FullName "bin\java.exe") } |
    Select-Object -First 1

if (-not $localJdk) {
    throw "Project-local JDK not found. Run .\scripts\bootstrap.ps1 first."
}

$python = Join-Path $projectRoot ".venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $python)) {
    throw "Project virtual environment not found. Run .\scripts\bootstrap.ps1 first."
}

$env:JAVA_HOME = $localJdk.FullName
$env:PYSPARK_PYTHON = $python
$env:PYSPARK_DRIVER_PYTHON = $python
$env:SPARK_LOCAL_IP = "127.0.0.1"
$env:PATH = "$(Join-Path $projectRoot '.venv\Scripts');$(Join-Path $localJdk.FullName 'bin');$env:PATH"

Write-Host "JAVA_HOME=$env:JAVA_HOME"
Write-Host "PYSPARK_PYTHON=$env:PYSPARK_PYTHON"

