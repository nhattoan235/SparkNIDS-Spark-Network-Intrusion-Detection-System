[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
$env:UV_CACHE_DIR = Join-Path $projectRoot ".uv-cache"
$env:UV_PYTHON_INSTALL_DIR = Join-Path $projectRoot ".uv-python"
$venvPython = Join-Path $projectRoot ".venv\Scripts\python.exe"
$requirements = Join-Path $projectRoot "requirements.txt"
$jdkRoot = Join-Path $projectRoot ".jdk"
$downloadRoot = Join-Path $projectRoot ".downloads"
$sparkJarRoot = Join-Path $projectRoot ".spark-jars"

Push-Location $projectRoot
try {
    uv venv --python 3.12 .venv
    uv pip install --python $venvPython -r $requirements

    $localJdk = Get-ChildItem -LiteralPath $jdkRoot -Directory -Filter "jdk-*" -ErrorAction SilentlyContinue |
        Where-Object { Test-Path -LiteralPath (Join-Path $_.FullName "bin\java.exe") } |
        Select-Object -First 1

    if (-not $localJdk) {
        New-Item -ItemType Directory -Force -Path $jdkRoot, $downloadRoot | Out-Null
        $archive = Join-Path $downloadRoot "temurin21.zip"
        $jdkUrl = "https://api.adoptium.net/v3/binary/latest/21/ga/windows/x64/jdk/hotspot/normal/eclipse?project=jdk"
        curl.exe -L $jdkUrl -o $archive
        Expand-Archive -LiteralPath $archive -DestinationPath $jdkRoot -Force
        Remove-Item -LiteralPath $archive
    }

    $localFsJar = Join-Path $sparkJarRoot "hadoop-bare-naked-local-fs-0.1.0.jar"
    $localFsSha1 = "$localFsJar.sha1"
    if (-not (Test-Path -LiteralPath $localFsJar)) {
        New-Item -ItemType Directory -Force -Path $sparkJarRoot | Out-Null
        $mavenRoot = "https://repo1.maven.org/maven2/com/globalmentor/hadoop-bare-naked-local-fs/0.1.0"
        curl.exe -L "$mavenRoot/hadoop-bare-naked-local-fs-0.1.0.jar" -o $localFsJar
        curl.exe -L "$mavenRoot/hadoop-bare-naked-local-fs-0.1.0.jar.sha1" -o $localFsSha1
    }
    $expectedSha1 = (Get-Content -Raw -LiteralPath $localFsSha1).Trim().ToLowerInvariant()
    $actualSha1 = (Get-FileHash -Algorithm SHA1 -LiteralPath $localFsJar).Hash.ToLowerInvariant()
    if ($actualSha1 -ne $expectedSha1) {
        throw "Checksum mismatch for the Windows local-filesystem support JAR."
    }

    Write-Host "Bootstrap complete. Run: .\.venv\Scripts\python.exe -m src.spark_session"
}
finally {
    Pop-Location
}
