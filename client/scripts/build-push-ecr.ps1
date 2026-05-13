<#
.SYNOPSIS
  Build the CRA client Docker image with REACT_APP_* baked in, then push to ECR.

.DESCRIPTION
  Reads optional values from client\.env (same keys as .env.example).
  You can override with parameters (they take precedence over .env).

.PARAMETER UpstashUrl
  REACT_APP_UPSTASH_REDIS_REST_URL (e.g. https://xxx.upstash.io)

.PARAMETER UpstashReadOnlyToken
  REACT_APP_UPSTASH_REDIS_REST_READ_ONLY_TOKEN

.PARAMETER LeaderFallbackUrl
  REACT_APP_LEADER_FALLBACK_URL (public leader base, e.g. http://1.2.3.4:8000)

.EXAMPLE
  .\scripts\build-push-ecr.ps1

.EXAMPLE
  .\scripts\build-push-ecr.ps1 -UpstashUrl "https://abc.upstash.io" -UpstashReadOnlyToken "..." -LeaderFallbackUrl "http://54.x.x.x:8000"
#>
param(
    [string]$Region = "us-east-1",
    [string]$AccountRegistry = "132866222339.dkr.ecr.us-east-1.amazonaws.com",
    [string]$Repository = "ai-gateway-client",
    [string]$ImageTag = "latest",
    [string]$EnvFile = "",
    [string]$UpstashUrl = "",
    [string]$UpstashReadOnlyToken = "",
    [string]$LeaderFallbackUrl = ""
)

$ErrorActionPreference = "Stop"
$ClientRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $ClientRoot

if (-not $EnvFile) { $EnvFile = Join-Path $ClientRoot ".env" }

function Get-DotEnvValue {
    param([string]$Path, [string]$Key)
    if (-not (Test-Path -LiteralPath $Path)) { return "" }
    foreach ($line in Get-Content -LiteralPath $Path) {
        $t = $line.Trim()
        if ($t.StartsWith("#") -or $t -eq "") { continue }
        if ($t -match "^$([regex]::Escape($Key))\s*=\s*(.*)$") {
            $val = $matches[1].Trim()
            if (($val.Length -ge 2) -and (($val.StartsWith('"') -and $val.EndsWith('"')) -or ($val.StartsWith("'") -and $val.EndsWith("'")))) {
                return $val.Substring(1, $val.Length - 2)
            }
            return $val
        }
    }
    return ""
}

if (-not $UpstashUrl) { $UpstashUrl = Get-DotEnvValue $EnvFile "REACT_APP_UPSTASH_REDIS_REST_URL" }
if (-not $UpstashReadOnlyToken) { $UpstashReadOnlyToken = Get-DotEnvValue $EnvFile "REACT_APP_UPSTASH_REDIS_REST_READ_ONLY_TOKEN" }
if (-not $LeaderFallbackUrl) {
    $LeaderFallbackUrl = Get-DotEnvValue $EnvFile "REACT_APP_LEADER_FALLBACK_URL"
    if (-not $LeaderFallbackUrl) { $LeaderFallbackUrl = "http://localhost:8000" }
}

$remote = "${AccountRegistry}/${Repository}:${ImageTag}"

Write-Host "Building in: $ClientRoot"
Write-Host "ECR image:   $remote"
if ($UpstashUrl) { Write-Host "Upstash URL: set" } else { Write-Host "Upstash URL: (empty - discovery from Upstash disabled)" }
if ($LeaderFallbackUrl -eq "http://localhost:8000") {
    Write-Host 'Leader fallback is localhost:8000 - for EC2 UI, set REACT_APP_LEADER_FALLBACK_URL or pass -LeaderFallbackUrl' -ForegroundColor Yellow
}

$dockerArgs = @("build", "-t", "ai-gateway-client")
if ($UpstashUrl) {
    $dockerArgs += "--build-arg", "REACT_APP_UPSTASH_REDIS_REST_URL=$UpstashUrl"
}
if ($UpstashReadOnlyToken) {
    $dockerArgs += "--build-arg", "REACT_APP_UPSTASH_REDIS_REST_READ_ONLY_TOKEN=$UpstashReadOnlyToken"
}
if ($LeaderFallbackUrl) {
    $dockerArgs += "--build-arg", "REACT_APP_LEADER_FALLBACK_URL=$LeaderFallbackUrl"
}
$dockerArgs += "."

& docker @dockerArgs
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "Logging in to ECR..."
$login = aws ecr get-login-password --region $Region 2>&1
if ($LASTEXITCODE -ne 0) { throw "aws ecr get-login-password failed: $login" }
$login | & docker login --username AWS --password-stdin $AccountRegistry
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "Tagging and pushing..."
& docker tag "ai-gateway-client:latest" $remote
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
& docker push $remote
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "Done. Pushed $remote" -ForegroundColor Green
