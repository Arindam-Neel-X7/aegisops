Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$RootPath = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location -Path $RootPath

function Write-Log {
    param ([string]$Message)
    Write-Host "[AegisOps Bootstrap] $Message"
}
function Write-Ok {
    param ([string]$Message)
    Write-Host "[AegisOps Bootstrap] [OK] $Message"
}
function Fail-Bootstrap {
    param ([string]$Message)
    Write-Host "[AegisOps Bootstrap] [ERROR] $Message" -ForegroundColor Red
    exit 1
}
function Assert-LastExitCode {
    param ([string]$CommandName)
    if ($LASTEXITCODE -ne 0) {
        Fail-Bootstrap "$CommandName failed with exit code $LASTEXITCODE"
    }
}

Write-Log "Stage 1/21: Checking required repository paths..."
$RequiredPaths = @(
    "backend\pyproject.toml",
    "backend\poetry.lock",
    "backend\alembic.ini",
    "frontend\package.json",
    "frontend\package-lock.json",
    "docker-compose.yml",
    "backend\app",
    "backend\tests",
    "frontend",
    "scripts"
)
foreach ($p in $RequiredPaths) {
    if (-not (Test-Path (Join-Path $RootPath $p))) {
        Fail-Bootstrap "Missing required path: $p"
    }
}
Write-Ok "All required paths present."

Write-Log "Stage 2/21: Checking prerequisite commands..."
$Prereqs = @("python", "poetry", "node", "npm.cmd", "docker")
foreach ($cmd in $Prereqs) {
    if (-not (Get-Command $cmd -ErrorAction SilentlyContinue)) {
        Fail-Bootstrap "Missing required command: $cmd"
    }
}
Write-Ok "All prerequisite commands present."

Write-Log "Stage 3/21: Validating PowerShell version..."
if ($PSVersionTable.PSVersion.Major -lt 5 -or ($PSVersionTable.PSVersion.Major -eq 5 -and $PSVersionTable.PSVersion.Minor -lt 1)) {
    Fail-Bootstrap "PowerShell >= 5.1 is required. Detected $($PSVersionTable.PSVersion)."
}
Write-Ok "PowerShell version ok."

Write-Log "Stage 4/21: Validating Python version..."
$PyVer = python -c 'import sys; print(str(sys.version_info.major) + chr(46) + str(sys.version_info.minor))'
Assert-LastExitCode "python version check"
if ($PyVer -ne "3.11") {
    Fail-Bootstrap "Python 3.11.x is required. Detected $PyVer."
}
Write-Ok "Python version ok."

Write-Log "Stage 5/21: Validating Poetry version..."
$PoetryOut = poetry --version
Assert-LastExitCode "poetry version check"
if ($PoetryOut -match "(\d+\.\d+\.\d+)") {
    $PoetryVer = $Matches[1]
    if ($PoetryVer -ne "2.4.3") {
        Fail-Bootstrap "Poetry 2.4.3 is required. Detected $PoetryVer."
    }
} else {
    Fail-Bootstrap "Could not parse Poetry version from: $PoetryOut"
}
Write-Ok "Poetry version ok."

Write-Log "Stage 6/21: Validating Node version..."
$NodeVer = node --version
Assert-LastExitCode "node version check"
if ($NodeVer -notmatch "^v24\.") {
    Fail-Bootstrap "Node 24.x is required for CI/bootstrap reproducibility. Detected $NodeVer."
}
Write-Ok "Node version ok."
$NpmVer = npm.cmd --version
Assert-LastExitCode "npm version check"
Write-Log "Detected npm version: $NpmVer"

Write-Log "Stage 7/21: Validating Docker & Compose..."
docker info > $null 2>&1
if ($LASTEXITCODE -ne 0) {
    Fail-Bootstrap "Docker daemon is unreachable. Is Docker running?"
}
docker compose version > $null 2>&1
if ($LASTEXITCODE -ne 0) {
    Fail-Bootstrap "'docker compose' v2 command is required."
}
Write-Ok "Docker & Compose ok."

Write-Log "Stage 8/21: Validating Compose Config..."
docker compose --profile core config --quiet
if ($LASTEXITCODE -ne 0) {
    Fail-Bootstrap "Docker Compose configuration is invalid."
}
Write-Ok "Compose config valid."

Write-Log "Stage 9/21: Checking port 8000 safety..."
$listener = $null
try {
    $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, 8000)
    $listener.Start()
    Write-Ok "Port 8000 is available."
} catch [System.Management.Automation.MethodInvocationException] {
    $baseException = $_.Exception.GetBaseException()
    if ($baseException -is [System.Net.Sockets.SocketException]) {
        Fail-Bootstrap "Port 8000 is already occupied. Bootstrap refuses to validate an unknown existing server."
    } else {
        Fail-Bootstrap "Unexpected error verifying port 8000 availability: $_"
    }
} catch {
    Fail-Bootstrap "Unexpected error verifying port 8000 availability: $_"
} finally {
    if ($listener) {
        $listener.Stop()
    }
}

$env:DATABASE_URL = "postgresql+asyncpg://aegisops:aegisops@localhost:5432/aegisops"
$env:REDIS_HOST = "localhost"
$env:REDIS_PORT = "6379"
Write-Log "Using local bootstrap database on localhost:5432"

Write-Log "Stage 10/21: Installing Backend Dependencies..."
Push-Location (Join-Path $RootPath "backend")
try {
    poetry install --no-interaction --no-ansi
    Assert-LastExitCode "poetry install"
} finally {
    Pop-Location
}
Write-Ok "Backend dependencies installed."

Write-Log "Stage 11/21: Installing Frontend Dependencies..."
Push-Location (Join-Path $RootPath "frontend")
try {
    npm.cmd ci
    Assert-LastExitCode "npm ci"
} finally {
    Pop-Location
}
Write-Ok "Frontend dependencies installed."

Write-Log "Stage 12/21: Starting Core Docker Services..."
docker compose --profile core up -d
if ($LASTEXITCODE -ne 0) {
    docker compose --profile core ps
    docker compose --profile core logs --no-color postgres redis
    Fail-Bootstrap "Failed to start core docker services."
}
Write-Ok "Core Docker services started."

Write-Log "Stage 13/21: Polling Docker Health (postgres, redis)..."
function Check-DockerHealth {
    param([string]$Svc)
    $Attempts = 30
    $Interval = 2
    for ($i = 0; $i -lt $Attempts; $i++) {
        $Cid = (docker compose --profile core ps -q $Svc).Trim()
        if (-not $Cid) {
            Fail-Bootstrap "Container for $Svc missing. (no container ID)"
        }
        
        $StateStr = docker inspect --format='{{.State.Status}}' $Cid 2>&1
        if ($LASTEXITCODE -ne 0) {
            Fail-Bootstrap "Failed to inspect container $Svc ($Cid)."
        }
        if ($StateStr -eq "exited") {
            Fail-Bootstrap "Container for $Svc exited prematurely."
        }
        
        $HealthStr = docker inspect --format='{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' $Cid 2>&1
        if ($LASTEXITCODE -ne 0) {
            Fail-Bootstrap "Failed to inspect health of $Svc ($Cid)."
        }
        
        if ($HealthStr -eq "healthy") {
            Write-Ok "$Svc is healthy."
            return
        } elseif ($HealthStr -eq "unhealthy") {
            Fail-Bootstrap "$Svc became unhealthy."
        }
        
        Start-Sleep -Seconds $Interval
    }
    docker compose --profile core ps
    docker compose --profile core logs --no-color $Svc
    Fail-Bootstrap "Timeout waiting for $Svc to become healthy."
}

Check-DockerHealth "postgres"
Check-DockerHealth "redis"

Write-Log "Stage 14/21: Postgres Functional Check..."
docker compose --profile core exec -T postgres pg_isready -U aegisops -d aegisops
if ($LASTEXITCODE -ne 0) {
    Fail-Bootstrap "pg_isready check failed."
}
Write-Ok "Postgres functional check passed."

Write-Log "Stage 15/21: Redis Functional Check..."
$RedisPong = (docker compose --profile core exec -T redis redis-cli ping).Trim()
if ($RedisPong -ne "PONG") {
    Fail-Bootstrap "Redis ping failed. Expected PONG, got: $RedisPong"
}
Write-Ok "Redis functional check passed."

Write-Log "Stage 16/21: Running Migrations..."
Push-Location (Join-Path $RootPath "backend")
try {
    poetry run alembic upgrade head
    Assert-LastExitCode "alembic upgrade head"
} finally {
    Pop-Location
}
Write-Ok "Migrations applied."

Write-Log "Stage 17/21: Starting Temporary Backend..."
$TempOut = [System.IO.Path]::GetTempFileName()
$TempErr = [System.IO.Path]::GetTempFileName()
$UvicornProc = $null

try {
    Push-Location (Join-Path $RootPath "backend")
    
    $PoetryEnvPathInfo = poetry env info --path
    Assert-LastExitCode "poetry env info --path"
    $PoetryEnvPath = ($PoetryEnvPathInfo -join "").Trim()
    
    $PythonExe = Join-Path $PoetryEnvPath "Scripts\python.exe"
    if (-not (Test-Path $PythonExe)) {
        Fail-Bootstrap "Virtualenv python executable not found at: $PythonExe"
    }

    $UvicornProc = Start-Process -FilePath $PythonExe -ArgumentList "-m uvicorn app.main:app --host 127.0.0.1 --port 8000" -WorkingDirectory (Get-Location).Path -RedirectStandardOutput $TempOut -RedirectStandardError $TempErr -PassThru -WindowStyle Hidden
    
    Start-Sleep -Seconds 2
    if ($UvicornProc.HasExited) {
        Fail-Bootstrap "Uvicorn exited early."
    }
    Write-Ok "Temporary backend started (PID $($UvicornProc.Id))."

    Write-Log "Stage 18/21: Polling /health Endpoint..."
    function Check-Http {
        param([string]$Url, [string]$ExpectStatus, [string]$ExpectVersion)
        for ($i = 0; $i -lt 15; $i++) {
            if ($UvicornProc.HasExited) {
                Fail-Bootstrap "Uvicorn exited prematurely during polling."
            }
            try {
                $Resp = Invoke-RestMethod -Uri $Url -Method Get -TimeoutSec 2 -ErrorAction Stop
                if ($Resp.status -eq $ExpectStatus) {
                    if (-not $ExpectVersion -or $Resp.version -eq $ExpectVersion) {
                        return $true
                    }
                }
            } catch {
                # Transient error
            }
            Start-Sleep -Seconds 2
        }
        return $false
    }
    
    if (-not (Check-Http "http://127.0.0.1:8000/health" "ok" "0.1.0")) {
        Fail-Bootstrap "/health endpoint failed semantic check or timed out."
    }
    Write-Ok "/health endpoint ok."

    Write-Log "Stage 19/21: Polling /ready Endpoint..."
    if (-not (Check-Http "http://127.0.0.1:8000/ready" "ready" "")) {
        Fail-Bootstrap "/ready endpoint failed semantic check or timed out."
    }
    Write-Ok "/ready endpoint ok."

    Write-Log "Stage 20/21: Terminating Temporary Backend..."
    if (-not $UvicornProc.HasExited) {
        Stop-Process -Id $UvicornProc.Id -Force -ErrorAction SilentlyContinue
        $UvicornProc.WaitForExit(5000) > $null
    }
    Remove-Item -Path $TempOut -Force -ErrorAction SilentlyContinue
    Remove-Item -Path $TempErr -Force -ErrorAction SilentlyContinue
    $UvicornProc = $null
    Write-Ok "Temporary backend terminated."

    Write-Log "Stage 21/21: Running Quality Gates..."
    Write-Log "Backend: ruff check..."
    poetry run ruff check app tests
    Assert-LastExitCode "ruff check"
    
    Write-Log "Backend: mypy..."
    poetry run mypy app
    Assert-LastExitCode "mypy"
    
    Write-Log "Backend: pytest..."
    poetry run pytest tests -q
    Assert-LastExitCode "pytest"
} finally {
    Pop-Location
    if ($UvicornProc -and -not $UvicornProc.HasExited) {
        Stop-Process -Id $UvicornProc.Id -Force -ErrorAction SilentlyContinue
        $UvicornProc.WaitForExit(5000) > $null
        
        Write-Host "[AegisOps Bootstrap] [ERROR] Uvicorn logs (stdout):" -ForegroundColor Red
        if (Test-Path $TempOut) { Get-Content $TempOut | Write-Host -ForegroundColor Yellow }
        Write-Host "[AegisOps Bootstrap] [ERROR] Uvicorn logs (stderr):" -ForegroundColor Red
        if (Test-Path $TempErr) { Get-Content $TempErr | Write-Host -ForegroundColor Yellow }
        
        Write-Log "Hint: Docker services were left running for debugging. Run 'docker compose --profile core down' to clean up."
        
        Remove-Item -Path $TempOut -Force -ErrorAction SilentlyContinue
        Remove-Item -Path $TempErr -Force -ErrorAction SilentlyContinue
    }
}

Push-Location (Join-Path $RootPath "frontend")
try {
    Write-Log "Frontend: lint..."
    npm.cmd run lint
    Assert-LastExitCode "npm run lint"
    
    Write-Log "Frontend: type-check..."
    npm.cmd run type-check
    Assert-LastExitCode "npm run type-check"
    
    Write-Log "Frontend: test..."
    npm.cmd test
    Assert-LastExitCode "npm test"
    
    Write-Log "Frontend: build..."
    npm.cmd run build
    Assert-LastExitCode "npm run build"
} finally {
    Pop-Location
}

Write-Ok "All quality gates passed."

Write-Log "`n--- SUCCESS SUMMARY ---"
Write-Log "  * Prerequisite validation passed"
Write-Log "  * Backend dependencies installed"
Write-Log "  * Frontend dependencies installed"
Write-Log "  * Postgres healthy"
Write-Log "  * Redis healthy"
Write-Log "  * Migrations applied/current"
Write-Log "  * /health passed"
Write-Log "  * /ready passed"
Write-Log "  * Backend Ruff/mypy/pytest passed"
Write-Log "  * Frontend lint/type/test/build passed"
Write-Log "  * Core Docker services intentionally left running"
Write-Log "`nBootstrap complete."
exit 0
