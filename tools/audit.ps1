param(
    [switch]$SkipRuff,
    [switch]$SkipPytest,
    [switch]$SkipMypy,
    [bool]$StrictMypy = $false
)

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$pythonExe = Join-Path $repoRoot ".venv\Scripts\python.exe"
$configPath = Join-Path $PSScriptRoot "audit.json"
$repoIdPath = Join-Path $repoRoot "REPO_ID.txt"

$cfg = @{
    pytest_args = @("-q")
    ruff_required = $true
    pytest_required = $true
    mypy_required = $false
}

function Convert-ToArgArray {
    param([object]$Value)

    if ($null -eq $Value) {
        return @()
    }

    if (($Value -is [System.Collections.IEnumerable]) -and -not ($Value -is [string])) {
        $out = @()
        foreach ($item in $Value) {
            $out += [string]$item
        }
        return $out
    }

    $text = [string]$Value
    if ([string]::IsNullOrWhiteSpace($text)) {
        return @()
    }

    $matches = [regex]::Matches($text, '([^\s"]+|"[^"]*")')
    $tokens = @()
    foreach ($m in $matches) {
        $token = $m.Value.Trim('"')
        if (-not [string]::IsNullOrWhiteSpace($token)) {
            $tokens += $token
        }
    }
    return $tokens
}

if (Test-Path $configPath) {
    try {
        $jsonCfg = Get-Content -Path $configPath -Raw | ConvertFrom-Json
        if ($jsonCfg -is [System.Collections.IDictionary]) {
            foreach ($k in $jsonCfg.Keys) {
                $cfg[$k] = $jsonCfg[$k]
            }
        }
        else {
            foreach ($prop in $jsonCfg.PSObject.Properties) {
                $cfg[$prop.Name] = $prop.Value
            }
        }
    }
    catch {
        Write-Error "AUDIT_CONFIG_PARSE_FAILED path=$configPath err=$($_.Exception.Message)"
        exit 3
    }
}

$repoId = "(missing REPO_ID.txt)"
if (Test-Path $repoIdPath) {
    $repoId = (Get-Content -Path $repoIdPath -TotalCount 1).Trim()
}
$gitRoot = (git -C $repoRoot rev-parse --show-toplevel 2>$null)
if (-not $gitRoot) { $gitRoot = "(unknown)" }
$origin = (git -C $repoRoot remote get-url origin 2>$null)
if (-not $origin) { $origin = "(no-origin)" }

Write-Host "[audit] REPO_ID=$repoId"
Write-Host "[audit] GIT_ROOT=$gitRoot"
Write-Host "[audit] ORIGIN=$origin"

if (-not (Test-Path $pythonExe)) {
    Write-Error "VENV_PYTHON_NOT_FOUND path=$pythonExe"
    exit 2
}

$ruffRequired = [bool]$cfg["ruff_required"]
$pytestRequired = [bool]$cfg["pytest_required"]
$mypyRequired = [bool]$cfg["mypy_required"]
$pytestArgs = Convert-ToArgArray -Value $cfg["pytest_args"]
if ($pytestArgs.Count -eq 0) {
    $pytestArgs = @("-q")
}

Push-Location $repoRoot
try {
    if ($SkipRuff) {
        Write-Host "[audit] ruff skipped by flag"
    }
    elseif (-not $ruffRequired) {
        Write-Host "[audit] ruff disabled by config"
    }
    else {
        Write-Host "[audit] ruff check ."
        & $pythonExe -m ruff check .
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    }

    if ($SkipPytest) {
        Write-Host "[audit] pytest skipped by flag"
    }
    elseif (-not $pytestRequired) {
        Write-Host "[audit] pytest disabled by config"
    }
    else {
        Write-Host "[audit] pytest $($pytestArgs -join ' ')"
        & $pythonExe -m pytest $pytestArgs
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    }

    if ($SkipMypy) {
        Write-Host "[audit] mypy skipped by flag"
    }
    else {
        Write-Host "[audit] mypy . (StrictMypy=$StrictMypy required=$mypyRequired)"
        & $pythonExe -m mypy .
        $mypyExit = $LASTEXITCODE
        if ($mypyExit -ne 0) {
            if ($StrictMypy -or $mypyRequired) {
                Write-Error "MYPY_FAILED strict=$StrictMypy required=$mypyRequired exit_code=$mypyExit"
                exit $mypyExit
            }
            Write-Warning "MYPY_FAILED_BUT_IGNORED strict=false required=false exit_code=$mypyExit"
        }
    }

    Write-Host "[audit] PASS"
    exit 0
}
finally {
    Pop-Location
}


