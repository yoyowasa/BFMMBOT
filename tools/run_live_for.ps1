param(
  [int]$Seconds = 300,
  [string]$Config = "configs/live.yml",
  [switch]$DryRun,
  [string]$Python = ".venv/Scripts/python.exe"
)

$ErrorActionPreference = 'Continue'
Set-StrictMode -Version Latest

function New-StopFile {
  try {
    if (Test-Path STOP) { Remove-Item STOP -Force -ErrorAction SilentlyContinue }
    New-Item STOP -ItemType File -ErrorAction SilentlyContinue | Out-Null
  } catch {}
}

function Remove-StopFile {
  try { if (Test-Path STOP) { Remove-Item STOP -Force -ErrorAction SilentlyContinue } } catch {}
}

function Start-Runner {
  param([string]$Config,[switch]$DryRun,[string]$Python)
  $args = @('-m','src.cli.trade','--config', $Config)
  if ($DryRun) { $args += '--dry-run' }
  Write-Host "[run] $Python $($args -join ' ')"
  $psi = New-Object System.Diagnostics.ProcessStartInfo
  $psi.FileName = $Python
  $psi.WorkingDirectory = (Get-Location).Path
  $psi.UseShellExecute = $false
  $psi.RedirectStandardOutput = $true
  $psi.RedirectStandardError = $true
  # 互換性重視: 引数は単純連結で渡す（ArgumentList.AddRange は環境により未実装）
  $psi.Arguments = ($args -join ' ')
  $p = New-Object System.Diagnostics.Process
  $p.StartInfo = $psi
  $null = $p.Start()
  return $p
}

function Summarize-Logs {
  Write-Host "[summary] collecting logs..."
  $orders = Get-ChildItem -Recurse logs/orders -Filter *order_log_*.ndjson -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending | Select-Object -First 1
  $trades = Get-ChildItem -Recurse logs/trades -Filter *trade_log_*.ndjson -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending | Select-Object -First 1
  $dec = Get-ChildItem -Recurse logs/analytics -Filter *decision_log_*.ndjson -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending | Select-Object -First 1
  $hb = Get-ChildItem logs/runtime -Filter *heartbeat*.ndjson -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending | Select-Object -First 1

  $place=0;$fill=0;$cancel=0;$ttl=@{}
  if ($orders) {
    Get-Content $orders.FullName | ForEach-Object {
      try { $o = $_ | ConvertFrom-Json } catch { return }
      if ($o.action -eq 'place') { $place++; if ($o.ttl_ms -ne $null) { $k=[int]$o.ttl_ms; if(-not $ttl.ContainsKey($k)){$ttl[$k]=0}; $ttl[$k]++ } }
      elseif ($o.action -eq 'cancel') { $cancel++ }
      elseif (($o.action -eq 'fill') -or ($o.action -eq 'partial')) { $fill++ }
    }
  }

  $trN=0;$trSz=0.0;$trPnl=0.0
  if ($trades) {
    Get-Content $trades.FullName | ForEach-Object {
      try { $t = $_ | ConvertFrom-Json } catch { return }
      $trN++
      try { $trSz += [double]$t.sz } catch {}
      try { $trPnl += [double]$t.pnl } catch {}
    }
  }

  $decN=0;$ttlSel=@{}
  if ($dec) {
    Get-Content $dec.FullName | ForEach-Object {
      try { $d = $_ | ConvertFrom-Json } catch { return }
      $decN++
      if ($d.features -and $d.features.stall_ttl_selected_ms -ne $null) {
        $v=[int]$d.features.stall_ttl_selected_ms
        if (-not $ttlSel.ContainsKey($v)) { $ttlSel[$v]=0 }
        $ttlSel[$v]++
      }
    }
  }

  Write-Host "ORDERS place=$place cancel=$cancel fill=$fill"
  if ($ttl.Keys.Count -gt 0) { Write-Host ("ORDERS_TTL: " + (($ttl.GetEnumerator() | Sort-Object Name | ForEach-Object { "{0}ms={1}" -f $_.Name, $_.Value }) -join ', ')) }
  Write-Host "TRADES n=$trN size_sum=$([math]::Round($trSz,4)) pnl_sum=$([math]::Round($trPnl,2))"
  if ($ttlSel.Keys.Count -gt 0) { Write-Host ("DECISION_TTL_SELECTED: " + (($ttlSel.GetEnumerator() | Sort-Object Name | ForEach-Object { "{0}ms={1}" -f $_.Name, $_.Value }) -join ', ')) }
  if ($hb) {
    $tail = Get-Content $hb.FullName -Tail 5
    Write-Host "HEARTBEAT tail:"; $tail | ForEach-Object { Write-Host "  $_" }
  }
}

try {
  Remove-StopFile
  $proc = Start-Runner -Config $Config -DryRun:$DryRun -Python $Python
  Write-Host "[run] pid=$($proc.Id) waiting $Seconds sec..."
  Start-Sleep -Seconds $Seconds
  Write-Host "[stop] creating STOP file for graceful shutdown"
  New-StopFile
  $proc.WaitForExit(15000) | Out-Null
  if (-not $proc.HasExited) {
    Write-Host "[stop] process still running -> sending Kill"
    try { $proc.Kill() } catch {}
  }
} finally {
  Remove-StopFile
  Summarize-Logs
}
