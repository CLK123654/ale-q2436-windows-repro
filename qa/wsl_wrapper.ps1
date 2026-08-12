param(
  [Parameter(Mandatory=$true)][ValidateSet('reference','verify')][string]$Mode
)
$ErrorActionPreference = 'Stop'
$repoWindows = (Resolve-Path '.').Path
$drive = $repoWindows.Substring(0, 1).ToLowerInvariant()
$rest = $repoWindows.Substring(2).Replace('\', '/')
$repoWsl = "/mnt/$drive$rest"
$script = if ($Mode -eq 'reference') { './qa/generate_reference.py' } else { './qa/windows_verify.py' }
$commitSha = $env:GITHUB_SHA
$workflowRunId = $env:GITHUB_RUN_ID
$runnerImage = $env:ImageOS
wsl.exe -d Ubuntu-24.04 -- bash -lc "set -e; cd '$repoWsl'; source .venv/bin/activate; export AIRFLOW__CORE__LOAD_EXAMPLES=False; export GITHUB_SHA='$commitSha'; export GITHUB_RUN_ID='$workflowRunId'; export ImageOS='$runnerImage'; python '$script'"
if ($LASTEXITCODE -ne 0) { throw "WSL task failed with exit code $LASTEXITCODE" }
