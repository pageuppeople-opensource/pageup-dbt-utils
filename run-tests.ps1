$ErrorActionPreference = "Stop"
Push-Location
cd .\integration_tests

try {
    Write-Warning "Running 'dbt deps' as administrator. Remove this when dbt issue with local dependencies is resolved: https://github.com/fishtown-analytics/dbt/issues/766"
    Write-Host "dbt deps" -ForegroundColor Cyan
    $process = Start-Process powershell -ArgumentList "
        cd $((Get-Item -Path ".\").FullName);         
        dbt deps;
        Exit `$LASTEXITCODE;" -Verb "runAs" -Wait -PassThru
    $process.WaitForExit()
    if ($process.ExitCode -gt 0) {
        throw "'dbt deps' returned exit code $($process.ExitCode)"
    }

    Write-Host "dbt seed --full-refresh" -ForegroundColor Cyan 
    dbt seed --full-refresh
    if ($LASTEXITCODE -gt 0) {
        throw "'dbt seed' returned exit code $LASTEXITCODE"
    }

    Write-Host "dbt run --full-refresh" -ForegroundColor Cyan 
    dbt run  --full-refresh
    if ($LASTEXITCODE -gt 0) {
        throw "'dbt run' returned exit code $LASTEXITCODE"
    }

    Write-Host "dbt test" -ForegroundColor Cyan 
    dbt test
    if ($LASTEXITCODE -gt 0) {
        throw "'dbt test' returned exit code $LASTEXITCODE"
    }
}
finally {
    Pop-Location
}
