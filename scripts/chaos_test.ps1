Write-Host "==========================================================" -ForegroundColor Red
Write-Host "   BNP Paribas SRE Resilience & Chaos Test Execution      " -ForegroundColor Red
Write-Host "==========================================================" -ForegroundColor Red

podman ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

Write-Host "`n[+] Injecting Chaos: Terminating Consumer Container..." -ForegroundColor Red
podman kill --signal SIGKILL bnp-consumer

Start-Sleep -Seconds 3

Write-Host "`n[+] Verifying Auto-Healing Recovery Status:" -ForegroundColor Green
podman ps --filter "name=bnp-consumer" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
