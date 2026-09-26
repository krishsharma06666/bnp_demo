Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "   BNP Paribas DevSecOps Platform Network Setup           " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

$networkExists = podman network ls --format "{{.Name}}" | Select-String -Pattern "^bnp-net$"

if ($networkExists) {
    Write-Host "[+] bnp-net network already exists." -ForegroundColor Yellow
} else {
    Write-Host "[+] Creating dual-stack bnp-net podman network..." -ForegroundColor Green
    podman network create bnp-net
    Write-Host "[+] Network bnp-net successfully provisioned." -ForegroundColor Green
}

podman network ls
