# ==========================================
# Sync-Portfolio.ps1
# Creates organized portfolio view at a fixed path
# ==========================================

# Set repo root (your Fabric Git-synced folder)
$RepoRoot = "C:\Users\miket\fabric-data-engineer-analytics\laserengrave-analytics-sync"

# Set absolute path for Portfolio folder
$PortfolioRoot = "C:\Users\miket\fabric-data-engineer-analytics\Portfolio"

# Define your portfolio mapping
$ArtifactMap = @{
    ".Dataflow"      = "Dataflows"
    ".Notebook"      = "Notebooks"
    ".SemanticModel" = "SemanticModels"
    ".Report"        = "Dashboards"
    ".DataPipeline"      = "CI-CD"
}

# Ensure destination folders exist
foreach ($dest in $ArtifactMap.Values) {
    $path = Join-Path $PortfolioRoot $dest
    New-Item -ItemType Directory -Force -Path $path | Out-Null
}

# Copy each artifact type
foreach ($ext in $ArtifactMap.Keys) {
    $srcFolders = Get-ChildItem -Path $RepoRoot -Directory | Where-Object { $_.Name -like "*$ext" }
    $destFolder = Join-Path $PortfolioRoot $ArtifactMap[$ext]

    if ($srcFolders.Count -eq 0) {
        Write-Host " No $ext folders found"
        continue
    }

    foreach ($src in $srcFolders) {
        $destPath = Join-Path $destFolder $src.Name
        Copy-Item -Path $src.FullName -Destination $destPath -Recurse -Force
    }

    Write-Host " Copied $($srcFolders.Count) $ext folders to $destFolder"
}

Write-Host "`n Portfolio folder structure updated successfully!"
