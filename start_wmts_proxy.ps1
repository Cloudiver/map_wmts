param(
    [Parameter(Mandatory = $false)]
    [string]$UpstreamTileUrlTemplate,
    [Parameter(Mandatory = $false)]
    [string]$WmtsLayersJson,
    [Parameter(Mandatory = $false)]
    [string]$UpstreamHeadersJson = '{"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36","Referer":"https://www.google.com/"}',
    [Parameter(Mandatory = $false)]
    [int]$Port = 8787,
    [Parameter(Mandatory = $false)]
    [int]$MinZoom = 0,
    [Parameter(Mandatory = $false)]
    [int]$MaxZoom = 18,
    [Parameter(Mandatory = $false)]
    [int]$MemCacheItems = 8000,
    [Parameter(Mandatory = $false)]
    [int]$MemCacheTtlSeconds = 1800,
    [Parameter(Mandatory = $false)]
    [int]$DiskCacheMaxFiles = 50000,
    [Parameter(Mandatory = $false)]
    [string]$DiskCacheDir = ".wmts_cache",
    [Parameter(Mandatory = $false)]
    [int]$RequestsPoolConnections = 64,
    [Parameter(Mandatory = $false)]
    [int]$RequestsPoolMaxsize = 64,
    [Parameter(Mandatory = $false)]
    [switch]$NoDiskCache,
    [Parameter(Mandatory = $false)]
    [switch]$NoMemCache,
    [Parameter(Mandatory = $false)]
    [switch]$NoRequestLog,
    [Parameter(Mandatory = $false)]
    [switch]$GoogleMultiLayers
)

$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot

if ($GoogleMultiLayers -and [string]::IsNullOrWhiteSpace($WmtsLayersJson)) {
    $tiandituTk = $env:TIANDITU_TK
    $layers = @(
        @{ id = "google_satellite"; title = "Google Satellite"; template = "..."; format = "image/jpeg" },
        @{ id = "google_roadmap"; title = "Google Roadmap"; template = "..."; format = "image/png" },
        @{ id = "google_hybrid"; title = "Google Hybrid"; template = "..."; format = "image/jpeg" },
        @{ id = "tencent_vector"; title = "Tencent Vector"; template = "..."; format = "image/png" },
        @{ id = "amap_satellite"; title = "Amap Satellite"; template = "..."; format = "image/jpeg" },
        @{ id = "amap_roadmap"; title = "Amap Vector"; template = "..."; format = "image/jpeg" }
    )

    if (-not [string]::IsNullOrWhiteSpace($tiandituTk)) {
        $layers += @(
            @{ id = "tianditu_vec"; title = "Tianditu Vector"; template = "..."; format = "image/png" },
            @{ id = "tianditu_vec_title"; title = "Tianditu Vector Title"; template = "..."; format = "image/png" }
        )
    }
    else {
        Write-Host "  TIANDITU_TK not set, skipping Tianditu layers."
    }

    $WmtsLayersJson = ($layers | ConvertTo-Json -Compress)
}

if ([string]::IsNullOrWhiteSpace($UpstreamTileUrlTemplate)) {
    $UpstreamTileUrlTemplate = $env:UPSTREAM_TILE_URL_TEMPLATE
}
if ([string]::IsNullOrWhiteSpace($WmtsLayersJson)) {
    $WmtsLayersJson = $env:WMTS_LAYERS_JSON
}

if ([string]::IsNullOrWhiteSpace($UpstreamTileUrlTemplate) -and [string]::IsNullOrWhiteSpace($WmtsLayersJson)) {
    throw "Missing upstream definition. Use -UpstreamTileUrlTemplate or -WmtsLayersJson (or -GoogleMultiLayers)."
}

$env:PORT = "$Port"
$env:WMTS_MIN_ZOOM = "$MinZoom"
$env:WMTS_MAX_ZOOM = "$MaxZoom"
if ([string]::IsNullOrWhiteSpace($UpstreamTileUrlTemplate)) {
    Remove-Item Env:UPSTREAM_TILE_URL_TEMPLATE -ErrorAction SilentlyContinue
} else {
    $env:UPSTREAM_TILE_URL_TEMPLATE = "$UpstreamTileUrlTemplate"
}
if ([string]::IsNullOrWhiteSpace($WmtsLayersJson)) {
    Remove-Item Env:WMTS_LAYERS_JSON -ErrorAction SilentlyContinue
} else {
    $env:WMTS_LAYERS_JSON = "$WmtsLayersJson"
}
$env:UPSTREAM_HEADERS_JSON = "$UpstreamHeadersJson"

$env:CACHE_ENABLED = if ($NoMemCache) { "0" } else { "1" }
$env:CACHE_MAX_ITEMS = "$MemCacheItems"
$env:CACHE_TTL_SECONDS = "$MemCacheTtlSeconds"

$env:DISK_CACHE_ENABLED = if ($NoDiskCache) { "0" } else { "1" }
$env:DISK_CACHE_DIR = "$DiskCacheDir"
$env:DISK_CACHE_MAX_FILES = "$DiskCacheMaxFiles"

$env:USE_REQUESTS_SESSION = "1"
$env:REQUESTS_POOL_CONNECTIONS = "$RequestsPoolConnections"
$env:REQUESTS_POOL_MAXSIZE = "$RequestsPoolMaxsize"
$env:REQUEST_LOG_ENABLED = if ($NoRequestLog) { "0" } else { "1" }

Write-Host "Starting WMTS proxy..."
Write-Host "  Port: $Port"
Write-Host "  Zoom: $MinZoom-$MaxZoom"
Write-Host "  Memory cache: $($env:CACHE_ENABLED) items=$MemCacheItems ttl=$MemCacheTtlSeconds"
Write-Host "  Disk cache: $($env:DISK_CACHE_ENABLED) dir=$DiskCacheDir max_files=$DiskCacheMaxFiles"
Write-Host "  Requests pool: connections=$RequestsPoolConnections maxsize=$RequestsPoolMaxsize"
Write-Host "  Request log: $($env:REQUEST_LOG_ENABLED)"
if (-not [string]::IsNullOrWhiteSpace($WmtsLayersJson)) {
    Write-Host "  WMTS layers: configured by WMTS_LAYERS_JSON"
} else {
    Write-Host "  WMTS layer: single layer via UpstreamTileUrlTemplate"
}

python .\wmts_proxy.py
