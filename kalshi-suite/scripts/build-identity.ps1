param(
    [Parameter(Mandatory = $true)]
    [string]$RepoRoot
)

$ErrorActionPreference = 'Stop'
$RepoRoot = (Resolve-Path -LiteralPath $RepoRoot).Path

$shortSha = (& git -C $RepoRoot rev-parse --short HEAD).Trim()
if (-not $shortSha) { $shortSha = 'unknown' }

$subject = (& git -C $RepoRoot log -1 --pretty=format:%s).Trim()
$releaseName = if ($subject) { ($subject -split '\s+', 2)[0] } else { '' }
$colors = @('red','orange','gold','yellow','lime','green','teal','cyan','blue','silver','indigo','violet','purple','magenta','pink','white')
$animals = @('rhino','gorilla','tiger','falcon','otter','badger','viper','heron','lynx','bison','marlin','wombat','ocelot','ibex','raven','gecko','panda','jackal','moose','cobra','walrus','ferret','osprey','stoat')
$validNames = foreach ($color in $colors) { foreach ($animal in $animals) { "$color$animal" } }
if ($releaseName -cnotin $validNames) { $releaseName = '' }

# Hash exactly the uncommitted source state that can enter these binaries. The tracked binary
# diff covers staged + unstaged edits, while sorted untracked paths include both names and bytes.
$trackedDiff = ((& git -C $RepoRoot diff --binary HEAD -- kalshi-suite pcrypto-server) -join "`n")
$untracked = @(& git -C $RepoRoot ls-files --others --exclude-standard -- kalshi-suite pcrypto-server) |
    Where-Object { $_ } |
    Sort-Object

$dirty = ($trackedDiff.Length -gt 0) -or ($untracked.Count -gt 0)
$versionStem = $shortSha
if ($dirty) {
    # The clean commit's animal is part of that immutable release, not this working tree.
    # Leave the linker value blank so runtime derives the deterministic animal from the
    # dirty source fingerprint below.
    $releaseName = ''
    $hash = [System.Security.Cryptography.IncrementalHash]::CreateHash(
        [System.Security.Cryptography.HashAlgorithmName]::SHA256)
    try {
        $utf8 = [System.Text.UTF8Encoding]::new($false)
        $hash.AppendData($utf8.GetBytes("tracked`n$trackedDiff`nuntracked`n"))
        foreach ($path in $untracked) {
            $hash.AppendData($utf8.GetBytes("$path`n"))
            $absolute = Join-Path $RepoRoot ($path -replace '/', [IO.Path]::DirectorySeparatorChar)
            if (Test-Path -LiteralPath $absolute -PathType Leaf) {
                $hash.AppendData([IO.File]::ReadAllBytes($absolute))
            }
            $hash.AppendData($utf8.GetBytes("`n"))
        }
        $fingerprint = ([BitConverter]::ToString($hash.GetHashAndReset()) -replace '-', '').ToLowerInvariant().Substring(0, 12)
    }
    finally {
        $hash.Dispose()
    }
    $versionStem = "$shortSha-dirty.$fingerprint"
}

# A pipe-delimited single line is intentionally easy and safe for build-suite.bat to consume.
Write-Output "$versionStem|$releaseName"
