# README 多语言切换行同步与校验
#
# 单一事实源：readme-languages.json。数组顺序即展示顺序，前三位固定为 English（默认落地页）、简体中文（双权威）与繁體中文。
# 各 README 只有 <!-- lang:start --> 与 <!-- lang:end --> 之间的内容由本脚本生成，
# 并省略当前语种自身，与新语种追加到末尾的约定配套。
#
# 用法：
#   pwsh -File tools/readme/sync-readme-languages.ps1           # 按配置重写各 README 的切换块
#   pwsh -File tools/readme/sync-readme-languages.ps1 -Check    # 只校验不写入，不一致退出码 1（CI 使用）
#   pwsh -File tools/readme/sync-readme-languages.ps1 -Structure # 校验译文与英文版结构一致（全译验收；摘要版会报差异）
#   pwsh -File tools/readme/sync-readme-languages.ps1 -Root D:\src\MyLib   # 下游在自己的仓根运行
param(
    [Switch]$Check,
    [Switch]$Structure,
    [String]$Root = (Split-Path -Parent (Split-Path -Parent $PSScriptRoot)),
    [String]$Config = "readme-languages.json"
)

$ErrorActionPreference = "Stop"

$StartMarker = "<!-- lang:start -->"
$EndMarker = "<!-- lang:end -->"

$configPath = Join-Path $Root $Config
if (!(Test-Path -LiteralPath $configPath)) { throw "未找到语言配置：$configPath" }

$configData = Get-Content -LiteralPath $configPath -Raw -Encoding UTF8 | ConvertFrom-Json
$langs = @($configData.languages)
if ($langs.Count -eq 0) { throw "$Config 未配置任何语种" }

$missing = @($langs | Where-Object { -not (Test-Path -Path (Join-Path $Root $_.file)) })
if ($missing.Count -gt 0) { throw "配置的语种文件不存在：$($missing.file -join ', ')" }

# 新增翻译若忘记登记，切换行会静默漏掉它，这里直接拦下
$registered = @($langs.file)
$unregistered = @(Get-ChildItem -Path $Root -File | Where-Object { $_.Name -match '^(README|Readme)(\.[A-Za-z-]+)?\.md$' -and $_.Name -notmatch '\.nuget\.md$' -and $registered -notcontains $_.Name })
if ($unregistered.Count -gt 0) { throw "根目录存在未登记的 README：$($unregistered.Name -join ', ')，请先加入 $Config" }

# 生成某个 README 的切换行（省略自身）
function Get-SwitcherLine($current) {
    $parts = @()
    foreach ($item in $langs) {
        if ($item.file -eq $current.file) { continue }
        $parts += "[$($item.name)]($($item.file))"
    }
    return "🌐 " + ($parts -join " | ")
}

# 提取文件中现有的切换内容，用于 -Check 的差异提示
function Get-CurrentSwitcher($text, $pattern) {
    $m = [regex]::Match($text, $pattern)
    if (-not $m.Success) { return "<未找到切换行>" }
    $lines = @($m.Value -split "`r?`n" | Where-Object { $_ -notlike '<!-- lang:*' })
    return ($lines -join " / ")
}

# 提取文档骨架：标题层级序列、围栏代码块、图片与表格行数（均跳过代码块内部）
function Get-ReadmeShape([String]$path) {
    $lines = [IO.File]::ReadAllText($path) -split "`r?`n"
    $inFence = $false
    $headings = [System.Collections.Generic.List[String]]::new()
    $fences = 0
    $images = 0
    $tableRows = 0
    foreach ($line in $lines) {
        if ($line -match '^\s*(```|~~~)') {
            if (!$inFence) { $fences++ }
            $inFence = !$inFence
            continue
        }
        if ($inFence) { continue }
        if ($line -match '^(#{1,6})\s') { $headings.Add($Matches[1]) }
        if ($line -match '!\[') { $images++ }
        if ($line -match '^\s*\|') { $tableRows++ }
    }
    return [pscustomobject]@{ Headings = $headings; Fences = $fences; Images = $images; TableRows = $tableRows }
}

$errors = 0
$changed = 0
foreach ($item in $langs) {
    $path = Join-Path $Root $item.file
    $text = [IO.File]::ReadAllText($path)
    $line = Get-SwitcherLine $item
    $newline = if ($text.Contains("`r`n")) { "`r`n" } else { "`n" }
    $block = $StartMarker + $newline + $line + $newline + $EndMarker

    if ($text.Contains($StartMarker) -and $text.Contains($EndMarker)) {
        $pattern = "(?s)" + [regex]::Escape($StartMarker) + ".*?" + [regex]::Escape($EndMarker)
    }
    else {
        # 首次迁移：用标记块包住原切换行
        $pattern = "(?m)^🌐[^\r\n]*"
        if (-not [regex]::IsMatch($text, $pattern)) { throw "$($item.file) 既无 $StartMarker 标记，也找不到 🌐 切换行" }
    }

    # 替换文本中的 $ 需要转义，避免被当作正则替换组引用
    $updated = [regex]::Replace($text, $pattern, $block.Replace('$', '$$'))
    if ($updated -eq $text) { continue }

    $changed++
    if ($Check) {
        $errors++
        Write-Host "✗ $($item.file)"
        Write-Host "  期望：$line"
        Write-Host "  实际：$(Get-CurrentSwitcher $text $pattern)"
    }
    else {
        [IO.File]::WriteAllText($path, $updated, (New-Object System.Text.UTF8Encoding($false)))
        Write-Host "✓ $($item.file)"
    }
}

if ($Check) {
    if ($errors -gt 0) {
        Write-Host "`n语言切换行不一致：$errors / $($langs.Count) 个文件。请运行 pwsh -File tools/readme/sync-readme-languages.ps1 后提交。"
        exit 1
    }
    Write-Host "语言切换行一致（$($langs.Count) 个语种）"
}
elseif ($changed -eq 0) {
    Write-Host "语言切换行已是最新（$($langs.Count) 个语种）"
}

if ($Structure) {
    $baseItem = $langs | Where-Object { $_.code -eq 'en' } | Select-Object -First 1
    if (!$baseItem) { throw "$Config 缺少 en 基准语种" }

    $baseShape = Get-ReadmeShape (Join-Path $Root $baseItem.file)
    $structureErrors = 0
    foreach ($item in $langs) {
        if ($item.file -eq $baseItem.file) { continue }

        $shape = Get-ReadmeShape (Join-Path $Root $item.file)
        $issues = @()
        if ($shape.Headings.Count -ne $baseShape.Headings.Count) {
            $issues += "标题数 $($shape.Headings.Count) ≠ $($baseShape.Headings.Count)"
        }
        else {
            for ($i = 0; $i -lt $shape.Headings.Count; $i++) {
                if ($shape.Headings[$i] -ne $baseShape.Headings[$i]) {
                    $issues += "第 $($i + 1) 个标题层级 $($shape.Headings[$i]) ≠ $($baseShape.Headings[$i])"
                    break
                }
            }
        }
        if ($shape.Fences -ne $baseShape.Fences) { $issues += "代码块 $($shape.Fences) ≠ $($baseShape.Fences)" }
        if ($shape.Images -ne $baseShape.Images) { $issues += "图片 $($shape.Images) ≠ $($baseShape.Images)" }
        if ($shape.TableRows -ne $baseShape.TableRows) { $issues += "表格行 $($shape.TableRows) ≠ $($baseShape.TableRows)" }

        if ($issues.Count -gt 0) {
            $structureErrors++
            Write-Host "✗ $($item.file)：$($issues -join '；')"
        }
    }

    if ($structureErrors -gt 0) {
        Write-Host "`nREADME 结构与英文版不一致：$structureErrors / $($langs.Count - 1) 个译文。全译完成前该检查仅作参考。"
        exit 1
    }
    Write-Host "README 结构与英文版一致（$($langs.Count - 1) 个译文）"
}
