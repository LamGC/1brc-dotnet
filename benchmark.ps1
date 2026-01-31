# --- 配置区 ---
$projectName = "onebrc" # 你的项目名称
$runtime = "win-x64"
$publishDir = ".\bin\Release\net10.0\$runtime\publish" # 请根据你的 .NET 版本调整 net9.0/net10.0
$exePath = "$publishDir\$projectName.exe"
$dataFile = if ($args[0]) { $args[0] } else { "1brc-official/measurements.txt" } # 确保数据文件在当前目录下
$iterations = 5

# 1. 编译发布
Write-Host "--- 正在发布项目 (Release - $runtime) ---" -ForegroundColor Cyan
dotnet publish -c Release -r $runtime --self-contained true /p:PublishReadyToRun=true

if (-not (Test-Path $exePath)) {
    Write-Error "找不到生成的可执行文件: $exePath"
    exit
}

# 2. 循环测试
$results = @()
Write-Host "`n--- 开始执行 $iterations 次测试 ---" -ForegroundColor Cyan

for ($i = 1; $i -le $iterations; $i++) {
    Write-Host "运行 #$i ... " -NoNewline
    
    # 测量执行时间
    $elapsed = Measure-Command { 
        & $exePath $dataFile | Out-Null 
    }
    
    $ms = $elapsed.TotalMilliseconds
    $results += $ms
    Write-Host "$([Math]::Round($ms, 2)) ms" -ForegroundColor Yellow
}

# 3. 数据分析
Write-Host "`n--- 测试统计结果 ---" -ForegroundColor Cyan

# 排序并去除一个最高分和一个最低分
$sortedResults = $results | Sort-Object
$filteredResults = $sortedResults[1..($iterations - 2)]

$avg = ($filteredResults | Measure-Object -Average).Average
$min = $filteredResults[0]
$max = $filteredResults[-1]

Write-Host "总运行次数: $iterations"
Write-Host "已排除项: $($sortedResults[0]) ms (最快), $($sortedResults[-1]) ms (最慢)" -ForegroundColor Gray
Write-Host "------------------------------------"
Write-Host "有效最小值 (Min): " -NoNewline; Write-Host "$([Math]::Round($min, 2)) ms" -ForegroundColor Green
Write-Host "有效最大值 (Max): " -NoNewline; Write-Host "$([Math]::Round($max, 2)) ms" -ForegroundColor Red
Write-Host "有效平均值 (Avg): " -NoNewline; Write-Host "$([Math]::Round($avg, 2)) ms" -ForegroundColor Cyan
Write-Host "------------------------------------"