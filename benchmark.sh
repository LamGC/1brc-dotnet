#!/bin/bash

PROJECT_NAME="onebrc"
RUNTIME="linux-x64"
PUBLISH_DIR="./bin/Release/net10.0/$RUNTIME/publish"
EXE_PATH="$PUBLISH_DIR/$PROJECT_NAME"
DATA_FILE="${1:-1brc-official/measurements.txt}"
ITERATIONS=5

BENCH_DIR=".1brc-benchmark"
OFFICIAL_DIR="1brc-official"
ANSWER_CSV="$OFFICIAL_DIR/answer.csv"
mkdir -p "$BENCH_DIR"

echo -e "\033[36m--- 正在发布项目 (Release - $RUNTIME) ---\033[0m"
dotnet publish -c Release -r $RUNTIME --self-contained true /p:PublishReadyToRun=true

if [ ! -f "$EXE_PATH" ]; then
    echo -e "\033[31m找不到生成的可执行文件: $EXE_PATH\033[0m"
    exit 1
fi

if [ ! -f "$ANSWER_CSV" ]; then
    echo -e "\033[33m未找到标准答案，正在调用官方 Baseline 生成 (这可能需要一点时间)... \033[0m"
    sh "$OFFICIAL_DIR/calculate_average_baseline.sh" "$DATA_FILE" > "$ANSWER_CSV"
fi

# --- 3. 循环测试 ---
results=()
echo -e "\n\033[36m--- 开始执行 $ITERATIONS 次测试 ---\033[0m"

for ((i=1; i<=ITERATIONS; i++))
do
    RAW_OUT="$BENCH_DIR/output.$i"
    CSV_OUT="$BENCH_DIR/output.$i.csv"
    
    echo -n "运行 #$i ... "
    
    # 执行并保存原始输出
    start_time=$(date +%s%N)
    ./"$EXE_PATH" "$DATA_FILE" > "$RAW_OUT"
    end_time=$(date +%s%N)
    
    # 转换为 CSV (假设 tocsv.sh 在官方目录下)
    sh "$OFFICIAL_DIR/tocsv.sh" < "$RAW_OUT" > "$CSV_OUT"
    
    # 结果校验
    if diff -q "$CSV_OUT" "$ANSWER_CSV" > /dev/null; then
        CHECK_STR="\033[32mPASS\033[0m"
    else
        CHECK_STR="\033[31mFAIL\033[0m"
    fi

    # 计算耗时
    ms=$(( (end_time - start_time) / 1000000 ))
    results+=("$ms")
    
    echo -e "\033[33m$ms ms\033[0m [$CHECK_STR]"
done

# --- 4. 数据分析 ---
echo -e "\n\033[36m--- 测试统计结果 ---\033[0m"
IFS=$'\n' sorted_results=($(sort -n <<<"${results[*]}"))
unset IFS

# 排除极值
filtered_results=("${sorted_results[@]:1:$((ITERATIONS-2))}")

min=${filtered_results[0]}
max=${filtered_results[-1]}
sum=0
for val in "${filtered_results[@]}"; do
    sum=$((sum + val))
done
avg=$(echo "scale=2; $sum / ${#filtered_results[@]}" | bc)

echo "总运行次数: $ITERATIONS"
echo -e "\033[90m已排除项: ${sorted_results[0]} ms, ${sorted_results[-1]} ms\033[0m"
echo "------------------------------------"
echo -ne "有效最小值 (Min): "; echo -e "\033[32m$min ms\033[0m"
echo -ne "有效最大值 (Max): "; echo -e "\033[31m$max ms\033[0m"
echo -ne "有效平均值 (Avg): "; echo -e "\033[36m$avg ms\033[0m"
echo "------------------------------------"
echo -e "详细输出已保存至: \033[34m$BENCH_DIR/\033[0m"