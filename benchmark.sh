#!/bin/bash

PROJECT_NAME="onebrc"
RUNTIME="linux-x64"
PUBLISH_DIR="./bin/Release/net10.0/$RUNTIME/publish"
EXE_PATH="$PUBLISH_DIR/$PROJECT_NAME"
DATA_FILE="measurements.txt"
ITERATIONS=5

echo -e "\033[36m--- 正在发布项目 (Release - $RUNTIME) ---\033[0m"
dotnet publish -c Release -r $RUNTIME --self-contained true /p:PublishReadyToRun=true

if [ ! -f "$EXE_PATH" ]; then
    echo -e "\033[31m找不到生成的可执行文件: $EXE_PATH\033[0m"
    exit 1
fi

results=()
echo -e "\n\033[36m--- 开始执行 $ITERATIONS 次测试 ---\033[0m"

for ((i=1; i<=ITERATIONS; i++))
do
    echo -n "运行 #$i ... "
    
    start_time=$(date +%s%N)
    ./"$EXE_PATH" "$DATA_FILE" > /dev/null
    end_time=$(date +%s%N)
    
    ms=$(( (end_time - start_time) / 1000000 ))
    results+=("$ms")
    
    echo -e "\033[33m$ms ms\033[0m"
done

echo -e "\n\033[36m--- 测试统计结果 ---\033[0m"
IFS=$'\n' sorted_results=($(sort -n <<<"${results[*]}"))
unset IFS

filtered_results=("${sorted_results[@]:1:$((ITERATIONS-2))}")

min=${filtered_results[0]}
max=${filtered_results[-1]}
sum=0
for val in "${filtered_results[@]}"; do
    sum=$((sum + val))
done
avg=$(echo "scale=2; $sum / ${#filtered_results[@]}" | bc)

echo "总运行次数: $ITERATIONS"
echo -e "\033[90m已排除项: ${sorted_results[0]} ms (最快), ${sorted_results[-1]} ms (最慢)\033[0m"
echo "------------------------------------"
echo -ne "有效最小值 (Min): "; echo -e "\033[32m$min ms\033[0m"
echo -ne "有效最大值 (Max): "; echo -e "\033[31m$max ms\033[0m"
echo -ne "有效平均值 (Avg): "; echo -e "\033[36m$avg ms\033[0m"
echo "------------------------------------"
