#!/bin/bash
# 停止所有 Polymarket 数据收集进程

echo "========================================="
echo "停止 Polymarket 数据收集系统"
echo "========================================="
echo ""

# 查找并停止 main.py 进程
PIDS=$(pgrep -f "python3 main.py")

if [ -z "$PIDS" ]; then
    echo "❌ 没有找到运行中的程序"
else
    echo "找到以下进程:"
    ps -fp $PIDS
    echo ""
    echo "正在停止进程..."
    pkill -f "python3 main.py"
    sleep 2
    echo "✅ 程序已停止"
fi

echo ""
echo "========================================="
