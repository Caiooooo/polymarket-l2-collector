#!/bin/bash
# 重启 Polymarket 数据收集系统

echo "========================================="
echo "重启 Polymarket 数据收集系统"
echo "========================================="
echo ""

PIDS=$(pgrep -f "python3 main.py")

if [ -n "$PIDS" ]; then
    echo "正在停止旧进程 (PID: $PIDS)..."
    pkill -f "python3 main.py"
    sleep 3
    echo "✅ 旧进程已停止"
else
    echo "没有运行中的进程"
fi

echo ""
bash "$(dirname "$0")/start_bg.sh"
