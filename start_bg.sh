#!/bin/bash
# 后台运行，日志输出到文件

LOG_FILE="poly_$(date +%Y%m%d).log"

echo "========================================="
echo "后台启动 Polymarket 数据收集系统"
echo "========================================="
echo ""

# 检查并停止已有的进程
PIDS=$(pgrep -f "python3 main.py")
if [ -n "$PIDS" ]; then
    echo "🔄 发现已有程序在运行 (PID: $PIDS)"
    echo "   正在停止旧进程..."
    pkill -f "python3 main.py"
    sleep 2
    echo "✅ 旧进程已停止"
    echo ""
fi

echo "日志文件: $LOG_FILE"
echo ""
echo "查看日志: tail -f $LOG_FILE"
echo "停止程序: bash stop.sh"
echo "========================================="
echo ""

nohup python3 main.py > "$LOG_FILE" 2>&1 &

PID=$!
echo ""
echo "✅ 程序已在后台启动"
echo "   进程 ID: $PID"
echo "   日志文件: $LOG_FILE"
echo ""

