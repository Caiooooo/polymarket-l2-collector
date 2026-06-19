#!/bin/bash
# 后台运行，日志输出到文件
# 守护进程负责异常退出 / OOM 后自动拉起 main.py

cd "$(dirname "$0")" || exit 1

LOG_FILE="poly_$(date +%Y%m%d).log"
DAEMON_LOG="daemon_$(date +%Y%m%d).log"

echo "========================================="
echo "后台启动 Polymarket 数据收集系统"
echo "========================================="
echo ""
echo "特性："
echo "  🔄 每日 03:00 自动重启"
echo "  💓 健康监控：价格 / WS 无数据自动重启"
echo "  🛡️ 守护进程：崩溃 / OOM / 异常退出后自动拉起"
echo ""

# 检查并停止已有的进程
DAEMON_PIDS=$(pgrep -f "[p]ython3 .*daemon.py")
MAIN_PIDS=$(pgrep -f "[p]ython3 .*main.py")
if [ -n "$DAEMON_PIDS$MAIN_PIDS" ]; then
    echo "🔄 发现已有程序在运行"
    [ -n "$DAEMON_PIDS" ] && echo "   守护进程 PID: $DAEMON_PIDS"
    [ -n "$MAIN_PIDS" ] && echo "   采集进程 PID: $MAIN_PIDS"
    echo "   正在停止旧进程..."
    pkill -f "[p]ython3 .*daemon.py"
    pkill -f "[p]ython3 .*main.py"
    sleep 3
    echo "✅ 旧进程已停止"
    echo ""
fi

echo "采集日志: $LOG_FILE"
echo "守护日志: $DAEMON_LOG"
echo ""
echo "查看日志: tail -f $LOG_FILE"
echo "查看守护日志: tail -f $DAEMON_LOG"
echo "停止程序: bash stop.sh"
echo "查看状态: bash see.sh"
echo "========================================="
echo ""

nohup python3 daemon.py > "$DAEMON_LOG" 2>&1 &

PID=$!
echo ""
echo "✅ 守护进程已在后台启动"
echo "   守护进程 ID: $PID"
echo "   采集日志: $LOG_FILE"
echo "   守护日志: $DAEMON_LOG"
echo ""

