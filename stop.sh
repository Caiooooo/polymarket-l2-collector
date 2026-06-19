#!/bin/bash
# 停止所有 Polymarket 数据收集进程

cd "$(dirname "$0")" || exit 1

echo "========================================="
echo "停止 Polymarket 数据收集系统"
echo "========================================="
echo ""

# 查找并停止守护进程和 main.py 进程
DAEMON_PIDS=$(pgrep -f "[p]ython3 .*daemon.py")
MAIN_PIDS=$(pgrep -f "[p]ython3 .*main.py")

if [ -z "$DAEMON_PIDS$MAIN_PIDS" ]; then
    echo "❌ 没有找到运行中的程序"
else
    echo "找到以下进程:"
    [ -n "$DAEMON_PIDS" ] && ps -fp $DAEMON_PIDS
    [ -n "$MAIN_PIDS" ] && ps -fp $MAIN_PIDS
    echo ""
    echo "正在停止进程..."
    pkill -f "[p]ython3 .*daemon.py"
    sleep 2
    pkill -f "[p]ython3 .*main.py"
    sleep 2
    rm -f collector_daemon.pid collector_child.pid
    echo "✅ 程序已停止"
fi

echo ""
echo "========================================="
