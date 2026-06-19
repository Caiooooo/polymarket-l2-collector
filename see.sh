#!/bin/bash
# 查看 Polymarket 数据收集系统的运行状态

cd "$(dirname "$0")" || exit 1

echo "========================================="
echo "Polymarket 数据收集系统 - 进程状态"
echo "========================================="
echo ""

# 查找守护进程和 main.py 进程
DAEMON_PIDS=$(pgrep -f "[p]ython3 .*daemon.py")
MAIN_PIDS=$(pgrep -f "[p]ython3 .*main.py")

if [ -z "$DAEMON_PIDS$MAIN_PIDS" ]; then
    echo "❌ 没有找到运行中的程序"
    echo ""
else
    echo "✅ 找到以下进程:"
    echo ""
    echo "PID    | CPU% | MEM% | 启动时间        | 命令"
    echo "-------|------|------|-----------------|----------------------------------"
    [ -n "$DAEMON_PIDS" ] && ps -o pid,pcpu,pmem,lstart,cmd -p $DAEMON_PIDS | tail -n +2
    [ -n "$MAIN_PIDS" ] && ps -o pid,pcpu,pmem,lstart,cmd -p $MAIN_PIDS | tail -n +2
    echo ""
    [ -n "$DAEMON_PIDS" ] && echo "守护进程 ID: $DAEMON_PIDS"
    [ -n "$MAIN_PIDS" ] && echo "采集进程 ID: $MAIN_PIDS"
    echo ""
    
    # 显示最新的日志文件
    LATEST_LOG=$(ls -t poly_*.log 2>/dev/null | head -1)
    if [ -n "$LATEST_LOG" ]; then
        echo "最新日志: $LATEST_LOG"
        echo "查看日志: tail -f $LATEST_LOG"
    fi
    LATEST_DAEMON_LOG=$(ls -t daemon_*.log 2>/dev/null | head -1)
    if [ -n "$LATEST_DAEMON_LOG" ]; then
        echo "守护日志: $LATEST_DAEMON_LOG"
        echo "查看守护日志: tail -f $LATEST_DAEMON_LOG"
    fi
fi

echo ""
echo "========================================="
echo "有用的命令:"
echo "  查看实时日志: tail -f poly_*.log"
echo "  查看守护日志: tail -f daemon_*.log"
echo "  停止程序:     bash stop.sh"
echo "  重启程序:     bash start_bg.sh"
echo "========================================="

