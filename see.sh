#!/bin/bash
# 查看 Polymarket 数据收集系统的运行状态

echo "========================================="
echo "Polymarket 数据收集系统 - 进程状态"
echo "========================================="
echo ""

# 查找 main.py 进程
PIDS=$(pgrep -f "python3 main.py")

if [ -z "$PIDS" ]; then
    echo "❌ 没有找到运行中的程序"
    echo ""
else
    echo "✅ 找到以下进程:"
    echo ""
    echo "PID    | CPU% | MEM% | 启动时间        | 命令"
    echo "-------|------|------|-----------------|----------------------------------"
    ps -o pid,pcpu,pmem,lstart,cmd -p $PIDS | tail -n +2
    echo ""
    echo "进程 ID: $PIDS"
    echo ""
    
    # 显示最新的日志文件
    LATEST_LOG=$(ls -t poly_*.log 2>/dev/null | head -1)
    if [ -n "$LATEST_LOG" ]; then
        echo "最新日志: $LATEST_LOG"
        echo "查看日志: tail -f $LATEST_LOG"
    fi
fi

echo ""
echo "========================================="
echo "有用的命令:"
echo "  查看实时日志: tail -f poly_*.log"
echo "  停止程序:     bash stop.sh"
echo "  重启程序:     bash start_bg.sh"
echo "========================================="

