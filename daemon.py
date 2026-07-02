#!/usr/bin/env python3
"""
Polymarket L2 Collector 守护进程
- 独立进程启动并监控 main.py
- main.py 崩溃 / 被 OOM kill / 异常退出后自动重启
- 支持 SIGTERM / SIGINT 优雅停止子进程
"""
import os
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
MAIN_MODULE = "polymarket_l2_collector.main"
PID_FILE = BASE_DIR / "collector_daemon.pid"
CHILD_PID_FILE = BASE_DIR / "collector_child.pid"
LOG_FILE = BASE_DIR / f"poly_{datetime.now().strftime('%Y%m%d')}.log"

RESTART_DELAY_SECONDS = int(os.getenv("DAEMON_RESTART_DELAY_SECONDS", "5"))
MAX_RESTART_DELAY_SECONDS = int(os.getenv("DAEMON_MAX_RESTART_DELAY_SECONDS", "120"))
QUICK_EXIT_SECONDS = int(os.getenv("DAEMON_QUICK_EXIT_SECONDS", "120"))

_stop_requested = False
_child = None


def _log(message):
    line = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | daemon | {message}"
    print(line, flush=True)


def _write_pid(path, pid):
    path.write_text(str(pid), encoding="utf-8")


def _remove_pid_files():
    for path in (PID_FILE, CHILD_PID_FILE):
        try:
            path.unlink()
        except FileNotFoundError:
            pass


def _is_pid_running(pid):
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _check_existing_daemon():
    if not PID_FILE.exists():
        return
    try:
        old_pid = int(PID_FILE.read_text(encoding="utf-8").strip())
    except ValueError:
        PID_FILE.unlink(missing_ok=True)
        return
    if old_pid != os.getpid() and _is_pid_running(old_pid):
        _log(f"已有守护进程在运行，PID={old_pid}")
        sys.exit(1)
    PID_FILE.unlink(missing_ok=True)


def _signal_handler(signum, frame):
    global _stop_requested
    _stop_requested = True
    _log(f"收到停止信号 {signum}，准备退出")
    _terminate_child()


def _terminate_child():
    global _child
    if _child is None or _child.poll() is not None:
        return
    _log(f"停止采集子进程 PID={_child.pid}")
    try:
        _child.terminate()
        _child.wait(timeout=20)
    except subprocess.TimeoutExpired:
        _log(f"子进程 PID={_child.pid} 未及时退出，强制 kill")
        _child.kill()
        _child.wait(timeout=10)
    finally:
        CHILD_PID_FILE.unlink(missing_ok=True)


def _start_child():
    env = os.environ.copy()
    env.setdefault("PYTHONUNBUFFERED", "1")
    log_handle = open(LOG_FILE, "a", buffering=1, encoding="utf-8")
    child = subprocess.Popen(
        [sys.executable, "-m", MAIN_MODULE],
        cwd=str(BASE_DIR),
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        stdin=subprocess.DEVNULL,
        env=env,
        start_new_session=True,
    )
    _write_pid(CHILD_PID_FILE, child.pid)
    _log(f"已启动采集子进程 PID={child.pid}，日志={LOG_FILE.name}")
    return child, log_handle


def main():
    global _child
    _check_existing_daemon()
    _write_pid(PID_FILE, os.getpid())

    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    _log(f"守护进程启动 PID={os.getpid()}")
    restart_delay = RESTART_DELAY_SECONDS

    try:
        while not _stop_requested:
            start_time = time.time()
            log_handle = None
            try:
                _child, log_handle = _start_child()
                exit_code = _child.wait()
            finally:
                CHILD_PID_FILE.unlink(missing_ok=True)
                if log_handle is not None:
                    log_handle.close()

            if _stop_requested:
                break

            runtime = time.time() - start_time
            if exit_code == 0:
                _log(f"采集进程正常退出，{restart_delay}s 后重新启动")
            elif exit_code < 0:
                _log(f"采集进程被信号 {-exit_code} 结束，可能是 OOM/kill，{restart_delay}s 后重新启动")
            else:
                _log(f"采集进程异常退出 exit_code={exit_code}，{restart_delay}s 后重新启动")

            time.sleep(restart_delay)
            if runtime < QUICK_EXIT_SECONDS:
                restart_delay = min(restart_delay * 2, MAX_RESTART_DELAY_SECONDS)
            else:
                restart_delay = RESTART_DELAY_SECONDS
    finally:
        _terminate_child()
        _remove_pid_files()
        _log("守护进程已退出")


if __name__ == "__main__":
    main()
