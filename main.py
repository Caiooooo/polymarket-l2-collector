#!/usr/bin/env python3
"""
Polymarket L2 数据收集器 - 主入口
- 同时运行币安价格 + 15m 市场 + 5m 市场 WebSocket
- 每日定时重启，防止内存泄漏 / 连接老化
- 健康监控：任一组件卡死或崩溃后自动重启全部
"""
import asyncio
import gc
import time
import signal
import sys
import json
from datetime import datetime
from logger_config import setup_logger

import binance_price
import poly_ws_15min
import poly_ws_5min

logger = setup_logger('main')

# ===================== 配置 =====================
RESTART_HOUR = 3       # 每日重启时间（24h）
RESTART_MINUTE = 0
HEALTH_CHECK_INTERVAL = 30      # 健康检查间隔（秒）
BINANCE_STALE_SECONDS = 300     # 币安价格超过此时间未更新 → 重启
POLY_WS_STALE_SECONDS = 600     # Poly WS 超过此时间无数据 → 重启

# ===================== 全局健康状态 =====================
_last_binance_update = time.time()
_last_activity = time.time()    # 任一组件有活动就更新


def touch_activity():
    """标记系统有活动"""
    global _last_activity
    _last_activity = time.time()


class GracefulKiller:
    """优雅退出：第一次 Ctrl+C 正常退出，第二次强制"""

    def __init__(self):
        self.kill_now = False
        self._count = 0
        signal.signal(signal.SIGINT, self._handler)
        signal.signal(signal.SIGTERM, self._handler)

    def _handler(self, signum, frame):
        self._count += 1
        if self._count >= 2:
            logger.warning("收到第二次中断信号，强制退出...")
            sys.exit(1)
        logger.info("收到退出信号，正在优雅关闭...")
        self.kill_now = True


# ===================== 包装任务（注入健康检查） =====================

async def _wrap_binance(killer: GracefulKiller):
    """包装币安订阅，catch 异常并更新健康时间戳"""
    global _last_binance_update
    try:
        # 注入健康心跳：直接检查币安 last_message_time（每条消息都刷新）
        async def health_monitor():
            nonlocal _last_binance_update
            while not killer.kill_now:
                await asyncio.sleep(HEALTH_CHECK_INTERVAL)
                last_msg = binance_price.last_message_time
                if last_msg > 0 and last_msg > _last_binance_update:
                    _last_binance_update = last_msg
                    touch_activity()

        monitor_task = asyncio.create_task(health_monitor())
        try:
            await binance_price.subscribe_book_ticker()
        finally:
            monitor_task.cancel()
            try:
                await monitor_task
            except asyncio.CancelledError:
                pass
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.error(f"[binance] 任务异常退出: {e}")
        raise


async def _wrap_poly_15m(killer: GracefulKiller):
    """包装 15m 市场订阅"""
    try:
        # 注入健康心跳：原 run_poly_ws_15min 每次收消息不会通知 main，
        # 我们在 poly_ws_15min 模块内部通过 touch_activity 来做
        await poly_ws_15min.run_poly_ws_15min()
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.error(f"[15m] 任务异常退出: {e}")
        raise


async def _wrap_poly_5m(killer: GracefulKiller):
    """包装 5m 市场订阅"""
    try:
        await poly_ws_5min.run_poly_ws_5m()
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.error(f"[5m] 任务异常退出: {e}")
        raise


# ===================== 健康监督器 =====================

async def _health_supervisor(killer: GracefulKiller, tasks: list):
    """定期检查所有子任务健康状况，触发重启"""
    while not killer.kill_now:
        await asyncio.sleep(HEALTH_CHECK_INTERVAL)

        now = time.time()
        binance_stale = now - _last_binance_update
        activity_stale = now - _last_activity

        # 检查各任务是否存活
        dead_tasks = []
        for t in tasks:
            if t.done():
                exc = t.exception()
                dead_tasks.append((t, exc))

        if dead_tasks:
            for t, exc in dead_tasks:
                logger.error(f"检测到任务崩溃: {t.get_name()} exc={exc}")
            logger.warning("有任务已崩溃，触发重启...")
            return  # 退出 supervisor，让外层重启

        # 健康检查：币安价格太旧
        if binance_stale > BINANCE_STALE_SECONDS:
            logger.error(f"币安价格 {binance_stale:.0f}s 未更新，触发重启...")
            return

        # 全局静默检查（所有 WS 都没数据）
        if activity_stale > POLY_WS_STALE_SECONDS:
            logger.error(f"系统静默 {activity_stale:.0f}s，触发重启...")
            return

    logger.info("健康监督器退出")


# ===================== 每日重启调度 =====================

def _seconds_until(hour: int, minute: int) -> float:
    """计算距离下一次指定时间还有多少秒"""
    now = datetime.now()
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        from datetime import timedelta
        target += timedelta(days=1)
    return (target - now).total_seconds()


async def _daily_restart_scheduler(killer: GracefulKiller):
    """每天在指定时间触发重启"""
    while not killer.kill_now:
        wait = _seconds_until(RESTART_HOUR, RESTART_MINUTE)
        logger.info(f"下次定时重启: {datetime.now().strftime('%Y-%m-%d')} "
                     f"{RESTART_HOUR:02d}:{RESTART_MINUTE:02d} (还剩 {wait/3600:.1f} 小时)")
        try:
            await asyncio.sleep(min(wait, 3600))  # 最多每小时检查一次，防止计算偏差
        except asyncio.CancelledError:
            raise
        # 精确判断是否到了重启时间
        now = datetime.now()
        if now.hour == RESTART_HOUR and now.minute == RESTART_MINUTE:
            logger.info("=" * 60)
            logger.info(f"⏰ 到达每日定时重启时间 {RESTART_HOUR:02d}:{RESTART_MINUTE:02d}")
            logger.info("=" * 60)
            return  # 退出 scheduler，触发 restart


# ===================== 主循环（带重启） =====================

async def _run_session(killer: GracefulKiller):
    """运行一次完整的数据采集会话（直到需要重启）"""
    global _last_binance_update, _last_activity
    _last_binance_update = time.time()
    _last_activity = time.time()

    # 创建子任务
    tasks = [
        asyncio.create_task(_wrap_binance(killer), name='binance'),
        asyncio.create_task(_wrap_poly_15m(killer), name='poly_15m'),
        asyncio.create_task(_wrap_poly_5m(killer), name='poly_5m'),
    ]

    # 启动健康监督器和定时重启调度器
    supervisor = asyncio.create_task(_health_supervisor(killer, tasks), name='supervisor')
    daily = asyncio.create_task(_daily_restart_scheduler(killer), name='daily_restart')

    all_tasks = tasks + [supervisor, daily]

    # 等待任意一个管理任务完成（意味着需要重启）
    done, pending = await asyncio.wait(
        all_tasks,
        return_when=asyncio.FIRST_COMPLETED,
    )

    finished_name = next(iter(done)).get_name() if done else 'unknown'
    logger.info(f"会话结束触发源: {finished_name}，正在清理所有任务...")

    # 取消所有剩余任务
    for t in pending:
        t.cancel()
    # 等待取消完成
    await asyncio.gather(*pending, return_exceptions=True)
    gc.collect()  # 会话清理后强制回收内存

    logger.info("当前会话已完全清理")


async def main():
    """主入口：循环重启，永不停止"""
    killer = GracefulKiller()

    session = 0
    consecutive_quick_restarts = 0  # 连续快速重启计数
    MIN_SESSION_DURATION = 120       # 低于此时间视为"快速重启"

    while not killer.kill_now:
        session += 1
        session_start = time.time()
        logger.info("=" * 60)
        logger.info(f"🚀 启动第 {session} 次会话")
        logger.info("=" * 60)
        try:
            await _run_session(killer)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"会话 {session} 异常退出: {e}")

        if killer.kill_now:
            break

        # 检测快速重启，避免 API 限流 / 连接风暴
        session_duration = time.time() - session_start
        if session_duration < MIN_SESSION_DURATION:
            consecutive_quick_restarts += 1
        else:
            consecutive_quick_restarts = 0

        # 基础等待 3s，快速重启时指数退避（最多 120s）
        restart_delay = 3
        if consecutive_quick_restarts > 3:
            restart_delay = min(30 * (2 ** min(consecutive_quick_restarts - 3, 3)), 120)
            logger.warning(f"检测到连续 {consecutive_quick_restarts} 次快速重启，退避 {restart_delay}s")

        logger.info(f"{restart_delay}s 后重启...")
        await asyncio.sleep(restart_delay)

    logger.info("程序已完全退出")


# ===================== 入口 =====================

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n程序已停止")

