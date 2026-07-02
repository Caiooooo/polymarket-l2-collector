"""
Polymarket L2 Collector — orchestration entry point.

Runs Binance price sync, and one ``Collector`` per configured interval,
along with health monitoring and daily restart.
"""

from __future__ import annotations

import asyncio
import gc
import resource
import signal
import sys
import time
from datetime import datetime

from . import binance_price as binance_price_mod
from .collector import Collector
from .config import load_settings
from .file_cache import drop_empty_cache_windows, flush_all_caches
from .logger_config import get_logger
from .ws_wallet import WalletService

logger = get_logger("main")


# ── Global health state ────────────────────────────────────────────

_last_binance_update: float = 0.0
_last_activity: float = 0.0


def touch_activity() -> None:
    global _last_activity
    _last_activity = time.time()


# ── Graceful shutdown ──────────────────────────────────────────────


class GracefulKiller:
    """First SIGINT/SIGTERM → graceful stop; second → hard exit."""

    def __init__(self) -> None:
        self.kill_now = False
        self._count = 0
        signal.signal(signal.SIGINT, self._handler)
        signal.signal(signal.SIGTERM, self._handler)

    def _handler(self, signum: int, frame) -> None:
        self._count += 1
        if self._count >= 2:
            logger.warning("Second interrupt — hard exit")
            sys.exit(1)
        logger.info("Shutdown signal received — stopping gracefully …")
        self.kill_now = True


# ── Wrapped tasks ──────────────────────────────────────────────────


async def _wrap_binance(killer: GracefulKiller) -> None:
    """Run Binance subscription with health-check monitor."""
    global _last_binance_update
    settings = load_settings()

    async def _monitor() -> None:
        global _last_binance_update
        while not killer.kill_now:
            await asyncio.sleep(settings.health_check_interval)
            if binance_price_mod.last_message_time > _last_binance_update:
                _last_binance_update = binance_price_mod.last_message_time
                touch_activity()

    monitor = asyncio.create_task(_monitor())
    try:
        await binance_price_mod.subscribe_book_ticker()
    finally:
        monitor.cancel()
        try:
            await monitor
        except asyncio.CancelledError:
            pass


async def _wrap_collector(interval: str, killer: GracefulKiller, wallet: WalletService | None = None) -> None:
    """Run one Collector for *interval*."""
    collector = Collector(interval=interval, touch_activity=touch_activity, wallet=wallet)
    try:
        await collector.run()
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.error("Collector '%s' exited: %s", interval, exc, extra={"interval": interval, "error": str(exc)[:200]})
        raise


# ── Supervisors ────────────────────────────────────────────────────


def _get_rss_mb() -> float:
    """Current RSS in MB (Linux /proc preferred)."""
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024
    except Exception:
        pass
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


async def _memory_supervisor(killer: GracefulKiller) -> None:
    """Flush caches on soft limit; trigger restart on hard limit."""
    settings = load_settings()
    while not killer.kill_now:
        await asyncio.sleep(settings.health_check_interval)
        rss = _get_rss_mb()
        if rss < settings.memory_soft_limit_mb:
            continue
        logger.warning(
            "RSS %.0f MB above %d MB — flushing",
            rss,
            settings.memory_soft_limit_mb,
            extra={"rss_mb": rss, "soft_limit_mb": settings.memory_soft_limit_mb},
        )
        try:
            flush_all_caches()
            drop_empty_cache_windows(settings.max_cached_windows)
        except Exception as exc:
            logger.error("Cache flush failed: %s", exc)
        gc.collect()
        after = _get_rss_mb()
        logger.info("RSS after GC: %.0f MB", after)
        if after >= settings.memory_hard_limit_mb:
            logger.error(
                "RSS %.0f MB above %d MB — restarting",
                after,
                settings.memory_hard_limit_mb,
                extra={"rss_mb": after, "hard_limit_mb": settings.memory_hard_limit_mb},
            )
            return


async def _health_supervisor(killer: GracefulKiller, tasks: list[asyncio.Task]) -> None:
    """Monitor task health and staleness; trigger restart on failure."""
    settings = load_settings()
    while not killer.kill_now:
        await asyncio.sleep(settings.health_check_interval)

        now = time.time()
        for t in tasks:
            if t.done():
                exc = t.exception()
                logger.error("Task %s died: %s", t.get_name(), exc)
                return

        if now - _last_binance_update > settings.binance_stale_seconds:
            logger.error(
            "Binance stale (%.0f s) — restarting",
            now - _last_binance_update,
            extra={"event": "binance_stale", "seconds": now - _last_binance_update},
        )
            return

        if now - _last_activity > settings.poly_ws_stale_seconds:
            logger.error(
                "System idle (%.0f s) — restarting",
                now - _last_activity,
                extra={"event": "idle", "seconds": now - _last_activity},
            )
            return


# ── Daily restart scheduler ────────────────────────────────────────


async def _daily_restart_scheduler(killer: GracefulKiller) -> None:
    """Signal restart at the configured daily time."""
    settings = load_settings()
    while not killer.kill_now:
        now = datetime.now()
        target = now.replace(hour=settings.restart_hour, minute=settings.restart_minute, second=0, microsecond=0)
        if target <= now:
            from datetime import timedelta

            target += timedelta(days=1)
        wait = min((target - now).total_seconds(), 3600)
        logger.info("Next restart: %02d:%02d (T-%ds)", settings.restart_hour, settings.restart_minute, int(wait))
        try:
            await asyncio.sleep(wait)
        except asyncio.CancelledError:
            raise
        now = datetime.now()
        if now.hour == settings.restart_hour and now.minute == settings.restart_minute:
            logger.info("Daily restart triggered")
            return


# ── Session lifecycle ──────────────────────────────────────────────


async def _run_session(killer: GracefulKiller) -> None:
    """Run one collection session (until restart or failure)."""
    global _last_binance_update, _last_activity
    _last_binance_update = time.time()
    _last_activity = time.time()

    settings = load_settings()

    wallet = WalletService()

    tasks = [
        asyncio.create_task(_wrap_binance(killer), name="binance"),
    ]
    for interval in settings.intervals:
        tasks.append(
            asyncio.create_task(
                _wrap_collector(interval, killer, wallet=wallet),
                name=f"poly_{interval}"
            )
        )

    supervisors = [
        asyncio.create_task(_health_supervisor(killer, tasks), name="supervisor"),
        asyncio.create_task(_memory_supervisor(killer), name="memory"),
        asyncio.create_task(_daily_restart_scheduler(killer), name="daily_restart"),
    ]
    all_tasks = tasks + supervisors

    done, pending = await asyncio.wait(all_tasks, return_when=asyncio.FIRST_COMPLETED)
    finished = next(iter(done)).get_name() if done else "unknown"
    logger.info("Session ended (trigger: %s) — cleaning up …", finished)

    for t in pending:
        t.cancel()
    await asyncio.gather(*pending, return_exceptions=True)

    try:
        await wallet.close()
    except Exception as exc:
        logger.error("Wallet close error: %s", exc)

    try:
        flush_all_caches()
        drop_empty_cache_windows(settings.max_cached_windows)
    except Exception as exc:
        logger.error("End-of-session cache flush failed: %s", exc)
    gc.collect()
    logger.info("Session cleaned up")


# ── Main loop ──────────────────────────────────────────────────────


async def main_async() -> None:
    """Main entry — loop sessions forever with restart back-off."""
    killer = GracefulKiller()
    session = 0
    quick_restarts = 0
    min_session_s = 120

    while not killer.kill_now:
        session += 1
        start = time.time()
        logger.info("=" * 60)
        logger.info("Session %d starting", session)
        logger.info("=" * 60)
        try:
            await _run_session(killer)
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.error("Session %d failed: %s", session, exc)

        if killer.kill_now:
            break

        duration = time.time() - start
        quick_restarts = quick_restarts + 1 if duration < min_session_s else 0
        delay = 3
        if quick_restarts > 3:
            delay = min(30 * (2 ** min(quick_restarts - 3, 3)), 120)
            logger.warning("Backing off %ds after %d quick restarts", delay, quick_restarts)
        logger.info("Restarting in %ds …", delay)
        await asyncio.sleep(delay)

    logger.info("Program exited")


def main() -> None:
    """Synchronous CLI entry point (``polymarket-l2-collector`` command)."""
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        print("\nProgram stopped by user")


if __name__ == "__main__":
    main()
