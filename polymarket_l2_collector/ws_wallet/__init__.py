"""Dual-WebSocket wallet module — primary/backup WS with data verification."""

from __future__ import annotations

from .chain_verify import ChainVerifyWorker
from .dual_ws import DualWsManager
from .verifier import Bucket, Verdict, Verifier
from .wallet import WalletService

__all__ = ["Bucket", "Verdict", "Verifier", "ChainVerifyWorker", "DualWsManager", "WalletService"]
