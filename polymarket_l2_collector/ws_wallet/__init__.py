"""Dual-WebSocket wallet module — primary/backup WS with data verification."""
from __future__ import annotations

from .dual_ws import DualWsManager
from .verifier import Bucket, Verdict, Verifier

__all__ = ["Bucket", "Verdict", "Verifier", "DualWsManager"]
