"""Compatibility facade for the status diagnostics service.

The implementation lives in bounded scanner modules. This module remains the
stable import location used by the CLI and existing downstream callers.
"""

from .contracts import ASSET_PAYLOAD_KEYS, STATUS_GUIDANCE, STATUS_PAYLOAD_KEYS
from .prerequisites import check_prerequisites
from .report import collect_status_report

__all__ = [
    "ASSET_PAYLOAD_KEYS",
    "STATUS_GUIDANCE",
    "STATUS_PAYLOAD_KEYS",
    "check_prerequisites",
    "collect_status_report",
]
