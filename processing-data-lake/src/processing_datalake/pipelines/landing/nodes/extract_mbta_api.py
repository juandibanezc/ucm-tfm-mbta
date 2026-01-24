"""Module to create nodes for MBTA API extraction."""
from datetime import datetime
from typing import Any, Dict


def last_ts() -> Dict[str, Any]:
    """Define last_ts"""
    return {
        "last_ts": datetime.now().strftime("%Y%m%d%H%M%S")
    }
