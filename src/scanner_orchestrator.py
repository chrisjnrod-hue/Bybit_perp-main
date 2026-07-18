# scanner.py (shim)
# Backwards-compatible shim so existing imports continue to work.
from .scanner_service import Scanner

__all__ = ["Scanner"]
