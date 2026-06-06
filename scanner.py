# scanner.py (compatibility wrapper)
# Keep this file small so existing imports continue to work.
from .scanner_core import Scanner

# Expose Scanner at module level for backward compatibility
__all__ = ["Scanner"]
