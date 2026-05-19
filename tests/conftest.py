"""
Pytest configuration for trade_scanner_fh tests.

Adds the parent directory (project root) to sys.path so that
`from trade_scanner_fh.X import Y` works when pytest is invoked from
within the trade_scanner_fh/ directory.
"""
from pathlib import Path
import sys

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
