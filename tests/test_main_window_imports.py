"""Static checks for main_window.py imports — catches bugs from the Phase 6
gui split where a method-local import resolves to the wrong namespace."""
import re
from pathlib import Path

_MAIN_WINDOW = (
    Path(__file__).resolve().parent.parent / "gui" / "main_window.py"
)


def test_no_single_dot_imports_of_top_level_modules():
    """Regression: `from .sector_map import …` and `from .earnings_cache
    import …` inside main_window.py would resolve to
    trade_scanner_fh.gui.sector_map / .earnings_cache — neither of which
    exist after the Phase 6 split into a `gui/` subpackage. Crashed the
    Data Coverage Gaps dialog on click. Catch any reintroduction of
    single-dot imports for top-level (non-gui) modules."""
    src = _MAIN_WINDOW.read_text(encoding="utf-8")
    # Top-level (non-gui) sibling modules that used to live in the same dir
    forbidden_targets = {
        "sector_map", "earnings_cache", "scanner", "indicators",
        "data_engine", "ticker_universe", "tradestation", "config",
    }
    pattern = re.compile(r"^\s*from \.([a-z_]+) import", re.MULTILINE)
    for match in pattern.finditer(src):
        target = match.group(1)
        assert target not in forbidden_targets, (
            f"main_window.py has a single-dot import of '{target}' which "
            f"resolves to trade_scanner_fh.gui.{target} — that path does "
            f"not exist after the Phase 6 gui split. Use '..{target}' "
            f"instead. Match: {match.group(0).strip()!r}"
        )
