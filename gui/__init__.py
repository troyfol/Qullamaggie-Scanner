"""Trading Scanner GUI subpackage.

Public entry point is `main()` — re-exported here so that
`from trade_scanner_fh.gui import main` continues to work after the
Phase 6 O1 split of the old monolithic gui.py into:

    theme.py         — dark stylesheet
    widgets.py       — IndicatorRow/Panel, LogPanel, ResultsTable, etc.
    workers.py       — QThread workers for scan/update/universe/bridge/fill
    dialogs.py       — WatchlistDialog
    main_window.py   — MainWindow + main()
"""
from .main_window import main

__all__ = ["main"]
