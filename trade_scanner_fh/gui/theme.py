"""Dark theme stylesheet for the Trading Scanner GUI."""

DARK_STYLESHEET = """
    QMainWindow, QWidget { background: #2b2b2b; color: #e0e0e0; }
    QToolBar { background: #333; border: none; padding: 4px; spacing: 4px; }
    QMenuBar { background: #333; color: #e0e0e0; }
    QMenuBar::item:selected { background: #4a90d9; }
    QMenu { background: #3c3c3c; color: #e0e0e0; border: 1px solid #555; }
    QMenu::item:selected { background: #4a90d9; }
    QMenu::separator { background: #555; height: 1px; margin: 4px 8px; }
    QLabel { color: #e0e0e0; }
    QGroupBox { color: #e0e0e0; border: 1px solid #555; border-radius: 4px;
                 margin-top: 8px; padding-top: 12px; }
    QGroupBox::title { subcontrol-origin: margin; left: 10px; }
    QCheckBox { color: #e0e0e0; spacing: 4px; }
    QCheckBox::indicator { width: 16px; height: 16px; }
    QSpinBox, QDoubleSpinBox, QDateEdit, QComboBox, QLineEdit {
        background: #3c3c3c; color: #e0e0e0; border: 1px solid #555;
        border-radius: 3px; padding: 2px 4px;
    }
    QSpinBox:focus, QDoubleSpinBox:focus, QDateEdit:focus,
    QComboBox:focus, QLineEdit:focus {
        border: 1px solid #4a90d9;
    }
    QPushButton {
        background: #444; color: #e0e0e0; border: 1px solid #555;
        border-radius: 3px; padding: 4px 12px;
    }
    QPushButton:hover { background: #555; }
    QPushButton:pressed { background: #333; }
    QPushButton:disabled { background: #3a3a3a; color: #666; }
    QTableView {
        background: #1e1e1e; color: #d4d4d4; gridline-color: #444;
        selection-background-color: #264f78; alternate-background-color: #252525;
    }
    QHeaderView::section {
        background: #333; color: #e0e0e0; border: 1px solid #444;
        padding: 4px; font-weight: bold;
    }
    QScrollArea { border: none; }
    QSplitter::handle { background: #444; }
    QStatusBar { background: #333; color: #aaa; }
    QScrollBar:vertical {
        background: #2b2b2b; width: 10px; margin: 0;
    }
    QScrollBar::handle:vertical {
        background: #555; min-height: 20px; border-radius: 5px;
    }
    QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
        height: 0;
    }
"""


# ── Spin-box arrows ─────────────────────────────────────────────────────
#
# Styling a QSpinBox's border/padding above switches Qt to full stylesheet
# rendering for the whole widget, and the up/down sub-controls then lose their
# native geometry: they collapse into two ~5 px stubs sitting SIDE BY SIDE at
# the right edge, drawn with no arrow glyph at all. The Deep OHLCV Refresh
# dialog was the visible symptom — its "Market days back" field showed two
# unusable dots and squeezed the number out of view — but every one of the ~14
# spin boxes in the app had it.
#
# Giving the sub-controls explicit geometry restores stacked buttons, but Qt
# still draws NO arrow inside a styled button unless an `image:` is supplied
# (verified: geometry alone yields a bare rectangle once a background is set).
# The two arrows are therefore painted once at startup into small PNGs and
# referenced by path — no packaged asset, so the .spec needs no data entry and
# a frozen build behaves exactly like a source run.

_ARROW_W, _ARROW_H = 9, 6


def _write_arrow(path, up: bool) -> None:
    """Paint one 9x6 triangle to ``path``. Raises on any failure."""
    from PyQt6.QtCore import QPoint, Qt
    from PyQt6.QtGui import QColor, QPainter, QPixmap, QPolygon

    pm = QPixmap(_ARROW_W, _ARROW_H)
    pm.fill(Qt.GlobalColor.transparent)
    painter = QPainter(pm)
    try:
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        painter.setBrush(QColor("#d0d0d0"))
        painter.setPen(Qt.PenStyle.NoPen)
        pts = ([QPoint(0, 5), QPoint(8, 5), QPoint(4, 0)] if up
               else [QPoint(0, 0), QPoint(8, 0), QPoint(4, 5)])
        painter.drawPolygon(QPolygon(pts))
    finally:
        painter.end()
    if not pm.save(str(path), "PNG"):
        raise OSError(f"could not write {path}")


# Geometry only. Qt falls back to its own (small, low-contrast) native arrows
# when no background is set on the button, so this stays usable if the PNGs
# cannot be written — a read-only DATA_DIR must not cost the user the arrows.
_SPIN_FALLBACK = """
    QSpinBox, QDoubleSpinBox { min-height: 20px; }
    QSpinBox::up-button, QDoubleSpinBox::up-button {
        subcontrol-origin: border; subcontrol-position: top right; width: 16px;
    }
    QSpinBox::down-button, QDoubleSpinBox::down-button {
        subcontrol-origin: border; subcontrol-position: bottom right;
        width: 16px;
    }
"""

_SPIN_TEMPLATE = """
    QSpinBox, QDoubleSpinBox {{ padding-right: 18px; min-height: 20px; }}
    QSpinBox::up-button, QDoubleSpinBox::up-button {{
        subcontrol-origin: border; subcontrol-position: top right;
        width: 16px; border-left: 1px solid #555; background: #454545;
        border-top-right-radius: 3px;
    }}
    QSpinBox::down-button, QDoubleSpinBox::down-button {{
        subcontrol-origin: border; subcontrol-position: bottom right;
        width: 16px; border-left: 1px solid #555; background: #454545;
        border-bottom-right-radius: 3px;
    }}
    QSpinBox::up-button:hover, QDoubleSpinBox::up-button:hover,
    QSpinBox::down-button:hover, QDoubleSpinBox::down-button:hover {{
        background: #5a5a5a;
    }}
    QSpinBox::up-button:pressed, QDoubleSpinBox::up-button:pressed,
    QSpinBox::down-button:pressed, QDoubleSpinBox::down-button:pressed {{
        background: #4a90d9;
    }}
    QSpinBox::up-button:disabled, QDoubleSpinBox::up-button:disabled,
    QSpinBox::down-button:disabled, QDoubleSpinBox::down-button:disabled {{
        background: #3a3a3a;
    }}
    QSpinBox::up-arrow, QDoubleSpinBox::up-arrow {{
        image: url({up}); width: {w}px; height: {h}px;
    }}
    QSpinBox::down-arrow, QDoubleSpinBox::down-arrow {{
        image: url({down}); width: {w}px; height: {h}px;
    }}
"""


def spinbox_arrow_css(asset_dir=None) -> str:
    """Stylesheet fragment that makes the spin-box arrows usable.

    Writes the two arrow PNGs into ``asset_dir`` (default
    ``config.DATA_DIR / "ui_assets"``) and returns rules pointing at them,
    falling back to geometry-only rules if anything about that fails.
    Requires a live QApplication — QPixmap cannot be constructed without one.
    """
    from pathlib import Path
    from PyQt6.QtGui import QGuiApplication
    if QGuiApplication.instance() is None:
        # QPixmap without a live application is a FATAL Qt error, not an
        # exception — it would abort the process rather than land in the
        # handler below. Refuse before touching it.
        return _SPIN_FALLBACK
    try:
        if asset_dir is None:
            from .. import config
            asset_dir = config.DATA_DIR / "ui_assets"
        asset_dir = Path(asset_dir)
        asset_dir.mkdir(parents=True, exist_ok=True)
        up = asset_dir / "spin_up.png"
        down = asset_dir / "spin_down.png"
        # Rewritten every launch: they are sub-millisecond to paint, and that
        # is cheaper than a staleness story for a cached asset.
        _write_arrow(up, up=True)
        _write_arrow(down, up=False)
        return _SPIN_TEMPLATE.format(
            # Qt stylesheet url() wants forward slashes on Windows too.
            up=str(up).replace("\\", "/"), down=str(down).replace("\\", "/"),
            w=_ARROW_W, h=_ARROW_H,
        )
    except Exception:
        return _SPIN_FALLBACK


def build_stylesheet(asset_dir=None) -> str:
    """The full application stylesheet. Call AFTER the QApplication exists."""
    return DARK_STYLESHEET + spinbox_arrow_css(asset_dir)
