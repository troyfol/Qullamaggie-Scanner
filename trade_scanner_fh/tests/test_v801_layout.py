"""v8.0.1 — the window fits narrower monitors and survives moving onto them.

Measured 2026-10-01: the main window could not be narrower than 2,116
logical px — every toolbar row and every filter row was a single-line
QHBoxLayout — while the user's monitors are 2,560 / 1,536 / 1,440 / 1,080
logical px wide (150 / 125 / 100 / 100 % scale). Dragged onto a narrower one,
Windows proposed a size Qt refused as below the minimum: the window jumped,
or hung across two screens. The filter panel's rows needed 1,963 px, so the
panel always carried a horizontal scrollbar.
"""
from __future__ import annotations

import pytest
from PyQt6.QtCore import QRect
from PyQt6.QtWidgets import QApplication, QLabel, QPushButton, QWidget

from trade_scanner_fh.gui.flow_layout import FlowLayout


@pytest.fixture(scope="module", autouse=True)
def qapp():
    from PyQt6.QtWidgets import QApplication
    return QApplication.instance() or QApplication([])


def _row(widths=(100, 140, 80, 120), gap_after=None):
    host = QWidget()
    flow = FlowLayout(host, h_spacing=6, v_spacing=4)
    buttons = []
    for i, w in enumerate(widths):
        b = QPushButton(f"b{i}")
        b.setFixedSize(w, 24)
        flow.addWidget(b)
        buttons.append(b)
        if gap_after == i:
            flow.addSpacing(30)
    flow.addStretch()                      # accepted, ignored
    return host, flow, buttons


# ======================================================================
# FlowLayout
# ======================================================================

def test_minimum_width_is_the_widest_single_item():
    _host, flow, _b = _row()
    assert flow.minimumSize().width() == 140


def test_one_line_when_there_is_room_and_wraps_when_not():
    _host, flow, _b = _row()
    one_line = 100 + 140 + 80 + 120 + 3 * 6
    assert flow.sizeHint().width() == one_line
    assert flow.heightForWidth(one_line) == 24
    assert flow.heightForWidth(260) == 24 * 2 + 4
    assert flow.heightForWidth(140) == 24 * 4 + 4 * 3


def test_items_never_overlap_and_stay_inside(qapp):
    host, flow, buttons = _row(gap_after=1)
    flow.setGeometry(QRect(0, 0, 260, flow.heightForWidth(260)))
    rects = [b.geometry() for b in buttons]
    for i, a in enumerate(rects):
        assert a.right() < 260
        for b in rects[i + 1:]:
            assert not a.intersects(b)


def test_a_gap_never_starts_a_line_and_hidden_widgets_take_no_room():
    # 180 + 6 + gap 30 overflows 200: the GAP forces the wrap, and must not
    # then lead line 2 (b1 would start 36 px in).
    _host, flow, buttons = _row(widths=(180, 100), gap_after=0)
    flow.setGeometry(QRect(0, 0, 200, flow.heightForWidth(200)))
    assert buttons[1].geometry().x() == 0
    buttons[1].hide()
    assert flow.heightForWidth(200) == 24


# ======================================================================
# The filter panel
# ======================================================================

@pytest.fixture(scope="module")
def panel(qapp):
    from trade_scanner_fh.gui.widgets import IndicatorPanel
    return IndicatorPanel()


def test_every_filter_row_now_fits_a_narrow_panel(panel):
    """Before: 31 of 144 rows were wider than 560 px, the widest 1,941."""
    widest = max(r.minimumSizeHint().width() for r in panel.rows.values())
    assert widest < 700, widest


def test_a_setting_never_wraps_away_from_its_label(panel):
    from PyQt6.QtWidgets import QCheckBox
    checked = 0
    for key in ("accel_eps_surp", "atr", "surge", "consec_eps_beats"):
        row = panel.rows[key]
        for name, widget in row.spinboxes.items():
            if isinstance(widget, QCheckBox):
                continue                 # a checkbox carries its own label
            box = widget.parentWidget()
            assert box is not row._params_box,                 f"{key}.{name}: input is a flow item on its own"
            labels = [c for c in box.children() if isinstance(c, QLabel)]
            assert labels, f"{key}.{name}: label and input share one item"
            checked += 1
    assert checked >= 10


def test_series_rows_wrap_instead_of_running_off_the_panel(panel):
    row = panel.rows["accel_eps_surp"]
    one_line = row.sizeHint().height()
    assert row.heightForWidth(560) > one_line, "wrapped onto more lines"


def test_panel_minimum_comes_from_its_content(panel, qapp):
    """No fixed 580: the panel is never narrower than its content, so it
    never needs a sideways scrollbar."""
    w = panel.minimumSizeHint().width()
    assert w >= panel.widget().minimumSizeHint().width()
    panel.resize(w, 700)
    panel.show()
    qapp.processEvents()
    assert panel.horizontalScrollBar().maximum() == 0
    panel.hide()


# ======================================================================
# The window
# ======================================================================

@pytest.fixture
def window(qapp, tmp_parquets, monkeypatch):
    from trade_scanner_fh.gui import main_window as mw_mod
    monkeypatch.setattr(mw_mod, "PRESETS_DIR", tmp_parquets / "presets")
    (tmp_parquets / "presets").mkdir(exist_ok=True)
    w = mw_mod.MainWindow()
    yield w
    w.close()
    w.deleteLater()


def test_window_fits_a_1080_wide_monitor(window):
    """Was 2,116 with the app's dark theme applied — the theme's padding is
    what makes the buttons wide, so it is measured WITH it. The user's
    narrowest monitor is 1,080 logical px."""
    from trade_scanner_fh.gui import theme
    from trade_scanner_fh.gui.flow_layout import WrappingBar
    app = QApplication.instance()
    before = app.styleSheet()
    app.setStyleSheet(theme.DARK_STYLESHEET)
    try:
        QApplication.processEvents()
        assert window.minimumSizeHint().width() <= 1060
    finally:
        app.setStyleSheet(before)
    assert (window.minimumWidth(), window.minimumHeight()) == (900, 600)
    # Every full-width row wraps: none may be a single-line box layout.
    lay = window.centralWidget().layout()
    for i in range(lay.count()):
        item = lay.itemAt(i)
        sub = item.layout()
        if sub is not None:
            assert isinstance(sub, FlowLayout), type(sub).__name__
        elif isinstance(item.widget(), WrappingBar):
            continue


class _Screen:
    def __init__(self, rect):
        self._r = rect

    def availableGeometry(self):
        return self._r

    def name(self):
        return "fake"


def test_fit_shrinks_and_pulls_the_window_onto_its_monitor(window,
                                                           monkeypatch):
    portrait = QRect(3840, -139, 1080, 1880)
    monkeypatch.setattr(window, "screen", lambda: _Screen(portrait))
    window.resize(2400, 1300)
    window.move(3000, 200)              # straddling two monitors
    assert window._fit_to_screen() is True
    f = window.frameGeometry()
    assert portrait.contains(f), (f, portrait)
    assert window._fit_to_screen() is False, "already fits: no change"


def test_fit_leaves_a_maximized_window_alone(window, monkeypatch):
    monkeypatch.setattr(window, "isMaximized", lambda: True)
    monkeypatch.setattr(window, "screen",
                        lambda: _Screen(QRect(0, 0, 500, 500)))
    window.resize(1500, 900)
    assert window._fit_to_screen() is False


def test_fit_waits_for_the_move_to_settle_on_another_monitor(window,
                                                           monkeypatch):
    from trade_scanner_fh.gui import main_window as mw_mod
    fits = []
    monkeypatch.setattr(window, "_fit_to_screen", lambda: fits.append(1))
    a, b = object(), object()
    current = {"s": a}
    monkeypatch.setattr(window, "screen", lambda: current["s"])
    window.show()                                  # hidden widgets defer moves
    QApplication.processEvents()
    window._last_settled_screen = a
    window.move(window.x() + 5, window.y())        # a real moveEvent
    QApplication.processEvents()
    assert window._move_timer is not None and window._move_timer.isActive()

    current["s"] = b
    held = {"down": True}
    monkeypatch.setattr(mw_mod, "_primary_mouse_button_down",
                        lambda: held["down"])
    window._on_move_settled()
    assert fits == [] and window._move_timer.isActive(),         "button still held: a paused drag, never fitted mid-drag"
    held["down"] = False
    window._on_move_settled()
    assert fits == [1], "fitted once the move ended on a new monitor"
    window._on_move_settled()
    assert fits == [1], "same monitor again: nothing to do"


def test_mouse_button_probe_is_safe_everywhere():
    from trade_scanner_fh.gui import main_window as mw_mod
    assert mw_mod._primary_mouse_button_down() in (True, False)


def test_no_native_event_override():
    """A Python nativeEvent that calls the base class crashes PyQt 6.7.1 /
    Qt 6.7.3 with an access violation when the window is shown (found by
    the live monitor test). It must not come back."""
    from trade_scanner_fh.gui import main_window as mw_mod
    assert "nativeEvent" not in mw_mod.MainWindow.__dict__


def test_a_bar_group_wraps_as_one_unit(qapp):
    """The top bar / search row / ribbon keep each label with its control:
    'Timeframe:' used to wrap away from its dropdown."""
    from trade_scanner_fh.gui.flow_layout import WrappingBar
    bar = WrappingBar(toolbar_look=False)
    lbl, combo, other = QLabel("Timeframe:"), QPushButton("combo"), QPushButton("x")
    bar.group()
    bar.addWidget(lbl)
    bar.addWidget(combo)
    bar.addSeparator()                     # ends the group
    bar.addWidget(other)
    assert lbl.parentWidget() is combo.parentWidget() is not bar
    assert other.parentWidget() is bar
    assert bar._flow.count() == 3          # group, gap, other


def test_the_top_bar_keeps_every_control_reachable(window):
    """The old QToolBar hid Preset / Load / Save / Delete / Universe / IPO /
    Columns behind its overflow arrow on a narrow window."""
    from trade_scanner_fh.gui.flow_layout import WrappingBar
    assert isinstance(window._top_bar, WrappingBar)
    for w in (window.preset_combo, window.chk_include_etf,
              window.chk_ipo_mode, window.btn_columns, window.date_start):
        assert window._top_bar.isAncestorOf(w)
