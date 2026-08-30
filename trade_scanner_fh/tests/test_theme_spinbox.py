"""The spin-box arrow assets and the stylesheet that points at them.

Styling QSpinBox's border in DARK_STYLESHEET switches Qt to full stylesheet
rendering, which collapses the up/down sub-controls into two ~5 px stubs side
by side with no arrow glyph — the Deep OHLCV Refresh dialog's "Market days
back" field was unusable because of it. These lock in the geometry rules and
the generated arrow images, and that a failure to write them degrades to the
geometry-only fallback rather than to nothing.
"""
import pytest

from trade_scanner_fh.gui import theme


def test_fallback_still_stacks_the_buttons():
    css = theme._SPIN_FALLBACK
    assert "subcontrol-position: top right" in css
    assert "subcontrol-position: bottom right" in css
    # Geometry only: a background on the button suppresses Qt's own arrow.
    assert "background" not in css


def test_no_qapplication_falls_back_instead_of_aborting():
    """QPixmap without a QGuiApplication is a fatal Qt error, not a Python
    exception, so the try/except below it cannot save the process."""
    from PyQt6.QtGui import QGuiApplication
    if QGuiApplication.instance() is not None:
        pytest.skip("a QApplication already exists in this process")
    assert theme.spinbox_arrow_css() == theme._SPIN_FALLBACK


def test_unwritable_asset_dir_falls_back_instead_of_raising(_qapp, tmp_path):
    blocker = tmp_path / "blocked"
    blocker.write_text("not a directory", encoding="utf-8")
    assert theme.spinbox_arrow_css(blocker) == theme._SPIN_FALLBACK


def test_build_stylesheet_keeps_the_base_sheet(_qapp, tmp_path):
    css = theme.build_stylesheet(tmp_path / "assets")
    assert css.startswith(theme.DARK_STYLESHEET)
    assert "QSpinBox::up-button" in css


@pytest.mark.parametrize("name", ["spin_up.png", "spin_down.png"])
def test_arrow_assets_are_written_and_referenced(_qapp, tmp_path, name):
    css = theme.spinbox_arrow_css(tmp_path)
    written = tmp_path / name
    assert written.exists() and written.stat().st_size > 0
    # Qt's url() wants forward slashes even on Windows.
    assert str(written).replace("\\", "/") in css
    assert "\\" not in css.split("image: url(")[1].split(")")[0]


def test_arrow_is_the_expected_size(_qapp, tmp_path):
    from PyQt6.QtGui import QPixmap
    theme.spinbox_arrow_css(tmp_path)
    pm = QPixmap(str(tmp_path / "spin_up.png"))
    assert (pm.width(), pm.height()) == (theme._ARROW_W, theme._ARROW_H)


def test_up_and_down_arrows_differ(_qapp, tmp_path):
    theme.spinbox_arrow_css(tmp_path)
    up = (tmp_path / "spin_up.png").read_bytes()
    down = (tmp_path / "spin_down.png").read_bytes()
    assert up != down


def test_hover_and_pressed_states_are_styled(_qapp, tmp_path):
    css = theme.spinbox_arrow_css(tmp_path)
    assert "up-button:hover" in css
    assert "down-button:pressed" in css
    assert "up-button:disabled" in css
