"""
FlowLayout (v8.0.1) — a row of widgets that wraps onto further lines when
it runs out of width, instead of forcing its parent to be as wide as the
whole row.

Why it exists: every toolbar row and every filter row was a single-line
QHBoxLayout, and a window can never be narrower than its widest such row.
Measured 2026-10-01 the main window could not go below 2,116 logical px
(bottom button row 1,846, the search / view row inside the results splitter
1,520 plus the filter panel's 580) while the user's monitors are 2,560,
1,536, 1,440 and 1,080 logical px wide. Dragging the window onto any of the
narrower ones left Windows proposing a size Qt refused as below the minimum
— the window jumped, or hung across two screens. The filter panel's rows
needed 1,963 px, so the panel always carried a horizontal scrollbar.

With a FlowLayout the minimum width is the widest SINGLE item, and the
height grows (height-for-width) as items wrap. At full width it lays out on
one line exactly like the QHBoxLayout it replaces. It accepts the same
calls the rows used: addWidget, addSpacing (a fixed gap that never starts a
line) and addStretch (ignored — a flow has no spare width to stretch into).
"""

from __future__ import annotations

from PyQt6.QtCore import QPoint, QRect, QSize, Qt
from PyQt6.QtWidgets import (
    QHBoxLayout, QLayout, QSizePolicy, QSpacerItem, QWidget,
)


class FlowLayout(QLayout):
    def __init__(self, parent: QWidget | None = None, *, margin: int = 0,
                 h_spacing: int = 6, v_spacing: int = 4):
        super().__init__(parent)
        self._items: list = []
        self._h = h_spacing
        self._v = v_spacing
        self.setContentsMargins(margin, margin, margin, margin)

    # -- QHBoxLayout-compatible helpers -----------------------------------

    def addSpacing(self, width: int) -> None:
        self.addItem(QSpacerItem(int(width), 0, QSizePolicy.Policy.Fixed,
                                 QSizePolicy.Policy.Minimum))

    def addStretch(self, stretch: int = 0) -> None:
        """No-op: kept so call sites written for QHBoxLayout still work."""

    # -- QLayout interface --------------------------------------------------

    def addItem(self, item) -> None:
        self._items.append(item)

    def count(self) -> int:
        return len(self._items)

    def itemAt(self, index: int):
        return self._items[index] if 0 <= index < len(self._items) else None

    def takeAt(self, index: int):
        return self._items.pop(index) if 0 <= index < len(self._items) else None

    def expandingDirections(self):
        return Qt.Orientation(0)

    def hasHeightForWidth(self) -> bool:
        return True

    def heightForWidth(self, width: int) -> int:
        return self._do_layout(QRect(0, 0, width, 0), apply=False)

    def setGeometry(self, rect: QRect) -> None:
        super().setGeometry(rect)
        self._do_layout(rect, apply=True)

    def sizeHint(self) -> QSize:
        """Everything on one line — what the row asks for when there is room."""
        w = h = 0
        first = True
        for item in self._visible():
            hint = item.sizeHint()
            w += hint.width() + (0 if first else self._h)
            h = max(h, hint.height())
            first = False
        m = self.contentsMargins()
        return QSize(w + m.left() + m.right(), h + m.top() + m.bottom())

    def minimumSize(self) -> QSize:
        """The widest single item: a flow can always wrap down to that."""
        size = QSize()
        for item in self._visible():
            if item.spacerItem() is None:
                size = size.expandedTo(item.minimumSize())
        m = self.contentsMargins()
        return size + QSize(m.left() + m.right(), m.top() + m.bottom())

    # -- layout -------------------------------------------------------------

    def _visible(self):
        """Items that take space: shown widgets and fixed gaps."""
        for item in self._items:
            if item.spacerItem() is not None or not item.isEmpty():
                yield item

    @staticmethod
    def _width_of(item) -> int:
        """Preferred width, never below the item's minimum."""
        return max(item.sizeHint().width(), item.minimumSize().width())

    def _do_layout(self, rect: QRect, *, apply: bool) -> int:
        """Place items left to right, wrapping when the next one would not
        fit; each line's items are centred vertically on the line. Returns
        the total height used. A gap never begins a line."""
        m = self.contentsMargins()
        area = rect.adjusted(m.left(), m.top(), -m.right(), -m.bottom())
        lines: list = []
        line: list = []
        x = 0
        for item in self._visible():
            w = self._width_of(item)
            is_gap = item.spacerItem() is not None
            if is_gap and not line:
                continue
            if line and x + w > area.width():
                lines.append(line)
                line, x = [], 0
                if is_gap:
                    continue
            line.append((item, w))
            x += w + self._h
        if line:
            lines.append(line)

        y = area.y()
        for n, ln in enumerate(lines):
            # A gap left dangling at the end of a line takes no room.
            while ln and ln[-1][0].spacerItem() is not None:
                ln = ln[:-1]
            if not ln:
                continue
            height = max(it.sizeHint().height() for it, _w in ln)
            if apply:
                cx = area.x()
                for it, w in ln:
                    h = min(it.sizeHint().height(), height)
                    if it.spacerItem() is None:
                        it.setGeometry(QRect(QPoint(cx, y + (height - h) // 2),
                                             QSize(w, h)))
                    cx += w + self._h
            y += height + (self._v if n < len(lines) - 1 else 0)
        return y - rect.y() + m.bottom()


class WrappingBar(QWidget):
    """The top control bar (v8.0.1) — replaces the QToolBar that held these
    controls. A QToolBar never wraps: on a narrow window it squeezed the
    date boxes to "2026-09-" and hid Preset, Load / Save / Delete, Universe,
    IPO Mode and Columns behind its overflow arrow, which on the user's
    1,080 px portrait monitor meant digging through » to load a preset.
    Here the controls wrap onto further lines instead. Keeps the two calls
    the build code used (addWidget / addSeparator) and adds group(), so a
    label and its control wrap together."""

    def __init__(self, *, toolbar_look: bool = True):
        super().__init__()
        if toolbar_look:
            self.setObjectName("mainTopBar")
            # Same look as the theme's QToolBar rule (background #333,
            # padding 4).
            self.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, True)
            self.setStyleSheet("#mainTopBar { background: #333; }")
        self._flow = FlowLayout(self, margin=4 if toolbar_look else 0,
                                h_spacing=4 if toolbar_look else 6,
                                v_spacing=4)
        self._group = None

    def addWidget(self, widget):
        if self._group is not None:
            self._group.addWidget(widget)
        else:
            self._flow.addWidget(widget)
        return widget

    def addSeparator(self):
        self.addSpacing(12)

    def addSpacing(self, width: int):
        self.end_group()
        self._flow.addSpacing(width)

    def addStretch(self, *_a):
        self.end_group()

    def group(self):
        """Start a unit that wraps as one; ends at end_group / addSeparator."""
        self.end_group()
        box = QWidget()
        lay = QHBoxLayout(box)
        lay.setContentsMargins(0, 0, 0, 0)
        lay.setSpacing(4)
        self._flow.addWidget(box)
        self._group = lay

    def end_group(self):
        self._group = None
