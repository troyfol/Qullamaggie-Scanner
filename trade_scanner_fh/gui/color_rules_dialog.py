"""
Colour Rules dialog (v8.0.0) — the editor for gui/coloring.py rules.

Non-modal: Apply repaints the table immediately so a rule can be tuned while
watching it. Rules are saved with the preset (MainWindow owns persistence);
this window only edits a working copy and hands it back via `rules_applied`.

Round-tripping is the contract the tests pin: loading any rule into the
editor and reading it straight back returns an identical rule, so opening
the dialog and pressing OK never changes what a rule does.
"""

from __future__ import annotations

import logging

from PyQt6.QtCore import Qt, QTimer, pyqtSignal
from PyQt6.QtGui import QColor, QIcon, QPixmap
from PyQt6.QtWidgets import (
    QAbstractItemView, QCheckBox, QColorDialog, QComboBox, QDialog,
    QDialogButtonBox, QFormLayout, QFrame, QGroupBox, QHBoxLayout, QLabel,
    QLineEdit, QListWidget, QListWidgetItem, QPushButton, QScrollArea,
    QSpinBox, QSplitter, QVBoxLayout, QWidget,
)

from . import coloring as C
from .widgets import format_human_number, parse_human_number

log = logging.getLogger("scanner.gui")

# ── Column choices ────────────────────────────────────────────────────
#
# The dialog is handed `[(label, key, kind)]` by MainWindow. `kind` is
# "num", "date" or "text" and only steers which operators / pickers make
# sense; the engine itself reads every column the same way.

VALUE_OP_LABELS = {
    ">": ">", ">=": "≥", "<": "<", "<=": "≤", "==": "=", "!=": "≠",
    "between": "between", "not_between": "not between",
    "blank": "is blank", "not_blank": "is not blank",
    "top_pct": "in top % of period", "bottom_pct": "in bottom % of period",
    "contains": "text contains", "not_contains": "text does not contain",
}
FILTER_OP_LABELS = {"fails": "fails its filter", "passes": "passes its filter"}
DATE_OP_LABELS = {"within": "within ± N days of",
                  "before": "at least N days before",
                  "after": "at least N days after"}
QUARTER_OP_LABELS = {k: VALUE_OP_LABELS.get(k, k) for k in C.QUARTER_OPS}
QUARTER_OP_LABELS["in_run"] = "is counted in the run of"
KIND_LABELS = {"value": "Value", "filter": "Filter result", "date": "Date",
               "quarter": "Quarter number",
               "earnings_match": "Earnings date match"}
TARGET_LABELS = {"row": "The whole row",
                 "columns": "Chosen columns",
                 "matched": "The cells the conditions use"}
SCOPE_LABELS = {"row": "Each row",
                "quarter": "Each quarter (Q-1 … Q-N)"}
MATCH_LABELS = {"all": "All conditions are true",
                "any": "Any condition is true"}
MODE_LABELS = {"none": "None", "fixed": "Fixed color",
               "random": "Random (palette)"}


def _combo(pairs, width=None) -> QComboBox:
    cb = QComboBox()
    for value, label in pairs:
        cb.addItem(label, userData=value)
    if width:
        cb.setMinimumWidth(width)
    return cb


def _set(cb: QComboBox, value) -> None:
    """Select `value` by userData; an unknown value is added (so a rule that
    references a column this scan lacks still round-trips unchanged)."""
    for i in range(cb.count()):
        if cb.itemData(i) == value:
            cb.setCurrentIndex(i)
            return
    cb.addItem(f"{value}  (not in this scan)", userData=value)
    cb.setCurrentIndex(cb.count() - 1)


def _swatch(hex_color: str, size: int = 14) -> QIcon:
    pm = QPixmap(size, size)
    pm.fill(QColor(hex_color))
    return QIcon(pm)


def _num_text(v) -> str:
    """Show a number the way it is best typed, but never lossily: a big
    round figure reads as "2.50B", anything the suffix form would round
    stays exact."""
    if v is None:
        return ""
    if isinstance(v, str):
        return v
    f = float(v)
    if abs(f) >= 1e6:
        human = format_human_number(f)
        if parse_human_number(human) == f:
            return human
    return format(f, ".15g")


def _parse_num(text: str):
    """K/M/B/T suffixes, commas, or plain / scientific notation."""
    v = parse_human_number(text or "")
    if v is None:
        try:
            v = float((text or "").replace(",", "").strip())
        except ValueError:
            return None
    return v


# ======================================================================
# Colour chooser
# ======================================================================

class PaletteDialog(QDialog):
    """Edit a random-colour palette: add (any colour), remove, reset."""

    def __init__(self, palette, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Random color palette")
        self.setMinimumWidth(320)
        lay = QVBoxLayout(self)
        lay.addWidget(QLabel(
            "Each match draws a color from this list. The draw is stable "
            "per ticker, and distinct matches in one row get distinct "
            "colors while the list has enough entries."))
        self.list = QListWidget()
        lay.addWidget(self.list, 1)
        row = QHBoxLayout()
        for text, fn in (("Add color…", self._add), ("Remove", self._remove),
                         ("Reset to default", self._reset)):
            b = QPushButton(text)
            b.clicked.connect(fn)
            row.addWidget(b)
        lay.addLayout(row)
        box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok
                               | QDialogButtonBox.StandardButton.Cancel)
        box.accepted.connect(self.accept)
        box.rejected.connect(self.reject)
        lay.addWidget(box)
        self._fill(palette)

    def _fill(self, palette):
        self.list.clear()
        for c in palette:
            it = QListWidgetItem(_swatch(c), c)
            it.setData(Qt.ItemDataRole.UserRole, c)
            self.list.addItem(it)

    def palette(self) -> list:
        return [self.list.item(i).data(Qt.ItemDataRole.UserRole)
                for i in range(self.list.count())]

    def add_color(self, hex_color: str) -> None:
        pal = self.palette() + [hex_color]
        self._fill(pal)

    def _add(self):
        c = QColorDialog.getColor(QColor("#4a90d9"), self, "Add color")
        if c.isValid():
            self.add_color(c.name())

    def _remove(self):
        row = self.list.currentRow()
        if row >= 0 and self.list.count() > 1:
            self.list.takeItem(row)

    def _reset(self):
        self._fill(list(C.DEFAULT_PALETTE))


class ColorChooser(QWidget):
    """None / Fixed colour / Random-from-palette, for one style channel."""

    changed = pyqtSignal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self._color = "#ffffff"
        self._palette = list(C.DEFAULT_PALETTE)
        lay = QHBoxLayout(self)
        lay.setContentsMargins(0, 0, 0, 0)
        self.mode = _combo(MODE_LABELS.items(), 150)
        self.btn_color = QPushButton()
        self.btn_color.setFixedWidth(110)
        self.btn_palette = QPushButton("Palette…")
        lay.addWidget(self.mode)
        lay.addWidget(self.btn_color)
        lay.addWidget(self.btn_palette)
        lay.addStretch()
        self.mode.currentIndexChanged.connect(self._sync)
        self.btn_color.clicked.connect(self._pick)
        self.btn_palette.clicked.connect(self._edit_palette)
        self._sync()

    def _sync(self):
        m = self.mode.currentData()
        self.btn_color.setVisible(m == "fixed")
        self.btn_palette.setVisible(m == "random")
        self.btn_color.setIcon(_swatch(self._color))
        self.btn_color.setText(self._color)
        self.btn_palette.setIcon(_swatch(self._palette[0]) if self._palette
                                 else QIcon())
        self.changed.emit()

    def _pick(self):
        c = QColorDialog.getColor(QColor(self._color), self, "Choose color")
        if c.isValid():
            self.set_color(c.name())

    def set_color(self, hex_color: str):
        self._color = hex_color
        self._sync()

    def _edit_palette(self):
        dlg = PaletteDialog(self._palette, self)
        if dlg.exec() == QDialog.DialogCode.Accepted:
            self._palette = dlg.palette() or list(C.DEFAULT_PALETTE)
            self._sync()

    def load(self, spec: C.ColorSpec):
        self._color = spec.color
        self._palette = list(spec.palette)
        _set(self.mode, spec.mode)
        self._sync()

    def spec(self) -> C.ColorSpec:
        return C.ColorSpec(mode=self.mode.currentData(), color=self._color,
                           palette=list(self._palette))


# ======================================================================
# Condition editor
# ======================================================================

class ConditionEditor(QFrame):
    """One condition row. Shows only the inputs its kind uses."""

    changed = pyqtSignal()
    remove_requested = pyqtSignal(object)

    def __init__(self, columns, parent=None):
        super().__init__(parent)
        self.setFrameShape(QFrame.Shape.StyledPanel)
        self._columns = columns
        self._scope = "row"
        lay = QHBoxLayout(self)
        lay.setContentsMargins(4, 2, 4, 2)

        self.kind = _combo(KIND_LABELS.items(), 150)
        self.column = QComboBox()
        self.column.setMinimumWidth(190)
        self.column.setEditable(False)
        self.op = QComboBox()
        self.op.setMinimumWidth(150)
        self.rhs = _combo((("number", "a number"), ("column", "a column")),
                          100)
        self.value = QLineEdit()
        self.value.setPlaceholderText("value")
        self.value.setFixedWidth(90)
        self.value2 = QLineEdit()
        self.value2.setPlaceholderText("and")
        self.value2.setFixedWidth(90)
        self.other = QComboBox()
        self.other.setMinimumWidth(190)
        self.days = QSpinBox()
        self.days.setRange(0, 3650)
        self.days.setSuffix(" d")
        self.info = QLabel("Uses the scan's own earnings-date matching "
                           "(Settings → match tolerance).")
        self.info.setStyleSheet("color: #999;")
        self.btn_remove = QPushButton("✕")
        self.btn_remove.setFixedWidth(28)
        self.btn_remove.setToolTip("Remove this condition")
        for w in (self.kind, self.column, self.op, self.rhs, self.value,
                  self.value2, self.other, self.days, self.info):
            lay.addWidget(w)
        lay.addStretch()
        lay.addWidget(self.btn_remove)

        self.kind.currentIndexChanged.connect(self._kind_changed)
        self.op.currentIndexChanged.connect(self._op_changed)
        self.rhs.currentIndexChanged.connect(self._layout)
        for w in (self.column, self.op, self.rhs, self.other):
            w.currentIndexChanged.connect(self.changed.emit)
        for w in (self.value, self.value2):
            w.textChanged.connect(self.changed.emit)
        self.days.valueChanged.connect(self.changed.emit)
        self.btn_remove.clicked.connect(
            lambda: self.remove_requested.emit(self))
        self._kind_changed()

    # -- column lists ---------------------------------------------------

    def set_scope(self, scope: str):
        """Quarter scope offers the Q-X templates; row scope does not."""
        if scope == self._scope:
            return
        cond = self.condition()
        self._scope = scope
        self._fill_columns()
        self._restore_columns(cond)

    def _in_run(self) -> bool:
        return (self.kind.currentData() == "quarter"
                and self.op.currentData() == "in_run")

    def _choices(self, kind: str, which: str) -> list:
        quarter = self._scope == "quarter"
        if kind == "quarter" and which == "right" and self._in_run():
            # "is counted in the run of" picks a series FILTER, not a column.
            return [(label, prefix) for prefix, label in C.RUN_SOURCES]
        out = []
        if kind == "filter" and which == "left":
            out.append(("Any display-only filter", C.ANY_FILTER))
        if kind == "date" and which == "right":
            out.append(("Any earnings report date", C.ANY_REPORT_DATE))
        for label, key, ckind in self._columns:
            is_template = "{k}" in key
            if is_template and not quarter:
                continue
            if kind == "date" and ckind != "date":
                continue
            if kind == "value" and which == "right" and ckind != "num":
                continue
            if kind == "quarter" and ckind != "num":
                continue
            out.append((label, key))
        return out

    def _fill_columns(self):
        k = self.kind.currentData()
        for cb, which in ((self.column, "left"), (self.other, "right")):
            cb.blockSignals(True)
            cb.clear()
            for label, key in self._choices(k, which):
                cb.addItem(label, userData=key)
            cb.blockSignals(False)

    def _restore_columns(self, cond: C.Condition):
        if cond.column:
            _set(self.column, cond.column)
        if cond.other:
            _set(self.other, cond.other)

    # -- layout ---------------------------------------------------------

    def _op_changed(self):
        """Switching a quarter test into or out of "in the run of" swaps the
        right-hand list between series filters and columns. The current pick
        survives when the new list has it (consec_eps_beats is both a column
        and a series filter); otherwise the list's first entry is taken."""
        if self.kind.currentData() == "quarter":
            keep = self.other.currentData()
            self._fill_columns()
            if keep is not None:
                for i in range(self.other.count()):
                    if self.other.itemData(i) == keep:
                        self.other.setCurrentIndex(i)
                        break
        self._layout()

    def _kind_changed(self):
        k = self.kind.currentData()
        ops = {"value": VALUE_OP_LABELS, "filter": FILTER_OP_LABELS,
               "date": DATE_OP_LABELS, "quarter": QUARTER_OP_LABELS,
               "earnings_match": {"match": "matches"}}[k]
        self.op.blockSignals(True)
        self.op.clear()
        for value, label in ops.items():
            self.op.addItem(label, userData=value)
        self.op.blockSignals(False)
        self._fill_columns()
        self._layout()

    _CMP_OPS = (">", ">=", "<", "<=", "==", "!=")

    def _flags(self):
        """What this kind + operator uses. Decided from DATA, never from
        widget visibility: a widget in a window not yet shown reports
        isVisible() False, which would make a rule read back before display
        silently lose its right-hand side."""
        k, op = self.kind.currentData(), self.op.currentData()
        shows_rhs = ((k == "value" and op in self._CMP_OPS)
                     or (k == "quarter" and op != "in_run"))
        use_col = shows_rhs and self.rhs.currentData() == "column"
        return k, op, shows_rhs, use_col

    def _layout(self):
        k, op, shows_rhs, use_col = self._flags()
        self.column.setVisible(k in ("value", "filter", "date"))
        self.op.setVisible(k != "earnings_match")
        self.rhs.setVisible(shows_rhs)
        self.value.setVisible(
            (k == "value" and op not in ("blank", "not_blank") and not use_col)
            or (k == "quarter" and op != "in_run" and not use_col))
        self.value.setPlaceholderText(
            "%" if op in ("top_pct", "bottom_pct")
            else "text" if op in ("contains", "not_contains") else "value")
        self.value2.setVisible(k == "value" and op in ("between",
                                                        "not_between"))
        self.other.setVisible((k == "date") or use_col
                              or (k == "quarter" and op == "in_run"))
        self.days.setVisible(k == "date")
        self.info.setVisible(k == "earnings_match")
        self.changed.emit()

    # -- load / read ----------------------------------------------------

    def load(self, cond: C.Condition):
        _set(self.kind, cond.kind)
        self._kind_changed()
        _set(self.op, cond.op)
        self._fill_columns()        # the op decides the quarter test's list
        if cond.kind in ("value", "quarter") and cond.op != "in_run":
            _set(self.rhs, "column" if cond.other else "number")
        self._restore_columns(cond)
        self.value.setText(_num_text(cond.value))
        self.value2.setText(_num_text(cond.value2))
        self.days.setValue(int(cond.days or 0))
        self._layout()

    def condition(self) -> C.Condition:
        k, op, _shows_rhs, use_col = self._flags()
        c = C.Condition(kind=k, op=op)
        if k == "earnings_match":
            c.op = "match"
            return c
        if k in ("value", "filter", "date"):
            c.column = self.column.currentData() or ""
        if k == "date":
            c.other = self.other.currentData() or ""
            c.days = int(self.days.value())
            return c
        if k == "filter":
            return c
        if k == "quarter" and op == "in_run":
            c.other = self.other.currentData() or ""
            return c
        if use_col:
            c.other = self.other.currentData() or ""
            return c
        if op in ("contains", "not_contains"):
            c.value = self.value.text()
        elif op not in ("blank", "not_blank"):
            c.value = _parse_num(self.value.text())
        if op in ("between", "not_between"):
            c.value2 = _parse_num(self.value2.text())
        return c


# ======================================================================
# Rule editor
# ======================================================================

class RuleEditor(QWidget):
    changed = pyqtSignal()

    def __init__(self, columns, target_columns, parent=None):
        super().__init__(parent)
        self._columns = columns
        self._target_columns = target_columns
        self._rule_id = ""
        self._loading = False
        outer = QVBoxLayout(self)

        form = QFormLayout()
        self.name = QLineEdit()
        self.scope = _combo(SCOPE_LABELS.items())
        self.match = _combo(MATCH_LABELS.items())
        form.addRow("Name", self.name)
        form.addRow("Test", self.scope)
        form.addRow("Color when", self.match)
        outer.addLayout(form)

        cond_box = QGroupBox("Conditions")
        self.cond_lay = QVBoxLayout(cond_box)
        self.btn_add_cond = QPushButton("+ Add condition")
        self.cond_lay.addWidget(self.btn_add_cond)
        outer.addWidget(cond_box)
        self.conditions: list = []

        tgt_box = QGroupBox("Color")
        tl = QVBoxLayout(tgt_box)
        self.target = _combo(TARGET_LABELS.items())
        tl.addWidget(self.target)
        self.target_filter = QLineEdit()
        self.target_filter.setPlaceholderText("Filter columns…")
        self.target_list = QListWidget()
        self.target_list.setSelectionMode(
            QAbstractItemView.SelectionMode.NoSelection)
        self.target_list.setMinimumHeight(140)
        for label, key in target_columns:
            it = QListWidgetItem(label)
            it.setData(Qt.ItemDataRole.UserRole, key)
            it.setFlags(it.flags() | Qt.ItemFlag.ItemIsUserCheckable)
            it.setCheckState(Qt.CheckState.Unchecked)
            self.target_list.addItem(it)
        tl.addWidget(self.target_filter)
        tl.addWidget(self.target_list)
        self.expand = QCheckBox(
            "Also color each matched date's linked value cells")
        tl.addWidget(self.expand)
        self.skip_blank = QCheckBox("Leave N/A cells uncolored")
        self.skip_blank.setToolTip(
            "A cell with no value is not colored even when the rule "
            "matches its row or quarter.")
        tl.addWidget(self.skip_blank)
        outer.addWidget(tgt_box)

        style_box = QGroupBox("Style")
        sl = QFormLayout(style_box)
        self.text = ColorChooser()
        self.background = ColorChooser()
        self.bold = QCheckBox("Bold")
        sl.addRow("Text color", self.text)
        sl.addRow("Background", self.background)
        sl.addRow("", self.bold)
        outer.addWidget(style_box)
        outer.addStretch()

        self.btn_add_cond.clicked.connect(lambda: self.add_condition())
        self.scope.currentIndexChanged.connect(self._scope_changed)
        self.target.currentIndexChanged.connect(self._target_layout)
        self.target_filter.textChanged.connect(self._filter_targets)
        for sig in (self.name.textChanged, self.match.currentIndexChanged,
                    self.target.currentIndexChanged,
                    self.target_list.itemChanged, self.expand.toggled,
                    self.skip_blank.toggled,
                    self.text.changed, self.background.changed,
                    self.bold.toggled):
            sig.connect(self._emit)
        self._target_layout()

    def _emit(self, *_a):
        if not self._loading:
            self.changed.emit()

    def add_condition(self, cond: C.Condition = None) -> ConditionEditor:
        ed = ConditionEditor(self._columns, self)
        ed.set_scope(self.scope.currentData())
        ed.load(cond or C.Condition(kind="value", op=">="))
        ed.changed.connect(self._emit)
        ed.remove_requested.connect(self._remove_condition)
        self.cond_lay.insertWidget(self.cond_lay.count() - 1, ed)
        self.conditions.append(ed)
        self._target_layout()
        self._emit()
        return ed

    def _remove_condition(self, ed):
        if ed in self.conditions:
            self.conditions.remove(ed)
            ed.setParent(None)
            ed.deleteLater()
            self._target_layout()
            self._emit()

    def _scope_changed(self):
        for ed in self.conditions:
            ed.set_scope(self.scope.currentData())
        self._emit()

    def _target_layout(self):
        chosen = self.target.currentData() == "columns"
        self.target_filter.setVisible(chosen)
        self.target_list.setVisible(chosen)
        has_date = any(ed.kind.currentData() == "date"
                       for ed in self.conditions)
        self.expand.setVisible(self.target.currentData() == "matched"
                               and has_date)

    def _filter_targets(self, text):
        t = (text or "").lower()
        for i in range(self.target_list.count()):
            it = self.target_list.item(i)
            it.setHidden(bool(t) and t not in it.text().lower())

    def load(self, rule: C.Rule):
        self._loading = True
        try:
            self._rule_id = rule.id
            self._enabled = rule.enabled
            self.name.setText(rule.name)
            _set(self.scope, rule.scope)
            _set(self.match, rule.match)
            for ed in list(self.conditions):
                self.conditions.remove(ed)
                ed.setParent(None)
                ed.deleteLater()
            for cond in rule.conditions:
                self.add_condition(cond)
            _set(self.target, rule.target)
            self._target_order = list(rule.target_columns)
            wanted = set(rule.target_columns)
            present = set()
            for i in range(self.target_list.count()):
                it = self.target_list.item(i)
                key = it.data(Qt.ItemDataRole.UserRole)
                it.setCheckState(Qt.CheckState.Checked if key in wanted
                                 else Qt.CheckState.Unchecked)
                present.add(key)
            for key in rule.target_columns:
                if key not in present:     # keep what this scan cannot show
                    it = QListWidgetItem(f"{key}  (not in this scan)")
                    it.setData(Qt.ItemDataRole.UserRole, key)
                    it.setFlags(it.flags() | Qt.ItemFlag.ItemIsUserCheckable)
                    it.setCheckState(Qt.CheckState.Checked)
                    self.target_list.addItem(it)
            self.expand.setChecked(rule.expand_units)
            self.skip_blank.setChecked(rule.skip_blank)
            self.text.load(rule.style.text)
            self.background.load(rule.style.background)
            self.bold.setChecked(rule.style.bold)
            self._target_layout()
        finally:
            self._loading = False

    def rule(self) -> C.Rule:
        checked = []
        for i in range(self.target_list.count()):
            it = self.target_list.item(i)
            if it.checkState() == Qt.CheckState.Checked:
                checked.append(it.data(Qt.ItemDataRole.UserRole))
        # Keep the rule's own order for the columns it already had, then
        # add newly ticked ones in list order — so loading a rule and
        # reading it straight back is an exact round trip.
        prior = [k for k in getattr(self, "_target_order", []) if k in checked]
        cols = prior + [k for k in checked if k not in set(prior)]
        return C.Rule(
            id=self._rule_id, name=self.name.text().strip() or "Rule",
            enabled=getattr(self, "_enabled", True),
            scope=self.scope.currentData(), match=self.match.currentData(),
            conditions=[ed.condition() for ed in self.conditions],
            target=self.target.currentData(), target_columns=cols,
            expand_units=self.expand.isChecked(),
            skip_blank=self.skip_blank.isChecked(),
            style=C.Style(text=self.text.spec(),
                          background=self.background.spec(),
                          bold=self.bold.isChecked()),
        )


# ======================================================================
# Dialog
# ======================================================================

class ColorRulesDialog(QDialog):
    """Edit the ranked rule list. Top of the list = strongest rule."""

    rules_applied = pyqtSignal(list)

    def __init__(self, rules, columns, target_columns, preview_fn=None,
                 parent=None):
        super().__init__(parent)
        self.setWindowTitle("Color Rules")
        self.setModal(False)
        self.resize(1100, 720)
        self._columns = columns
        self._target_columns = target_columns
        self._preview_fn = preview_fn
        self._rules: list = [r.copy() for r in rules]
        self._current = -1

        outer = QVBoxLayout(self)
        blurb = QLabel(
            "Rules higher in the list win. Each style — text color, "
            "background, bold — is decided separately, so a lower rule can "
            "still set a background where a higher one only sets text. Rules "
            "are saved with the preset.")
        blurb.setWordWrap(True)
        blurb.setStyleSheet("color: #aaa;")
        outer.addWidget(blurb)

        split = QSplitter(Qt.Orientation.Horizontal)
        left = QWidget()
        ll = QVBoxLayout(left)
        self.list = QListWidget()
        ll.addWidget(self.list, 1)
        btns = QHBoxLayout()
        self.btn_add = QPushButton("Add")
        self.btn_dup = QPushButton("Duplicate")
        self.btn_del = QPushButton("Delete")
        self.btn_up = QPushButton("▲")
        self.btn_down = QPushButton("▼")
        for b in (self.btn_add, self.btn_dup, self.btn_del, self.btn_up,
                  self.btn_down):
            btns.addWidget(b)
        ll.addLayout(btns)
        self.btn_defaults = QPushButton("Restore default rules")
        ll.addWidget(self.btn_defaults)
        split.addWidget(left)

        self.editor = RuleEditor(columns, target_columns)
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setWidget(self.editor)
        split.addWidget(scroll)
        split.setSizes([330, 770])
        outer.addWidget(split, 1)

        self.status = QLabel("")
        self.status.setStyleSheet("color: #bbb;")
        outer.addWidget(self.status)
        box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok
                               | QDialogButtonBox.StandardButton.Apply
                               | QDialogButtonBox.StandardButton.Cancel)
        self.btn_apply = box.button(QDialogButtonBox.StandardButton.Apply)
        box.accepted.connect(self._ok)
        box.rejected.connect(self.reject)
        self.btn_apply.clicked.connect(self.apply)
        outer.addWidget(box)

        self._debounce = QTimer(self)
        self._debounce.setSingleShot(True)
        self._debounce.setInterval(250)
        self._debounce.timeout.connect(self._refresh_status)

        self.btn_add.clicked.connect(self.add_rule)
        self.btn_dup.clicked.connect(self.duplicate_rule)
        self.btn_del.clicked.connect(self.delete_rule)
        self.btn_up.clicked.connect(lambda: self.move_rule(-1))
        self.btn_down.clicked.connect(lambda: self.move_rule(1))
        self.btn_defaults.clicked.connect(self.restore_defaults)
        self.list.currentRowChanged.connect(self._select)
        self.list.itemChanged.connect(self._item_toggled)
        self.editor.changed.connect(self._editor_changed)
        self._rebuild_list(0)

    # -- list -----------------------------------------------------------

    def _commit_editor(self):
        if 0 <= self._current < len(self._rules):
            edited = self.editor.rule()
            edited.enabled = self._rules[self._current].enabled
            self._rules[self._current] = edited

    @staticmethod
    def _swatch_for(r: C.Rule) -> str:
        """The list icon: background if the rule sets one (it is what the
        eye sees first), else its text colour, else a neutral grey."""
        for spec in (r.style.background, r.style.text):
            if spec.mode == "fixed":
                return spec.color
            if spec.mode == "random" and spec.palette:
                return spec.palette[0]
        return "#555555"

    def _rebuild_list(self, select: int):
        self.list.blockSignals(True)
        self.list.clear()
        for r in self._rules:
            it = QListWidgetItem(r.name)
            it.setFlags(it.flags() | Qt.ItemFlag.ItemIsUserCheckable)
            it.setCheckState(Qt.CheckState.Checked if r.enabled
                             else Qt.CheckState.Unchecked)
            it.setIcon(_swatch(self._swatch_for(r)))
            self.list.addItem(it)
        self.list.blockSignals(False)
        self._current = -1
        if self._rules:
            select = max(0, min(select, len(self._rules) - 1))
            self.list.setCurrentRow(select)
            self._select(select)
        else:
            self.editor.setEnabled(False)
        self._schedule_status()

    def _select(self, row: int):
        if row == self._current:
            return
        self._commit_editor()
        self._current = row
        ok = 0 <= row < len(self._rules)
        self.editor.setEnabled(ok)
        if ok:
            self.editor.load(self._rules[row])

    def _item_toggled(self, item):
        row = self.list.row(item)
        if 0 <= row < len(self._rules):
            self._rules[row].enabled = (item.checkState()
                                        == Qt.CheckState.Checked)
            self._schedule_status()

    def _editor_changed(self):
        self._commit_editor()
        it = self.list.item(self._current)
        if it is not None:
            self.list.blockSignals(True)
            rule = self._rules[self._current]
            it.setText(rule.name)
            it.setIcon(_swatch(self._swatch_for(rule)))
            self.list.blockSignals(False)
        self._schedule_status()

    def add_rule(self):
        self._commit_editor()
        rule = C.Rule(name="New rule",
                      conditions=[C.Condition(kind="value", op=">=")],
                      style=C.Style(text=C.ColorSpec("fixed", "#ffd54f")))
        self._rules.insert(max(self._current, 0), rule)
        self._rebuild_list(max(self._current, 0))

    def duplicate_rule(self):
        if not (0 <= self._current < len(self._rules)):
            return
        self._commit_editor()
        dup = self._rules[self._current].copy()
        dup.id = C.Rule().id
        dup.name = f"{dup.name} (copy)"
        self._rules.insert(self._current + 1, dup)
        self._rebuild_list(self._current + 1)

    def delete_rule(self):
        if not (0 <= self._current < len(self._rules)):
            return
        row = self._current
        del self._rules[row]
        self._current = -1
        self._rebuild_list(row)

    def move_rule(self, delta: int):
        row = self._current
        new = row + delta
        if not (0 <= row < len(self._rules)) or not (0 <= new < len(self._rules)):
            return
        self._commit_editor()
        self._rules[row], self._rules[new] = self._rules[new], self._rules[row]
        self._current = -1
        self._rebuild_list(new)

    def restore_defaults(self):
        self._rules = C.default_rules()
        self._current = -1
        self._rebuild_list(0)

    # -- status / apply -------------------------------------------------

    def _schedule_status(self):
        self._debounce.start()

    def _refresh_status(self):
        if self._preview_fn is None:
            return
        try:
            report = self._preview_fn(self.rules())
        except Exception as exc:
            self.status.setText(f"Preview unavailable: {exc}")
            return
        for i, r in enumerate(self._rules):
            it = self.list.item(i)
            if it is None:
                continue
            info = report.get(r.id, {})
            if info.get("inactive"):
                note = "off" if not r.enabled else "incomplete"
            elif info.get("error"):
                note = "error"
            elif info.get("missing"):
                note = "needs " + ", ".join(info["missing"][:2])
            else:
                note = f"{info.get('rows', 0)} rows"
            self.list.blockSignals(True)
            it.setText(f"{r.name}   ·   {note}")
            self.list.blockSignals(False)
        cur = (report.get(self._rules[self._current].id, {})
               if 0 <= self._current < len(self._rules) else {})
        if cur.get("missing"):
            self.status.setText(
                "This rule reads columns this scan does not produce: "
                + ", ".join(cur["missing"]) + ". Turn those rows on (Filter "
                "or Display Only) and re-scan; it stays saved either way.")
        elif cur.get("error"):
            self.status.setText(f"This rule failed: {cur['error']}")
        elif cur:
            self.status.setText(
                f"This rule matches {cur.get('rows', 0)} row(s) in the "
                f"period on screen.")
        else:
            self.status.setText("")

    def rules(self) -> list:
        self._commit_editor()
        return [r.copy() for r in self._rules]

    def set_rules(self, rules):
        """Replace the working copy (e.g. a preset was loaded meanwhile)."""
        self._rules = [r.copy() for r in rules]
        self._current = -1
        self._rebuild_list(0)

    def apply(self):
        self.rules_applied.emit(self.rules())
        self._refresh_status()

    def _ok(self):
        self.apply()
        self.accept()
