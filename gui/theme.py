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
