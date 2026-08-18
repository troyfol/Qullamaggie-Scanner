"""Tests for data_engine.is_share_split — the structural filter that keeps
spinoffs / special distributions out of the split-anchor machinery.

Cases are the ones measured against the live cache (2,420 reverse events over
the 5-year window): 69 events fail the whole-number test, and every one is a
distribution rather than a share split.
"""
import pytest

from trade_scanner_fh.data_engine import is_share_split


# ----------------------------------------------------------------------
# Real reverse splits — 1-for-N, so 1/ratio is a whole number
# ----------------------------------------------------------------------

@pytest.mark.parametrize("ratio,label", [
    (0.05,        "1-for-20"),
    (0.04,        "1-for-25"),
    (0.0625,      "1-for-16"),
    (0.1,         "1-for-10"),
    (0.2,         "1-for-5"),
    (0.5,         "1-for-2"),
    (0.0769231,   "1-for-13   (LOCL, float-noisy)"),
    (0.00298507,  "1-for-335  (ACON)"),
    (0.007143,    "1-for-140  (BESS)"),
    (0.000001,    "1-for-1e6  (SONG, extreme but whole)"),
])
def test_real_reverse_splits_accepted(ratio, label):
    assert is_share_split(ratio) is True, label


# ----------------------------------------------------------------------
# Real forward splits — ratio itself is a whole number
# ----------------------------------------------------------------------

@pytest.mark.parametrize("ratio,label", [
    (2.0,   "2-for-1"),
    (3.0,   "3-for-1"),
    (4.0,   "4-for-1"),
    (10.0,  "10-for-1"),
    (20.0,  "20-for-1"),
])
def test_real_forward_splits_accepted(ratio, label):
    assert is_share_split(ratio) is True, label


# ----------------------------------------------------------------------
# Spinoffs / special distributions — the 69 measured rejections
# ----------------------------------------------------------------------

@pytest.mark.parametrize("ratio,label", [
    (0.9760, "OUT   1/r=1.025, share count flat across the date"),
    (0.9700, "SLG   1/r=1.031, share count flat across the date"),
    (0.9630, "TRI   1/r=1.038"),
    (0.9590, "RBGLY 1/r=1.043"),
    (0.9535, "HON   1/r=1.049, Solstice separation"),
    (0.9280, "NWG   1/r=1.078"),
    (0.9090, "LFDR  1/r=1.100"),
    (0.9070, "IRS   1/r=1.103"),
    (0.9000, "CD    1/r=1.111"),
    (0.8880, "UL    1/r=1.126, Unilever demerger"),
    (0.8358, "FSEA  1/r=1.196"),
    (0.8330, "LIAU  1/r=1.200"),
    (0.8000, "GSK   1/r=1.250, Haleon demerger"),
    (0.7690, "LIAE  1/r=1.300"),
])
def test_distributions_rejected(ratio, label):
    assert is_share_split(ratio) is False, label


def test_magnitude_threshold_would_not_substitute():
    """Only 11 of the 69 measured rejections have ratio >= 0.9, so a size
    cut-off misses most of them. GSK at 0.80 is well clear of any sane
    'near 1.0' band yet is still not a share split."""
    assert is_share_split(0.80) is False      # GSK / Haleon
    assert is_share_split(0.769) is False     # LIAE
    assert is_share_split(0.05) is True       # a real 1-for-20 at similar scale


# ----------------------------------------------------------------------
# Degenerate input
# ----------------------------------------------------------------------

@pytest.mark.parametrize("value", [
    0.0, -1.0, -0.05, 1.0, None, "", "abc", float("nan"),
    float("inf"), float("-inf"),
])
def test_degenerate_values_rejected(value):
    assert is_share_split(value) is False


def test_no_split_sentinel_is_rejected():
    """The cache stores 0.0 on every ordinary bar; that must never read as an
    event, or every bar in the file becomes an anchor."""
    assert is_share_split(0.0) is False


# ----------------------------------------------------------------------
# Documented limitation — conservative, and deliberately so
# ----------------------------------------------------------------------

@pytest.mark.parametrize("ratio,label", [
    (1.5,      "3-for-2 forward — real split, rejected"),
    (0.666667, "2-for-3 reverse — ALM's case, rejected"),
])
def test_n_for_m_splits_are_rejected_conservatively(ratio, label):
    """An N-for-M split with M > 1 fails the whole-number test. Rejection only
    means "not used as an anchor", which is the safe direction, so this is
    accepted behaviour rather than a bug. Pinned here so a future change to the
    predicate is a deliberate one."""
    assert is_share_split(ratio) is False, label


def test_tolerance_admits_float_noise_but_not_real_gaps():
    assert is_share_split(1.0 / 20.0000001) is True    # noise around 1-for-20
    assert is_share_split(1.0 / 20.5) is False         # genuinely between
