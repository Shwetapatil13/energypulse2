"""
Unit tests for deterministic high-power threshold alert logic.
NOTE: This logic is deterministic rule-based, NOT machine learning.
"""

import pytest


def evaluate_high_power_alert(global_active_power: float, threshold_kw: float = 5.0) -> bool:
    """Deterministic rule-based condition: active power exceeds threshold."""
    return float(global_active_power) > float(threshold_kw)


def test_alert_triggered_above_threshold():
    assert evaluate_high_power_alert(5.001, 5.0) is True
    assert evaluate_high_power_alert(5.360, 5.0) is True
    assert evaluate_high_power_alert(11.122, 5.0) is True


def test_alert_not_triggered_at_or_below_threshold():
    assert evaluate_high_power_alert(5.0, 5.0) is False
    assert evaluate_high_power_alert(4.999, 5.0) is False
    assert evaluate_high_power_alert(1.250, 5.0) is False
    assert evaluate_high_power_alert(0.0, 5.0) is False


def test_custom_threshold():
    assert evaluate_high_power_alert(3.5, threshold_kw=3.0) is True
    assert evaluate_high_power_alert(3.0, threshold_kw=3.0) is False
