"""
Unit Tests for Output Formatting Module.

Tests formatting functions for currency, numbers, percentages,
and console output of analysis results.
"""

from decimal import Decimal
from typing import Any

import pytest

from src.formatters import (
    format_analysis_01,
    format_analysis_02,
    format_analysis_03,
    format_analysis_04,
    format_analysis_05,
    format_analysis_06,
    format_analysis_07,
    format_analysis_08,
    format_analysis_09,
    format_analysis_10,
    format_analysis_11,
    format_analysis_12,
    format_analysis_13,
    format_analysis_14,
    format_currency,
    format_number,
    format_percentage,
    print_overall_summary,
    print_section_header,
    print_separator,
)


# ==============================================================================
# Basic Formatting Functions Tests
# ==============================================================================


def test_format_currency_normal() -> None:
    """Test currency formatting with typical values."""
    assert format_currency(Decimal("1234.56")) == "$1,234.56"
    assert format_currency(Decimal("1234567.89")) == "$1,234,567.89"
    assert format_currency(Decimal("100.00")) == "$100.00"


def test_format_currency_zero() -> None:
    """Test currency formatting with zero."""
    assert format_currency(Decimal("0.00")) == "$0.00"
    assert format_currency(Decimal("0")) == "$0.00"


def test_format_currency_negative() -> None:
    """Test currency formatting with negative values."""
    assert format_currency(Decimal("-100.50")) == "$-100.50"
    assert format_currency(Decimal("-1234567.89")) == "$-1,234,567.89"


def test_format_currency_small_amount() -> None:
    """Test currency formatting with small amounts."""
    assert format_currency(Decimal("0.01")) == "$0.01"
    assert format_currency(Decimal("0.99")) == "$0.99"
    assert format_currency(Decimal("1.00")) == "$1.00"


def test_format_currency_large_amount() -> None:
    """Test currency formatting with very large amounts."""
    assert format_currency(Decimal("1000000.00")) == "$1,000,000.00"
    assert format_currency(Decimal("999999999.99")) == "$999,999,999.99"


def test_format_currency_rounding() -> None:
    """Test currency formatting rounds to 2 decimal places."""
    assert format_currency(Decimal("123.456")) == "$123.46"
    assert format_currency(Decimal("123.454")) == "$123.45"


def test_format_percentage_normal() -> None:
    """Test percentage formatting with typical values."""
    assert format_percentage(45.67) == "45.67%"
    assert format_percentage(100.0) == "100.00%"
    assert format_percentage(0.5) == "0.50%"


def test_format_percentage_zero() -> None:
    """Test percentage formatting with zero."""
    assert format_percentage(0.0) == "0.00%"


def test_format_percentage_negative() -> None:
    """Test percentage formatting with negative values."""
    assert format_percentage(-10.5) == "-10.50%"
    assert format_percentage(-100.0) == "-100.00%"


def test_format_percentage_large() -> None:
    """Test percentage formatting with large values."""
    assert format_percentage(250.75) == "250.75%"
    assert format_percentage(999.99) == "999.99%"


def test_format_percentage_rounding() -> None:
    """Test percentage formatting rounds to 2 decimal places."""
    assert format_percentage(45.678) == "45.68%"
    assert format_percentage(45.674) == "45.67%"


def test_format_number_normal() -> None:
    """Test number formatting with typical values."""
    assert format_number(1234567) == "1,234,567"
    assert format_number(100) == "100"
    assert format_number(1000) == "1,000"


def test_format_number_zero() -> None:
    """Test number formatting with zero."""
    assert format_number(0) == "0"


def test_format_number_negative() -> None:
    """Test number formatting with negative values."""
    assert format_number(-100) == "-100"
    assert format_number(-1234567) == "-1,234,567"


def test_format_number_large() -> None:
    """Test number formatting with very large numbers."""
    assert format_number(1000000) == "1,000,000"
    assert format_number(999999999) == "999,999,999"


def test_format_number_single_digit() -> None:
    """Test number formatting with single digits."""
    assert format_number(5) == "5"
    assert format_number(9) == "9"


# ==============================================================================
# Console Output Functions Tests (using capsys to capture stdout)
# ==============================================================================


def test_print_section_header_default_width(capsys: pytest.CaptureFixture[str]) -> None:
    """Test section header printing with default width."""
    print_section_header("Test Header")
    captured = capsys.readouterr()

    lines = captured.out.strip().split("\n")
    assert len(lines) == 3
    assert lines[0] == "=" * 80
    assert lines[1] == "Test Header"
    assert lines[2] == "-" * 80


def test_print_section_header_custom_width(capsys: pytest.CaptureFixture[str]) -> None:
    """Test section header printing with custom width."""
    print_section_header("Test Header", width=50)
    captured = capsys.readouterr()

    lines = captured.out.strip().split("\n")
    assert len(lines) == 3
    assert lines[0] == "=" * 50
    assert lines[1] == "Test Header"
    assert lines[2] == "-" * 50


def test_print_separator_default_width(capsys: pytest.CaptureFixture[str]) -> None:
    """Test separator printing with default width."""
    print_separator()
    captured = capsys.readouterr()

    assert captured.out.strip() == "=" * 80


def test_print_separator_custom_width(capsys: pytest.CaptureFixture[str]) -> None:
    """Test separator printing with custom width."""
    print_separator(width=40)
    captured = capsys.readouterr()

    assert captured.out.strip() == "=" * 40


# ==============================================================================
# Analysis Formatting Functions Tests
# ==============================================================================


def test_format_analysis_01(capsys: pytest.CaptureFixture[str]) -> None:
    """Test formatting of Analysis 1 (Revenue by Category)."""
    result = [
        ("Electronics", Decimal("1234567.89")),
        ("Clothing", Decimal("987654.32")),
    ]

    format_analysis_01(result)
    captured = capsys.readouterr()

    assert "Analysis 1" in captured.out
    assert "Electronics" in captured.out
    assert "Clothing" in captured.out
    assert "$1,234,567.89" in captured.out
    assert "$987,654.32" in captured.out


def test_format_analysis_01_empty(capsys: pytest.CaptureFixture[str]) -> None:
    """Test formatting of Analysis 1 with empty results."""
    format_analysis_01([])
    captured = capsys.readouterr()

    assert "Analysis 1" in captured.out


def test_format_analysis_02(capsys: pytest.CaptureFixture[str]) -> None:
    """Test formatting of Analysis 2 (Top Products)."""
    result = [
        ("PROD-001", "Wireless Headphones", 5432),
        ("PROD-002", "Yoga Mat", 4876),
    ]

    format_analysis_02(result)
    captured = capsys.readouterr()

    assert "Analysis 2" in captured.out
    assert "Rank" in captured.out
    assert "PROD-001" in captured.out
    assert "Wireless Headphones" in captured.out
    assert "5,432" in captured.out


def test_format_analysis_02_truncates_long_names(
    capsys: pytest.CaptureFixture[str]
) -> None:
    """Test that Analysis 2 truncates very long product names."""
    result = [("PROD-001", "A" * 50, 100)]

    format_analysis_02(result)
    captured = capsys.readouterr()

    # Should truncate to 28 characters
    assert "A" * 28 in captured.out


def test_format_analysis_03(capsys: pytest.CaptureFixture[str]) -> None:
    """Test formatting of Analysis 3 (Average Transaction by Segment)."""
    result = {
        "Enterprise": Decimal("1245.67"),
        "SMB": Decimal("687.89"),
        "Individual": Decimal("234.56"),
    }

    format_analysis_03(result)
    captured = capsys.readouterr()

    assert "Analysis 3" in captured.out
    assert "Enterprise" in captured.out
    assert "$1,245.67" in captured.out


def test_format_analysis_04(capsys: pytest.CaptureFixture[str]) -> None:
    """Test formatting of Analysis 4 (Monthly Sales Trend)."""
    result = [
        ("2023-01", Decimal("100000.00")),
        ("2023-02", Decimal("150000.00")),
    ]

    format_analysis_04(result)
    captured = capsys.readouterr()

    assert "Analysis 4" in captured.out
    assert "2023-01" in captured.out
    assert "$100,000.00" in captured.out
    assert "#" in captured.out  # Bar chart should be present


def test_format_analysis_05(capsys: pytest.CaptureFixture[str]) -> None:
    """Test formatting of Analysis 5 (Revenue by Region and Payment)."""
    result = {
        "North": {"Credit Card": Decimal("50000.00"), "Cash": Decimal("30000.00")},
        "South": {"Debit Card": Decimal("40000.00")},
    }

    format_analysis_05(result)
    captured = capsys.readouterr()

    assert "Analysis 5" in captured.out
    assert "North:" in captured.out
    assert "South:" in captured.out
    assert "Credit Card" in captured.out
    assert "$50,000.00" in captured.out


def test_format_analysis_06(capsys: pytest.CaptureFixture[str]) -> None:
    """Test formatting of Analysis 6 (Discount Impact)."""
    result = {
        "discounted_count": 1000,
        "non_discounted_count": 2000,
        "avg_discounted": Decimal("150.00"),
        "avg_non_discounted": Decimal("200.00"),
        "percentage_difference": Decimal("-25.00"),
    }

    format_analysis_06(result)
    captured = capsys.readouterr()

    assert "Analysis 6" in captured.out
    assert "1,000" in captured.out
    assert "2,000" in captured.out
    assert "$150.00" in captured.out
    assert "$200.00" in captured.out
    assert "-25.00%" in captured.out


def test_format_analysis_07(capsys: pytest.CaptureFixture[str]) -> None:
    """Test formatting of Analysis 7 (Sales Rep Performance)."""
    result = [
        (
            "REP-001",
            {
                "total_revenue": Decimal("100000.00"),
                "transaction_count": 500,
                "avg_deal_size": Decimal("200.00"),
            },
        ),
        (
            "REP-002",
            {
                "total_revenue": Decimal("80000.00"),
                "transaction_count": 400,
                "avg_deal_size": Decimal("200.00"),
            },
        ),
    ]

    format_analysis_07(result)
    captured = capsys.readouterr()

    assert "Analysis 7" in captured.out
    assert "REP-001" in captured.out
    assert "$100,000.00" in captured.out
    assert "500" in captured.out


def test_format_analysis_08(capsys: pytest.CaptureFixture[str]) -> None:
    """Test formatting of Analysis 8 (Customer Purchase Frequency)."""
    result = {
        "1 purchase": 100,
        "2-5 purchases": 50,
        "6-10 purchases": 20,
        "10+ purchases": 10,
    }

    format_analysis_08(result)
    captured = capsys.readouterr()

    assert "Analysis 8" in captured.out
    assert "1 purchase" in captured.out
    assert "100" in captured.out
    assert "customers" in captured.out


def test_format_analysis_08_missing_categories(
    capsys: pytest.CaptureFixture[str]
) -> None:
    """Test Analysis 8 handles missing frequency categories gracefully."""
    result = {"1 purchase": 100}  # Only one category

    format_analysis_08(result)
    captured = capsys.readouterr()

    # Should still print all categories, missing ones as 0
    assert "Analysis 8" in captured.out
    assert "0 customers" in captured.out


def test_format_analysis_09(capsys: pytest.CaptureFixture[str]) -> None:
    """Test formatting of Analysis 9 (Seasonal Sales Pattern)."""
    result = {
        "2023": {
            "Q1": Decimal("100000.00"),
            "Q2": Decimal("120000.00"),
            "Q3": Decimal("110000.00"),
            "Q4": Decimal("130000.00"),
        },
        "2024": {
            "Q1": Decimal("105000.00"),
            "Q2": Decimal("125000.00"),
        },
    }

    format_analysis_09(result)
    captured = capsys.readouterr()

    assert "Analysis 9" in captured.out
    assert "2023" in captured.out
    assert "2024" in captured.out
    assert "Q1" in captured.out
    assert "$100,000.00" in captured.out


def test_format_analysis_10_with_results(capsys: pytest.CaptureFixture[str]) -> None:
    """Test formatting of Analysis 10 (High-Value Transactions) with results."""
    result = {
        "threshold": Decimal("1000.00"),
        "count": 50,
        "total_revenue": Decimal("75000.00"),
        "avg_amount": Decimal("1500.00"),
        "top_category": "Electronics",
        "top_region": "North",
        "top_payment": "Credit Card",
    }

    format_analysis_10(result)
    captured = capsys.readouterr()

    assert "Analysis 10" in captured.out
    assert "$1,000.00" in captured.out
    assert "50" in captured.out
    assert "Electronics" in captured.out
    assert "North" in captured.out


def test_format_analysis_10_no_results(capsys: pytest.CaptureFixture[str]) -> None:
    """Test formatting of Analysis 10 with no high-value transactions."""
    result = {
        "threshold": Decimal("10000.00"),
        "count": 0,
    }

    format_analysis_10(result)
    captured = capsys.readouterr()

    assert "Analysis 10" in captured.out
    assert "0" in captured.out
    # Should not show category, region, payment info when count is 0
    assert "Most Common Category" not in captured.out


def test_format_analysis_11(capsys: pytest.CaptureFixture[str]) -> None:
    """Test formatting of Analysis 11 (Category Mix by Region)."""
    result = {
        "North": {"Electronics": 45.5, "Clothing": 30.2, "Books": 24.3},
        "South": {"Electronics": 50.0, "Clothing": 50.0},
    }

    format_analysis_11(result)
    captured = capsys.readouterr()

    assert "Analysis 11" in captured.out
    assert "North:" in captured.out
    assert "South:" in captured.out
    assert "Electronics" in captured.out
    assert "45.50%" in captured.out
    assert "#" in captured.out  # Bar chart should be present


def test_format_analysis_12(capsys: pytest.CaptureFixture[str]) -> None:
    """Test formatting of Analysis 12 (Customer Lifetime Value)."""
    result = [
        (
            "CUST-00001",
            {
                "total_revenue": Decimal("50000.00"),
                "transaction_count": 100,
                "avg_order_value": Decimal("500.00"),
            },
        ),
        (
            "CUST-00002",
            {
                "total_revenue": Decimal("40000.00"),
                "transaction_count": 80,
                "avg_order_value": Decimal("500.00"),
            },
        ),
    ]

    format_analysis_12(result)
    captured = capsys.readouterr()

    assert "Analysis 12" in captured.out
    assert "CUST-00001" in captured.out
    assert "$50,000.00" in captured.out
    assert "100" in captured.out


def test_format_analysis_13(capsys: pytest.CaptureFixture[str]) -> None:
    """Test formatting of Analysis 13 (Payment Preference by Segment)."""
    result = {
        "Enterprise": {"preferred_method": "Credit Card", "usage_percentage": 75.5},
        "SMB": {"preferred_method": "Debit Card", "usage_percentage": 60.2},
        "Individual": {"preferred_method": "Cash", "usage_percentage": 45.8},
    }

    format_analysis_13(result)
    captured = capsys.readouterr()

    assert "Analysis 13" in captured.out
    assert "Enterprise" in captured.out
    assert "Credit Card" in captured.out
    assert "75.50%" in captured.out


def test_format_analysis_14(capsys: pytest.CaptureFixture[str]) -> None:
    """Test formatting of Analysis 14 (Price Range Distribution)."""
    result: dict[str, dict[str, Any]] = {
        "Under $50": {"count": 1000, "revenue": Decimal("25000.00")},
        "$50-$200": {"count": 500, "revenue": Decimal("75000.00")},
        "$200-$1000": {"count": 200, "revenue": Decimal("100000.00")},
        "$1000+": {"count": 50, "revenue": Decimal("75000.00")},
    }

    format_analysis_14(result)
    captured = capsys.readouterr()

    assert "Analysis 14" in captured.out
    assert "Under $50" in captured.out
    assert "1,000" in captured.out
    assert "$25,000.00" in captured.out


def test_format_analysis_14_missing_ranges(capsys: pytest.CaptureFixture[str]) -> None:
    """Test Analysis 14 handles missing price ranges gracefully."""
    result: dict[str, dict[str, Any]] = {
        "Under $50": {"count": 100, "revenue": Decimal("2500.00")},
        # Missing other ranges
    }

    format_analysis_14(result)
    captured = capsys.readouterr()

    assert "Analysis 14" in captured.out
    assert "Under $50" in captured.out
    # Should not crash with missing ranges


def test_print_overall_summary(capsys: pytest.CaptureFixture[str]) -> None:
    """Test overall summary printing."""
    print_overall_summary(
        total_transactions=25000,
        total_revenue=Decimal("21371482.11"),
        date_range=("2023-01-01", "2024-12-31"),
        execution_time=0.847,
    )
    captured = capsys.readouterr()

    assert "SUMMARY" in captured.out
    assert "25,000" in captured.out
    assert "$21,371,482.11" in captured.out
    assert "2023-01-01" in captured.out
    assert "2024-12-31" in captured.out
    assert "0.847 seconds" in captured.out
    assert "14/14" in captured.out


def test_print_overall_summary_zero_transactions(
    capsys: pytest.CaptureFixture[str]
) -> None:
    """Test overall summary with zero transactions."""
    print_overall_summary(
        total_transactions=0,
        total_revenue=Decimal("0.00"),
        date_range=("2023-01-01", "2023-12-31"),
        execution_time=0.001,
    )
    captured = capsys.readouterr()

    assert "SUMMARY" in captured.out
    assert "0" in captured.out
    assert "$0.00" in captured.out
    # Should not calculate average transaction when count is 0
    assert "Average Transaction" not in captured.out


def test_print_overall_summary_calculates_average(
    capsys: pytest.CaptureFixture[str]
) -> None:
    """Test that overall summary calculates and displays average transaction."""
    print_overall_summary(
        total_transactions=1000,
        total_revenue=Decimal("100000.00"),
        date_range=("2023-01-01", "2023-12-31"),
        execution_time=0.5,
    )
    captured = capsys.readouterr()

    assert "Average Transaction" in captured.out
    # 100000 / 1000 = 100
    assert "$100.00" in captured.out


# ==============================================================================
# Edge Cases and Integration Tests
# ==============================================================================


def test_format_analysis_02_empty(capsys: pytest.CaptureFixture[str]) -> None:
    """Test Analysis 2 with empty results."""
    format_analysis_02([])
    captured = capsys.readouterr()

    assert "Analysis 2" in captured.out
    # Should print header but no data rows


def test_format_analysis_03_empty(capsys: pytest.CaptureFixture[str]) -> None:
    """Test Analysis 3 with empty results."""
    format_analysis_03({})
    captured = capsys.readouterr()

    assert "Analysis 3" in captured.out


def test_format_analysis_05_empty(capsys: pytest.CaptureFixture[str]) -> None:
    """Test Analysis 5 with empty results."""
    format_analysis_05({})
    captured = capsys.readouterr()

    assert "Analysis 5" in captured.out


def test_format_analysis_07_empty(capsys: pytest.CaptureFixture[str]) -> None:
    """Test Analysis 7 with empty results."""
    format_analysis_07([])
    captured = capsys.readouterr()

    assert "Analysis 7" in captured.out


def test_format_analysis_09_partial_quarters(capsys: pytest.CaptureFixture[str]) -> None:
    """Test Analysis 9 handles missing quarters gracefully."""
    result = {
        "2023": {
            "Q1": Decimal("100000.00"),
            # Q2, Q3, Q4 missing
        }
    }

    format_analysis_09(result)
    captured = capsys.readouterr()

    assert "Analysis 9" in captured.out
    assert "2023" in captured.out
    # Should show $0.00 for missing quarters


def test_format_analysis_11_empty(capsys: pytest.CaptureFixture[str]) -> None:
    """Test Analysis 11 with empty results."""
    format_analysis_11({})
    captured = capsys.readouterr()

    assert "Analysis 11" in captured.out


def test_format_analysis_12_empty(capsys: pytest.CaptureFixture[str]) -> None:
    """Test Analysis 12 with empty results."""
    format_analysis_12([])
    captured = capsys.readouterr()

    assert "Analysis 12" in captured.out


def test_format_analysis_13_empty(capsys: pytest.CaptureFixture[str]) -> None:
    """Test Analysis 13 with empty results."""
    format_analysis_13({})
    captured = capsys.readouterr()

    assert "Analysis 13" in captured.out

