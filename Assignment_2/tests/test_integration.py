"""
Integration Tests.

Tests end-to-end functionality with larger datasets.
"""

import pytest

from src import analyses


@pytest.mark.integration
def test_all_analyses_run_successfully(large_transaction_set):
    """Test that all 14 analyses run without errors on large dataset."""
    # Analysis 1
    result_01 = analyses.analysis_01_revenue_by_category(large_transaction_set)
    assert len(result_01) > 0

    # Analysis 2
    result_02 = analyses.analysis_02_top_products_by_volume(
        large_transaction_set, top_n=10
    )
    assert len(result_02) > 0

    # Analysis 3
    result_03 = analyses.analysis_03_avg_transaction_by_segment(large_transaction_set)
    assert len(result_03) > 0

    # Analysis 4
    result_04 = analyses.analysis_04_monthly_sales_trend(large_transaction_set)
    assert len(result_04) > 0

    # Analysis 5
    result_05 = analyses.analysis_05_revenue_by_region_and_payment(
        large_transaction_set
    )
    assert len(result_05) > 0

    # Analysis 6
    result_06 = analyses.analysis_06_discount_impact(large_transaction_set)
    assert "discounted_count" in result_06

    # Analysis 7
    result_07 = analyses.analysis_07_sales_rep_performance(
        large_transaction_set, top_n=10
    )
    assert len(result_07) > 0

    # Analysis 8
    result_08 = analyses.analysis_08_customer_purchase_frequency(large_transaction_set)
    assert len(result_08) == 4

    # Analysis 9
    result_09 = analyses.analysis_09_seasonal_pattern(large_transaction_set)
    assert len(result_09) > 0

    # Analysis 10
    result_10 = analyses.analysis_10_high_value_transactions(
        large_transaction_set, percentile=95.0
    )
    assert "count" in result_10

    # Analysis 11
    result_11 = analyses.analysis_11_category_mix_by_region(large_transaction_set)
    assert len(result_11) > 0

    # Analysis 12
    result_12 = analyses.analysis_12_customer_lifetime_value(
        large_transaction_set, top_n=20
    )
    assert len(result_12) > 0

    # Analysis 13
    result_13 = analyses.analysis_13_payment_preference_by_segment(
        large_transaction_set
    )
    assert len(result_13) > 0

    # Analysis 14
    result_14 = analyses.analysis_14_price_range_distribution(large_transaction_set)
    assert len(result_14) == 4


@pytest.mark.integration
def test_revenue_consistency(large_transaction_set):
    """Test that revenue calculations are consistent across analyses."""
    from decimal import Decimal

    from src.aggregations import sum_by

    # Calculate total revenue directly
    total_revenue = sum_by(large_transaction_set, lambda t: t.total_amount)

    # Sum of category revenues should equal total
    category_revenues = analyses.analysis_01_revenue_by_category(large_transaction_set)
    category_sum = sum(rev for _, rev in category_revenues)
    assert abs(category_sum - total_revenue) < Decimal("0.01")

    # Sum of regional revenues should equal total
    regional_revenues = analyses.analysis_05_revenue_by_region_and_payment(
        large_transaction_set
    )
    regional_sum = Decimal(0)
    for region, payment_methods in regional_revenues.items():
        for payment_method, revenue in payment_methods.items():
            regional_sum += revenue
    assert abs(regional_sum - total_revenue) < Decimal("0.01")


@pytest.mark.integration
def test_transaction_count_consistency(large_transaction_set):
    """Test that transaction counts are consistent."""
    total_count = len(large_transaction_set)

    # Discount impact counts
    discount_impact = analyses.analysis_06_discount_impact(large_transaction_set)
    discount_total = (
        discount_impact["discounted_count"] + discount_impact["non_discounted_count"]
    )
    assert discount_total == total_count


@pytest.mark.integration
def test_performance_acceptable(large_transaction_set):
    """Test that analyses complete in reasonable time."""
    import time

    start_time = time.time()

    # Run all analyses
    analyses.analysis_01_revenue_by_category(large_transaction_set)
    analyses.analysis_02_top_products_by_volume(large_transaction_set)
    analyses.analysis_03_avg_transaction_by_segment(large_transaction_set)
    analyses.analysis_04_monthly_sales_trend(large_transaction_set)
    analyses.analysis_05_revenue_by_region_and_payment(large_transaction_set)
    analyses.analysis_06_discount_impact(large_transaction_set)
    analyses.analysis_07_sales_rep_performance(large_transaction_set)
    analyses.analysis_08_customer_purchase_frequency(large_transaction_set)
    analyses.analysis_09_seasonal_pattern(large_transaction_set)
    analyses.analysis_10_high_value_transactions(large_transaction_set)
    analyses.analysis_11_category_mix_by_region(large_transaction_set)
    analyses.analysis_12_customer_lifetime_value(large_transaction_set)
    analyses.analysis_13_payment_preference_by_segment(large_transaction_set)
    analyses.analysis_14_price_range_distribution(large_transaction_set)

    elapsed_time = time.time() - start_time

    # Should complete in less than 5 seconds for 100 transactions
    assert elapsed_time < 5.0


@pytest.mark.integration
def test_data_immutability(sample_transactions):
    """Test that analyses don't modify input data."""
    from copy import deepcopy

    original = deepcopy(sample_transactions)

    # Run several analyses
    analyses.analysis_01_revenue_by_category(sample_transactions)
    analyses.analysis_02_top_products_by_volume(sample_transactions)
    analyses.analysis_03_avg_transaction_by_segment(sample_transactions)

    # Data should be unchanged
    assert sample_transactions == original
