"""
Unit Tests for Analysis Functions.

Tests all 14 analysis functions with sample data.
"""

from decimal import Decimal

from src import analyses


def test_analysis_01_revenue_by_category(sample_transactions):
    """Test revenue by category analysis."""
    result = analyses.analysis_01_revenue_by_category(sample_transactions)

    # Should return sorted list of (category, revenue) tuples
    assert len(result) > 0
    assert isinstance(result, list)
    assert all(isinstance(item, tuple) and len(item) == 2 for item in result)

    # Check that revenues are sorted descending
    revenues = [r[1] for r in result]
    assert revenues == sorted(revenues, reverse=True)

    # Electronics should have highest revenue (199.98 + 47.97 = 247.95)
    assert result[0][0] == "Electronics"
    assert result[0][1] == Decimal("247.95")


def test_analysis_02_top_products_by_volume(sample_transactions):
    """Test top products by volume analysis."""
    result = analyses.analysis_02_top_products_by_volume(sample_transactions, top_n=5)

    assert len(result) > 0
    assert all(isinstance(item, tuple) and len(item) == 3 for item in result)

    # Check that quantities are sorted descending
    quantities = [r[2] for r in result]
    assert quantities == sorted(quantities, reverse=True)


def test_analysis_03_avg_transaction_by_segment(sample_transactions):
    """Test average transaction by segment analysis."""
    result = analyses.analysis_03_avg_transaction_by_segment(sample_transactions)

    assert isinstance(result, dict)
    assert "Individual" in result
    assert "SMB" in result
    assert "Enterprise" in result
    assert all(isinstance(v, Decimal) for v in result.values())


def test_analysis_04_monthly_sales_trend(sample_transactions):
    """Test monthly sales trend analysis."""
    result = analyses.analysis_04_monthly_sales_trend(sample_transactions)

    assert isinstance(result, list)
    assert len(result) > 0

    # Check chronological ordering
    months = [r[0] for r in result]
    assert months == sorted(months)

    # Verify format YYYY-MM
    assert all(len(month) == 7 and month[4] == "-" for month in months)


def test_analysis_05_revenue_by_region_and_payment(sample_transactions):
    """Test revenue by region and payment method analysis."""
    result = analyses.analysis_05_revenue_by_region_and_payment(sample_transactions)

    assert isinstance(result, dict)
    assert "North" in result
    assert "South" in result
    assert "East" in result

    # Check nested structure
    for region, payment_methods in result.items():
        assert isinstance(payment_methods, dict)
        assert all(isinstance(v, Decimal) for v in payment_methods.values())


def test_analysis_06_discount_impact(sample_transactions):
    """Test discount impact analysis."""
    result = analyses.analysis_06_discount_impact(sample_transactions)

    assert isinstance(result, dict)
    assert "discounted_count" in result
    assert "non_discounted_count" in result
    assert "avg_discounted" in result
    assert "avg_non_discounted" in result
    assert "percentage_difference" in result

    # Verify counts
    assert result["discounted_count"] + result["non_discounted_count"] == len(
        sample_transactions
    )


def test_analysis_07_sales_rep_performance(sample_transactions):
    """Test sales rep performance analysis."""
    result = analyses.analysis_07_sales_rep_performance(sample_transactions, top_n=5)

    assert isinstance(result, list)
    assert len(result) > 0

    # Check structure
    for rep_id, metrics in result:
        assert isinstance(rep_id, str)
        assert isinstance(metrics, dict)
        assert "total_revenue" in metrics
        assert "transaction_count" in metrics
        assert "avg_deal_size" in metrics


def test_analysis_08_customer_purchase_frequency(sample_transactions):
    """Test customer purchase frequency analysis."""
    result = analyses.analysis_08_customer_purchase_frequency(sample_transactions)

    assert isinstance(result, dict)
    assert "1 purchase" in result
    assert "2-5 purchases" in result
    assert "6-10 purchases" in result
    assert "10+ purchases" in result


def test_analysis_09_seasonal_pattern(sample_transactions):
    """Test seasonal sales pattern analysis."""
    result = analyses.analysis_09_seasonal_pattern(sample_transactions)

    assert isinstance(result, dict)

    # Check structure: {year: {quarter: revenue}}
    for year, quarters in result.items():
        assert isinstance(year, str)
        assert isinstance(quarters, dict)
        for quarter, revenue in quarters.items():
            assert quarter.startswith("Q")
            assert isinstance(revenue, Decimal)


def test_analysis_10_high_value_transactions(sample_transactions):
    """Test high-value transactions analysis."""
    result = analyses.analysis_10_high_value_transactions(
        sample_transactions, percentile=95.0
    )

    assert isinstance(result, dict)
    assert "count" in result
    assert "threshold" in result


def test_analysis_11_category_mix_by_region(sample_transactions):
    """Test category mix by region analysis."""
    result = analyses.analysis_11_category_mix_by_region(sample_transactions)

    assert isinstance(result, dict)

    # Check structure and percentage sum
    for region, categories in result.items():
        assert isinstance(categories, dict)
        # Percentages should sum to approximately 100
        total_percentage = sum(categories.values())
        assert 99.0 <= total_percentage <= 101.0  # Allow for rounding


def test_analysis_12_customer_lifetime_value(sample_transactions):
    """Test customer lifetime value analysis."""
    result = analyses.analysis_12_customer_lifetime_value(sample_transactions, top_n=5)

    assert isinstance(result, list)

    for customer_id, metrics in result:
        assert isinstance(customer_id, str)
        assert "total_revenue" in metrics
        assert "transaction_count" in metrics
        assert "avg_order_value" in metrics


def test_analysis_13_payment_preference_by_segment(sample_transactions):
    """Test payment preference by segment analysis."""
    result = analyses.analysis_13_payment_preference_by_segment(sample_transactions)

    assert isinstance(result, dict)

    for segment, info in result.items():
        assert "preferred_method" in info
        assert "usage_percentage" in info
        assert "usage_count" in info


def test_analysis_14_price_range_distribution(sample_transactions):
    """Test price range distribution analysis."""
    result = analyses.analysis_14_price_range_distribution(sample_transactions)

    assert isinstance(result, dict)
    assert "Under $50" in result
    assert "$50-$200" in result
    assert "$200-$1000" in result
    assert "$1000+" in result

    for price_range, data in result.items():
        assert "count" in data
        assert "revenue" in data


def test_empty_transactions():
    """Test analyses handle empty transaction lists gracefully."""
    empty_list = []

    # Most analyses should return empty or zero results
    result_01 = analyses.analysis_01_revenue_by_category(empty_list)
    assert result_01 == []

    result_02 = analyses.analysis_02_top_products_by_volume(empty_list)
    assert result_02 == []

    result_08 = analyses.analysis_08_customer_purchase_frequency(empty_list)
    assert all(count == 0 for count in result_08.values())


def test_single_transaction(single_transaction):
    """Test analyses work with single transaction."""
    result_01 = analyses.analysis_01_revenue_by_category(single_transaction)
    assert len(result_01) == 1

    result_03 = analyses.analysis_03_avg_transaction_by_segment(single_transaction)
    assert len(result_03) == 1
