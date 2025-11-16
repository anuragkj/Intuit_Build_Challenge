"""
Sales Data Analysis Functions.

Implements 14 comprehensive analysis functions demonstrating:
- Functional programming patterns
- Stream operations (via itertools)
- Data aggregation and grouping
- Lambda expressions
- Multiple data perspectives

Each analysis showcases different functional programming concepts
and provides business insights from sales data.
"""

from collections import Counter
from collections.abc import Iterable
from datetime import datetime
from decimal import Decimal
from typing import Any

from .aggregations import avg_by, count_by, percentile_by, sum_by
from .data_loader import SalesTransaction
from .filtering import filter_by, filter_top_n
from .grouping import count_by_key, group_and_aggregate, group_by, nested_group_by


def analysis_01_revenue_by_category(
    transactions: Iterable[SalesTransaction],
) -> list[tuple[str, Decimal]]:
    """
    Analysis 1: Total Revenue by Product Category.

    Demonstrates: groupby + reduce pattern

    Groups transactions by product category and sums total revenue.
    Returns sorted by revenue (descending).

    Args:
        transactions: Iterable of sales transactions

    Returns:
        List of (category, revenue) tuples sorted by revenue

    Time Complexity: O(n + k log k) where k is number of categories
    """
    category_revenue = group_and_aggregate(
        transactions,
        key_func=lambda t: t.product_category,
        aggregate_func=lambda group: sum_by(group, lambda t: t.total_amount),
    )

    # Sort by revenue descending
    return sorted(category_revenue.items(), key=lambda x: x[1], reverse=True)


def analysis_02_top_products_by_volume(
    transactions: Iterable[SalesTransaction], top_n: int = 10
) -> list[tuple[str, str, int]]:
    """
    Analysis 2: Top N Products by Sales Volume.

    Demonstrates: grouping + sorting + slicing

    Args:
        transactions: Iterable of sales transactions
        top_n: Number of top products to return

    Returns:
        List of (product_id, product_name, total_quantity) tuples

    Time Complexity: O(n + k log k) where k is number of products
    """
    # Group by product and sum quantities
    product_volumes: dict[str, tuple[str, int]] = {}

    for transaction in transactions:
        pid = transaction.product_id
        if pid not in product_volumes:
            product_volumes[pid] = (transaction.product_name, 0)

        name, quantity = product_volumes[pid]
        product_volumes[pid] = (name, quantity + transaction.quantity)

    # Sort by quantity and take top N
    sorted_products = sorted(
        [(pid, name, qty) for pid, (name, qty) in product_volumes.items()],
        key=lambda x: x[2],
        reverse=True,
    )

    return sorted_products[:top_n]


def analysis_03_avg_transaction_by_segment(
    transactions: Iterable[SalesTransaction],
) -> dict[str, Decimal]:
    """
    Analysis 3: Average Transaction Value by Customer Segment.

    Demonstrates: grouping + statistical aggregation

    Args:
        transactions: Iterable of sales transactions

    Returns:
        Dictionary mapping segments to average transaction values

    Time Complexity: O(n)
    """
    return group_and_aggregate(
        transactions,
        key_func=lambda t: t.customer_segment,
        aggregate_func=lambda group: avg_by(group, lambda t: t.total_amount),
    )


def analysis_04_monthly_sales_trend(
    transactions: Iterable[SalesTransaction],
) -> list[tuple[str, Decimal]]:
    """
    Analysis 4: Monthly Sales Trend.

    Demonstrates: date parsing + temporal grouping + chronological sorting

    Args:
        transactions: Iterable of sales transactions

    Returns:
        List of (year-month, revenue) tuples sorted chronologically

    Time Complexity: O(n + k log k) where k is number of months
    """

    def extract_year_month(transaction: SalesTransaction) -> str:
        """Extract YYYY-MM from transaction date."""
        date = datetime.strptime(transaction.date, "%Y-%m-%d")
        return date.strftime("%Y-%m")

    monthly_revenue = group_and_aggregate(
        transactions,
        key_func=extract_year_month,
        aggregate_func=lambda group: sum_by(group, lambda t: t.total_amount),
    )

    # Sort chronologically
    return sorted(monthly_revenue.items(), key=lambda x: x[0])


def analysis_05_revenue_by_region_and_payment(
    transactions: Iterable[SalesTransaction],
) -> dict[str, dict[str, Decimal]]:
    """
    Analysis 5: Revenue by Region and Payment Method.

    Demonstrates: multi-level (nested) grouping

    Args:
        transactions: Iterable of sales transactions

    Returns:
        Nested dictionary: {region: {payment_method: revenue}}

    Time Complexity: O(n)
    """
    nested_groups = nested_group_by(
        transactions, lambda t: t.region, lambda t: t.payment_method
    )

    # Aggregate revenue at leaf level
    result: dict[str, dict[str, Decimal]] = {}
    for region, payment_groups in nested_groups.items():
        result[region] = {}
        for payment_method, trans_list in payment_groups.items():
            result[region][payment_method] = sum_by(
                trans_list, lambda t: t.total_amount
            )

    return result


def analysis_06_discount_impact(
    transactions: Iterable[SalesTransaction],
) -> dict[str, Any]:
    """
    Analysis 6: Discount Impact Analysis.

    Demonstrates: partitioning + comparative statistics

    Compares average transaction value for discounted vs non-discounted sales.

    Args:
        transactions: Iterable of sales transactions

    Returns:
        Dictionary with discount statistics

    Time Complexity: O(n)
    """
    trans_list = list(transactions)

    # Partition into discounted and non-discounted
    discounted = [t for t in trans_list if t.discount_percent > 0]
    non_discounted = [t for t in trans_list if t.discount_percent == 0]

    result: dict[str, Any] = {
        "discounted_count": len(discounted),
        "non_discounted_count": len(non_discounted),
    }

    if discounted:
        result["avg_discounted"] = avg_by(discounted, lambda t: t.total_amount)
    else:
        result["avg_discounted"] = Decimal(0)

    if non_discounted:
        result["avg_non_discounted"] = avg_by(non_discounted, lambda t: t.total_amount)
    else:
        result["avg_non_discounted"] = Decimal(0)

    # Calculate percentage difference
    if result["avg_non_discounted"] > 0:
        diff = (
            (result["avg_discounted"] - result["avg_non_discounted"])
            / result["avg_non_discounted"]
            * Decimal(100)
        )
        result["percentage_difference"] = diff
    else:
        result["percentage_difference"] = Decimal(0)

    return result


def analysis_07_sales_rep_performance(
    transactions: Iterable[SalesTransaction], top_n: int = 10
) -> list[tuple[str, dict[str, Any]]]:
    """
    Analysis 7: Sales Representative Performance.

    Demonstrates: grouping + multi-metric aggregation

    Calculates multiple performance metrics per sales rep.

    Args:
        transactions: Iterable of sales transactions
        top_n: Number of top performers to return

    Returns:
        List of (sales_rep_id, metrics_dict) tuples

    Time Complexity: O(n + k log k) where k is number of reps
    """
    # Filter out transactions without sales rep
    valid_transactions = list(
        filter_by(transactions, lambda t: t.sales_rep_id is not None)
    )

    # Group by sales rep
    rep_groups = group_by(valid_transactions, lambda t: t.sales_rep_id or "")

    performance: list[tuple[str, dict[str, Any]]] = []

    for rep_id, trans_list in rep_groups.items():
        total_revenue = sum_by(trans_list, lambda t: t.total_amount)
        transaction_count = len(trans_list)
        avg_deal_size = (
            total_revenue / Decimal(transaction_count)
            if transaction_count > 0
            else Decimal(0)
        )

        metrics = {
            "total_revenue": total_revenue,
            "transaction_count": transaction_count,
            "avg_deal_size": avg_deal_size,
        }

        performance.append((rep_id, metrics))

    # Sort by total revenue descending and take top N
    performance.sort(key=lambda x: x[1]["total_revenue"], reverse=True)
    return performance[:top_n]


def analysis_08_customer_purchase_frequency(
    transactions: Iterable[SalesTransaction],
) -> dict[str, int]:
    """
    Analysis 8: Customer Purchase Frequency Distribution.

    Demonstrates: grouping + frequency analysis + binning

    Buckets customers by number of purchases.

    Args:
        transactions: Iterable of sales transactions

    Returns:
        Dictionary mapping frequency ranges to customer counts

    Time Complexity: O(n)
    """
    # Filter valid customers
    valid_transactions = list(
        filter_by(transactions, lambda t: t.customer_id is not None)
    )

    # Count purchases per customer
    customer_purchases = count_by_key(valid_transactions, lambda t: t.customer_id or "")

    # Define frequency buckets
    buckets = {
        "1 purchase": 0,
        "2-5 purchases": 0,
        "6-10 purchases": 0,
        "10+ purchases": 0,
    }

    for count in customer_purchases.values():
        if count == 1:
            buckets["1 purchase"] += 1
        elif 2 <= count <= 5:
            buckets["2-5 purchases"] += 1
        elif 6 <= count <= 10:
            buckets["6-10 purchases"] += 1
        else:
            buckets["10+ purchases"] += 1

    return buckets


def analysis_09_seasonal_pattern(
    transactions: Iterable[SalesTransaction],
) -> dict[str, dict[str, Decimal]]:
    """
    Analysis 9: Seasonal Sales Pattern (Quarterly).

    Demonstrates: temporal extraction + grouping + year-over-year comparison

    Args:
        transactions: Iterable of sales transactions

    Returns:
        Nested dictionary: {year: {quarter: revenue}}

    Time Complexity: O(n)
    """

    def extract_year_quarter(transaction: SalesTransaction) -> tuple[str, str]:
        """Extract (year, quarter) from transaction date."""
        date = datetime.strptime(transaction.date, "%Y-%m-%d")
        year = str(date.year)
        quarter = f"Q{(date.month - 1) // 3 + 1}"
        return year, quarter

    trans_list = list(transactions)

    # Group by year and quarter
    yearly_quarterly_revenue: dict[str, dict[str, Decimal]] = {}

    for transaction in trans_list:
        year, quarter = extract_year_quarter(transaction)

        if year not in yearly_quarterly_revenue:
            yearly_quarterly_revenue[year] = {}

        if quarter not in yearly_quarterly_revenue[year]:
            yearly_quarterly_revenue[year][quarter] = Decimal(0)

        yearly_quarterly_revenue[year][quarter] += transaction.total_amount

    return yearly_quarterly_revenue


def analysis_10_high_value_transactions(
    transactions: Iterable[SalesTransaction], percentile: float = 95.0
) -> dict[str, Any]:
    """
    Analysis 10: High-Value Transaction Analysis.

    Demonstrates: percentile calculation + conditional filtering + analysis

    Analyzes characteristics of high-value transactions.

    Args:
        transactions: Iterable of sales transactions
        percentile: Percentile threshold for high-value

    Returns:
        Dictionary with high-value transaction statistics

    Time Complexity: O(n log n) due to percentile calculation
    """
    trans_list = list(transactions)

    # Calculate threshold
    threshold = percentile_by(trans_list, lambda t: t.total_amount, percentile)

    # Filter high-value transactions
    high_value = list(filter_by(trans_list, lambda t: t.total_amount >= threshold))

    if not high_value:
        return {"count": 0, "threshold": threshold}

    # Analyze characteristics
    category_counts = Counter(t.product_category for t in high_value)
    region_counts = Counter(t.region for t in high_value)
    payment_counts = Counter(t.payment_method for t in high_value)

    return {
        "count": len(high_value),
        "threshold": threshold,
        "total_revenue": sum_by(high_value, lambda t: t.total_amount),
        "avg_amount": avg_by(high_value, lambda t: t.total_amount),
        "top_category": (
            category_counts.most_common(1)[0][0] if category_counts else None
        ),
        "top_region": region_counts.most_common(1)[0][0] if region_counts else None,
        "top_payment": payment_counts.most_common(1)[0][0] if payment_counts else None,
    }


def analysis_11_category_mix_by_region(
    transactions: Iterable[SalesTransaction],
) -> dict[str, dict[str, float]]:
    """
    Analysis 11: Product Category Mix by Region.

    Demonstrates: cross-tabulation + percentage calculations

    Args:
        transactions: Iterable of sales transactions

    Returns:
        Nested dictionary: {region: {category: percentage}}

    Time Complexity: O(n)
    """
    # Group by region and category
    nested_groups = nested_group_by(
        transactions, lambda t: t.region, lambda t: t.product_category
    )

    result: dict[str, dict[str, float]] = {}

    for region, category_groups in nested_groups.items():
        # Calculate revenue per category
        category_revenue: dict[str, Decimal] = {}
        total_revenue = Decimal(0)

        for category, trans_list in category_groups.items():
            revenue = sum_by(trans_list, lambda t: t.total_amount)
            category_revenue[category] = revenue
            total_revenue += revenue

        # Calculate percentages
        result[region] = {}
        for category, revenue in category_revenue.items():
            percentage = (
                float(revenue / total_revenue * Decimal(100))
                if total_revenue > 0
                else 0.0
            )
            result[region][category] = round(percentage, 2)

    return result


def analysis_12_customer_lifetime_value(
    transactions: Iterable[SalesTransaction], top_n: int = 20
) -> list[tuple[str, dict[str, Any]]]:
    """
    Analysis 12: Customer Lifetime Value (Top N).

    Demonstrates: customer-level aggregation + ranking

    Args:
        transactions: Iterable of sales transactions
        top_n: Number of top customers to return

    Returns:
        List of (customer_id, metrics) tuples

    Time Complexity: O(n + k log k) where k is number of customers
    """
    # Filter valid customers
    valid_transactions = list(
        filter_by(transactions, lambda t: t.customer_id is not None)
    )

    # Group by customer
    customer_groups = group_by(valid_transactions, lambda t: t.customer_id or "")

    customer_metrics: list[tuple[str, dict[str, Any]]] = []

    for customer_id, trans_list in customer_groups.items():
        total_revenue = sum_by(trans_list, lambda t: t.total_amount)
        transaction_count = len(trans_list)
        avg_order_value = (
            total_revenue / Decimal(transaction_count)
            if transaction_count > 0
            else Decimal(0)
        )

        metrics = {
            "total_revenue": total_revenue,
            "transaction_count": transaction_count,
            "avg_order_value": avg_order_value,
        }

        customer_metrics.append((customer_id, metrics))

    # Sort by total revenue and take top N
    customer_metrics.sort(key=lambda x: x[1]["total_revenue"], reverse=True)
    return customer_metrics[:top_n]


def analysis_13_payment_preference_by_segment(
    transactions: Iterable[SalesTransaction],
) -> dict[str, dict[str, Any]]:
    """
    Analysis 13: Payment Method Preference by Customer Segment.

    Demonstrates: mode calculation + categorical analysis

    Args:
        transactions: Iterable of sales transactions

    Returns:
        Dictionary: {segment: {preferred_method, usage_percentage}}

    Time Complexity: O(n)
    """
    # Group by customer segment
    segment_groups = group_by(transactions, lambda t: t.customer_segment)

    result: dict[str, dict[str, Any]] = {}

    for segment, trans_list in segment_groups.items():
        # Count payment methods
        payment_counts = Counter(t.payment_method for t in trans_list)

        if payment_counts:
            most_common = payment_counts.most_common(1)[0]
            preferred_method = most_common[0]
            count = most_common[1]
            total = len(trans_list)
            percentage = round((count / total) * 100, 2) if total > 0 else 0.0

            result[segment] = {
                "preferred_method": preferred_method,
                "usage_percentage": percentage,
                "usage_count": count,
            }

    return result


def analysis_14_price_range_distribution(
    transactions: Iterable[SalesTransaction],
) -> dict[str, dict[str, Any]]:
    """
    Analysis 14: Price Range Distribution.

    Demonstrates: numerical binning + distribution analysis

    Args:
        transactions: Iterable of sales transactions

    Returns:
        Dictionary mapping price ranges to statistics

    Time Complexity: O(n)
    """
    # Define price range buckets
    buckets: dict[str, dict[str, int | Decimal]] = {
        "Under $50": {"count": 0, "revenue": Decimal(0)},
        "$50-$200": {"count": 0, "revenue": Decimal(0)},
        "$200-$1000": {"count": 0, "revenue": Decimal(0)},
        "$1000+": {"count": 0, "revenue": Decimal(0)},
    }

    for transaction in transactions:
        price = float(transaction.unit_price)
        revenue = transaction.total_amount

        if price < 50:
            bucket = "Under $50"
        elif 50 <= price < 200:
            bucket = "$50-$200"
        elif 200 <= price < 1000:
            bucket = "$200-$1000"
        else:
            bucket = "$1000+"

        bucket_data = buckets[bucket]
        bucket_data["count"] = bucket_data["count"] + 1
        bucket_data["revenue"] = bucket_data["revenue"] + revenue

    return buckets
