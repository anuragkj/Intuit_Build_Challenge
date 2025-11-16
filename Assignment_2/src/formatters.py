"""
Output Formatting Module.

Provides formatting functions for console output of analysis results.
Creates professional, readable output with proper alignment and styling.
"""

from decimal import Decimal
from typing import Any


def format_currency(amount: Decimal) -> str:
    """
    Format decimal as currency with thousands separators.

    Args:
        amount: Decimal amount

    Returns:
        Formatted currency string

    Example:
        >>> format_currency(Decimal('1234567.89'))
        '$1,234,567.89'
    """
    return f"${amount:,.2f}"


def format_percentage(value: float) -> str:
    """
    Format float as percentage.

    Args:
        value: Percentage value (0-100)

    Returns:
        Formatted percentage string

    Example:
        >>> format_percentage(45.67)
        '45.67%'
    """
    return f"{value:.2f}%"


def format_number(value: int) -> str:
    """
    Format integer with thousands separators.

    Args:
        value: Integer value

    Returns:
        Formatted number string

    Example:
        >>> format_number(1234567)
        '1,234,567'
    """
    return f"{value:,}"


def print_section_header(title: str, width: int = 80) -> None:
    """
    Print section header with title.

    Args:
        title: Section title
        width: Total width of header
    """
    print("\n" + "=" * width)
    print(title)
    print("-" * width)


def print_separator(width: int = 80) -> None:
    """Print horizontal separator line."""
    print("=" * width)


def format_analysis_01(result: list[tuple[str, Decimal]]) -> None:
    """Format Analysis 1: Revenue by Category."""
    print_section_header("Analysis 1: Total Revenue by Product Category")

    for category, revenue in result:
        print(f"{category:25s} {format_currency(revenue):>20s}")

    print()


def format_analysis_02(result: list[tuple[str, str, int]]) -> None:
    """Format Analysis 2: Top Products by Volume."""
    print_section_header("Analysis 2: Top 10 Products by Sales Volume")

    print(f"{'Rank':<6} {'Product ID':<15} {'Product Name':<30} {'Units Sold':>15}")
    print("-" * 80)

    for rank, (product_id, product_name, quantity) in enumerate(result, start=1):
        print(
            f"{rank:<6} {product_id:<15} {product_name[:28]:<30} {format_number(quantity):>15}"
        )

    print()


def format_analysis_03(result: dict[str, Decimal]) -> None:
    """Format Analysis 3: Average Transaction by Segment."""
    print_section_header("Analysis 3: Average Transaction Value by Customer Segment")

    sorted_segments = sorted(result.items(), key=lambda x: x[1], reverse=True)

    for segment, avg_value in sorted_segments:
        print(f"{segment:20s} {format_currency(avg_value):>20s}")

    print()


def format_analysis_04(result: list[tuple[str, Decimal]]) -> None:
    """Format Analysis 4: Monthly Sales Trend."""
    print_section_header("Analysis 4: Monthly Sales Trend")

    for month, revenue in result:
        # Create simple bar chart (using # for Windows compatibility)
        bar_length = int(float(revenue) / 10000)
        bar = "#" * min(bar_length, 50)
        print(f"{month}  {format_currency(revenue):>15s}  {bar}")

    print()


def format_analysis_05(result: dict[str, dict[str, Decimal]]) -> None:
    """Format Analysis 5: Revenue by Region and Payment."""
    print_section_header("Analysis 5: Revenue by Region and Payment Method")

    for region in sorted(result.keys()):
        print(f"\n{region}:")
        payment_methods = result[region]
        sorted_methods = sorted(
            payment_methods.items(), key=lambda x: x[1], reverse=True
        )

        for payment_method, revenue in sorted_methods:
            print(f"  {payment_method:20s} {format_currency(revenue):>20s}")

    print()


def format_analysis_06(result: dict[str, Any]) -> None:
    """Format Analysis 6: Discount Impact."""
    print_section_header("Analysis 6: Discount Impact Analysis")

    print(f"Discounted Transactions:     {format_number(result['discounted_count'])}")
    print(
        f"Non-Discounted Transactions: {format_number(result['non_discounted_count'])}"
    )
    print()
    print(f"Average Discounted Sale:     {format_currency(result['avg_discounted'])}")
    print(
        f"Average Non-Discounted Sale: {format_currency(result['avg_non_discounted'])}"
    )
    print()
    print(
        f"Percentage Difference:       {format_percentage(float(result['percentage_difference']))}"
    )

    print()


def format_analysis_07(result: list[tuple[str, dict[str, Any]]]) -> None:
    """Format Analysis 7: Sales Rep Performance."""
    print_section_header("Analysis 7: Top 10 Sales Representatives by Performance")

    print(
        f"{'Rank':<6} {'Rep ID':<12} {'Total Revenue':>18} {'Transactions':>15} {'Avg Deal':>15}"
    )
    print("-" * 80)

    for rank, (rep_id, metrics) in enumerate(result, start=1):
        print(
            f"{rank:<6} {rep_id:<12} "
            f"{format_currency(metrics['total_revenue']):>18} "
            f"{format_number(metrics['transaction_count']):>15} "
            f"{format_currency(metrics['avg_deal_size']):>15}"
        )

    print()


def format_analysis_08(result: dict[str, int]) -> None:
    """Format Analysis 8: Customer Purchase Frequency."""
    print_section_header("Analysis 8: Customer Purchase Frequency Distribution")

    # Order by frequency
    ordered_keys = ["1 purchase", "2-5 purchases", "6-10 purchases", "10+ purchases"]

    for frequency_range in ordered_keys:
        count = result.get(frequency_range, 0)
        print(f"{frequency_range:20s} {format_number(count):>15} customers")

    print()


def format_analysis_09(result: dict[str, dict[str, Decimal]]) -> None:
    """Format Analysis 9: Seasonal Sales Pattern."""
    print_section_header("Analysis 9: Seasonal Sales Pattern (Quarterly)")

    quarters = ["Q1", "Q2", "Q3", "Q4"]

    # Print header
    print(f"{'Year':<8}", end="")
    for q in quarters:
        print(f"{q:>18}", end="")
    print()
    print("-" * 80)

    # Print data
    for year in sorted(result.keys()):
        print(f"{year:<8}", end="")
        for quarter in quarters:
            revenue = result[year].get(quarter, Decimal(0))
            print(f"{format_currency(revenue):>18}", end="")
        print()

    print()


def format_analysis_10(result: dict[str, Any]) -> None:
    """Format Analysis 10: High-Value Transactions."""
    print_section_header(
        "Analysis 10: High-Value Transaction Analysis (95th Percentile)"
    )

    print(f"Threshold Amount:     {format_currency(result['threshold'])}")
    print(f"High-Value Count:     {format_number(result['count'])}")

    if result["count"] > 0:
        print(f"Total Revenue:        {format_currency(result['total_revenue'])}")
        print(f"Average Amount:       {format_currency(result['avg_amount'])}")
        print()
        print(f"Most Common Category: {result['top_category']}")
        print(f"Most Common Region:   {result['top_region']}")
        print(f"Most Common Payment:  {result['top_payment']}")

    print()


def format_analysis_11(result: dict[str, dict[str, float]]) -> None:
    """Format Analysis 11: Category Mix by Region."""
    print_section_header("Analysis 11: Product Category Mix by Region (% of Revenue)")

    for region in sorted(result.keys()):
        print(f"\n{region}:")
        categories = result[region]
        sorted_categories = sorted(categories.items(), key=lambda x: x[1], reverse=True)

        for category, percentage in sorted_categories:
            bar_length = int(percentage / 2)  # Scale for display
            bar = "#" * bar_length
            print(f"  {category:20s} {format_percentage(percentage):>8}  {bar}")

    print()


def format_analysis_12(result: list[tuple[str, dict[str, Any]]]) -> None:
    """Format Analysis 12: Customer Lifetime Value."""
    print_section_header("Analysis 12: Top 20 Customers by Lifetime Value")

    print(
        f"{'Rank':<6} {'Customer ID':<15} {'Total Revenue':>18} {'Transactions':>15} {'Avg Order':>15}"
    )
    print("-" * 80)

    for rank, (customer_id, metrics) in enumerate(result, start=1):
        print(
            f"{rank:<6} {customer_id:<15} "
            f"{format_currency(metrics['total_revenue']):>18} "
            f"{format_number(metrics['transaction_count']):>15} "
            f"{format_currency(metrics['avg_order_value']):>15}"
        )

    print()


def format_analysis_13(result: dict[str, dict[str, Any]]) -> None:
    """Format Analysis 13: Payment Preference by Segment."""
    print_section_header("Analysis 13: Payment Method Preference by Customer Segment")

    for segment in sorted(result.keys()):
        info = result[segment]
        print(
            f"{segment:20s} → {info['preferred_method']:15s} ({format_percentage(info['usage_percentage'])})"
        )

    print()


def format_analysis_14(result: dict[str, dict[str, Any]]) -> None:
    """Format Analysis 14: Price Range Distribution."""
    print_section_header("Analysis 14: Price Range Distribution")

    # Order by price range
    ordered_ranges = ["Under $50", "$50-$200", "$200-$1000", "$1000+"]

    print(f"{'Price Range':<20} {'Transactions':>15} {'Revenue':>20}")
    print("-" * 80)

    for price_range in ordered_ranges:
        if price_range in result:
            data = result[price_range]
            print(
                f"{price_range:<20} "
                f"{format_number(data['count']):>15} "
                f"{format_currency(data['revenue']):>20}"
            )

    print()


def print_overall_summary(
    total_transactions: int,
    total_revenue: Decimal,
    date_range: tuple[str, str],
    execution_time: float,
) -> None:
    """Print overall analysis summary."""
    print_separator()
    print("SUMMARY")
    print_separator()

    print(f"Total Transactions:     {format_number(total_transactions)}")
    print(f"Total Revenue:          {format_currency(total_revenue)}")

    if total_transactions > 0:
        avg_transaction = total_revenue / Decimal(total_transactions)
        print(f"Average Transaction:    {format_currency(avg_transaction)}")

    print(f"Date Range:             {date_range[0]} to {date_range[1]}")
    print(f"Execution Time:         {execution_time:.3f} seconds")
    print(f"Analyses Completed:     14/14")

    print_separator()
    print()
