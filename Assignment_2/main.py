"""
Main Entry Point for Sales Data Analysis.

Runs all 14 analyses and prints results to console.
Demonstrates complete functional programming pipeline.

Usage:
    python main.py
    python main.py --data-file path/to/custom_data.csv
"""

import argparse
import time
from decimal import Decimal
from pathlib import Path

from src import analyses, formatters
from src.data_loader import load_csv_as_list


def run_all_analyses(data_file: str) -> None:
    """
    Run all 14 analyses and print results.

    Args:
        data_file: Path to CSV data file
    """
    print("\n")
    print("=" * 80)
    print("SALES DATA ANALYSIS - Functional Programming Approach")
    print("=" * 80)
    print()

    # Check if data file exists
    if not Path(data_file).exists():
        print(f"Error: Data file not found: {data_file}")
        print("\nPlease generate data first:")
        print("  python scripts/generate_sales_data.py")
        return

    # Load data
    print(f"Loading data from: {data_file}")
    start_time = time.time()

    try:
        transactions = load_csv_as_list(data_file)
    except Exception as e:
        print(f"Error loading data: {e}")
        return

    load_time = time.time() - start_time
    print(f"Loaded {len(transactions):,} transactions in {load_time:.3f}s")

    if not transactions:
        print("Error: No transactions loaded")
        return

    # Calculate basic statistics
    total_revenue = sum(t.total_amount for t in transactions)
    dates = sorted(t.date for t in transactions)
    date_range = (dates[0], dates[-1])

    print()
    print("=" * 80)

    # Run all analyses
    analysis_start = time.time()

    # Analysis 1: Revenue by Category
    result_01 = analyses.analysis_01_revenue_by_category(transactions)
    formatters.format_analysis_01(result_01)

    # Analysis 2: Top Products by Volume
    result_02 = analyses.analysis_02_top_products_by_volume(transactions, top_n=10)
    formatters.format_analysis_02(result_02)

    # Analysis 3: Average Transaction by Segment
    result_03 = analyses.analysis_03_avg_transaction_by_segment(transactions)
    formatters.format_analysis_03(result_03)

    # Analysis 4: Monthly Sales Trend
    result_04 = analyses.analysis_04_monthly_sales_trend(transactions)
    formatters.format_analysis_04(result_04)

    # Analysis 5: Revenue by Region and Payment
    result_05 = analyses.analysis_05_revenue_by_region_and_payment(transactions)
    formatters.format_analysis_05(result_05)

    # Analysis 6: Discount Impact
    result_06 = analyses.analysis_06_discount_impact(transactions)
    formatters.format_analysis_06(result_06)

    # Analysis 7: Sales Rep Performance
    result_07 = analyses.analysis_07_sales_rep_performance(transactions, top_n=10)
    formatters.format_analysis_07(result_07)

    # Analysis 8: Customer Purchase Frequency
    result_08 = analyses.analysis_08_customer_purchase_frequency(transactions)
    formatters.format_analysis_08(result_08)

    # Analysis 9: Seasonal Pattern
    result_09 = analyses.analysis_09_seasonal_pattern(transactions)
    formatters.format_analysis_09(result_09)

    # Analysis 10: High-Value Transactions
    result_10 = analyses.analysis_10_high_value_transactions(transactions, percentile=95.0)
    formatters.format_analysis_10(result_10)

    # Analysis 11: Category Mix by Region
    result_11 = analyses.analysis_11_category_mix_by_region(transactions)
    formatters.format_analysis_11(result_11)

    # Analysis 12: Customer Lifetime Value
    result_12 = analyses.analysis_12_customer_lifetime_value(transactions, top_n=20)
    formatters.format_analysis_12(result_12)

    # Analysis 13: Payment Preference by Segment
    result_13 = analyses.analysis_13_payment_preference_by_segment(transactions)
    formatters.format_analysis_13(result_13)

    # Analysis 14: Price Range Distribution
    result_14 = analyses.analysis_14_price_range_distribution(transactions)
    formatters.format_analysis_14(result_14)

    analysis_time = time.time() - analysis_start

    # Print overall summary
    formatters.print_overall_summary(
        total_transactions=len(transactions),
        total_revenue=total_revenue,
        date_range=date_range,
        execution_time=analysis_time,
    )

    print("✓ All analyses completed successfully!")
    print()


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Run sales data analysis with functional programming"
    )
    parser.add_argument(
        "--data-file",
        type=str,
        default="data/sales_data.csv",
        help="Path to CSV data file",
    )

    args = parser.parse_args()

    run_all_analyses(args.data_file)


if __name__ == "__main__":
    main()

