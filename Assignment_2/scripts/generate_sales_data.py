"""
Sales Data Generator Script.

Generates realistic synthetic sales data for analysis demonstrations.
Produces a CSV file with configurable number of rows, featuring:
- Multiple product categories
- Various customer segments
- Seasonal patterns
- Realistic pricing and discounts
- Edge cases for robust testing

Usage:
    python scripts/generate_sales_data.py
    python scripts/generate_sales_data.py --rows 50000 --seed 42
"""

import argparse
import csv
import logging
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@dataclass
class GeneratorConfig:
    """Configuration for data generation."""

    num_rows: int = 25000
    start_date: str = "2023-01-01"
    end_date: str = "2024-12-31"
    num_customers: int = 500
    num_products: int = 100
    num_sales_reps: int = 30
    seed: int = 42
    output_path: str = "data/sales_data.csv"
    null_probability: float = 0.03  # 3% missing values


# Business data - realistic product categories and payment methods
CATEGORIES = [
    "Electronics",
    "Clothing",
    "Home & Garden",
    "Sports & Outdoors",
    "Books",
    "Toys & Games",
    "Health & Beauty",
    "Automotive",
]

PRODUCT_NAMES = {
    "Electronics": [
        "Wireless Headphones",
        "Laptop Stand",
        "USB-C Cable",
        "Bluetooth Speaker",
        "Webcam",
        "Keyboard",
        "Mouse",
        "Monitor",
        "Power Bank",
        "Smart Watch",
    ],
    "Clothing": [
        "T-Shirt",
        "Jeans",
        "Sneakers",
        "Jacket",
        "Dress",
        "Sweater",
        "Shorts",
        "Hoodie",
        "Socks",
        "Hat",
    ],
    "Home & Garden": [
        "Plant Pot",
        "LED Bulb",
        "Storage Box",
        "Picture Frame",
        "Curtains",
        "Rug",
        "Pillow",
        "Blanket",
        "Vase",
        "Clock",
    ],
    "Sports & Outdoors": [
        "Yoga Mat",
        "Water Bottle",
        "Dumbbell Set",
        "Running Shoes",
        "Backpack",
        "Tent",
        "Sleeping Bag",
        "Bicycle",
        "Helmet",
        "Sunglasses",
    ],
    "Books": [
        "Fiction Novel",
        "Cookbook",
        "Biography",
        "Self-Help Book",
        "Technical Manual",
        "Children's Book",
        "Magazine",
        "Comic Book",
        "Dictionary",
        "Atlas",
    ],
    "Toys & Games": [
        "Board Game",
        "Puzzle",
        "Action Figure",
        "Doll",
        "LEGO Set",
        "Card Game",
        "Video Game",
        "Plush Toy",
        "RC Car",
        "Building Blocks",
    ],
    "Health & Beauty": [
        "Shampoo",
        "Face Cream",
        "Toothbrush",
        "Perfume",
        "Soap",
        "Lotion",
        "Makeup Kit",
        "Hair Dryer",
        "Nail Polish",
        "Vitamins",
    ],
    "Automotive": [
        "Car Charger",
        "Phone Mount",
        "Air Freshener",
        "Floor Mats",
        "Wiper Blades",
        "Oil Filter",
        "Tire Gauge",
        "Jump Starter",
        "Dash Cam",
        "Seat Covers",
    ],
}

PAYMENT_METHODS = ["Credit Card", "Debit Card", "Cash", "PayPal", "Bank Transfer"]
REGIONS = ["North", "South", "East", "West", "Central"]
CUSTOMER_SEGMENTS = ["Enterprise", "SMB", "Individual"]

# Price ranges by category (min, max)
PRICE_RANGES = {
    "Electronics": (15.0, 2000.0),
    "Clothing": (10.0, 300.0),
    "Home & Garden": (5.0, 500.0),
    "Sports & Outdoors": (15.0, 1500.0),
    "Books": (5.0, 150.0),
    "Toys & Games": (10.0, 300.0),
    "Health & Beauty": (5.0, 200.0),
    "Automotive": (10.0, 500.0),
}


def generate_transaction_id(index: int) -> str:
    """Generate unique transaction ID."""
    return f"TXN-{index:07d}"


def generate_customer_id(config: GeneratorConfig) -> str:
    """Generate random customer ID."""
    return f"CUST-{random.randint(1, config.num_customers):05d}"


def generate_product_id(config: GeneratorConfig) -> str:
    """Generate random product ID."""
    return f"PROD-{random.randint(1, config.num_products):04d}"


def generate_sales_rep_id(config: GeneratorConfig) -> str:
    """Generate random sales rep ID."""
    return f"REP-{random.randint(1, config.num_sales_reps):03d}"


def generate_date_in_range(start_date: datetime, end_date: datetime) -> datetime:
    """Generate random date within range."""
    time_between = end_date - start_date
    days_between = time_between.days
    random_days = random.randint(0, days_between)
    return start_date + timedelta(days=random_days)


def maybe_null(value: Any, config: GeneratorConfig) -> Any:
    """Randomly return null value for robustness testing."""
    if random.random() < config.null_probability:
        return ""
    return value


def generate_row(index: int, config: GeneratorConfig) -> dict[str, Any]:
    """Generate a single transaction row with realistic data."""
    # Date with seasonal bias (more sales in Q4)
    start = datetime.strptime(config.start_date, "%Y-%m-%d")
    end = datetime.strptime(config.end_date, "%Y-%m-%d")
    transaction_date = generate_date_in_range(start, end)

    # Bias towards Q4 (holiday season)
    if transaction_date.month >= 10:
        # Add extra Q4 transactions by potentially regenerating
        if random.random() > 0.3:
            transaction_date = datetime(
                transaction_date.year, random.randint(10, 12), random.randint(1, 28)
            )

    # Product details
    category = random.choice(CATEGORIES)
    product_names = PRODUCT_NAMES[category]
    product_name = random.choice(product_names)
    product_id = generate_product_id(config)

    # Pricing
    min_price, max_price = PRICE_RANGES[category]
    unit_price = round(random.uniform(min_price, max_price), 2)

    # Quantity (realistic distribution - most orders are 1-3 items)
    quantity = random.choices(
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], weights=[30, 25, 20, 10, 5, 4, 3, 1, 1, 1]
    )[0]

    # Discount (most have no discount)
    discount_percent = random.choices(
        [0, 5, 10, 15, 20, 25, 30, 50], weights=[50, 20, 15, 8, 4, 2, 0.5, 0.5]
    )[0]

    # Calculate total
    total_amount = round(quantity * unit_price * (1 - discount_percent / 100), 2)

    # Customer and transaction details
    customer_id = generate_customer_id(config)
    customer_segment = random.choice(CUSTOMER_SEGMENTS)
    payment_method = random.choice(PAYMENT_METHODS)
    region = random.choice(REGIONS)
    sales_rep_id = generate_sales_rep_id(config)

    return {
        "transaction_id": generate_transaction_id(index),
        "date": transaction_date.strftime("%Y-%m-%d"),
        "timestamp": transaction_date.strftime("%Y-%m-%d %H:%M:%S"),
        "customer_id": maybe_null(customer_id, config),
        "product_id": product_id,
        "product_category": category,
        "product_name": product_name,
        "quantity": quantity,
        "unit_price": unit_price,
        "total_amount": total_amount,
        "discount_percent": discount_percent,
        "payment_method": payment_method,
        "region": region,
        "sales_rep_id": maybe_null(sales_rep_id, config),
        "customer_segment": customer_segment,
    }


def generate_dataset(config: GeneratorConfig) -> list[dict[str, Any]]:
    """Generate complete dataset."""
    logger.info(f"Generating {config.num_rows} rows of sales data...")
    random.seed(config.seed)

    rows = [generate_row(i, config) for i in range(config.num_rows)]

    logger.info(f"Generated {len(rows)} transactions")
    return rows


def write_to_csv(rows: list[dict[str, Any]], output_path: str) -> None:
    """Write generated data to CSV file."""
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "transaction_id",
        "date",
        "timestamp",
        "customer_id",
        "product_id",
        "product_category",
        "product_name",
        "quantity",
        "unit_price",
        "total_amount",
        "discount_percent",
        "payment_method",
        "region",
        "sales_rep_id",
        "customer_segment",
    ]

    with output_file.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    logger.info(f"Data written to {output_path}")


def print_statistics(rows: list[dict[str, Any]]) -> None:
    """Print dataset statistics."""
    total_revenue = sum(float(row["total_amount"]) for row in rows)
    avg_transaction = total_revenue / len(rows)

    categories = {}
    for row in rows:
        cat = row["product_category"]
        categories[cat] = categories.get(cat, 0) + 1

    logger.info("\n" + "=" * 60)
    logger.info("DATASET STATISTICS")
    logger.info("=" * 60)
    logger.info(f"Total Transactions: {len(rows):,}")
    logger.info(f"Total Revenue: ${total_revenue:,.2f}")
    logger.info(f"Average Transaction: ${avg_transaction:.2f}")
    logger.info(f"\nTransactions by Category:")
    for cat, count in sorted(categories.items(), key=lambda x: x[1], reverse=True):
        logger.info(f"  {cat:20s}: {count:,} ({count/len(rows)*100:.1f}%)")
    logger.info("=" * 60)


def main() -> None:
    """Main entry point for data generation."""
    parser = argparse.ArgumentParser(description="Generate synthetic sales data")
    parser.add_argument(
        "--rows", type=int, default=25000, help="Number of rows to generate"
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument(
        "--output", type=str, default="data/sales_data.csv", help="Output CSV path"
    )

    args = parser.parse_args()

    config = GeneratorConfig(
        num_rows=args.rows, seed=args.seed, output_path=args.output
    )

    logger.info("Starting data generation...")
    logger.info(f"Configuration: {args.rows} rows, seed={args.seed}")

    rows = generate_dataset(config)
    write_to_csv(rows, config.output_path)
    print_statistics(rows)

    logger.info("\n✓ Data generation completed successfully!")


if __name__ == "__main__":
    main()

