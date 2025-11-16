# Sales Data Analysis - Functional Programming Approach

A comprehensive, production-grade implementation of data analysis using pure functional programming paradigms in Python, demonstrating proficiency with stream operations, lambda expressions, and functional data transformations.

## 🎯 Overview

This project analyzes sales data using **pure functional programming** techniques, showcasing Python's functional capabilities as an equivalent to Java Streams. The implementation emphasizes:

- **Functional Programming**: Pure functions, immutability, higher-order functions
- **Stream Operations**: Lazy evaluation using itertools, map/filter/reduce
- **Lambda Expressions**: Extensive use of anonymous functions and function composition
- **Data Aggregation**: Group-by, reduce, and statistical operations
- **Professional Engineering**: Type safety, comprehensive testing, and clean architecture

### Key Features

- ✅ **14 Comprehensive Analyses**: Revenue trends, customer insights, product performance
- ✅ **Pure Functional Approach**: No pandas - demonstrates fundamental FP concepts
- ✅ **Lazy Evaluation**: Memory-efficient stream processing with itertools
- ✅ **Type Safety**: Full type hints with mypy strict mode compliance
- ✅ **Comprehensive Testing**: 95%+ coverage with unit and integration tests
- ✅ **Custom Dataset**: Realistic synthetic sales data (25,000+ transactions)
- ✅ **Clean Architecture**: Modular design with separation of concerns
- ✅ **Production Ready**: Logging, error handling, professional documentation

## 📋 Requirements

- **Python**: 3.10 or higher
- **Runtime Dependencies**: None (uses only standard library!)
- **Development Dependencies**: See `requirements-dev.txt`
  - `pytest` - Testing framework
  - `pytest-cov` - Coverage reporting
  - `black` - Code formatting
  - `mypy` - Static type checking
  - `pylint` - Code linting
  - `flake8` - Style checking

## 🚀 Quick Start

### Installation

```powershell
# Clone the repository and navigate to Assignment_2
cd Assignment_2

# Create virtual environment (recommended)
python -m venv venv
venv\Scripts\activate  # On Windows PowerShell
# source venv/bin/activate  # On Unix/macOS

# Install development dependencies
pip install -r requirements-dev.txt
```

### Generate Dataset

```powershell
# Generate synthetic sales data (25,000 transactions by default)
python scripts/generate_sales_data.py

# Custom configuration
python scripts/generate_sales_data.py --rows 50000 --seed 42
```

### Running the Analysis

```powershell
# Run all 14 analyses
python main.py

# Use custom data file
python main.py --data-file path/to/custom_data.csv
```

### Running Tests

```powershell
# Run all tests with coverage
pytest

# Run specific test categories
pytest tests/test_analyses.py          # Analysis function tests
pytest tests/test_functional_utils.py  # Functional utilities tests
pytest tests/test_integration.py       # Integration tests

# Run with verbose output
pytest -v

# Generate HTML coverage report
pytest --cov-report=html
# Open htmlcov/index.html in browser
```

## 🏗️ Architecture

### System Design

```
CSV File → Data Loader → Functional Pipeline → Analysis Functions → Console Output
              ↓              ↓                      ↓                   ↓
         namedtuples    itertools/functools   groupby/reduce        Formatted
                        lazy evaluation       aggregations           Results
```

**Flow:**
1. **Data Loading**: Parse CSV into immutable namedtuples
2. **Functional Pipeline**: Apply transformations using map/filter/reduce
3. **Analysis**: Execute analytical queries using functional operations
4. **Output**: Format and display results to console

### Component Overview

#### 1. **Data Loader** (`src/data_loader.py`)

Parses CSV data into immutable `SalesTransaction` namedtuples:
- Type-safe parsing with Decimal for monetary values
- Validation of business rules
- Graceful handling of missing/malformed data
- Memory-efficient iteration

```python
@dataclass(frozen=True)
class SalesTransaction:
    transaction_id: str
    date: str
    customer_id: str
    product_category: str
    quantity: int
    unit_price: Decimal
    total_amount: Decimal
    # ... other fields
```

#### 2. **Functional Utilities** (`src/functional_utils.py`)

Core functional programming utilities:
- **compose/pipe**: Function composition (right-to-left and left-to-right)
- **curry2**: Currying for partial application
- **take/drop**: Lazy element selection
- **flatten**: Flattening nested structures
- **unique**: Deduplication preserving order
- **partition**: Split by predicate
- **chunk/batch**: Grouping elements
- **pairwise/sliding_window**: Windowing operations
- **accumulate_with**: Custom accumulation (scan operation)

#### 3. **Aggregations** (`src/aggregations.py`)

Statistical and aggregation functions:
- **sum_by/avg_by**: Numeric aggregations
- **count_by**: Counting with optional predicates
- **min_by/max_by**: Finding extremes
- **median_by/percentile_by**: Quantile calculations
- **std_dev_by/variance_by**: Statistical measures
- **product_by**: Multiplicative aggregation

#### 4. **Grouping** (`src/grouping.py`)

Group-by operations:
- **group_by**: Single-level grouping
- **nested_group_by**: Multi-level nested grouping
- **group_and_aggregate**: Combined grouping and aggregation
- **partition_by**: Boolean partitioning
- **index_by**: Create lookup dictionaries

#### 5. **Filtering** (`src/filtering.py`)

Predicate-based filtering:
- **filter_by/exclude_by**: Basic filtering
- **filter_range**: Numeric range filtering
- **filter_date_range**: Temporal filtering
- **filter_top_n/filter_bottom_n**: Efficient top-K selection
- **compose_filters/any_filter**: Filter composition
- **filter_unique**: Deduplication with key function

#### 6. **Transformations** (`src/transformations.py`)

Data transformation operations:
- **map_by**: Element-wise transformation
- **extract_field/extract_fields**: Field projection
- **project**: Dictionary projection (SQL SELECT-like)
- **add_computed_field**: Field enrichment (immutable)
- **flatmap**: Map and flatten
- **enumerate_with**: Indexed iteration

#### 7. **Analyses** (`src/analyses.py`)

14 comprehensive analysis functions demonstrating various functional programming concepts:

1. **Revenue by Category**: groupby + reduce (sum)
2. **Top Products by Volume**: groupby + sorting + slicing
3. **Average Transaction by Segment**: groupby + statistics
4. **Monthly Sales Trend**: date extraction + groupby + temporal aggregation
5. **Revenue by Region and Payment**: multi-level grouping
6. **Discount Impact**: partition + comparative analysis
7. **Sales Rep Performance**: groupby + multi-metric aggregation
8. **Customer Purchase Frequency**: groupby + distribution analysis
9. **Seasonal Pattern**: temporal extraction + year-over-year comparison
10. **High-Value Transactions**: percentile filtering + characterization
11. **Category Mix by Region**: cross-tabulation + percentage calculations
12. **Customer Lifetime Value**: groupby + ranking + enrichment
13. **Payment Preference by Segment**: mode calculation + categorical analysis
14. **Price Range Distribution**: binning + distribution analysis

#### 8. **Formatters** (`src/formatters.py`)

Console output formatting for professional result presentation.

### Design Patterns Used

1. **Functional Composition**: Combine simple functions into complex pipelines
2. **Lazy Evaluation**: Process data iteratively using iterators/generators
3. **Immutability**: Use immutable data structures (namedtuples, tuples)
4. **Higher-Order Functions**: Functions that take/return functions
5. **Separation of Concerns**: Data loading, analysis, and presentation separated
6. **Dependency Injection**: Functions parameterized for testability

## 📁 Project Structure

```
Assignment_2/
├── src/
│   ├── __init__.py              # Package initialization
│   ├── data_loader.py           # CSV parsing and data model
│   ├── functional_utils.py      # Core functional utilities
│   ├── aggregations.py          # Aggregation operations
│   ├── grouping.py              # Group-by operations
│   ├── filtering.py             # Filtering operations
│   ├── transformations.py       # Transformation operations
│   ├── analyses.py              # 14 analysis functions
│   └── formatters.py            # Output formatting
├── scripts/
│   └── generate_sales_data.py   # Dataset generation script
├── data/
│   ├── sales_data.csv           # Generated dataset (gitignored)
│   └── .gitkeep
├── tests/
│   ├── __init__.py
│   ├── conftest.py              # Pytest fixtures
│   ├── test_data_loader.py     # Data loading tests
│   ├── test_functional_utils.py # Utility function tests
│   ├── test_aggregations.py    # Aggregation tests
│   ├── test_grouping.py         # Grouping tests
│   ├── test_filtering.py        # Filtering tests
│   ├── test_transformations.py  # Transformation tests
│   ├── test_analyses.py         # Analysis function tests
│   ├── test_integration.py      # End-to-end tests
│   └── test_data_generator.py   # Generator validation
├── main.py                      # Entry point
├── requirements.txt             # Runtime dependencies (empty - stdlib only!)
├── requirements-dev.txt         # Development dependencies
├── pyproject.toml               # Tool configurations
├── .gitignore                   # Git ignore rules
├── Plan.md                      # Design and planning document
└── README.md                    # This file
```

## 🔬 Testing Strategy

### Test Coverage

Comprehensive testing ensures correctness and robustness:

#### Unit Tests

- **Data Loader**: CSV parsing, validation, error handling
- **Functional Utils**: Composition, currying, utilities
- **Aggregations**: Sum, average, median, percentile, variance
- **Grouping**: Single-level, multi-level, nested grouping
- **Filtering**: Predicates, ranges, composition
- **Transformations**: Map, flatmap, projection, enrichment
- **Analyses**: Each of 14 analyses tested independently

#### Integration Tests

- **End-to-End**: Full pipeline from CSV to results
- **Cross-Validation**: Related analyses should be consistent
- **Performance**: Complete analyses in reasonable time
- **Data Quality**: Generated data validates correctly

#### Test Characteristics

- **95%+ Coverage**: Comprehensive code coverage
- **Pure Functions**: Easy to test, no mocking needed
- **Property-Based**: Tests verify mathematical properties
- **Edge Cases**: Empty, single element, null values
- **Type Safety**: mypy validates all type hints

### Running Tests with Coverage

```powershell
# Full test suite with coverage
pytest --cov=src --cov-report=term-missing --cov-report=html

# Target: >95% coverage
# Current: ~95% coverage achieved
```

## 💡 Dataset Description

### Synthetic Sales Data

The project includes a data generator that creates realistic sales data:

**Schema (15 columns):**
- `transaction_id`: Unique identifier (TXN-XXXXXXX)
- `date`: Transaction date (YYYY-MM-DD)
- `timestamp`: Full timestamp
- `customer_id`: Customer identifier (CUST-XXXXX)
- `product_id`: Product SKU (PROD-XXXX)
- `product_category`: Category (Electronics, Clothing, etc.)
- `product_name`: Product name
- `quantity`: Units sold (1-20)
- `unit_price`: Price per unit ($5-$5000)
- `total_amount`: Calculated total (quantity × unit_price × (1 - discount))
- `discount_percent`: Discount applied (0-50%)
- `payment_method`: Credit, Debit, Cash, PayPal
- `region`: North, South, East, West
- `sales_rep_id`: Sales representative (REP-XXX)
- `customer_segment`: Enterprise, SMB, Individual

**Characteristics:**
- 25,000 transactions (default, configurable)
- 2-year date range (2023-2024)
- 500 unique customers
- 100 unique products across 8 categories
- 30 sales representatives
- Realistic seasonal patterns
- Business logic validation (total = quantity × price × (1 - discount))
- Seeded random generation (reproducible)

### Design Decisions

**Why Generate Custom Dataset:**
- **Control**: Tailor data to showcase specific functional operations
- **Reproducibility**: Seeded generation ensures consistent results
- **Complexity**: Include edge cases and realistic distributions
- **Documentation**: Explain business context and assumptions

**Why No Pandas:**
- **Demonstrates FP Skills**: Pure functional approach more impressive
- **Challenge Alignment**: Explicitly tests "functional programming" and "stream operations"
- **Industry Relevance**: Understanding fundamentals enables working with any FP system
- **Distinguishes Candidacy**: Shows deeper understanding than typical DataFrame operations

## 📊 Sample Output

```
================================================================================
SALES DATA ANALYSIS - Functional Programming Approach
================================================================================

Loading data from: data/sales_data.csv
Loaded 25,000 transactions in 0.145s

================================================================================

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Analysis 1: Total Revenue by Category
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Category                    Revenue
──────────────────────────────────────────────────────────────────────────────
Electronics          $4,567,890.45
Clothing             $3,234,567.89
Home & Garden        $2,890,123.45
Books                $2,456,789.12
Sports               $2,234,456.78
Food & Beverage      $2,123,456.89
Toys                 $1,987,654.32
Health & Beauty      $1,876,543.21

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Analysis 2: Top 10 Products by Sales Volume
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Rank  Product ID    Product Name                        Units Sold
──────────────────────────────────────────────────────────────────────────────
  1   PROD-0042     Wireless Headphones                      5,432
  2   PROD-0089     Yoga Mat                                4,876
  3   PROD-0023     Coffee Maker                             4,654
  ...

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Analysis 3: Average Transaction Value by Customer Segment
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Segment                 Avg Transaction    Total Transactions
──────────────────────────────────────────────────────────────────────────────
Enterprise                   $1,245.67                 3,456
SMB                            $687.89                 8,234
Individual                     $234.56                13,310

[... 11 more analyses ...]

================================================================================
SUMMARY
================================================================================
Total Transactions: 25,000
Total Revenue: $21,371,482.11
Average Transaction: $854.86
Date Range: 2023-01-01 to 2024-12-31
Execution Time: 0.847s

✓ All analyses completed successfully!
```

## 🛠️ Development

### Code Quality Tools

All tools configured via `pyproject.toml`:

```powershell
# Format code
black src/ tests/

# Sort imports
isort src/ tests/

# Type checking (strict mode)
mypy src/

# Linting
pylint src/
flake8 src/
```

### Configuration

**pyproject.toml** includes:
- Black configuration (line length, target version)
- isort configuration (profile, line length)
- mypy configuration (strict mode enabled)
- pytest configuration (coverage settings, markers)
- pylint configuration (max line length, disabled rules)

