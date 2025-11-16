# Intuit Build Challenge

A collection of production-grade Python implementations demonstrating advanced programming concepts, best practices, and professional software engineering.

## 📚 Assignments

### [Assignment 1: Producer-Consumer Pattern](./Assignment_1/)

**Concurrent Programming with Thread Synchronization**

A comprehensive implementation of the classic producer-consumer pattern featuring:
- Custom thread-safe bounded queue using `threading.Lock` and `Condition` variables
- Multiple producers and consumers with graceful shutdown
- Full compatibility with Python's standard library `queue.Queue`
- 98% test coverage with extensive unit and integration tests

**Technologies:** Threading, Synchronization Primitives, Concurrent Programming  
**[View Details →](./Assignment_1/README.md)**

---

### [Assignment 2: Functional Data Analysis](./Assignment_2/)

**Pure Functional Programming with Stream Operations**

A sales data analysis system built entirely with functional programming paradigms:
- 14 comprehensive analyses using pure functions and stream operations
- Lazy evaluation with `itertools`, `functools`, and functional composition
- Custom synthetic dataset generator (25,000+ transactions)
- 95% test coverage demonstrating FP best practices

**Technologies:** Functional Programming, Data Analysis, Stream Processing  
**[View Details →](./Assignment_2/README.md)**

---

## 🛠️ Tech Stack

- **Language:** Python 3.10+
- **Runtime:** Standard library only (no external dependencies in production)
- **Testing:** pytest, pytest-cov, pytest-xdist
- **Code Quality:** mypy (strict mode), black, pylint, flake8, isort

## 🚀 Quick Start

```bash
# Clone the repository
git clone <repository-url>
cd Intuit_Build_Challenge

# Navigate to specific assignment
cd Assignment_1  # or Assignment_2

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install development dependencies
pip install -r requirements-dev.txt

# Run tests
pytest

# Run the main program
python main.py
```

## 📋 Project Structure

```
Intuit_Build_Challenge/
├── Assignment_1/          # Producer-Consumer Pattern
│   ├── src/              # Source code
│   ├── tests/            # Comprehensive test suite
│   ├── main.py           # Demo script
│   └── README.md         # Detailed documentation
│
├── Assignment_2/          # Functional Data Analysis
│   ├── src/              # Functional programming modules
│   ├── tests/            # Test suite
│   ├── scripts/          # Data generation
│   ├── main.py           # Analysis runner
│   └── README.md         # Comprehensive guide
│
└── README.md             
```
