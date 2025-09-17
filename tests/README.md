# AirPipe Test Suite

## Quick Start

```bash
# Install test dependencies
pip install -r requirements.txt

# Run all tests
pytest tests/

# Run specific test categories
pytest tests/core/           # Core framework tests
pytest tests/components/     # Business logic tests
pytest tests/workflows/      # Workflow integration tests
```

## Test Organization

The test suite follows a **clear separation of concerns**:

### 🔧 Core Framework Tests (`tests/core/`)
- **Purpose**: Test AirPipe's internal mechanisms
- **Coverage**: 87 tests
- **Components**: TaskPipeline, Artifacts, Streaming, Visualizers, Utils
- **Run**: `pytest tests/core/`

### 💼 Business Component Tests (`tests/components/`)
- **Purpose**: Test business logic and domain rules
- **Coverage**: 40 tests
- **Components**: Employee, Sales, Inventory, Simple pipelines
- **Run**: `pytest tests/components/`

### 🔄 Workflow Tests (`tests/workflows/`)
- **Purpose**: Test end-to-end workflow execution
- **Coverage**: 10 tests
- **Components**: Complete pipeline orchestration
- **Run**: `pytest tests/workflows/`

### 🛠️ Integration Tests (`tests/integration/`)
- **Purpose**: Cross-workflow and system integration
- **Coverage**: 16 tests
- **Run**: `pytest tests/integration/`

## Total Test Coverage

- **Total Tests**: 153 tests
- **Test Files**: 20+ test modules
- **Fixtures**: Comprehensive factories and mocks

## Running Tests by Category

### Core Framework Only
```bash
# Test framework internals
pytest tests/core/ -v

# Test specific core component
pytest tests/core/streaming/ -v
pytest tests/core/test_artifacts.py -v
```

### Business Logic Only
```bash
# Test all business components
pytest tests/components/ -v

# Test specific pipeline components
pytest tests/components/employee/ -v
pytest tests/components/simple/ -v
```

### Workflow Tests
```bash
# Test all workflows
pytest tests/workflows/ -v

# Test specific workflow
pytest tests/workflows/test_simple_workflow.py -v
```

## Test Coverage Reports

```bash
# Generate coverage report
pytest tests/ --cov=airpipe --cov-report=html

# View coverage in browser
open htmlcov/index.html
```

## Selective Testing

### By Pipeline
```bash
# Employee pipeline (components + workflow)
pytest tests/components/employee/ tests/workflows/test_employee_workflow.py

# Simple pipeline
pytest tests/components/simple/ tests/workflows/test_simple_workflow.py
```

### By Test Type
```bash
# Unit tests only
pytest tests/core/ tests/components/ -v

# Integration tests only
pytest tests/workflows/ tests/integration/ -v
```

### By Speed
```bash
# Fast tests only (exclude slow markers)
pytest tests/ -m "not slow"

# Run parallel (requires pytest-xdist)
pip install pytest-xdist
pytest tests/ -n auto
```

## Test Structure

```
tests/
├── core/                 # Framework tests (87 tests)
│   ├── streaming/        # Streaming engine tests
│   ├── visualizers/      # DAG visualization tests
│   └── utils/            # Utility function tests
│
├── components/           # Business logic tests (40 tests)
│   ├── employee/         # Employee pipeline logic
│   └── simple/           # Example pipeline logic
│
├── workflows/            # E2E workflow tests (10 tests)
│
├── integration/          # System integration (16 tests)
│
└── fixtures/             # Test utilities
    ├── factories.py      # Data generation
    ├── mocks.py         # Mock objects
    └── base.py          # Base test classes
```

## Benefits of This Structure

1. **Clear Separation**: Core framework vs business logic
2. **Maintainability**: Changes isolated to relevant tests
3. **Parallel Execution**: Run test categories independently
4. **Faster Feedback**: Run only affected test suites
5. **Better Organization**: Easy to find and add tests

## Writing New Tests

### For Core Framework Features
```python
# tests/core/test_new_feature.py
from tests.base import BaseTestCase

class TestNewFeature(BaseTestCase):
    def test_framework_behavior(self):
        # Test AirPipe internals
        pass
```

### For Business Logic
```python
# tests/components/pipeline_name/test_component.py
from tests.base import BaseTestCase

class TestBusinessComponent(BaseTestCase):
    def test_business_rule(self):
        # Test domain-specific logic
        pass
```

### For Workflows
```python
# tests/workflows/test_new_workflow.py
from tests.base import PipelineTestCase

class TestNewWorkflow(PipelineTestCase):
    def test_end_to_end(self):
        # Test complete workflow
        pass
```

## Continuous Integration

```yaml
# .github/workflows/tests.yml
test:
  strategy:
    matrix:
      test-suite:
        - core
        - components
        - workflows
  steps:
    - run: pytest tests/${{ matrix.test-suite }}/
```

## Troubleshooting

### Import Errors
```bash
# Ensure package is installed
pip install -e .
```

### Path Issues
```bash
# Run tests from project root
cd /path/to/AirPipe
pytest tests/
```

### Missing Dependencies
```bash
# Install all test dependencies
pip install -r requirements.txt
```

## Documentation

- [Test Structure Details](TEST_STRUCTURE.md)
- [Test Summary](TEST_SUMMARY.md)
- [Contributing Guide](../CONTRIBUTING.md)