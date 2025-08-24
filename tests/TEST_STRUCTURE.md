# AirPipe Test Structure - Separation of Concerns

## Overview
The test suite is organized to clearly separate core framework tests from business logic component tests, providing better maintainability and clarity.

## Test Structure

```
tests/
├── core/                          # Core Framework Tests (AirPipe internals)
│   ├── __init__.py
│   ├── test_task_pipeline.py     # TaskPipeline functionality
│   ├── test_artifacts.py         # DataArtifact and ArtifactStore
│   ├── visualizers/
│   │   └── test_visualizers.py   # DAG visualization
│   ├── streaming/
│   │   ├── test_micro_batch.py   # Batch processing engine
│   │   └── test_sources.py       # Data source abstractions
│   └── utils/
│       ├── test_csv_utils.py     # Generic CSV utilities
│       └── test_filter_utils.py  # Generic filter utilities
│
├── components/                    # Business Logic Component Tests
│   ├── __init__.py
│   ├── employee/                 # Employee pipeline business logic
│   │   ├── test_employee_extractor.py
│   │   └── test_salary_transformer.py
│   └── simple/                   # Simple pipeline business logic
│       └── test_value_transformer.py
│
├── workflows/                     # Workflow Integration Tests
│   ├── __init__.py
│   └── test_simple_workflow.py   # End-to-end workflow testing
│
├── integration/                   # System Integration Tests
│   └── test_workflows.py         # Cross-workflow integration
│
└── fixtures/                      # Shared Test Utilities
    ├── factories.py              # Data generation
    ├── mocks.py                  # Mock objects
    └── base.py                   # Base test classes
```

## Test Categories

### 1. Core Framework Tests (`tests/core/`)
**Purpose**: Test AirPipe's core functionality independent of business logic

#### What belongs here:
- TaskPipeline mechanics (registration, execution, dependency resolution)
- Artifact system (creation, storage, retrieval)
- DAG operations (validation, visualization)
- Streaming engine (batch processing, buffering)
- Generic utilities (CSV reading, filtering, aggregation)

#### Example:
```python
# tests/core/test_task_pipeline.py
def test_pipeline_dependency_resolution():
    """Test that pipeline correctly resolves task dependencies"""
    pipeline = TaskPipeline("test")
    # Test framework behavior, not business logic
```

### 2. Business Logic Component Tests (`tests/components/`)
**Purpose**: Test domain-specific business rules and transformations

#### What belongs here:
- Employee salary calculations and filters
- Department-specific transformations
- Sales data processing rules
- Inventory management logic
- Business validation rules

#### Example:
```python
# tests/components/employee/test_salary_transformer.py
def test_filter_high_earners():
    """Test business rule: employees earning > $80,000"""
    transformer = SalaryTransformer()
    result = transformer.filter_high_earners(df, threshold=80000)
    assert all(result['salary'] > 80000)
```

### 3. Workflow Tests (`tests/workflows/`)
**Purpose**: Test complete workflow orchestration and data flow

#### What belongs here:
- End-to-end workflow execution
- Task orchestration validation
- Data flow between pipeline stages
- Workflow-specific error handling

#### Example:
```python
# tests/workflows/test_employee_workflow.py
def test_employee_workflow_execution():
    """Test complete employee data processing workflow"""
    from workflows.employee_task_workflow import pipeline
    results = pipeline.execute()
    assert results['tasks_executed'] == 5
```

## Separation Benefits

### 1. Clear Responsibilities
- **Core tests** ensure framework reliability
- **Component tests** validate business rules
- **Workflow tests** verify integration

### 2. Maintainability
- Business logic changes only affect component tests
- Framework updates only impact core tests
- New pipelines add their own component test directory

### 3. Test Execution
```bash
# Run only core framework tests
pytest tests/core/

# Run only business logic tests
pytest tests/components/

# Run specific pipeline tests
pytest tests/components/employee/

# Run workflow integration tests
pytest tests/workflows/
```

### 4. CI/CD Integration
```yaml
# Example CI pipeline
test:
  stage: test
  parallel:
    matrix:
      - TEST_SUITE: core
        COMMAND: pytest tests/core/
      - TEST_SUITE: components
        COMMAND: pytest tests/components/
      - TEST_SUITE: workflows
        COMMAND: pytest tests/workflows/
```

## Test Examples by Category

### Core Framework Test
```python
# Testing framework capability, not business logic
class TestTaskPipeline:
    def test_parallel_execution(self):
        """Test that independent tasks execute in parallel"""
        pipeline = create_test_pipeline()
        results = pipeline.execute(parallel=True)
        # Verify parallel execution mechanics
```

### Business Component Test
```python
# Testing business rules and domain logic
class TestSalaryTransformer:
    def test_calculate_salary_bands(self):
        """Test salary band categorization business rules"""
        # Junior: < $50k, Mid: $50-80k, Senior: $80-120k, Executive: > $120k
        result = transformer.calculate_salary_bands(employee_df)
        assert result.loc[0, 'salary_band'] == 'Junior'  # $45k salary
```

### Workflow Integration Test
```python
# Testing complete workflow orchestration
class TestEmployeeWorkflow:
    def test_data_flow(self):
        """Test data flows correctly through all pipeline stages"""
        pipeline.execute()
        # Verify data transformations at each stage
        assert len(pipeline.get_artifact('filtered_employees')) < len(pipeline.get_artifact('raw_employees'))
```

## Migration Guide

### Moving from Mixed to Separated Structure

1. **Identify test type**:
   - Does it test framework mechanics? → `core/`
   - Does it test business rules? → `components/`
   - Does it test workflow orchestration? → `workflows/`

2. **Update imports**:
   ```python
   # Old
   from tests.unit.core.test_task import TestTaskPipeline
   
   # New
   from tests.core.test_task_pipeline import TestTaskPipeline
   ```

3. **Organize by pipeline**:
   ```
   components/
   ├── employee/      # All employee business logic tests
   ├── sales/         # All sales business logic tests
   └── inventory/     # All inventory business logic tests
   ```

## Best Practices

### 1. Test Naming
- Core: `test_<framework_feature>.py`
- Components: `test_<business_component>.py`
- Workflows: `test_<workflow_name>_workflow.py`

### 2. Mock Usage
- Core tests: Mock external dependencies
- Component tests: Mock data sources, focus on logic
- Workflow tests: Minimal mocking, test real integration

### 3. Test Data
- Core tests: Generic test data
- Component tests: Domain-specific test data
- Workflow tests: Realistic production-like data

### 4. Assertions
- Core tests: Framework behavior assertions
- Component tests: Business rule validations
- Workflow tests: End-to-end data validations

## Coverage Goals

| Test Category | Coverage Target | Focus Areas |
|--------------|-----------------|-------------|
| Core Framework | 95%+ | Critical paths, error handling |
| Business Components | 90%+ | Business rules, edge cases |
| Workflows | 80%+ | Happy paths, integration points |
| Integration | 70%+ | Cross-system interactions |

## Running Tests

### By Category
```bash
# Core framework only
pytest tests/core/ -v

# Business components only
pytest tests/components/ -v

# Workflows only
pytest tests/workflows/ -v
```

### By Pipeline
```bash
# Employee pipeline tests
pytest tests/components/employee/ tests/workflows/test_employee_workflow.py

# Sales pipeline tests
pytest tests/components/sales/ tests/workflows/test_sales_workflow.py
```

### With Coverage
```bash
# Core coverage
pytest tests/core/ --cov=airpipe.core

# Component coverage
pytest tests/components/employee/ --cov=airpipe.extractors.employee --cov=airpipe.transformers.employee
```

This separation ensures clean, maintainable, and focused testing across the AirPipe framework and its business logic components.