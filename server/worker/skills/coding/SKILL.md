---
name: python-clean-code-with-type-hints
description: Write clean, readable Python code with strong type hints and useful comments.
---

# Clean, Well-Commented Python Code with Type Hints

## Overview

This skill teaches how to write Python that is easy to read, maintain, and safely refactor. Good code uses clear names, small functions, type hints for contracts, and comments only where they add value. The goal is to help future readers understand both the *what* and the *why* without clutter.

## Key Principles

1. **Prefer clarity over cleverness** — choose simple control flow and descriptive names.
2. **Add type hints to public functions and important variables** — make interfaces explicit.
3. **Comment the intent, not the obvious** — explain business rules, assumptions, or tricky decisions.
4. **Keep functions focused** — one purpose per function, with small, testable units.
5. **Use docstrings for modules, classes, and public functions** — summarize behavior and arguments.

## Examples

### Function with type hints and a concise docstring
```python
from typing import Sequence

def average(values: Sequence[float]) -> float:
    """Return the arithmetic mean of a non-empty sequence."""
    if not values:
        raise ValueError("values must not be empty")
    return sum(values) / len(values)
```

### Commenting non-obvious logic
```python
def calculate_discount(total: float, is_member: bool) -> float:
    # Members get an extra 10% only on orders above the loyalty threshold.
    if is_member and total >= 100:
        return total * 0.9
    return total
```

### Typed data structure
```python
from dataclasses import dataclass

@dataclass
class User:
    id: int
    email: str
    is_active: bool = True
```

## Best Practices

- Use `list[str]`, `dict[str, int]`, `Optional[T]`, and `Union`/`|` where appropriate.
- Avoid redundant comments like `# increment i by 1`.
- Keep comments up to date; outdated comments are worse than none.
- Run static type checkers such as `mypy` or `pyright`.
- Prefer meaningful names over excessive comments.