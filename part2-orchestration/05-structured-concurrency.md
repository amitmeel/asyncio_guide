# Chapter 5: Structured Concurrency

## Introduction

Structured concurrency is one of the most important modern patterns in asyncio. Introduced in Python 3.11 with `TaskGroup`, it provides automatic cleanup, error propagation, and cancellation - making async code more robust and maintainable.

By the end of this chapter, you'll understand:
- Why structured concurrency matters
- How `TaskGroup` works internally
- Automatic failure propagation and sibling cancellation
- `ExceptionGroup` handling
- Scoped task lifetimes
- When to use `TaskGroup` vs `gather()`

---

## The Problem with Unstructured Concurrency

Before `TaskGroup`, managing multiple tasks was error-prone.

### The Old Way: Manual Task Management

```python
import asyncio

async def fetch_data(source):
    if source == "bad":
        raise ValueError(f"Error from {source}")
    await asyncio.sleep(0.1)
    return f"Data from {source}"

async def old_way():
    # Create tasks manually
    task1 = asyncio.create_task(fetch_data("source1"))
    task2 = asyncio.create_task(fetch_data("bad"))
    task3 = asyncio.create_task(fetch_data("source3"))
    
    try:
        results = await asyncio.gather(task1, task2, task3)
    except ValueError as e:
        print(f"Error: {e}")
        # BUG: Other tasks might still be running!
        # Need to manually cancel them
        task1.cancel()
        task3.cancel()
        # And wait for cancellation
        await asyncio.gather(task1, task3, return_exceptions=True)

asyncio.run(old_way())
```

**Problems:**
1. Manual cancellation required
2. Easy to forget cleanup
3. Tasks might leak
4. Complex error handling

---

## Structured Concurrency: The Solution

Structured concurrency ensures that:
1. All child tasks complete before parent continues
2. If one task fails, siblings are automatically cancelled
3. Resources are always cleaned up
4. Task lifetimes are scoped

### The Modern Way: TaskGroup

```python
import asyncio

async def fetch_data(source):
    if source == "bad":
        raise ValueError(f"Error from {source}")
    await asyncio.sleep(0.1)
    return f"Data from {source}"

async def modern_way():
    try:
        async with asyncio.TaskGroup() as tg:
            task1 = tg.create_task(fetch_data("source1"))
            task2 = tg.create_task(fetch_data("bad"))
            task3 = tg.create_task(fetch_data("source3"))
    except* ValueError as eg:
        print(f"Errors occurred: {eg.exceptions}")
        # All tasks automatically cancelled and cleaned up!

asyncio.run(modern_way())

# Output:
# Errors occurred: (ValueError('Error from bad'),)
```

**Benefits:**
1. ✅ Automatic cancellation of siblings
2. ✅ Guaranteed cleanup
3. ✅ No task leaks
4. ✅ Clear error handling with `ExceptionGroup`

---

## TaskGroup Deep Dive

Let's understand how `TaskGroup` works internally.

### Basic Usage

```python
import asyncio

async def worker(name, duration):
    print(f"{name}: Starting")
    await asyncio.sleep(duration)
    print(f"{name}: Done")
    return f"{name} result"

async def main():
    async with asyncio.TaskGroup() as tg:
        # Create tasks within the group
        task1 = tg.create_task(worker("Worker-1", 1))
        task2 = tg.create_task(worker("Worker-2", 2))
        task3 = tg.create_task(worker("Worker-3", 1.5))
    
    # All tasks complete before reaching here
    print("All workers complete")
    
    # Access results
    print(f"Results: {task1.result()}, {task2.result()}, {task3.result()}")

asyncio.run(main())

# Output:
# Worker-1: Starting
# Worker-2: Starting
# Worker-3: Starting
# Worker-1: Done
# Worker-3: Done
# Worker-2: Done
# All workers complete
# Results: Worker-1 result, Worker-2 result, Worker-3 result
```

### TaskGroup Lifecycle

```
┌─────────────────────────────────────────────────────────┐
│                    TaskGroup Lifecycle                   │
│                                                          │
│  1. Enter context (async with)                          │
│     ↓                                                    │
│  2. Create tasks with tg.create_task()                  │
│     ↓                                                    │
│  3. Tasks execute concurrently                          │
│     ↓                                                    │
│  4. Wait for all tasks to complete                      │
│     ↓                                                    │
│  5. Exit context                                        │
│     ↓                                                    │
│  6. If any task failed:                                 │
│     - Cancel all other tasks                            │
│     - Raise ExceptionGroup                              │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

### Scoped Lifetimes

```python
import asyncio

async def demonstrate_scoping():
    print("Before TaskGroup")
    
    async with asyncio.TaskGroup() as tg:
        print("Inside TaskGroup - creating tasks")
        tg.create_task(asyncio.sleep(1))
        tg.create_task(asyncio.sleep(2))
        print("Tasks created - still inside context")
        # Cannot exit until all tasks complete
    
    print("After TaskGroup - all tasks guaranteed complete")

asyncio.run(demonstrate_scoping())

# Output:
# Before TaskGroup
# Inside TaskGroup - creating tasks
# Tasks created - still inside context
# (2 second pause)
# After TaskGroup - all tasks guaranteed complete
```

**Key insight:** You cannot exit the `async with` block until all tasks complete. This guarantees no orphan tasks.

---

## Failure Propagation

When one task fails, `TaskGroup` automatically handles cleanup.

### Single Task Failure

```python
import asyncio

async def good_task(name):
    await asyncio.sleep(0.5)
    print(f"{name}: Completed successfully")
    return f"{name} result"

async def bad_task(name):
    await asyncio.sleep(0.2)
    print(f"{name}: About to fail")
    raise ValueError(f"{name} failed!")

async def main():
    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(good_task("Task-1"))
            tg.create_task(bad_task("Task-2"))
            tg.create_task(good_task("Task-3"))
    except* ValueError as eg:
        print(f"\nCaught {len(eg.exceptions)} exception(s):")
        for exc in eg.exceptions:
            print(f"  - {exc}")

asyncio.run(main())

# Output:
# Task-2: About to fail
# 
# Caught 1 exception(s):
#   - Task-2 failed!
```

**What happened:**
1. Task-2 failed after 0.2s
2. TaskGroup immediately cancelled Task-1 and Task-3
3. All tasks cleaned up
4. ExceptionGroup raised with Task-2's exception

### Multiple Task Failures

```python
import asyncio

async def failing_task(name, delay):
    await asyncio.sleep(delay)
    raise ValueError(f"{name} failed after {delay}s")

async def main():
    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(failing_task("Task-1", 0.1))
            tg.create_task(failing_task("Task-2", 0.2))
            tg.create_task(failing_task("Task-3", 0.3))
    except* ValueError as eg:
        print(f"Caught {len(eg.exceptions)} exceptions:")
        for exc in eg.exceptions:
            print(f"  - {exc}")

asyncio.run(main())

# Output:
# Caught 3 exceptions:
#   - Task-1 failed after 0.1s
#   - Task-2 failed after 0.2s
#   - Task-3 failed after 0.3s
```

**Note:** All three exceptions are collected and raised together as an `ExceptionGroup`.

---

## Sibling Cancellation

When one task fails, all sibling tasks are automatically cancelled.

### Observing Cancellation

```python
import asyncio

async def long_running_task(name):
    try:
        print(f"{name}: Starting long operation")
        await asyncio.sleep(10)  # Long operation
        print(f"{name}: Completed")  # Won't reach here
    except asyncio.CancelledError:
        print(f"{name}: Cancelled!")
        raise  # Must re-raise

async def failing_task():
    await asyncio.sleep(0.5)
    raise ValueError("Intentional failure")

async def main():
    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(long_running_task("Task-1"))
            tg.create_task(long_running_task("Task-2"))
            tg.create_task(failing_task())
    except* ValueError:
        print("Main: Caught failure")

asyncio.run(main())

# Output:
# Task-1: Starting long operation
# Task-2: Starting long operation
# Task-1: Cancelled!
# Task-2: Cancelled!
# Main: Caught failure
```

**Timeline:**

```
Time  | Task-1              | Task-2              | failing_task
------|---------------------|---------------------|------------------
0.0s  | Start sleep(10)     | Start sleep(10)     | Start sleep(0.5)
0.5s  | (sleeping)          | (sleeping)          | Raise ValueError
0.5s  | Cancelled!          | Cancelled!          | Exception raised
0.5s  | CancelledError      | CancelledError      | 
```

---

## ExceptionGroup Handling

`ExceptionGroup` is a special exception that contains multiple exceptions.

### Basic ExceptionGroup Handling

```python
import asyncio

async def main():
    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(failing_task("A", ValueError("Error A")))
            tg.create_task(failing_task("B", TypeError("Error B")))
            tg.create_task(failing_task("C", RuntimeError("Error C")))
    except ExceptionGroup as eg:
        print(f"ExceptionGroup with {len(eg.exceptions)} exceptions:")
        for exc in eg.exceptions:
            print(f"  {type(exc).__name__}: {exc}")

async def failing_task(name, exception):
    await asyncio.sleep(0.1)
    raise exception

asyncio.run(main())

# Output:
# ExceptionGroup with 3 exceptions:
#   ValueError: Error A
#   TypeError: Error B
#   RuntimeError: Error C
```

### Selective Exception Handling with `except*`

```python
import asyncio

async def main():
    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(failing_task("A", ValueError("Value error")))
            tg.create_task(failing_task("B", TypeError("Type error")))
            tg.create_task(failing_task("C", ValueError("Another value error")))
    except* ValueError as eg:
        print(f"Caught {len(eg.exceptions)} ValueError(s):")
        for exc in eg.exceptions:
            print(f"  - {exc}")
    except* TypeError as eg:
        print(f"Caught {len(eg.exceptions)} TypeError(s):")
        for exc in eg.exceptions:
            print(f"  - {exc}")

asyncio.run(main())

# Output:
# Caught 2 ValueError(s):
#   - Value error
#   - Another value error
# Caught 1 TypeError(s):
#   - Type error
```

**Key insight:** `except*` allows handling specific exception types from an `ExceptionGroup`.

### Nested ExceptionGroups

```python
import asyncio

async def nested_failures():
    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(failing_task("Inner", ValueError("Inner error")))
    except ExceptionGroup as eg:
        # Re-raise as part of outer group
        raise RuntimeError("Outer error") from eg

async def main():
    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(nested_failures())
            tg.create_task(failing_task("Direct", TypeError("Direct error")))
    except ExceptionGroup as eg:
        print("Exception tree:")
        print_exception_tree(eg)

def print_exception_tree(eg, indent=0):
    for exc in eg.exceptions:
        print("  " * indent + f"- {type(exc).__name__}: {exc}")
        if isinstance(exc, ExceptionGroup):
            print_exception_tree(exc, indent + 1)
        elif exc.__cause__:
            print("  " * (indent + 1) + f"Caused by: {exc.__cause__}")

asyncio.run(main())
```

---

## TaskGroup vs gather()

Understanding when to use each is important.

### gather(): Flexible but Manual

```python
import asyncio

async def with_gather():
    tasks = [
        asyncio.create_task(worker(i))
        for i in range(5)
    ]
    
    # Flexible options
    results = await asyncio.gather(
        *tasks,
        return_exceptions=True  # Don't cancel on failure
    )
    
    # Manual error handling
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            print(f"Task {i} failed: {result}")
        else:
            print(f"Task {i} result: {result}")

async def worker(n):
    if n == 2:
        raise ValueError(f"Worker {n} failed")
    await asyncio.sleep(0.1)
    return f"Result {n}"

asyncio.run(with_gather())

# Output:
# Task 0 result: Result 0
# Task 1 result: Result 1
# Task 2 failed: Worker 2 failed
# Task 3 result: Result 3
# Task 4 result: Result 4
```

### TaskGroup: Structured and Safe

```python
import asyncio

async def with_taskgroup():
    try:
        async with asyncio.TaskGroup() as tg:
            tasks = [
                tg.create_task(worker(i))
                for i in range(5)
            ]
    except* ValueError as eg:
        print(f"Some tasks failed: {eg.exceptions}")
        # All tasks automatically cancelled and cleaned up

asyncio.run(with_taskgroup())

# Output:
# Some tasks failed: (ValueError('Worker 2 failed'),)
```

### Comparison Table

| Feature | gather() | TaskGroup |
|---------|----------|-----------|
| Automatic cancellation | ❌ Manual | ✅ Automatic |
| Cleanup guarantee | ❌ Manual | ✅ Automatic |
| Scoped lifetimes | ❌ No | ✅ Yes |
| Exception handling | Single exception or list | ExceptionGroup |
| Flexibility | High (return_exceptions) | Lower (fail-fast) |
| Best for | Optional failures | Critical operations |

### When to Use Each

**Use `gather()` when:**
- You want to continue even if some tasks fail
- You need `return_exceptions=True` behavior
- You're working with pre-created tasks
- You need maximum flexibility

```python
# Good use of gather()
async def fetch_optional_data():
    results = await asyncio.gather(
        fetch_user_profile(),
        fetch_user_preferences(),  # Optional
        fetch_user_history(),       # Optional
        return_exceptions=True
    )
    # Process results, ignoring failures
```

**Use `TaskGroup` when:**
- All tasks must succeed
- You want automatic cleanup
- You need structured concurrency
- You want clear error propagation

```python
# Good use of TaskGroup
async def process_critical_data():
    async with asyncio.TaskGroup() as tg:
        tg.create_task(validate_data())
        tg.create_task(transform_data())
        tg.create_task(save_data())
    # All succeeded or all cancelled
```

---

## Real-world Example: Parallel Data Processing

```python
import asyncio
from typing import List, Dict
import aiohttp

class DataProcessor:
    def __init__(self, api_url: str):
        self.api_url = api_url
    
    async def fetch_item(self, item_id: int) -> Dict:
        """Fetch single item from API"""
        async with aiohttp.ClientSession() as session:
            url = f"{self.api_url}/items/{item_id}"
            async with session.get(url) as response:
                if response.status != 200:
                    raise ValueError(f"Failed to fetch item {item_id}")
                return await response.json()
    
    async def process_item(self, item: Dict) -> Dict:
        """Process item data"""
        await asyncio.sleep(0.1)  # Simulate processing
        return {
            "id": item["id"],
            "processed": True,
            "value": item.get("value", 0) * 2
        }
    
    async def save_item(self, item: Dict) -> None:
        """Save processed item"""
        await asyncio.sleep(0.05)  # Simulate save
        print(f"Saved item {item['id']}")
    
    async def process_batch(self, item_ids: List[int]) -> List[Dict]:
        """Process batch of items with structured concurrency"""
        try:
            # Phase 1: Fetch all items
            async with asyncio.TaskGroup() as tg:
                fetch_tasks = [
                    tg.create_task(self.fetch_item(item_id))
                    for item_id in item_ids
                ]
            
            # All fetches succeeded
            items = [task.result() for task in fetch_tasks]
            
            # Phase 2: Process all items
            async with asyncio.TaskGroup() as tg:
                process_tasks = [
                    tg.create_task(self.process_item(item))
                    for item in items
                ]
            
            # All processing succeeded
            processed_items = [task.result() for task in process_tasks]
            
            # Phase 3: Save all items
            async with asyncio.TaskGroup() as tg:
                for item in processed_items:
                    tg.create_task(self.save_item(item))
            
            # All saves succeeded
            return processed_items
            
        except* ValueError as eg:
            print(f"Fetch errors: {eg.exceptions}")
            raise
        except* Exception as eg:
            print(f"Processing errors: {eg.exceptions}")
            raise

async def main():
    processor = DataProcessor("https://api.example.com")
    
    try:
        results = await processor.process_batch([1, 2, 3, 4, 5])
        print(f"Successfully processed {len(results)} items")
    except ExceptionGroup as eg:
        print(f"Batch processing failed with {len(eg.exceptions)} errors")

asyncio.run(main())
```

**Benefits of this approach:**
1. Clear phases (fetch → process → save)
2. Automatic rollback if any phase fails
3. No partial state
4. Clean error handling

---

## Advanced Patterns

### Nested TaskGroups

```python
import asyncio

async def process_category(category: str, items: List[int]):
    """Process all items in a category"""
    async with asyncio.TaskGroup() as tg:
        for item in items:
            tg.create_task(process_item(category, item))

async def process_all_categories():
    """Process multiple categories in parallel"""
    categories = {
        "electronics": [1, 2, 3],
        "books": [4, 5, 6],
        "clothing": [7, 8, 9]
    }
    
    async with asyncio.TaskGroup() as tg:
        for category, items in categories.items():
            tg.create_task(process_category(category, items))
    
    print("All categories processed")

async def process_item(category: str, item_id: int):
    await asyncio.sleep(0.1)
    print(f"Processed {category}/{item_id}")

asyncio.run(process_all_categories())
```

### Conditional Task Creation

```python
import asyncio

async def conditional_processing(items: List[Dict]):
    """Create tasks conditionally based on item properties"""
    async with asyncio.TaskGroup() as tg:
        for item in items:
            if item.get("needs_validation"):
                tg.create_task(validate_item(item))
            
            if item.get("needs_enrichment"):
                tg.create_task(enrich_item(item))
            
            # Always process
            tg.create_task(process_item(item))
    
    print("All conditional processing complete")
```

### Timeout with TaskGroup

```python
import asyncio

async def with_timeout():
    """TaskGroup with overall timeout"""
    try:
        async with asyncio.timeout(5.0):
            async with asyncio.TaskGroup() as tg:
                tg.create_task(long_task(1))
                tg.create_task(long_task(2))
                tg.create_task(long_task(3))
    except asyncio.TimeoutError:
        print("TaskGroup timed out - all tasks cancelled")

async def long_task(n):
    try:
        await asyncio.sleep(10)
    except asyncio.CancelledError:
        print(f"Task {n} cancelled")
        raise

asyncio.run(with_timeout())

# Output:
# Task 1 cancelled
# Task 2 cancelled
# Task 3 cancelled
# TaskGroup timed out - all tasks cancelled
```

---

## Best Practices

### ✅ DO: Use TaskGroup for critical operations

```python
async def good_critical_operation():
    async with asyncio.TaskGroup() as tg:
        tg.create_task(validate_input())
        tg.create_task(check_permissions())
        tg.create_task(verify_resources())
    # All checks passed or all cancelled
```

### ✅ DO: Handle ExceptionGroup properly

```python
async def good_error_handling():
    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(risky_operation())
    except* ValueError as eg:
        log_errors(eg.exceptions)
        # Handle specific error type
    except* Exception as eg:
        log_errors(eg.exceptions)
        # Handle other errors
```

### ❌ DON'T: Swallow CancelledError in tasks

```python
async def bad_task():
    try:
        await asyncio.sleep(10)
    except asyncio.CancelledError:
        print("Cancelled")
        # BUG: Not re-raising!
        # This breaks TaskGroup cancellation

async def good_task():
    try:
        await asyncio.sleep(10)
    except asyncio.CancelledError:
        print("Cancelled")
        raise  # ✅ Must re-raise
```

### ✅ DO: Use nested TaskGroups for phases

```python
async def good_phased_operation():
    # Phase 1
    async with asyncio.TaskGroup() as tg:
        tg.create_task(fetch_data())
    
    # Phase 2 (only if phase 1 succeeded)
    async with asyncio.TaskGroup() as tg:
        tg.create_task(process_data())
    
    # Phase 3 (only if phase 2 succeeded)
    async with asyncio.TaskGroup() as tg:
        tg.create_task(save_data())
```

---

## Summary: Structured Concurrency Mental Model

✅ **TaskGroup provides structured concurrency** - scoped task lifetimes

✅ **Automatic cleanup** - no orphan tasks, guaranteed resource cleanup

✅ **Fail-fast behavior** - one failure cancels all siblings

✅ **ExceptionGroup** - collect and handle multiple exceptions

✅ **Use `except*`** for selective exception handling

✅ **Prefer TaskGroup** for critical operations where all must succeed

✅ **Use gather()** when you need flexibility or optional failures

✅ **Always re-raise CancelledError** in tasks

---

## What's Next?

Now that you understand structured concurrency, we'll explore other waiting primitives.

In [Chapter 6: Waiting Primitives](./06-waiting-primitives.md), we'll cover:
- `gather()` in depth
- `wait()` for fine-grained control
- `as_completed()` for processing results as they arrive
- `shield()` for protecting from cancellation
- Ordering guarantees and exception propagation

These primitives give you precise control over concurrent execution.

---

**Previous:** [← Chapter 4: Tasks and Scheduling](./04-tasks-and-scheduling.md)  
**Next:** [Chapter 6: Waiting Primitives →](./06-waiting-primitives.md)