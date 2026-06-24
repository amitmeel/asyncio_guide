# Chapter 6: Waiting Primitives

## Introduction

Asyncio provides several primitives for waiting on multiple concurrent operations. Each has specific use cases, ordering guarantees, and exception handling behavior. Understanding these primitives gives you precise control over concurrent execution.

By the end of this chapter, you'll understand:
- `gather()` for concurrent execution with result collection
- `wait()` for fine-grained control over completion
- `as_completed()` for processing results as they arrive
- `shield()` for protecting operations from cancellation
- Ordering guarantees and exception propagation
- When to use each primitive

---

## `gather()`: Concurrent Execution with Results

`gather()` is the most commonly used waiting primitive. It runs multiple awaitables concurrently and collects their results.

### Basic Usage

```python
import asyncio

async def fetch_data(source, delay):
    await asyncio.sleep(delay)
    return f"Data from {source}"

async def main():
    # Run three operations concurrently
    results = await asyncio.gather(
        fetch_data("API-1", 1.0),
        fetch_data("API-2", 0.5),
        fetch_data("API-3", 1.5)
    )
    
    print(results)
    # ['Data from API-1', 'Data from API-2', 'Data from API-3']

asyncio.run(main())
```

**Key characteristics:**
- Results returned in **input order**, not completion order
- All awaitables run concurrently
- Waits for all to complete
- Returns list of results

### Result Ordering

```python
import asyncio
import time

async def timed_task(name, duration):
    start = time.time()
    await asyncio.sleep(duration)
    elapsed = time.time() - start
    print(f"{name} completed after {elapsed:.2f}s")
    return f"{name} result"

async def main():
    results = await asyncio.gather(
        timed_task("Slow", 2.0),    # Completes last
        timed_task("Fast", 0.5),    # Completes first
        timed_task("Medium", 1.0)   # Completes second
    )
    
    print(f"\nResults in input order: {results}")

asyncio.run(main())

# Output:
# Fast completed after 0.50s
# Medium completed after 1.00s
# Slow completed after 2.00s
# 
# Results in input order: ['Slow result', 'Fast result', 'Medium result']
```

**Critical insight:** Results are always in the order you passed them to `gather()`, regardless of completion order.

---

## Exception Handling with `gather()`

`gather()` has two modes for handling exceptions.

### Default: Fail-Fast Mode

```python
import asyncio

async def successful_task(name):
    await asyncio.sleep(0.5)
    return f"{name} succeeded"

async def failing_task(name):
    await asyncio.sleep(0.2)
    raise ValueError(f"{name} failed!")

async def main():
    try:
        results = await asyncio.gather(
            successful_task("Task-1"),
            failing_task("Task-2"),
            successful_task("Task-3")
        )
    except ValueError as e:
        print(f"Caught exception: {e}")
        # Other tasks are NOT automatically cancelled!

asyncio.run(main())

# Output:
# Caught exception: Task-2 failed!
```

**Important:** In fail-fast mode:
- First exception is raised immediately
- Other tasks continue running
- Their results are lost
- No automatic cancellation

### `return_exceptions=True`: Collect All Results

```python
import asyncio

async def main():
    results = await asyncio.gather(
        successful_task("Task-1"),
        failing_task("Task-2"),
        successful_task("Task-3"),
        return_exceptions=True  # Don't raise, return exceptions
    )
    
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            print(f"Task {i+1}: Failed with {result}")
        else:
            print(f"Task {i+1}: {result}")

asyncio.run(main())

# Output:
# Task 1: Task-1 succeeded
# Task 2: Failed with Task-2 failed!
# Task 3: Task-3 succeeded
```

**Use `return_exceptions=True` when:**
- You want to handle failures individually
- Some failures are acceptable
- You need all results, including errors

---

## `gather()` with Pre-created Tasks

You can pass tasks (not just coroutines) to `gather()`.

### Mixing Coroutines and Tasks

```python
import asyncio

async def worker(name):
    await asyncio.sleep(0.5)
    return f"{name} done"

async def main():
    # Create some tasks early
    task1 = asyncio.create_task(worker("Task-1"))
    task2 = asyncio.create_task(worker("Task-2"))
    
    # Mix tasks and coroutines
    results = await asyncio.gather(
        task1,                    # Pre-created task
        task2,                    # Pre-created task
        worker("Task-3"),         # Coroutine (auto-wrapped in task)
        worker("Task-4")          # Coroutine (auto-wrapped in task)
    )
    
    print(results)

asyncio.run(main())
```

### Cancelling gather()

```python
import asyncio

async def long_task(name):
    try:
        print(f"{name}: Starting")
        await asyncio.sleep(10)
        print(f"{name}: Completed")
    except asyncio.CancelledError:
        print(f"{name}: Cancelled")
        raise

async def main():
    gather_task = asyncio.create_task(
        asyncio.gather(
            long_task("Task-1"),
            long_task("Task-2"),
            long_task("Task-3")
        )
    )
    
    # Let tasks start
    await asyncio.sleep(0.5)
    
    # Cancel the gather
    gather_task.cancel()
    
    try:
        await gather_task
    except asyncio.CancelledError:
        print("Gather cancelled")

asyncio.run(main())

# Output:
# Task-1: Starting
# Task-2: Starting
# Task-3: Starting
# Task-1: Cancelled
# Task-2: Cancelled
# Task-3: Cancelled
# Gather cancelled
```

**Key insight:** Cancelling `gather()` cancels all its child tasks.

---

## `wait()`: Fine-Grained Control

`wait()` provides more control than `gather()`. It returns when a specified condition is met.

### Basic Usage

```python
import asyncio

async def worker(name, duration):
    await asyncio.sleep(duration)
    return f"{name} result"

async def main():
    tasks = [
        asyncio.create_task(worker("Task-1", 1.0)),
        asyncio.create_task(worker("Task-2", 2.0)),
        asyncio.create_task(worker("Task-3", 3.0))
    ]
    
    # Wait for all to complete
    done, pending = await asyncio.wait(tasks)
    
    print(f"Done: {len(done)}, Pending: {len(pending)}")
    
    # Get results
    for task in done:
        print(f"Result: {task.result()}")

asyncio.run(main())

# Output:
# Done: 3, Pending: 0
# Result: Task-1 result
# Result: Task-2 result
# Result: Task-3 result
```

**Return value:** `wait()` returns two sets:
- `done`: Tasks that completed
- `pending`: Tasks still running

### Wait Strategies

`wait()` supports different completion strategies via the `return_when` parameter.

#### FIRST_COMPLETED: Return When Any Task Completes

```python
import asyncio

async def main():
    tasks = [
        asyncio.create_task(worker("Fast", 0.5)),
        asyncio.create_task(worker("Medium", 1.5)),
        asyncio.create_task(worker("Slow", 3.0))
    ]
    
    # Return as soon as ANY task completes
    done, pending = await asyncio.wait(
        tasks,
        return_when=asyncio.FIRST_COMPLETED
    )
    
    print(f"Done: {len(done)}, Pending: {len(pending)}")
    print(f"First result: {list(done)[0].result()}")
    
    # Cancel remaining tasks
    for task in pending:
        task.cancel()
    
    # Wait for cancellation
    await asyncio.wait(pending)

asyncio.run(main())

# Output:
# Done: 1, Pending: 2
# First result: Fast result
```

**Use case:** Racing multiple operations, taking the first result.

#### FIRST_EXCEPTION: Return When Any Task Fails

```python
import asyncio

async def good_task(name):
    await asyncio.sleep(2.0)
    return f"{name} succeeded"

async def bad_task(name):
    await asyncio.sleep(0.5)
    raise ValueError(f"{name} failed!")

async def main():
    tasks = [
        asyncio.create_task(good_task("Task-1")),
        asyncio.create_task(bad_task("Task-2")),
        asyncio.create_task(good_task("Task-3"))
    ]
    
    # Return as soon as ANY task raises exception
    done, pending = await asyncio.wait(
        tasks,
        return_when=asyncio.FIRST_EXCEPTION
    )
    
    print(f"Done: {len(done)}, Pending: {len(pending)}")
    
    # Check for exceptions
    for task in done:
        try:
            task.result()
        except Exception as e:
            print(f"Exception: {e}")
    
    # Cancel remaining
    for task in pending:
        task.cancel()
    await asyncio.wait(pending)

asyncio.run(main())

# Output:
# Done: 1, Pending: 2
# Exception: Task-2 failed!
```

**Use case:** Fail-fast behavior with manual cleanup.

#### ALL_COMPLETED: Wait for All (Default)

```python
async def main():
    tasks = [
        asyncio.create_task(worker("Task-1", 1.0)),
        asyncio.create_task(worker("Task-2", 2.0))
    ]
    
    # Wait for ALL tasks (default behavior)
    done, pending = await asyncio.wait(
        tasks,
        return_when=asyncio.ALL_COMPLETED
    )
    
    print(f"Done: {len(done)}, Pending: {len(pending)}")

asyncio.run(main())

# Output:
# Done: 2, Pending: 0
```

### Timeout with `wait()`

```python
import asyncio

async def main():
    tasks = [
        asyncio.create_task(worker("Task-1", 1.0)),
        asyncio.create_task(worker("Task-2", 5.0)),  # Long task
        asyncio.create_task(worker("Task-3", 2.0))
    ]
    
    # Wait with timeout
    done, pending = await asyncio.wait(
        tasks,
        timeout=2.5  # Wait max 2.5 seconds
    )
    
    print(f"Done: {len(done)}, Pending: {len(pending)}")
    
    # Process completed tasks
    for task in done:
        print(f"Completed: {task.result()}")
    
    # Cancel pending tasks
    for task in pending:
        print(f"Cancelling: {task.get_name()}")
        task.cancel()
    
    # Wait for cancellation
    await asyncio.wait(pending)

asyncio.run(main())

# Output:
# Done: 2, Pending: 1
# Completed: Task-1 result
# Completed: Task-3 result
# Cancelling: Task-2
```

---

## `as_completed()`: Process Results as They Arrive

`as_completed()` returns an iterator that yields tasks as they complete, allowing you to process results immediately.

### Basic Usage

```python
import asyncio
import time

async def fetch_data(source, delay):
    await asyncio.sleep(delay)
    return f"Data from {source}"

async def main():
    tasks = [
        fetch_data("Fast-API", 0.5),
        fetch_data("Slow-API", 2.0),
        fetch_data("Medium-API", 1.0)
    ]
    
    start = time.time()
    
    # Process results as they complete
    for coro in asyncio.as_completed(tasks):
        result = await coro
        elapsed = time.time() - start
        print(f"[{elapsed:.1f}s] Got: {result}")

asyncio.run(main())

# Output:
# [0.5s] Got: Data from Fast-API
# [1.0s] Got: Data from Medium-API
# [2.0s] Got: Data from Slow-API
```

**Key insight:** Results arrive in **completion order**, not input order.

### Real-world Example: Progressive UI Updates

```python
import asyncio
from typing import List, Dict

async def fetch_user_data(user_id: int, delay: float) -> Dict:
    """Simulate fetching user data with varying delays"""
    await asyncio.sleep(delay)
    return {
        "id": user_id,
        "name": f"User {user_id}",
        "delay": delay
    }

async def fetch_all_users_progressive(user_ids: List[int]):
    """Fetch users and display results as they arrive"""
    
    # Create tasks with varying delays
    tasks = [
        fetch_user_data(uid, delay=uid * 0.3)
        for uid in user_ids
    ]
    
    print("Fetching users...")
    completed = 0
    total = len(tasks)
    
    # Process as they complete
    for coro in asyncio.as_completed(tasks):
        user = await coro
        completed += 1
        print(f"[{completed}/{total}] Loaded: {user['name']} (took {user['delay']}s)")

async def main():
    await fetch_all_users_progressive([1, 2, 3, 4, 5])

asyncio.run(main())

# Output:
# Fetching users...
# [1/5] Loaded: User 1 (took 0.3s)
# [2/5] Loaded: User 2 (took 0.6s)
# [3/5] Loaded: User 3 (took 0.9s)
# [4/5] Loaded: User 4 (took 1.2s)
# [5/5] Loaded: User 5 (took 1.5s)
```

### Timeout with `as_completed()`

```python
import asyncio

async def main():
    tasks = [
        fetch_data("API-1", 0.5),
        fetch_data("API-2", 3.0),  # Too slow
        fetch_data("API-3", 1.0)
    ]
    
    # Process with timeout
    for coro in asyncio.as_completed(tasks, timeout=2.0):
        try:
            result = await coro
            print(f"Got: {result}")
        except asyncio.TimeoutError:
            print("Timeout reached!")
            break

asyncio.run(main())

# Output:
# Got: Data from API-1
# Got: Data from API-3
# Timeout reached!
```

---

## `shield()`: Protecting from Cancellation

`shield()` protects an operation from being cancelled, even if the parent task is cancelled.

### Basic Shield Usage

```python
import asyncio

async def critical_operation():
    """Operation that must complete"""
    print("Critical: Starting")
    await asyncio.sleep(2.0)
    print("Critical: Completed")
    return "Critical result"

async def main():
    # Shield the critical operation
    task = asyncio.create_task(
        asyncio.shield(critical_operation())
    )
    
    # Wait briefly then cancel
    await asyncio.sleep(0.5)
    task.cancel()
    
    try:
        result = await task
        print(f"Result: {result}")
    except asyncio.CancelledError:
        print("Task was cancelled, but critical operation continues")
        # The shielded operation is still running!

asyncio.run(main())

# Output:
# Critical: Starting
# Task was cancelled, but critical operation continues
# Critical: Completed
```

**Important:** `shield()` protects the operation from cancellation, but the task wrapping it can still be cancelled.

### Proper Shield Pattern

```python
import asyncio

async def save_to_database(data):
    """Critical save operation"""
    print(f"Saving {data}...")
    await asyncio.sleep(1.0)
    print(f"Saved {data}")
    return f"Saved: {data}"

async def process_with_shield(data):
    """Process data with shielded save"""
    try:
        # Do some work
        await asyncio.sleep(0.5)
        
        # Shield the critical save
        save_task = asyncio.create_task(save_to_database(data))
        result = await asyncio.shield(save_task)
        
        return result
        
    except asyncio.CancelledError:
        print("Process cancelled, but waiting for save to complete")
        # Wait for the shielded operation
        result = await save_task
        print(f"Save completed: {result}")
        raise  # Re-raise cancellation

async def main():
    task = asyncio.create_task(process_with_shield("important_data"))
    
    # Cancel after brief delay
    await asyncio.sleep(0.7)
    task.cancel()
    
    try:
        await task
    except asyncio.CancelledError:
        print("Main: Task cancelled")

asyncio.run(main())

# Output:
# Saving important_data...
# Process cancelled, but waiting for save to complete
# Saved important_data
# Save completed: Saved: important_data
# Main: Task cancelled
```

### When to Use Shield

**✅ Use shield for:**
- Database transactions
- File writes
- Network commits
- Cleanup operations
- Resource releases

**❌ Don't use shield for:**
- Regular operations
- Cancellable work
- Non-critical tasks

---

## Comparison: gather() vs wait() vs as_completed()

### Feature Comparison

| Feature | gather() | wait() | as_completed() |
|---------|----------|--------|----------------|
| Result order | Input order | Unordered (set) | Completion order |
| Return type | List | (done, pending) sets | Iterator |
| Exception handling | Raise or collect | Check task.result() | Check per result |
| Partial results | No | Yes (done set) | Yes (as they arrive) |
| Timeout | No | Yes | Yes |
| Cancellation | Cancels all | Manual | Manual |
| Best for | Simple concurrent ops | Fine control | Progressive processing |

### When to Use Each

#### Use `gather()` when:

```python
# Simple concurrent execution with all results
async def use_gather():
    results = await asyncio.gather(
        fetch_user(),
        fetch_posts(),
        fetch_comments()
    )
    user, posts, comments = results
    return {"user": user, "posts": posts, "comments": comments}
```

#### Use `wait()` when:

```python
# Need fine-grained control or timeout
async def use_wait():
    tasks = [create_task(fetch(i)) for i in range(10)]
    
    # Wait for first 5 to complete
    done, pending = await asyncio.wait(
        tasks,
        return_when=asyncio.FIRST_COMPLETED
    )
    
    # Process first result, cancel rest
    result = list(done)[0].result()
    for task in pending:
        task.cancel()
    
    return result
```

#### Use `as_completed()` when:

```python
# Process results progressively
async def use_as_completed():
    tasks = [fetch_data(i) for i in range(100)]
    
    results = []
    for coro in asyncio.as_completed(tasks):
        result = await coro
        results.append(result)
        # Update UI or progress bar
        print(f"Progress: {len(results)}/100")
    
    return results
```

---

## Real-world Example: Resilient API Client

```python
import asyncio
import aiohttp
from typing import List, Dict, Optional
import time

class ResilientAPIClient:
    def __init__(self, base_url: str, timeout: float = 5.0):
        self.base_url = base_url
        self.timeout = timeout
    
    async def fetch_endpoint(self, endpoint: str) -> Dict:
        """Fetch single endpoint with timeout"""
        async with aiohttp.ClientSession() as session:
            url = f"{self.base_url}/{endpoint}"
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=self.timeout)) as response:
                    return {
                        "endpoint": endpoint,
                        "status": response.status,
                        "data": await response.json()
                    }
            except asyncio.TimeoutError:
                return {"endpoint": endpoint, "error": "timeout"}
            except Exception as e:
                return {"endpoint": endpoint, "error": str(e)}
    
    async def fetch_all_progressive(self, endpoints: List[str]) -> List[Dict]:
        """Fetch all endpoints, processing results as they arrive"""
        tasks = [self.fetch_endpoint(ep) for ep in endpoints]
        
        results = []
        for coro in asyncio.as_completed(tasks):
            result = await coro
            results.append(result)
            
            if "error" in result:
                print(f"❌ {result['endpoint']}: {result['error']}")
            else:
                print(f"✅ {result['endpoint']}: Status {result['status']}")
        
        return results
    
    async def fetch_with_fallback(self, primary: str, fallback: str) -> Dict:
        """Try primary, fall back to secondary if it fails"""
        primary_task = asyncio.create_task(self.fetch_endpoint(primary))
        
        # Wait for primary with timeout
        done, pending = await asyncio.wait(
            [primary_task],
            timeout=self.timeout
        )
        
        if done and not primary_task.exception():
            # Primary succeeded
            return primary_task.result()
        else:
            # Primary failed or timed out, try fallback
            print(f"Primary {primary} failed, trying fallback {fallback}")
            primary_task.cancel()
            return await self.fetch_endpoint(fallback)
    
    async def fetch_fastest(self, endpoints: List[str]) -> Dict:
        """Race multiple endpoints, return fastest"""
        tasks = [
            asyncio.create_task(self.fetch_endpoint(ep))
            for ep in endpoints
        ]
        
        # Wait for first to complete
        done, pending = await asyncio.wait(
            tasks,
            return_when=asyncio.FIRST_COMPLETED
        )
        
        # Get first result
        result = list(done)[0].result()
        
        # Cancel remaining
        for task in pending:
            task.cancel()
        await asyncio.wait(pending)
        
        return result
    
    async def fetch_critical_with_shield(self, endpoint: str) -> Dict:
        """Fetch endpoint with shielded operation"""
        fetch_task = asyncio.create_task(self.fetch_endpoint(endpoint))
        
        try:
            # Shield from cancellation
            result = await asyncio.shield(fetch_task)
            return result
        except asyncio.CancelledError:
            # Even if cancelled, wait for fetch to complete
            print(f"Cancelled, but waiting for {endpoint} to complete")
            result = await fetch_task
            print(f"Fetch completed: {endpoint}")
            raise

async def main():
    client = ResilientAPIClient("https://api.example.com")
    
    # Progressive fetching
    print("=== Progressive Fetching ===")
    await client.fetch_all_progressive([
        "users/1",
        "users/2",
        "users/3"
    ])
    
    # Fallback pattern
    print("\n=== Fallback Pattern ===")
    result = await client.fetch_with_fallback(
        primary="primary-api/data",
        fallback="backup-api/data"
    )
    print(f"Result: {result}")
    
    # Racing pattern
    print("\n=== Racing Pattern ===")
    fastest = await client.fetch_fastest([
        "cdn1/data",
        "cdn2/data",
        "cdn3/data"
    ])
    print(f"Fastest: {fastest}")

asyncio.run(main())
```

---

## Best Practices

### ✅ DO: Use gather() for simple cases

```python
# Simple and clear
results = await asyncio.gather(
    fetch_user(),
    fetch_posts(),
    fetch_comments()
)
```

### ✅ DO: Use as_completed() for progressive updates

```python
# Good for UI updates
for coro in asyncio.as_completed(tasks):
    result = await coro
    update_progress_bar()
```

### ✅ DO: Use wait() for complex control flow

```python
# Fine-grained control
done, pending = await asyncio.wait(
    tasks,
    return_when=asyncio.FIRST_EXCEPTION
)
# Handle done and pending separately
```

### ✅ DO: Shield critical operations

```python
# Protect database commits
save_task = asyncio.create_task(save_to_db(data))
await asyncio.shield(save_task)
```

### ❌ DON'T: Forget to cancel pending tasks

```python
# BAD: Leaks tasks
done, pending = await asyncio.wait(tasks, timeout=5.0)
# Forgot to cancel pending!

# GOOD: Clean up
done, pending = await asyncio.wait(tasks, timeout=5.0)
for task in pending:
    task.cancel()
await asyncio.wait(pending)
```

---

## Summary: Waiting Primitives Mental Model

✅ **`gather()`** - Simple concurrent execution, results in input order

✅ **`wait()`** - Fine-grained control, returns done/pending sets

✅ **`as_completed()`** - Process results as they arrive, completion order

✅ **`shield()`** - Protect critical operations from cancellation

✅ **Choose based on needs:** simplicity vs control vs progressive processing

✅ **Always clean up** pending tasks after `wait()` or `as_completed()`

✅ **Use `return_exceptions=True`** in `gather()` for optional failures

---

## What's Next?

You now understand task orchestration primitives. Next, we'll explore communication patterns between tasks.

In [Chapter 7: Queues](../part3-communication/07-queues.md), we'll cover:
- Queue types (Queue, PriorityQueue, LifoQueue)
- Producer-consumer patterns
- Bounded queues and backpressure
- Work distribution systems
- Real-world queue implementations

Queues are essential for building scalable async systems.

---

**Previous:** [← Chapter 5: Structured Concurrency](./05-structured-concurrency.md)  
**Next:** [Chapter 7: Queues →](../part3-communication/07-queues.md)