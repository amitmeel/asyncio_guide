# Chapter 4: Tasks and Scheduling

## Introduction

Tasks are the workhorses of asyncio - they wrap coroutines and schedule them for execution. Understanding tasks deeply is essential for building robust async applications.

By the end of this chapter, you'll understand:
- What tasks are and how they differ from coroutines
- The complete task lifecycle
- Task states and transitions
- How to create and manage tasks
- Orphan tasks and cleanup strategies
- Eager task execution (Python 3.12+)
- What the event loop does with tasks

---

## What is a Task?

A **Task** is a wrapper around a coroutine that schedules it for execution on the event loop. While a coroutine is just a suspended function, a task actively manages its execution.

### Coroutine vs Task

```python
import asyncio

async def my_coroutine():
    await asyncio.sleep(1)
    return "done"

async def main():
    # Just a coroutine object - not scheduled
    coro = my_coroutine()
    print(type(coro))  # <class 'coroutine'>
    
    # Task - scheduled for execution
    task = asyncio.create_task(my_coroutine())
    print(type(task))  # <class 'asyncio.Task'>
    
    # Clean up
    coro.close()
    await task

asyncio.run(main())
```

**Key differences:**

| Coroutine | Task |
|-----------|------|
| Passive - must be awaited | Active - runs automatically |
| Not scheduled | Scheduled on event loop |
| No state tracking | Tracks state (pending/running/done) |
| Can't be cancelled directly | Can be cancelled |
| No result storage | Stores result or exception |

---

## Creating Tasks: `create_task()`

The primary way to create tasks is with `asyncio.create_task()`.

### Basic Task Creation

```python
import asyncio

async def background_work(name, duration):
    print(f"{name}: Starting")
    await asyncio.sleep(duration)
    print(f"{name}: Done")
    return f"{name} result"

async def main():
    # Create task - immediately scheduled
    task = asyncio.create_task(background_work("Task-1", 2))
    
    print("Task created, doing other work...")
    await asyncio.sleep(1)
    print("Still doing other work...")
    
    # Wait for task to complete
    result = await task
    print(f"Result: {result}")

asyncio.run(main())

# Output:
# Task created, doing other work...
# Task-1: Starting
# Still doing other work...
# Task-1: Done
# Result: Task-1 result
```

**What happens:**

```
Time  | Main                          | Task-1
------|-------------------------------|------------------
0.0s  | create_task()                 | Scheduled
0.0s  | print("Task created...")      | 
0.0s  | await sleep(1)                | Starts: print("Starting")
0.0s  |                               | await sleep(2)
1.0s  | Resumes: print("Still...")    | (sleeping)
1.0s  | await task                    | (sleeping)
2.0s  |                               | Resumes: print("Done")
2.0s  | Resumes with result           | Returns result
```

### Task Naming (Python 3.8+)

```python
import asyncio

async def worker():
    await asyncio.sleep(1)
    return "done"

async def main():
    # Named task - helpful for debugging
    task = asyncio.create_task(worker(), name="worker-1")
    print(f"Task name: {task.get_name()}")
    
    # Can change name
    task.set_name("renamed-worker")
    print(f"New name: {task.get_name()}")
    
    await task

asyncio.run(main())
```

---

## Task Lifecycle

Tasks go through a well-defined lifecycle from creation to completion.

### Task States

```python
import asyncio

async def lifecycle_demo():
    await asyncio.sleep(0.1)
    return "completed"

async def main():
    task = asyncio.create_task(lifecycle_demo())
    
    # State 1: Pending (not yet done)
    print(f"1. Pending: {not task.done()}")
    print(f"   Cancelled: {task.cancelled()}")
    
    # Let it run
    await asyncio.sleep(0.05)
    
    # State 2: Still pending (running)
    print(f"2. Pending: {not task.done()}")
    
    # Wait for completion
    result = await task
    
    # State 3: Done
    print(f"3. Done: {task.done()}")
    print(f"   Result: {task.result()}")

asyncio.run(main())

# Output:
# 1. Pending: True
#    Cancelled: False
# 2. Pending: True
# 3. Done: True
#    Result: completed
```

### Complete State Diagram

```
┌─────────────┐
│   CREATED   │ (task = create_task(coro))
└──────┬──────┘
       │
       ↓
┌─────────────┐
│   PENDING   │ (scheduled, not yet done)
└──────┬──────┘
       │
       ├──→ [Running] ──→ [Suspended] ──→ [Running] ──→ ...
       │
       ↓
┌─────────────┐
│    DONE     │ (completed with result or exception)
└─────────────┘
       │
       ├──→ [Result available]
       └──→ [Exception raised]

Special transition:
PENDING ──→ CANCELLED (via task.cancel())
```

### Checking Task State

```python
import asyncio

async def check_states():
    async def worker():
        await asyncio.sleep(1)
        return 42
    
    task = asyncio.create_task(worker())
    
    # Check various states
    print(f"Done: {task.done()}")           # False
    print(f"Cancelled: {task.cancelled()}") # False
    
    # Try to get result before done - raises InvalidStateError
    try:
        task.result()
    except asyncio.InvalidStateError:
        print("Can't get result - task not done yet")
    
    # Wait for completion
    await task
    
    print(f"Done: {task.done()}")           # True
    print(f"Result: {task.result()}")       # 42

asyncio.run(check_states())
```

---

## Task Scheduling Semantics

Understanding when and how tasks are scheduled is crucial.

### Immediate Scheduling

```python
import asyncio

async def immediate_demo():
    print("A: Before create_task")
    
    task = asyncio.create_task(worker())
    # Task is scheduled but hasn't run yet!
    
    print("B: After create_task")
    
    # Yield control to let task start
    await asyncio.sleep(0)
    
    print("C: After yield")
    await task

async def worker():
    print("  Worker: Running")
    await asyncio.sleep(0.1)
    print("  Worker: Done")

asyncio.run(immediate_demo())

# Output:
# A: Before create_task
# B: After create_task
# Worker: Running
# C: After yield
# Worker: Done
```

**Key insight:** `create_task()` schedules the task but doesn't run it immediately. You must yield control (with `await`) for it to start.

### Execution Order

```python
import asyncio

async def task_a():
    print("A: Start")
    await asyncio.sleep(0)
    print("A: End")

async def task_b():
    print("B: Start")
    await asyncio.sleep(0)
    print("B: End")

async def main():
    # Create tasks in order
    t1 = asyncio.create_task(task_a())
    t2 = asyncio.create_task(task_b())
    
    # Yield control
    await asyncio.sleep(0)
    
    # Wait for both
    await asyncio.gather(t1, t2)

asyncio.run(main())

# Output:
# A: Start
# B: Start
# A: End
# B: End
```

**Execution timeline:**

```
Ready Queue: [main]
→ main: create_task(task_a) → adds task_a to queue
Ready Queue: [main, task_a]
→ main: create_task(task_b) → adds task_b to queue
Ready Queue: [main, task_a, task_b]
→ main: await sleep(0) → main yields
Ready Queue: [task_a, task_b, main]
→ task_a: print("A: Start")
→ task_a: await sleep(0) → task_a yields
Ready Queue: [task_b, main, task_a]
→ task_b: print("B: Start")
→ task_b: await sleep(0) → task_b yields
Ready Queue: [main, task_a, task_b]
→ main: resumes
→ main: await gather() → waits for tasks
Ready Queue: [task_a, task_b]
→ task_a: print("A: End")
→ task_b: print("B: End")
```

---

## Task References and Garbage Collection

Tasks must be referenced to prevent premature garbage collection.

### The Orphan Task Problem

```python
import asyncio

async def orphan_worker():
    print("Worker: Starting")
    await asyncio.sleep(2)
    print("Worker: Done")  # May never print!

async def bad_example():
    # BUG: Task created but not referenced
    asyncio.create_task(orphan_worker())
    
    # Main completes immediately
    print("Main: Done")
    # Task may be garbage collected!

asyncio.run(bad_example())

# Output (may vary):
# Main: Done
# Worker: Starting
# (Worker may not complete!)
```

**Problem:** The task has no strong reference, so it may be garbage collected before completing.

### Solution 1: Keep References

```python
import asyncio

async def good_example():
    # Keep reference to task
    task = asyncio.create_task(orphan_worker())
    
    print("Main: Doing other work")
    await asyncio.sleep(1)
    
    # Wait for task
    await task
    print("Main: Done")

asyncio.run(good_example())

# Output:
# Main: Doing other work
# Worker: Starting
# Worker: Done
# Main: Done
```

### Solution 2: Task Groups (Modern Approach)

```python
import asyncio

async def modern_example():
    async with asyncio.TaskGroup() as tg:
        # Tasks are automatically tracked
        tg.create_task(orphan_worker())
        tg.create_task(orphan_worker())
    
    # All tasks complete before exiting context
    print("Main: All tasks done")

asyncio.run(modern_example())
```

### Tracking Background Tasks

```python
import asyncio
from typing import Set

# Global set to track background tasks
background_tasks: Set[asyncio.Task] = set()

async def background_worker(name):
    print(f"{name}: Starting")
    await asyncio.sleep(1)
    print(f"{name}: Done")

async def create_background_task(name):
    task = asyncio.create_task(background_worker(name))
    
    # Add to set to keep reference
    background_tasks.add(task)
    
    # Remove from set when done
    task.add_done_callback(background_tasks.discard)
    
    return task

async def main():
    # Create multiple background tasks
    await create_background_task("Task-1")
    await create_background_task("Task-2")
    await create_background_task("Task-3")
    
    # Do other work
    await asyncio.sleep(0.5)
    
    # Wait for all background tasks
    await asyncio.gather(*background_tasks)
    print("All background tasks complete")

asyncio.run(main())
```

---

## Task Results and Exceptions

Tasks store their results or exceptions for later retrieval.

### Getting Task Results

```python
import asyncio

async def compute(x):
    await asyncio.sleep(0.1)
    return x * 2

async def main():
    task = asyncio.create_task(compute(21))
    
    # Wait for completion
    await task
    
    # Get result (can call multiple times)
    result = task.result()
    print(f"Result: {result}")
    
    # Can also get result again
    print(f"Result again: {task.result()}")

asyncio.run(main())

# Output:
# Result: 42
# Result again: 42
```

### Handling Task Exceptions

```python
import asyncio

async def failing_task():
    await asyncio.sleep(0.1)
    raise ValueError("Something went wrong!")

async def main():
    task = asyncio.create_task(failing_task())
    
    try:
        await task
    except ValueError as e:
        print(f"Caught exception: {e}")
    
    # Exception is stored in task
    try:
        task.result()  # Re-raises exception
    except ValueError as e:
        print(f"Exception from result(): {e}")
    
    # Check if task raised exception
    print(f"Exception: {task.exception()}")

asyncio.run(main())

# Output:
# Caught exception: Something went wrong!
# Exception from result(): Something went wrong!
# Exception: Something went wrong!
```

### Exception Propagation

```python
import asyncio

async def level_3():
    await asyncio.sleep(0.1)
    raise RuntimeError("Error in level 3")

async def level_2():
    await level_3()

async def level_1():
    task = asyncio.create_task(level_2())
    await task  # Exception propagates here

async def main():
    try:
        await level_1()
    except RuntimeError as e:
        print(f"Caught: {e}")

asyncio.run(main())

# Output:
# Caught: Error in level 3
```

---

## Task Cancellation

Tasks can be cancelled, which is essential for timeouts and cleanup.

### Basic Cancellation

```python
import asyncio

async def long_running_task():
    try:
        print("Task: Starting")
        await asyncio.sleep(10)  # Long operation
        print("Task: Completed")
    except asyncio.CancelledError:
        print("Task: Cancelled!")
        raise  # Must re-raise

async def main():
    task = asyncio.create_task(long_running_task())
    
    # Let it start
    await asyncio.sleep(0.1)
    
    # Cancel it
    task.cancel()
    
    try:
        await task
    except asyncio.CancelledError:
        print("Main: Task was cancelled")

asyncio.run(main())

# Output:
# Task: Starting
# Task: Cancelled!
# Main: Task was cancelled
```

### Cancellation with Message (Python 3.9+)

```python
import asyncio

async def cancellable_task():
    try:
        await asyncio.sleep(10)
    except asyncio.CancelledError as e:
        print(f"Cancelled with message: {e.args[0] if e.args else 'No message'}")
        raise

async def main():
    task = asyncio.create_task(cancellable_task())
    await asyncio.sleep(0.1)
    
    # Cancel with custom message
    task.cancel("User requested cancellation")
    
    try:
        await task
    except asyncio.CancelledError:
        pass

asyncio.run(main())

# Output:
# Cancelled with message: User requested cancellation
```

### Checking Cancellation Status

```python
import asyncio

async def main():
    async def worker():
        await asyncio.sleep(1)
    
    task = asyncio.create_task(worker())
    
    print(f"Cancelled: {task.cancelled()}")  # False
    
    task.cancel()
    
    print(f"Cancelled: {task.cancelled()}")  # True
    
    try:
        await task
    except asyncio.CancelledError:
        pass
    
    print(f"Done: {task.done()}")            # True
    print(f"Cancelled: {task.cancelled()}")  # True

asyncio.run(main())
```

---

## Task Callbacks

Tasks can have callbacks that execute when they complete.

### Adding Done Callbacks

```python
import asyncio

def task_done_callback(task):
    print(f"Callback: Task {task.get_name()} completed")
    try:
        result = task.result()
        print(f"Callback: Result = {result}")
    except Exception as e:
        print(f"Callback: Exception = {e}")

async def worker(value):
    await asyncio.sleep(0.1)
    return value * 2

async def main():
    task = asyncio.create_task(worker(21), name="worker-1")
    
    # Add callback
    task.add_done_callback(task_done_callback)
    
    # Wait for completion
    await task
    print("Main: Task awaited")

asyncio.run(main())

# Output:
# Callback: Task worker-1 completed
# Callback: Result = 42
# Main: Task awaited
```

### Multiple Callbacks

```python
import asyncio

async def main():
    task = asyncio.create_task(asyncio.sleep(0.1))
    
    # Add multiple callbacks
    task.add_done_callback(lambda t: print("Callback 1"))
    task.add_done_callback(lambda t: print("Callback 2"))
    task.add_done_callback(lambda t: print("Callback 3"))
    
    await task

asyncio.run(main())

# Output:
# Callback 1
# Callback 2
# Callback 3
```

### Callback for Cleanup

```python
import asyncio

class ResourceManager:
    def __init__(self):
        self.active_tasks = set()
    
    def create_task(self, coro):
        task = asyncio.create_task(coro)
        self.active_tasks.add(task)
        
        # Auto-cleanup when done
        task.add_done_callback(self.active_tasks.discard)
        
        return task
    
    async def wait_all(self):
        if self.active_tasks:
            await asyncio.gather(*self.active_tasks, return_exceptions=True)

async def main():
    manager = ResourceManager()
    
    # Create tasks
    manager.create_task(asyncio.sleep(0.1))
    manager.create_task(asyncio.sleep(0.2))
    manager.create_task(asyncio.sleep(0.3))
    
    print(f"Active tasks: {len(manager.active_tasks)}")
    
    # Wait for all
    await manager.wait_all()
    
    print(f"Active tasks: {len(manager.active_tasks)}")

asyncio.run(main())

# Output:
# Active tasks: 3
# Active tasks: 0
```

---

## Eager Task Execution (Python 3.12+)

Python 3.12 introduced eager task execution for better performance.

### Traditional vs Eager Execution

```python
import asyncio
import sys

async def worker():
    print("Worker executing")
    return 42

async def traditional():
    print("Creating task")
    task = asyncio.create_task(worker())
    print("Task created")
    # Worker hasn't run yet - needs await to start
    await asyncio.sleep(0)  # Yield control
    result = await task
    return result

async def eager():
    print("Creating eager task")
    # In Python 3.12+, task starts immediately if possible
    task = asyncio.create_task(worker())
    print("Task created")
    # Worker may have already started!
    result = await task
    return result

# Python 3.12+ behavior
if sys.version_info >= (3, 12):
    print("=== Eager Execution (Python 3.12+) ===")
    asyncio.run(eager())
else:
    print("=== Traditional Execution ===")
    asyncio.run(traditional())
```

### Eager Task Factory

```python
import asyncio
import sys

if sys.version_info >= (3, 12):
    async def main():
        # Set eager task factory
        loop = asyncio.get_running_loop()
        loop.set_task_factory(asyncio.eager_task_factory)
        
        # Tasks now execute eagerly
        task = asyncio.create_task(worker())
        # worker() may have already started!
        
        await task
    
    asyncio.run(main())
```

---

## Real-world Example: Concurrent API Requests

Let's build a practical example managing multiple tasks:

```python
import asyncio
import aiohttp
from typing import List, Dict
import time

class APIClient:
    def __init__(self, base_url: str, max_concurrent: int = 10):
        self.base_url = base_url
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.active_tasks: set = set()
    
    async def fetch(self, endpoint: str) -> Dict:
        """Fetch single endpoint"""
        async with self.semaphore:  # Limit concurrency
            async with aiohttp.ClientSession() as session:
                url = f"{self.base_url}/{endpoint}"
                async with session.get(url) as response:
                    return await response.json()
    
    def create_fetch_task(self, endpoint: str) -> asyncio.Task:
        """Create and track a fetch task"""
        task = asyncio.create_task(
            self.fetch(endpoint),
            name=f"fetch-{endpoint}"
        )
        
        # Track task
        self.active_tasks.add(task)
        task.add_done_callback(self.active_tasks.discard)
        
        return task
    
    async def fetch_multiple(self, endpoints: List[str]) -> List[Dict]:
        """Fetch multiple endpoints concurrently"""
        tasks = [self.create_fetch_task(ep) for ep in endpoints]
        
        # Wait for all with error handling
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        successful = []
        failed = []
        
        for endpoint, result in zip(endpoints, results):
            if isinstance(result, Exception):
                failed.append((endpoint, result))
            else:
                successful.append(result)
        
        if failed:
            print(f"Failed requests: {len(failed)}")
            for endpoint, error in failed:
                print(f"  {endpoint}: {error}")
        
        return successful
    
    async def wait_all(self):
        """Wait for all active tasks"""
        if self.active_tasks:
            await asyncio.gather(*self.active_tasks, return_exceptions=True)

async def main():
    client = APIClient("https://api.github.com", max_concurrent=5)
    
    endpoints = [
        "users/python",
        "users/microsoft",
        "users/google",
        "users/facebook",
        "users/apple",
    ]
    
    start = time.time()
    results = await client.fetch_multiple(endpoints)
    duration = time.time() - start
    
    print(f"\nFetched {len(results)} endpoints in {duration:.2f}s")
    print(f"Active tasks remaining: {len(client.active_tasks)}")

asyncio.run(main())
```

---

## Task Management Best Practices

### ✅ DO: Always await or track tasks

```python
async def good():
    # Keep reference
    task = asyncio.create_task(worker())
    # ... do other work ...
    await task  # Wait for completion
```

### ❌ DON'T: Create orphan tasks

```python
async def bad():
    # BUG: No reference, may be garbage collected
    asyncio.create_task(worker())
    # Task may not complete!
```

### ✅ DO: Use TaskGroup for related tasks

```python
async def good():
    async with asyncio.TaskGroup() as tg:
        tg.create_task(worker1())
        tg.create_task(worker2())
    # All tasks complete before continuing
```

### ✅ DO: Handle task exceptions

```python
async def good():
    task = asyncio.create_task(worker())
    try:
        result = await task
    except Exception as e:
        print(f"Task failed: {e}")
```

### ✅ DO: Cancel tasks on shutdown

```python
async def good():
    tasks = [asyncio.create_task(worker()) for _ in range(10)]
    
    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        # Cancel all tasks
        for task in tasks:
            task.cancel()
        # Wait for cancellation
        await asyncio.gather(*tasks, return_exceptions=True)
```

---

## Summary: Task Mental Model

✅ **Tasks wrap coroutines** and schedule them for execution

✅ **`create_task()` schedules immediately** but doesn't run until you yield control

✅ **Tasks have states:** pending → running → done (or cancelled)

✅ **Always keep task references** to prevent garbage collection

✅ **Tasks store results or exceptions** for later retrieval

✅ **Tasks can be cancelled** with `task.cancel()`

✅ **Use callbacks for cleanup** with `add_done_callback()`

✅ **Python 3.12+ has eager execution** for better performance

---

## What's Next?

Now that you understand tasks, we'll explore modern structured concurrency patterns.

In [Chapter 5: Structured Concurrency](./05-structured-concurrency.md), we'll cover:
- Why structured concurrency matters
- `TaskGroup` - the modern way to manage tasks
- Automatic cancellation and cleanup
- `ExceptionGroup` handling
- When to use `TaskGroup` vs `gather()`

This is a critical chapter for writing robust production async code.

---

**Previous:** [← Chapter 3: Coroutines Deep Dive](../part1-foundations/03-coroutines.md)  
**Next:** [Chapter 5: Structured Concurrency →](./05-structured-concurrency.md)