# Chapter 13: Cancellation Model

## Introduction

Cancellation is one of asyncio's most powerful and misunderstood features. It allows you to stop tasks gracefully, implement timeouts, and handle shutdown cleanly. Understanding cancellation is critical for writing robust async code.

By the end of this chapter, you'll understand:
- How `task.cancel()` works internally
- `CancelledError` propagation
- Injection points (where cancellation happens)
- Cooperative cancellation model
- Cancellation vs exceptions
- When and how to cancel tasks
- Common cancellation patterns

---

## What is Cancellation?

Cancellation is a mechanism to request that a task stop executing. It's **cooperative** - the task must check for cancellation and respond appropriately.

### Basic Cancellation

```python
import asyncio

async def basic_cancellation():
    async def worker():
        try:
            print("Worker: Starting")
            await asyncio.sleep(10.0)  # Long operation
            print("Worker: Completed")
        except asyncio.CancelledError:
            print("Worker: Cancelled!")
            raise  # Re-raise to propagate
    
    task = asyncio.create_task(worker())
    
    await asyncio.sleep(1.0)
    
    # Cancel the task
    task.cancel()
    print("Main: Cancelled task")
    
    try:
        await task
    except asyncio.CancelledError:
        print("Main: Task was cancelled")

asyncio.run(basic_cancellation())

# Output:
# Worker: Starting
# Main: Cancelled task
# Worker: Cancelled!
# Main: Task was cancelled
```

**Key points:**
- `task.cancel()` requests cancellation
- `CancelledError` is raised at next `await`
- Task must handle or propagate `CancelledError`

---

## How Cancellation Works Internally

### The Cancellation Process

1. **Request:** `task.cancel()` sets a cancellation flag
2. **Injection:** At next `await`, event loop checks flag
3. **Exception:** If flag set, raise `CancelledError`
4. **Propagation:** Exception propagates up the call stack
5. **Cleanup:** `finally` blocks execute
6. **Completion:** Task enters cancelled state

### Cancellation Injection Points

```python
import asyncio

async def injection_points():
    """Demonstrate where cancellation is checked"""
    
    async def worker():
        print("1. Before first await")
        
        # Cancellation checked here
        await asyncio.sleep(0)
        print("2. After first await")
        
        # Cancellation checked here
        await asyncio.sleep(0)
        print("3. After second await")
        
        # No await = no cancellation check
        for i in range(1000000):
            pass  # Cancellation NOT checked here
        
        print("4. After loop")
        
        # Cancellation checked here
        await asyncio.sleep(0)
        print("5. After third await")
    
    task = asyncio.create_task(worker())
    await asyncio.sleep(0.1)
    
    task.cancel()
    
    try:
        await task
    except asyncio.CancelledError:
        print("Task cancelled")

asyncio.run(injection_points())

# Output:
# 1. Before first await
# 2. After first await
# 3. After second await
# 4. After loop
# Task cancelled  ← Cancelled at next await
```

**Critical insight:** Cancellation only happens at `await` points!

---

## CancelledError

`CancelledError` is a special exception that signals cancellation.

### CancelledError Characteristics

```python
import asyncio

async def cancelled_error_demo():
    async def worker():
        try:
            await asyncio.sleep(10.0)
        except asyncio.CancelledError as e:
            print(f"CancelledError caught: {e}")
            print(f"Type: {type(e)}")
            print(f"Is BaseException: {isinstance(e, BaseException)}")
            print(f"Is Exception: {isinstance(e, Exception)}")
            raise  # Must re-raise!
    
    task = asyncio.create_task(worker())
    await asyncio.sleep(0.1)
    task.cancel()
    
    try:
        await task
    except asyncio.CancelledError:
        print("Task was cancelled")

asyncio.run(cancelled_error_demo())

# Output:
# CancelledError caught: 
# Type: <class 'asyncio.exceptions.CancelledError'>
# Is BaseException: True
# Is Exception: True  ← In Python 3.8+
# Task was cancelled
```

**Important:** In Python 3.8+, `CancelledError` inherits from `Exception`, not `BaseException`.

### Must Re-raise CancelledError

```python
import asyncio

async def must_reraise():
    # ❌ BAD: Swallowing CancelledError
    async def bad_worker():
        try:
            await asyncio.sleep(10.0)
        except asyncio.CancelledError:
            print("Cancelled, but ignoring")
            # NOT re-raising - BAD!
    
    # ✅ GOOD: Re-raising CancelledError
    async def good_worker():
        try:
            await asyncio.sleep(10.0)
        except asyncio.CancelledError:
            print("Cancelled, cleaning up")
            # Do cleanup
            raise  # Re-raise - GOOD!
    
    # Bad worker won't actually cancel
    task1 = asyncio.create_task(bad_worker())
    task1.cancel()
    await task1  # Completes normally (wrong!)
    print(f"Bad task cancelled: {task1.cancelled()}")  # False
    
    # Good worker cancels properly
    task2 = asyncio.create_task(good_worker())
    task2.cancel()
    try:
        await task2
    except asyncio.CancelledError:
        pass
    print(f"Good task cancelled: {task2.cancelled()}")  # True

asyncio.run(must_reraise())
```

---

## Cooperative Cancellation

Cancellation is **cooperative** - tasks must cooperate by checking for cancellation.

### Long-Running Computation

```python
import asyncio

async def cooperative_cancellation():
    # ❌ BAD: No cancellation checks
    async def bad_worker():
        result = 0
        for i in range(10_000_000):
            result += i
        return result
    
    # ✅ GOOD: Periodic cancellation checks
    async def good_worker():
        result = 0
        for i in range(10_000_000):
            result += i
            
            # Check for cancellation every 100k iterations
            if i % 100_000 == 0:
                await asyncio.sleep(0)  # Cancellation point
        
        return result
    
    # Bad worker can't be cancelled during computation
    task1 = asyncio.create_task(bad_worker())
    await asyncio.sleep(0.1)
    task1.cancel()
    # Task continues running until computation completes
    
    # Good worker can be cancelled
    task2 = asyncio.create_task(good_worker())
    await asyncio.sleep(0.1)
    task2.cancel()
    # Task stops at next cancellation point

asyncio.run(cooperative_cancellation())
```

### Manual Cancellation Checks

```python
import asyncio

async def manual_cancellation_check():
    async def worker():
        for i in range(100):
            # Manual check
            if asyncio.current_task().cancelled():
                print(f"Cancelled at iteration {i}")
                raise asyncio.CancelledError()
            
            # Do work
            print(f"Working: {i}")
            await asyncio.sleep(0.1)
    
    task = asyncio.create_task(worker())
    await asyncio.sleep(0.5)
    task.cancel()
    
    try:
        await task
    except asyncio.CancelledError:
        print("Task cancelled")

asyncio.run(manual_cancellation_check())
```

---

## Cancellation Propagation

Cancellation propagates through the call stack.

### Propagation Example

```python
import asyncio

async def cancellation_propagation():
    async def level_3():
        print("Level 3: Starting")
        await asyncio.sleep(10.0)
        print("Level 3: Done")
    
    async def level_2():
        print("Level 2: Starting")
        try:
            await level_3()
        except asyncio.CancelledError:
            print("Level 2: Caught cancellation")
            raise  # Propagate
    
    async def level_1():
        print("Level 1: Starting")
        try:
            await level_2()
        except asyncio.CancelledError:
            print("Level 1: Caught cancellation")
            raise  # Propagate
    
    task = asyncio.create_task(level_1())
    await asyncio.sleep(0.1)
    
    task.cancel()
    
    try:
        await task
    except asyncio.CancelledError:
        print("Main: Task cancelled")

asyncio.run(cancellation_propagation())

# Output:
# Level 1: Starting
# Level 2: Starting
# Level 3: Starting
# Level 3: Caught cancellation (implicit)
# Level 2: Caught cancellation
# Level 1: Caught cancellation
# Main: Task cancelled
```

---

## Cancellation vs Exceptions

Cancellation is different from regular exceptions.

### Comparison

```python
import asyncio

async def cancellation_vs_exception():
    # Regular exception
    async def worker_with_exception():
        try:
            raise ValueError("Something went wrong")
        except ValueError:
            print("Caught ValueError")
            # Can handle and continue
    
    # Cancellation
    async def worker_with_cancellation():
        try:
            await asyncio.sleep(10.0)
        except asyncio.CancelledError:
            print("Caught CancelledError")
            raise  # MUST re-raise
    
    await worker_with_exception()  # Completes normally
    
    task = asyncio.create_task(worker_with_cancellation())
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        print("Task cancelled")

asyncio.run(cancellation_vs_exception())
```

**Key differences:**
- Regular exceptions: Can be handled and suppressed
- `CancelledError`: Must be re-raised (usually)
- Regular exceptions: Indicate errors
- `CancelledError`: Indicates intentional cancellation

---

## When to Cancel Tasks

### Use Case 1: Timeouts

```python
import asyncio

async def timeout_example():
    async def slow_operation():
        await asyncio.sleep(10.0)
        return "result"
    
    task = asyncio.create_task(slow_operation())
    
    try:
        # Wait max 2 seconds
        result = await asyncio.wait_for(task, timeout=2.0)
    except asyncio.TimeoutError:
        print("Operation timed out")
        # Task is automatically cancelled

asyncio.run(timeout_example())
```

### Use Case 2: Shutdown

```python
import asyncio

async def shutdown_example():
    async def worker(worker_id):
        try:
            while True:
                print(f"Worker {worker_id}: Working")
                await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            print(f"Worker {worker_id}: Shutting down")
            raise
    
    # Start workers
    tasks = [
        asyncio.create_task(worker(i))
        for i in range(3)
    ]
    
    # Run for a while
    await asyncio.sleep(3.0)
    
    # Shutdown: Cancel all workers
    print("Initiating shutdown")
    for task in tasks:
        task.cancel()
    
    # Wait for cancellation
    await asyncio.gather(*tasks, return_exceptions=True)
    print("Shutdown complete")

asyncio.run(shutdown_example())
```

### Use Case 3: User Cancellation

```python
import asyncio

async def user_cancellation_example():
    async def long_download():
        try:
            for i in range(100):
                print(f"Downloading: {i}%")
                await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            print("Download cancelled by user")
            raise
    
    task = asyncio.create_task(long_download())
    
    # Simulate user pressing cancel after 1 second
    await asyncio.sleep(1.0)
    print("User pressed cancel")
    task.cancel()
    
    try:
        await task
    except asyncio.CancelledError:
        print("Download stopped")

asyncio.run(user_cancellation_example())
```

---

## Cancellation Patterns

### Pattern 1: Graceful Cancellation

```python
import asyncio

async def graceful_cancellation():
    async def worker():
        try:
            while True:
                print("Working...")
                await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            print("Cancellation requested")
            
            # Cleanup
            print("Saving state...")
            await asyncio.sleep(0.5)
            
            print("Closing connections...")
            await asyncio.sleep(0.5)
            
            print("Cleanup complete")
            raise
    
    task = asyncio.create_task(worker())
    await asyncio.sleep(3.0)
    
    task.cancel()
    
    try:
        await task
    except asyncio.CancelledError:
        print("Worker stopped gracefully")

asyncio.run(graceful_cancellation())
```

### Pattern 2: Cancellation with Timeout

```python
import asyncio

async def cancellation_with_timeout():
    async def worker():
        try:
            while True:
                await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            print("Cancelling...")
            
            # Cleanup with timeout
            try:
                async with asyncio.timeout(2.0):
                    await cleanup()
            except asyncio.TimeoutError:
                print("Cleanup timed out!")
            
            raise
    
    async def cleanup():
        print("Cleaning up...")
        await asyncio.sleep(1.0)
        print("Cleanup done")
    
    task = asyncio.create_task(worker())
    await asyncio.sleep(2.0)
    task.cancel()
    
    try:
        await task
    except asyncio.CancelledError:
        pass

asyncio.run(cancellation_with_timeout())
```

### Pattern 3: Ignore Cancellation (Rare)

```python
import asyncio

async def ignore_cancellation_pattern():
    """
    Rarely needed: Complete critical operation despite cancellation.
    Use with extreme caution!
    """
    async def critical_operation():
        try:
            while True:
                await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            print("Cancellation requested, but completing critical work")
            
            # Shield critical operation from cancellation
            try:
                await asyncio.shield(save_critical_data())
            except asyncio.CancelledError:
                # Even shield can be cancelled if we await it
                pass
            
            print("Critical work complete")
            raise  # Now propagate cancellation
    
    async def save_critical_data():
        print("Saving critical data...")
        await asyncio.sleep(2.0)
        print("Data saved")
    
    task = asyncio.create_task(critical_operation())
    await asyncio.sleep(0.5)
    task.cancel()
    
    try:
        await task
    except asyncio.CancelledError:
        print("Task cancelled after critical work")

asyncio.run(ignore_cancellation_pattern())
```

---

## Real-world: Cancellable Worker Pool

```python
import asyncio
from typing import Callable, Any, List

class CancellableWorkerPool:
    """
    Worker pool with graceful cancellation support.
    
    Workers can be cancelled cleanly with proper cleanup.
    """
    
    def __init__(self, num_workers: int):
        self.num_workers = num_workers
        self.queue = asyncio.Queue()
        self.workers: List[asyncio.Task] = []
        self.shutdown_event = asyncio.Event()
    
    async def worker(self, worker_id: int):
        """Worker that processes tasks from queue"""
        print(f"Worker {worker_id}: Started")
        
        try:
            while not self.shutdown_event.is_set():
                try:
                    # Get task with timeout
                    task_func, args = await asyncio.wait_for(
                        self.queue.get(),
                        timeout=1.0
                    )
                    
                    print(f"Worker {worker_id}: Processing task")
                    await task_func(*args)
                    
                    self.queue.task_done()
                    
                except asyncio.TimeoutError:
                    # No task available, check shutdown
                    continue
                
        except asyncio.CancelledError:
            print(f"Worker {worker_id}: Cancellation requested")
            
            # Finish current task if any
            if not self.queue.empty():
                print(f"Worker {worker_id}: Finishing current task")
                try:
                    task_func, args = self.queue.get_nowait()
                    await task_func(*args)
                    self.queue.task_done()
                except asyncio.QueueEmpty:
                    pass
            
            print(f"Worker {worker_id}: Stopped")
            raise
    
    async def start(self):
        """Start worker pool"""
        self.workers = [
            asyncio.create_task(self.worker(i))
            for i in range(self.num_workers)
        ]
    
    async def submit(self, func: Callable, *args):
        """Submit task to pool"""
        await self.queue.put((func, args))
    
    async def shutdown(self, wait: bool = True):
        """Shutdown worker pool"""
        print("Shutting down worker pool")
        
        # Signal shutdown
        self.shutdown_event.set()
        
        if wait:
            # Wait for queue to empty
            await self.queue.join()
        
        # Cancel workers
        for worker in self.workers:
            worker.cancel()
        
        # Wait for workers to stop
        await asyncio.gather(*self.workers, return_exceptions=True)
        
        print("Worker pool shutdown complete")

async def demo_cancellable_pool():
    """Demonstrate cancellable worker pool"""
    
    async def task(task_id):
        print(f"  Task {task_id}: Processing")
        await asyncio.sleep(1.0)
        print(f"  Task {task_id}: Done")
    
    pool = CancellableWorkerPool(num_workers=3)
    await pool.start()
    
    # Submit tasks
    for i in range(10):
        await pool.submit(task, i)
    
    # Let some tasks complete
    await asyncio.sleep(3.0)
    
    # Shutdown (cancels remaining tasks)
    await pool.shutdown(wait=False)

asyncio.run(demo_cancellable_pool())
```

---

## Common Pitfalls

### Pitfall 1: Not Re-raising CancelledError

```python
# ❌ BAD
async def bad():
    try:
        await asyncio.sleep(10.0)
    except asyncio.CancelledError:
        print("Cancelled")
        # Not re-raising!

# ✅ GOOD
async def good():
    try:
        await asyncio.sleep(10.0)
    except asyncio.CancelledError:
        print("Cancelled")
        raise  # Re-raise
```

### Pitfall 2: Catching Too Broadly

```python
# ❌ BAD
async def bad():
    try:
        await asyncio.sleep(10.0)
    except Exception:  # Catches CancelledError!
        print("Error")

# ✅ GOOD
async def good():
    try:
        await asyncio.sleep(10.0)
    except asyncio.CancelledError:
        raise  # Handle cancellation separately
    except Exception:
        print("Error")
```

### Pitfall 3: No Cancellation Points

```python
# ❌ BAD
async def bad():
    for i in range(1_000_000):
        # No await = no cancellation
        pass

# ✅ GOOD
async def good():
    for i in range(1_000_000):
        if i % 10_000 == 0:
            await asyncio.sleep(0)  # Cancellation point
```

---

## Best Practices

### ✅ DO: Always re-raise CancelledError

```python
try:
    await operation()
except asyncio.CancelledError:
    # Cleanup
    raise  # Always re-raise
```

### ✅ DO: Add cancellation points in long operations

```python
async def long_operation():
    for i in range(1_000_000):
        # Work
        if i % 10_000 == 0:
            await asyncio.sleep(0)  # Cancellation point
```

### ✅ DO: Use finally for cleanup

```python
try:
    await operation()
finally:
    # Cleanup always runs
    await cleanup()
```

### ✅ DO: Handle CancelledError separately

```python
try:
    await operation()
except asyncio.CancelledError:
    raise  # Handle separately
except Exception as e:
    # Handle other errors
    pass
```

### ❌ DON'T: Suppress CancelledError

```python
# Bad
try:
    await operation()
except asyncio.CancelledError:
    pass  # Don't suppress!
```

---

## Summary: Cancellation Mental Model

✅ **Cancellation is cooperative** - tasks must check for it

✅ **CancelledError at await points** - only checked during `await`

✅ **Must re-raise CancelledError** - propagate cancellation

✅ **Use for timeouts and shutdown** - clean task termination

✅ **Add cancellation points** - in long-running operations

✅ **Cleanup in finally** - guaranteed execution

✅ **Handle separately** - don't catch with broad `except`

---

## What's Next?

Understanding cancellation is just the start. Writing cancellation-safe code requires careful attention to cleanup and resource management.

In [Chapter 14: Cancellation Safety](./14-cancellation-safety.md), we'll cover:
- Cleanup patterns
- Resource release during cancellation
- Lock release safety
- Partial state recovery
- Atomic operations
- Testing cancellation safety

Cancellation safety is critical for production code.

---

**Previous:** [← Chapter 12: Semaphore - Concurrency Limiting](../part4-synchronization/12-semaphore.md)  
**Next:** [Chapter 14: Cancellation Safety →](./14-cancellation-safety.md)