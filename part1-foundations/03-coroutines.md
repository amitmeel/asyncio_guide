# Chapter 3: Coroutines Deep Dive

## Introduction

Coroutines are the fundamental building blocks of asyncio. Understanding them deeply is essential for mastering async programming. This chapter explores what coroutines actually are, how they differ from regular functions, and the mechanics of suspension and resumption.

By the end, you'll understand:
- What `async def` creates
- The difference between a coroutine object and its execution
- How `await` works at a deep level
- Suspension points and stack unwinding
- Coroutine lifecycle and state transitions

---

## What is a Coroutine?

A coroutine is a **function that can be suspended and resumed**. Unlike regular functions that run to completion, coroutines can pause execution and yield control back to the caller.

### Regular Function vs Coroutine

```python
# Regular function
def regular_function():
    print("Start")
    result = compute_something()  # Runs to completion
    print("End")
    return result

# Coroutine function
async def coroutine_function():
    print("Start")
    result = await async_compute()  # Can suspend here
    print("End")
    return result
```

**Key differences:**

| Regular Function | Coroutine |
|-----------------|-----------|
| Runs to completion | Can suspend and resume |
| Returns result immediately | Returns coroutine object |
| Blocking | Non-blocking (with await) |
| Single execution path | Multiple suspension points |

---

## `async def`: Creating Coroutines

The `async def` keyword defines a **coroutine function**. When called, it returns a **coroutine object**, not the result.

### The Critical Distinction

```python
import asyncio

async def fetch_data():
    await asyncio.sleep(1)
    return "data"

# Calling the function creates a coroutine object
coro = fetch_data()
print(type(coro))  # <class 'coroutine'>
print(coro)        # <coroutine object fetch_data at 0x...>

# The function body hasn't executed yet!
# To execute it, you must await it:
result = asyncio.run(fetch_data())
print(result)  # "data"
```

**Mental model:**

```python
# When you write:
async def my_function():
    return 42

# Python creates something like:
class CoroutineObject:
    def __init__(self):
        self.state = "created"
        self.result = None
    
    def send(self, value):
        # Execute until next await or return
        pass
    
    def throw(self, exc):
        # Inject exception
        pass

# Calling my_function() returns CoroutineObject instance
coro = my_function()  # Creates object, doesn't execute
```

---

## Coroutine Objects: The Execution Container

A coroutine object is a **suspended execution context** that can be resumed.

### Coroutine Object Attributes

```python
import asyncio
import inspect

async def example():
    await asyncio.sleep(1)
    return "done"

coro = example()

# Inspect the coroutine object
print(f"Is coroutine: {inspect.iscoroutine(coro)}")
print(f"Coroutine name: {coro.__name__}")
print(f"Coroutine qualname: {coro.__qualname__}")

# Coroutine state (internal)
print(f"State: {inspect.getcoroutinestate(coro)}")
# Output: CORO_CREATED

# Clean up
coro.close()
```

### Coroutine States

A coroutine goes through several states:

```python
import asyncio
import inspect

async def stateful_coro():
    print("Started")
    await asyncio.sleep(0.1)
    print("Resumed")
    return "done"

async def main():
    coro = stateful_coro()
    
    # State 1: CORO_CREATED
    print(f"1. {inspect.getcoroutinestate(coro)}")
    
    # Start execution
    task = asyncio.create_task(coro)
    
    # State 2: CORO_RUNNING (briefly)
    await asyncio.sleep(0)
    
    # State 3: CORO_SUSPENDED (waiting on sleep)
    print(f"2. {inspect.getcoroutinestate(coro)}")
    
    # Wait for completion
    await task
    
    # State 4: CORO_CLOSED
    print(f"3. {inspect.getcoroutinestate(coro)}")

asyncio.run(main())

# Output:
# 1. CORO_CREATED
# Started
# 2. CORO_SUSPENDED
# Resumed
# 3. CORO_CLOSED
```

**State diagram:**

```
CORO_CREATED
    ↓ (first await/send)
CORO_RUNNING
    ↓ (await suspends)
CORO_SUSPENDED
    ↓ (resume)
CORO_RUNNING
    ↓ (return/exception)
CORO_CLOSED
```

---

## The `await` Expression

`await` is the mechanism for suspending and resuming coroutines. Understanding it deeply is crucial.

### What `await` Does

```python
import asyncio

async def example():
    print("Before await")
    result = await asyncio.sleep(1)  # Suspension point
    print("After await")
    return result
```

**Step-by-step execution:**

```
1. Execute: print("Before await")
2. Evaluate: asyncio.sleep(1) → returns awaitable
3. Suspend: Save current state
4. Yield: Return control to event loop
5. Wait: Event loop schedules wakeup after 1 second
6. Resume: Restore state after 1 second
7. Execute: print("After await")
8. Return: result
```

### What Can Be Awaited?

Only **awaitable** objects can be used with `await`:

```python
import asyncio
from collections.abc import Awaitable

# 1. Coroutines
async def coro():
    return 42

result = await coro()  # ✅

# 2. Tasks
task = asyncio.create_task(coro())
result = await task  # ✅

# 3. Futures
future = asyncio.Future()
future.set_result(42)
result = await future  # ✅

# 4. Objects with __await__ method
class CustomAwaitable:
    def __await__(self):
        # Must return an iterator
        yield
        return 42

result = await CustomAwaitable()  # ✅

# 5. Regular values - NOT awaitable
result = await 42  # ❌ TypeError: object int can't be used in 'await'
```

### The `__await__` Protocol

```python
import asyncio

class Timer:
    """Custom awaitable that waits for specified duration"""
    
    def __init__(self, duration):
        self.duration = duration
    
    def __await__(self):
        # Delegate to asyncio.sleep
        return asyncio.sleep(self.duration).__await__()

async def main():
    print("Start")
    await Timer(1.0)  # Custom awaitable
    print("End")

asyncio.run(main())
```

---

## Suspension Points: Where Coroutines Pause

A coroutine can only suspend at **explicit suspension points** marked by `await`.

### Identifying Suspension Points

```python
import asyncio

async def example():
    # No suspension - runs immediately
    x = 1 + 1
    y = compute(x)
    z = [i for i in range(100)]
    
    # SUSPENSION POINT 1
    await asyncio.sleep(0.1)
    
    # No suspension - runs immediately
    result = process(z)
    
    # SUSPENSION POINT 2
    data = await fetch_data()
    
    # No suspension - runs immediately
    return result + data

# Between suspension points, code runs atomically
# No other coroutine can execute
```

**Critical insight:** Between `await` statements, your code runs **atomically** without interruption. This is why async code doesn't need locks for simple operations.

### Implicit vs Explicit Suspension

```python
import asyncio

async def implicit_suspension():
    # These all contain await internally
    async with asyncio.Lock():  # await lock.acquire()
        pass
    
    async for item in async_generator():  # await anext()
        pass

async def explicit_suspension():
    # Explicit await
    await asyncio.sleep(1)
    
    # Explicit await with assignment
    result = await fetch_data()
    
    # Explicit await in expression
    total = (await get_a()) + (await get_b())
```

---

## Stack Unwinding and Resumption

Understanding how the call stack behaves during suspension is key to debugging async code.

### Call Stack During Suspension

```python
import asyncio

async def level_3():
    print("Level 3: Before await")
    await asyncio.sleep(0.1)
    print("Level 3: After await")
    return "result"

async def level_2():
    print("Level 2: Before calling level_3")
    result = await level_3()
    print(f"Level 2: Got {result}")
    return result

async def level_1():
    print("Level 1: Before calling level_2")
    result = await level_2()
    print(f"Level 1: Got {result}")
    return result

asyncio.run(level_1())
```

**Execution trace:**

```
Call Stack                          | Action
------------------------------------|----------------------------------
[level_1]                           | Print "Level 1: Before..."
[level_1, level_2]                  | Print "Level 2: Before..."
[level_1, level_2, level_3]         | Print "Level 3: Before await"
[level_1, level_2, level_3]         | await sleep(0.1) → SUSPEND
                                    | Stack saved, control to event loop
--- 0.1 seconds pass ---
[level_1, level_2, level_3]         | RESUME → Print "Level 3: After..."
[level_1, level_2, level_3]         | Return "result"
[level_1, level_2]                  | Print "Level 2: Got result"
[level_1, level_2]                  | Return "result"
[level_1]                           | Print "Level 1: Got result"
[level_1]                           | Return "result"
[]                                  | Complete
```

**Key insight:** The entire call stack is preserved during suspension. When resumed, execution continues exactly where it left off.

### Stack Unwinding on Exception

```python
import asyncio

async def level_3():
    print("Level 3: Start")
    await asyncio.sleep(0.1)
    raise ValueError("Error in level 3")

async def level_2():
    print("Level 2: Start")
    try:
        await level_3()
    except ValueError as e:
        print(f"Level 2: Caught {e}")
        raise  # Re-raise

async def level_1():
    print("Level 1: Start")
    try:
        await level_2()
    except ValueError as e:
        print(f"Level 1: Caught {e}")

asyncio.run(level_1())

# Output:
# Level 1: Start
# Level 2: Start
# Level 3: Start
# Level 2: Caught Error in level 3
# Level 1: Caught Error in level 3
```

**Exception propagation:**

```
[level_1, level_2, level_3] → Exception raised
[level_1, level_2]          → Exception caught, re-raised
[level_1]                   → Exception caught, handled
```

---

## Coroutine Lifecycle

Let's trace the complete lifecycle of a coroutine from creation to completion.

### Complete Lifecycle Example

```python
import asyncio
import sys

async def lifecycle_demo():
    print("1. Coroutine body starts")
    
    print("2. Before first await")
    await asyncio.sleep(0.1)
    print("3. After first await")
    
    print("4. Before second await")
    await asyncio.sleep(0.1)
    print("5. After second await")
    
    print("6. Returning")
    return "completed"

async def main():
    print("A. Creating coroutine object")
    coro = lifecycle_demo()
    print(f"   Type: {type(coro)}")
    print(f"   Repr: {coro}")
    
    print("\nB. Creating task (schedules execution)")
    task = asyncio.create_task(coro)
    
    print("\nC. Yielding control")
    await asyncio.sleep(0)  # Let task start
    
    print("\nD. Waiting for completion")
    result = await task
    print(f"   Result: {result}")
    
    print("\nE. Task complete")

asyncio.run(main())
```

**Output:**

```
A. Creating coroutine object
   Type: <class 'coroutine'>
   Repr: <coroutine object lifecycle_demo at 0x...>

B. Creating task (schedules execution)

C. Yielding control
1. Coroutine body starts
2. Before first await

D. Waiting for completion
3. After first await
4. Before second await
5. After second await
6. Returning
   Result: completed

E. Task complete
```

### Lifecycle State Transitions

```python
import asyncio
import inspect

async def track_states():
    states = []
    
    async def tracked_coro():
        await asyncio.sleep(0.1)
        return "done"
    
    coro = tracked_coro()
    states.append(("Created", inspect.getcoroutinestate(coro)))
    
    task = asyncio.create_task(coro)
    await asyncio.sleep(0)  # Let it start
    states.append(("Running/Suspended", inspect.getcoroutinestate(coro)))
    
    await task
    states.append(("Closed", inspect.getcoroutinestate(coro)))
    
    for state_name, state in states:
        print(f"{state_name}: {state}")

asyncio.run(track_states())

# Output:
# Created: CORO_CREATED
# Running/Suspended: CORO_SUSPENDED
# Closed: CORO_CLOSED
```

---

## Coroutine Chaining

Coroutines can call other coroutines, creating chains of async operations.

### Simple Chaining

```python
import asyncio

async def fetch_user(user_id):
    print(f"Fetching user {user_id}")
    await asyncio.sleep(0.1)
    return {"id": user_id, "name": f"User{user_id}"}

async def fetch_user_posts(user_id):
    print(f"Fetching posts for user {user_id}")
    await asyncio.sleep(0.1)
    return [{"id": 1, "title": "Post 1"}, {"id": 2, "title": "Post 2"}]

async def get_user_with_posts(user_id):
    # Chain coroutines
    user = await fetch_user(user_id)
    posts = await fetch_user_posts(user_id)
    
    return {
        "user": user,
        "posts": posts
    }

async def main():
    result = await get_user_with_posts(123)
    print(result)

asyncio.run(main())
```

### Parallel Chaining

```python
import asyncio

async def get_user_with_posts_parallel(user_id):
    # Execute both concurrently
    user_task = asyncio.create_task(fetch_user(user_id))
    posts_task = asyncio.create_task(fetch_user_posts(user_id))
    
    # Wait for both
    user = await user_task
    posts = await posts_task
    
    return {
        "user": user,
        "posts": posts
    }

# Or using gather:
async def get_user_with_posts_gather(user_id):
    user, posts = await asyncio.gather(
        fetch_user(user_id),
        fetch_user_posts(user_id)
    )
    
    return {
        "user": user,
        "posts": posts
    }
```

---

## Generator-based Coroutines (Legacy)

Before `async`/`await`, coroutines were implemented using generators. Understanding this helps grasp the underlying mechanism.

### Old Style (Python 3.4)

```python
import asyncio

# Old generator-based coroutine
@asyncio.coroutine
def old_style_coro():
    print("Start")
    yield from asyncio.sleep(1)  # yield from instead of await
    print("End")
    return "done"

# Modern async/await (Python 3.5+)
async def new_style_coro():
    print("Start")
    await asyncio.sleep(1)  # await instead of yield from
    print("End")
    return "done"

# Both work the same way
asyncio.run(old_style_coro())  # Still works but deprecated
asyncio.run(new_style_coro())  # Preferred
```

**Why this matters:** Understanding that coroutines are built on generators helps understand:
- Why `await` can only be used in `async def`
- How suspension/resumption works
- The `send()` and `throw()` methods

### The Generator Connection

```python
# Simplified: How async/await relates to generators

# This async function:
async def async_func():
    result = await some_awaitable()
    return result

# Is conceptually similar to:
def generator_func():
    result = yield from some_generator()
    return result

# The event loop essentially does:
coro = async_func()
try:
    coro.send(None)  # Start execution
    # ... handle suspension ...
    coro.send(value)  # Resume with value
except StopIteration as e:
    result = e.value  # Get return value
```

---

## Coroutine Introspection

Python provides tools to inspect coroutines at runtime.

### Inspecting Coroutine State

```python
import asyncio
import inspect

async def inspectable():
    await asyncio.sleep(0.1)
    return "done"

async def main():
    coro = inspectable()
    
    # Check if it's a coroutine
    print(f"Is coroutine: {inspect.iscoroutine(coro)}")
    print(f"Is coroutine function: {inspect.iscoroutinefunction(inspectable)}")
    
    # Get coroutine info
    print(f"Name: {coro.__name__}")
    print(f"Qualname: {coro.__qualname__}")
    
    # Get state
    print(f"State: {inspect.getcoroutinestate(coro)}")
    
    # Execute
    result = await coro
    print(f"Result: {result}")
    
    # State after completion
    print(f"Final state: {inspect.getcoroutinestate(coro)}")

asyncio.run(main())
```

### Getting Coroutine Stack

```python
import asyncio
import traceback

async def deep_function():
    await asyncio.sleep(0.1)

async def middle_function():
    await deep_function()

async def top_function():
    await middle_function()

async def main():
    task = asyncio.create_task(top_function())
    await asyncio.sleep(0.05)  # Let it suspend
    
    # Get the stack trace
    print("Coroutine stack:")
    task.print_stack()
    
    await task

asyncio.run(main())
```

---

## Common Patterns and Anti-patterns

### ✅ Pattern: Proper Coroutine Usage

```python
import asyncio

async def good_example():
    # Create tasks for concurrent execution
    tasks = [
        asyncio.create_task(fetch_data(i))
        for i in range(10)
    ]
    
    # Wait for all
    results = await asyncio.gather(*tasks)
    return results
```

### ❌ Anti-pattern: Forgetting await

```python
async def bad_example():
    # BUG: Forgot await - returns coroutine object!
    result = fetch_data()  # ❌
    print(result)  # Prints: <coroutine object fetch_data>
    
    # Correct:
    result = await fetch_data()  # ✅
    print(result)  # Prints actual data
```

### ❌ Anti-pattern: Sequential when concurrent is possible

```python
async def bad_sequential():
    # Slow: Waits for each sequentially
    result1 = await fetch_data(1)  # 1 second
    result2 = await fetch_data(2)  # 1 second
    result3 = await fetch_data(3)  # 1 second
    # Total: 3 seconds

async def good_concurrent():
    # Fast: All run concurrently
    results = await asyncio.gather(
        fetch_data(1),
        fetch_data(2),
        fetch_data(3)
    )
    # Total: 1 second
```

### ✅ Pattern: Error Handling in Coroutines

```python
import asyncio

async def safe_fetch(url):
    try:
        result = await fetch_data(url)
        return result
    except asyncio.TimeoutError:
        print(f"Timeout fetching {url}")
        return None
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None

async def main():
    results = await asyncio.gather(
        safe_fetch("url1"),
        safe_fetch("url2"),
        safe_fetch("url3"),
        return_exceptions=False  # Exceptions handled in safe_fetch
    )
```

---

## Real-world Example: HTTP Client

Let's build a practical example showing coroutine usage:

```python
import asyncio
import aiohttp
from typing import List, Dict

async def fetch_url(session: aiohttp.ClientSession, url: str) -> Dict:
    """Fetch a single URL"""
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as response:
            data = await response.json()
            return {
                "url": url,
                "status": response.status,
                "data": data
            }
    except asyncio.TimeoutError:
        return {"url": url, "error": "timeout"}
    except Exception as e:
        return {"url": url, "error": str(e)}

async def fetch_multiple_urls(urls: List[str]) -> List[Dict]:
    """Fetch multiple URLs concurrently"""
    async with aiohttp.ClientSession() as session:
        # Create tasks for all URLs
        tasks = [
            asyncio.create_task(fetch_url(session, url))
            for url in urls
        ]
        
        # Wait for all to complete
        results = await asyncio.gather(*tasks)
        return results

async def main():
    urls = [
        "https://api.github.com/users/python",
        "https://api.github.com/users/microsoft",
        "https://api.github.com/users/google",
    ]
    
    print("Fetching URLs...")
    results = await fetch_multiple_urls(urls)
    
    for result in results:
        if "error" in result:
            print(f"❌ {result['url']}: {result['error']}")
        else:
            print(f"✅ {result['url']}: Status {result['status']}")

# Run
asyncio.run(main())
```

**Key coroutine concepts demonstrated:**

1. **Coroutine chaining:** `main()` → `fetch_multiple_urls()` → `fetch_url()`
2. **Concurrent execution:** Multiple `fetch_url()` coroutines run concurrently
3. **Suspension points:** `await response.json()`, `await asyncio.gather()`
4. **Error handling:** Try/except in async context
5. **Resource management:** `async with` for session

---

## Performance Characteristics

Understanding coroutine performance helps optimize applications.

### Coroutine Creation Overhead

```python
import asyncio
import time

async def empty_coro():
    pass

async def benchmark_creation():
    start = time.perf_counter()
    
    # Create 100,000 coroutine objects
    coros = [empty_coro() for _ in range(100000)]
    
    creation_time = time.perf_counter() - start
    print(f"Created 100,000 coroutines in {creation_time:.3f}s")
    print(f"Per coroutine: {creation_time/100000*1000000:.2f}µs")
    
    # Clean up
    for coro in coros:
        coro.close()

asyncio.run(benchmark_creation())

# Typical output:
# Created 100,000 coroutines in 0.015s
# Per coroutine: 0.15µs
```

**Insight:** Coroutine creation is extremely cheap (~0.15µs), making them suitable for fine-grained concurrency.

### Suspension/Resumption Overhead

```python
import asyncio
import time

async def suspend_resume():
    await asyncio.sleep(0)  # Immediate suspension/resumption

async def benchmark_suspension():
    start = time.perf_counter()
    
    # 10,000 suspend/resume cycles
    tasks = [suspend_resume() for _ in range(10000)]
    await asyncio.gather(*tasks)
    
    duration = time.perf_counter() - start
    print(f"10,000 suspend/resume cycles in {duration:.3f}s")
    print(f"Per cycle: {duration/10000*1000:.2f}ms")

asyncio.run(benchmark_suspension())

# Typical output:
# 10,000 suspend/resume cycles in 0.050s
# Per cycle: 0.005ms
```

---

## Summary: Coroutine Mental Model

✅ **Coroutines are suspendable functions** created with `async def`

✅ **Calling a coroutine returns a coroutine object**, not the result

✅ **`await` suspends execution** and yields control to the event loop

✅ **Suspension only happens at `await` points** - code between awaits runs atomically

✅ **The call stack is preserved** during suspension and restored on resumption

✅ **Coroutines are cheap** - create thousands without worry

✅ **Chain coroutines** to build complex async workflows

✅ **Always await coroutines** - forgetting await is a common bug

---

## What's Next?

You now understand the three foundational concepts:
1. Why async exists (Chapter 1)
2. How the event loop works (Chapter 2)
3. What coroutines are (Chapter 3)

In [Chapter 4: Tasks and Scheduling](../part2-orchestration/04-tasks-and-scheduling.md), we'll explore:
- How to create and manage tasks
- Task lifecycle and states
- Scheduling semantics
- Orphan tasks and cleanup
- Eager task execution (Python 3.12+)

This moves us from understanding the primitives to orchestrating concurrent work.

---

**Previous:** [← Chapter 2: Event Loop Internals](./02-event-loop-internals.md)  
**Next:** [Chapter 4: Tasks and Scheduling →](../part2-orchestration/04-tasks-and-scheduling.md)