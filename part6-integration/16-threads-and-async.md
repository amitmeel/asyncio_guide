# Chapter 16: Threads and Async

## Introduction

Real-world applications often need to mix async code with blocking operations. Python provides tools to integrate threads with asyncio safely. This chapter teaches you when and how to use threads with async code.

By the end of this chapter, you'll understand:
- `asyncio.to_thread()` for blocking operations (Python 3.9+)
- `loop.run_in_executor()` for thread pools
- `loop.call_soon_threadsafe()` for thread-safe scheduling
- When to use threads vs async
- Common integration patterns
- Thread safety considerations

---

## The Problem: Blocking Operations

Blocking operations freeze the event loop, preventing other tasks from running.

### Blocking the Event Loop

```python
import asyncio
import time

async def blocking_problem():
    """Demonstrate how blocking operations freeze the event loop"""
    
    async def fast_task(task_id):
        for i in range(3):
            print(f"Task {task_id}: iteration {i}")
            await asyncio.sleep(0.1)
    
    async def blocking_task():
        print("Blocking task: Starting")
        time.sleep(3.0)  # ❌ BLOCKS EVENT LOOP!
        print("Blocking task: Done")
    
    # Start tasks
    await asyncio.gather(
        fast_task(1),
        fast_task(2),
        blocking_task()
    )

asyncio.run(blocking_problem())

# Output shows fast tasks freeze during blocking_task:
# Task 1: iteration 0
# Task 2: iteration 0
# Blocking task: Starting
# (3 second freeze - no other tasks run!)
# Blocking task: Done
# Task 1: iteration 1
# Task 2: iteration 1
# ...
```

**Problem:** `time.sleep()` blocks the entire event loop.

---

## Solution 1: asyncio.to_thread()

Python 3.9+ provides `asyncio.to_thread()` to run blocking functions in threads.

### Basic to_thread Usage

```python
import asyncio
import time

async def to_thread_demo():
    """Using to_thread for blocking operations"""
    
    def blocking_operation():
        """Blocking function"""
        print("Blocking operation: Starting")
        time.sleep(3.0)  # Blocking call
        print("Blocking operation: Done")
        return "result"
    
    async def fast_task(task_id):
        for i in range(5):
            print(f"Task {task_id}: iteration {i}")
            await asyncio.sleep(0.5)
    
    # Run blocking operation in thread
    result = await asyncio.gather(
        asyncio.to_thread(blocking_operation),
        fast_task(1),
        fast_task(2)
    )
    
    print(f"Result: {result[0]}")

asyncio.run(to_thread_demo())

# Output shows fast tasks continue during blocking operation:
# Blocking operation: Starting
# Task 1: iteration 0
# Task 2: iteration 0
# Task 1: iteration 1
# Task 2: iteration 1
# ...
# Blocking operation: Done
```

**Key insight:** `to_thread()` runs the function in a thread pool, allowing other tasks to continue.

### to_thread with Arguments

```python
import asyncio
import time

async def to_thread_with_args():
    """Pass arguments to blocking functions"""
    
    def blocking_compute(x, y, operation="add"):
        """Blocking computation"""
        time.sleep(1.0)  # Simulate heavy computation
        
        if operation == "add":
            return x + y
        elif operation == "multiply":
            return x * y
        else:
            raise ValueError(f"Unknown operation: {operation}")
    
    # Run multiple blocking operations concurrently
    results = await asyncio.gather(
        asyncio.to_thread(blocking_compute, 10, 20, operation="add"),
        asyncio.to_thread(blocking_compute, 10, 20, operation="multiply"),
        asyncio.to_thread(blocking_compute, 5, 3, operation="add")
    )
    
    print(f"Results: {results}")

asyncio.run(to_thread_with_args())

# Output:
# Results: [30, 200, 8]
```

---

## Solution 2: run_in_executor()

For more control, use `loop.run_in_executor()` with custom thread pools.

### Basic run_in_executor

```python
import asyncio
import time
from concurrent.futures import ThreadPoolExecutor

async def run_in_executor_demo():
    """Using run_in_executor with custom thread pool"""
    
    def blocking_operation(name):
        print(f"{name}: Starting")
        time.sleep(2.0)
        print(f"{name}: Done")
        return f"Result from {name}"
    
    loop = asyncio.get_running_loop()
    
    # Use default thread pool
    result1 = await loop.run_in_executor(
        None,  # None = default thread pool
        blocking_operation,
        "Operation-1"
    )
    
    # Use custom thread pool
    with ThreadPoolExecutor(max_workers=4) as executor:
        result2 = await loop.run_in_executor(
            executor,
            blocking_operation,
            "Operation-2"
        )
    
    print(f"Results: {result1}, {result2}")

asyncio.run(run_in_executor_demo())
```

### Custom Thread Pool

```python
import asyncio
import time
from concurrent.futures import ThreadPoolExecutor

async def custom_thread_pool():
    """Custom thread pool for blocking operations"""
    
    def cpu_bound_work(n):
        """Simulate CPU-bound work"""
        result = 0
        for i in range(n):
            result += i ** 2
        return result
    
    loop = asyncio.get_running_loop()
    
    # Create custom thread pool
    with ThreadPoolExecutor(max_workers=4) as executor:
        # Run multiple operations concurrently
        tasks = [
            loop.run_in_executor(executor, cpu_bound_work, 1_000_000)
            for _ in range(10)
        ]
        
        results = await asyncio.gather(*tasks)
    
    print(f"Completed {len(results)} operations")

asyncio.run(custom_thread_pool())
```

---

## Thread-Safe Scheduling

When threads need to schedule work on the event loop, use `call_soon_threadsafe()`.

### call_soon_threadsafe Basics

```python
import asyncio
import threading
import time

async def call_soon_threadsafe_demo():
    """Schedule work from threads"""
    
    results = []
    
    def thread_worker(worker_id):
        """Worker running in thread"""
        time.sleep(1.0)
        
        # Schedule callback on event loop (thread-safe)
        loop.call_soon_threadsafe(
            results.append,
            f"Result from worker {worker_id}"
        )
    
    loop = asyncio.get_running_loop()
    
    # Start threads
    threads = [
        threading.Thread(target=thread_worker, args=(i,))
        for i in range(5)
    ]
    
    for thread in threads:
        thread.start()
    
    # Wait for threads
    for thread in threads:
        thread.join()
    
    # Give event loop time to process callbacks
    await asyncio.sleep(0.1)
    
    print(f"Results: {results}")

asyncio.run(call_soon_threadsafe_demo())
```

### Thread to Async Communication

```python
import asyncio
import threading
import time
from typing import Any

class ThreadToAsyncBridge:
    """Bridge for thread-to-async communication"""
    
    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.loop = loop
        self.queue = asyncio.Queue()
    
    def send_from_thread(self, item: Any):
        """Send item from thread to async code"""
        # Schedule put operation on event loop
        asyncio.run_coroutine_threadsafe(
            self.queue.put(item),
            self.loop
        )
    
    async def receive(self) -> Any:
        """Receive item in async code"""
        return await self.queue.get()

async def thread_to_async_demo():
    """Demonstrate thread-to-async communication"""
    
    loop = asyncio.get_running_loop()
    bridge = ThreadToAsyncBridge(loop)
    
    def thread_worker():
        """Worker running in thread"""
        for i in range(5):
            time.sleep(0.5)
            bridge.send_from_thread(f"Message {i}")
        
        bridge.send_from_thread(None)  # Sentinel
    
    # Start thread
    thread = threading.Thread(target=thread_worker)
    thread.start()
    
    # Receive messages
    while True:
        message = await bridge.receive()
        if message is None:
            break
        print(f"Received: {message}")
    
    thread.join()
    print("Done")

asyncio.run(thread_to_async_demo())
```

---

## When to Use Threads vs Async

### Decision Matrix

```python
# Use ASYNC for:
# - I/O-bound operations (network, disk)
# - Many concurrent operations (1000s)
# - Operations with async libraries available

async def io_bound():
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.text()

# Use THREADS for:
# - Blocking I/O without async alternative
# - CPU-bound operations (with GIL limitations)
# - Legacy blocking libraries

async def blocking_io():
    result = await asyncio.to_thread(
        requests.get,  # Blocking library
        url
    )
    return result.text

# Use MULTIPROCESSING for:
# - True CPU-bound parallelism
# - Bypassing GIL
# - Heavy computation

# (Covered in separate chapter)
```

### Example: File I/O

```python
import asyncio
import aiofiles

async def file_io_comparison():
    """Compare async vs thread-based file I/O"""
    
    # Async file I/O (preferred)
    async def async_read():
        async with aiofiles.open("data.txt", "r") as f:
            return await f.read()
    
    # Thread-based file I/O (for blocking operations)
    def blocking_read():
        with open("data.txt", "r") as f:
            return f.read()
    
    # Use async when available
    content1 = await async_read()
    
    # Use thread for blocking operations
    content2 = await asyncio.to_thread(blocking_read)
```

---

## Real-world: Database Connection Pool

Integrate blocking database library with async code.

```python
import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Optional
import sqlite3

class AsyncDatabasePool:
    """
    Async wrapper for blocking database operations.
    
    Uses thread pool to run blocking database calls.
    """
    
    def __init__(self, db_path: str, max_workers: int = 5):
        self.db_path = db_path
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self._initialize_db()
    
    def _initialize_db(self):
        """Initialize database (blocking)"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                name TEXT,
                email TEXT
            )
        """)
        conn.commit()
        conn.close()
    
    async def execute(self, query: str, params: tuple = ()) -> Any:
        """Execute query in thread pool"""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self.executor,
            self._execute_blocking,
            query,
            params
        )
    
    def _execute_blocking(self, query: str, params: tuple) -> Any:
        """Execute query (blocking)"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute(query, params)
            
            if query.strip().upper().startswith("SELECT"):
                result = cursor.fetchall()
            else:
                conn.commit()
                result = cursor.lastrowid
            
            return result
        finally:
            conn.close()
    
    async def insert_user(self, name: str, email: str) -> int:
        """Insert user"""
        return await self.execute(
            "INSERT INTO users (name, email) VALUES (?, ?)",
            (name, email)
        )
    
    async def get_user(self, user_id: int) -> Optional[tuple]:
        """Get user by ID"""
        results = await self.execute(
            "SELECT * FROM users WHERE id = ?",
            (user_id,)
        )
        return results[0] if results else None
    
    async def get_all_users(self) -> list:
        """Get all users"""
        return await self.execute("SELECT * FROM users")
    
    async def close(self):
        """Shutdown thread pool"""
        self.executor.shutdown(wait=True)

async def database_pool_demo():
    """Demonstrate async database pool"""
    
    pool = AsyncDatabasePool("test.db", max_workers=3)
    
    try:
        # Insert users concurrently
        user_ids = await asyncio.gather(
            pool.insert_user("Alice", "alice@example.com"),
            pool.insert_user("Bob", "bob@example.com"),
            pool.insert_user("Charlie", "charlie@example.com")
        )
        
        print(f"Inserted users: {user_ids}")
        
        # Query users concurrently
        users = await asyncio.gather(
            pool.get_user(user_ids[0]),
            pool.get_user(user_ids[1]),
            pool.get_user(user_ids[2])
        )
        
        print(f"Users: {users}")
        
        # Get all users
        all_users = await pool.get_all_users()
        print(f"All users: {all_users}")
    
    finally:
        await pool.close()

asyncio.run(database_pool_demo())
```

---

## Real-world: Background Worker

Run background tasks in threads while maintaining async interface.

```python
import asyncio
import threading
import time
from typing import Callable, Any
from queue import Queue

class BackgroundWorker:
    """
    Background worker that runs in a thread.
    
    Allows async code to submit work to a background thread.
    """
    
    def __init__(self):
        self.queue = Queue()
        self.thread: Optional[threading.Thread] = None
        self.running = False
        self.loop: Optional[asyncio.AbstractEventLoop] = None
    
    def start(self, loop: asyncio.AbstractEventLoop):
        """Start background worker"""
        self.loop = loop
        self.running = True
        self.thread = threading.Thread(target=self._worker_loop, daemon=True)
        self.thread.start()
    
    def _worker_loop(self):
        """Worker loop (runs in thread)"""
        while self.running:
            try:
                # Get work with timeout
                work = self.queue.get(timeout=1.0)
                
                if work is None:
                    break
                
                func, args, future = work
                
                try:
                    # Execute work
                    result = func(*args)
                    
                    # Set result on event loop
                    self.loop.call_soon_threadsafe(
                        future.set_result,
                        result
                    )
                
                except Exception as e:
                    # Set exception on event loop
                    self.loop.call_soon_threadsafe(
                        future.set_exception,
                        e
                    )
            
            except:
                continue
    
    async def submit(self, func: Callable, *args) -> Any:
        """Submit work to background worker"""
        if not self.running:
            raise RuntimeError("Worker not started")
        
        # Create future for result
        future = asyncio.Future()
        
        # Queue work
        self.queue.put((func, args, future))
        
        # Wait for result
        return await future
    
    def stop(self):
        """Stop background worker"""
        self.running = False
        self.queue.put(None)
        if self.thread:
            self.thread.join()

async def background_worker_demo():
    """Demonstrate background worker"""
    
    def heavy_computation(n):
        """Simulate heavy computation"""
        time.sleep(2.0)
        return sum(i ** 2 for i in range(n))
    
    loop = asyncio.get_running_loop()
    worker = BackgroundWorker()
    worker.start(loop)
    
    try:
        # Submit work to background worker
        results = await asyncio.gather(
            worker.submit(heavy_computation, 1000),
            worker.submit(heavy_computation, 2000),
            worker.submit(heavy_computation, 3000)
        )
        
        print(f"Results: {results}")
    
    finally:
        worker.stop()

asyncio.run(background_worker_demo())
```

---

## Common Patterns

### Pattern 1: Parallel File Processing

```python
import asyncio
import os

async def parallel_file_processing():
    """Process files in parallel using threads"""
    
    def process_file(filepath):
        """Process single file (blocking)"""
        with open(filepath, 'r') as f:
            content = f.read()
        
        # Simulate processing
        import time
        time.sleep(0.5)
        
        return len(content)
    
    # Get all files
    files = [f for f in os.listdir('.') if f.endswith('.txt')]
    
    # Process in parallel
    results = await asyncio.gather(*[
        asyncio.to_thread(process_file, f)
        for f in files
    ])
    
    print(f"Processed {len(results)} files")
```

### Pattern 2: Async Wrapper for Blocking Library

```python
import asyncio
from functools import wraps

def async_wrap(func):
    """Decorator to wrap blocking function for async use"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        return await asyncio.to_thread(func, *args, **kwargs)
    return wrapper

# Usage
@async_wrap
def blocking_api_call(url):
    import requests
    return requests.get(url).text

async def demo():
    result = await blocking_api_call("https://example.com")
```

### Pattern 3: Thread Pool Manager

```python
import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager

@asynccontextmanager
async def thread_pool(max_workers=5):
    """Context manager for thread pool"""
    executor = ThreadPoolExecutor(max_workers=max_workers)
    try:
        yield executor
    finally:
        executor.shutdown(wait=True)

async def demo():
    async with thread_pool(max_workers=10) as executor:
        loop = asyncio.get_running_loop()
        results = await asyncio.gather(*[
            loop.run_in_executor(executor, blocking_func, i)
            for i in range(100)
        ])
```

---

## Best Practices

### ✅ DO: Use to_thread for simple cases

```python
# Good: Simple and clean
result = await asyncio.to_thread(blocking_func, arg1, arg2)
```

### ✅ DO: Use custom thread pools for control

```python
# Good: Control over thread pool size
with ThreadPoolExecutor(max_workers=10) as executor:
    result = await loop.run_in_executor(executor, blocking_func)
```

### ✅ DO: Use call_soon_threadsafe from threads

```python
# Good: Thread-safe scheduling
loop.call_soon_threadsafe(callback, arg)
```

### ❌ DON'T: Block the event loop

```python
# Bad: Blocks event loop
time.sleep(1.0)

# Good: Use async sleep
await asyncio.sleep(1.0)

# Good: Use thread for blocking
await asyncio.to_thread(time.sleep, 1.0)
```

### ❌ DON'T: Share mutable state without locks

```python
# Bad: Race condition
shared_list = []

def thread_func():
    shared_list.append(1)  # Not thread-safe!

# Good: Use thread-safe structures
from queue import Queue
shared_queue = Queue()
```

---

## Summary: Threads and Async Mental Model

✅ **Use `to_thread()` for blocking operations** - simple and effective

✅ **Use `run_in_executor()` for control** - custom thread pools

✅ **Use `call_soon_threadsafe()` from threads** - schedule on event loop

✅ **Prefer async over threads** - when async libraries available

✅ **Threads for blocking I/O** - when no async alternative

✅ **Thread pools for parallelism** - multiple blocking operations

✅ **Be careful with shared state** - use thread-safe structures

---

## What's Next?

Threads handle blocking I/O. For running external programs, we need subprocesses.

In [Chapter 17: Subprocesses](./17-subprocesses.md), we'll cover:
- `asyncio.create_subprocess_exec()`
- `asyncio.create_subprocess_shell()`
- Streaming stdout/stderr
- Process communication
- Timeout and cancellation
- Practical subprocess patterns

Subprocesses are essential for running external tools and commands.

---

**Previous:** [← Chapter 15: Timeouts](../part5-cancellation/15-timeouts.md)  
**Next:** [Chapter 17: Subprocesses →](./17-subprocesses.md)