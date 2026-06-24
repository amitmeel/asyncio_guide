# Chapter 12: Semaphore - Concurrency Limiting

## Introduction

`asyncio.Semaphore` limits the number of tasks that can access a resource simultaneously. Unlike locks (which allow only one task), semaphores allow N tasks to proceed concurrently.

By the end of this chapter, you'll understand:
- Concurrency limiting with semaphores
- Resource pools and connection management
- Rate limiting patterns
- Semaphore vs Queue for resource management
- BoundedSemaphore for safety
- Real-world patterns: API rate limiting, connection pools, worker pools

---

## What is a Semaphore?

A semaphore maintains a counter. Tasks can acquire (decrement) and release (increment) the counter. When the counter reaches zero, tasks block until another task releases.

### Basic Semaphore Usage

```python
import asyncio

async def basic_semaphore_demo():
    # Allow max 3 concurrent tasks
    semaphore = asyncio.Semaphore(3)
    
    async def worker(worker_id):
        print(f"Worker {worker_id}: Waiting to acquire")
        
        async with semaphore:
            print(f"Worker {worker_id}: Acquired (running)")
            await asyncio.sleep(1.0)
            print(f"Worker {worker_id}: Done")
        
        print(f"Worker {worker_id}: Released")
    
    # Start 10 workers, but only 3 run at a time
    await asyncio.gather(*[worker(i) for i in range(10)])

asyncio.run(basic_semaphore_demo())

# Output:
# Worker 0: Waiting to acquire
# Worker 1: Waiting to acquire
# Worker 2: Waiting to acquire
# Worker 3: Waiting to acquire
# ...
# Worker 0: Acquired (running)
# Worker 1: Acquired (running)
# Worker 2: Acquired (running)
# (Workers 3-9 wait)
# Worker 0: Done
# Worker 0: Released
# Worker 3: Acquired (running)  ← Next worker starts
# ...
```

**Key characteristics:**
- Limits concurrent access to N tasks
- Tasks block when limit reached
- Automatic release with context manager
- Perfect for resource pools

---

## Semaphore Operations

### Core Operations

```python
import asyncio

semaphore = asyncio.Semaphore(3)

# Acquire (decrement counter)
async with semaphore:
    # Do work
    pass

# Manual acquire/release
await semaphore.acquire()
try:
    # Do work
    pass
finally:
    semaphore.release()

# Check if can acquire without blocking
if semaphore.locked():
    print("Semaphore is at capacity")
```

### Semaphore State

```python
import asyncio

async def semaphore_state():
    sem = asyncio.Semaphore(3)
    
    print(f"Initial value: {sem._value}")  # 3
    
    await sem.acquire()
    print(f"After acquire: {sem._value}")  # 2
    
    await sem.acquire()
    print(f"After acquire: {sem._value}")  # 1
    
    await sem.acquire()
    print(f"After acquire: {sem._value}")  # 0
    
    print(f"Locked: {sem.locked()}")  # True
    
    sem.release()
    print(f"After release: {sem._value}")  # 1

asyncio.run(semaphore_state())
```

---

## Concurrency Limiting

Semaphores excel at limiting concurrent operations.

### Limiting Concurrent Downloads

```python
import asyncio
import aiohttp

async def download_with_limit():
    # Limit to 5 concurrent downloads
    semaphore = asyncio.Semaphore(5)
    
    async def download(session, url, url_id):
        async with semaphore:
            print(f"Downloading {url_id}...")
            async with session.get(url) as response:
                data = await response.read()
                print(f"Downloaded {url_id}: {len(data)} bytes")
                return data
    
    urls = [f"https://httpbin.org/delay/1?id={i}" for i in range(20)]
    
    async with aiohttp.ClientSession() as session:
        tasks = [download(session, url, i) for i, url in enumerate(urls)]
        results = await asyncio.gather(*tasks)
    
    print(f"Downloaded {len(results)} files")

# Only 5 downloads run simultaneously
asyncio.run(download_with_limit())
```

### Limiting Database Connections

```python
import asyncio

class DatabasePool:
    """Database connection pool using semaphore"""
    
    def __init__(self, max_connections=10):
        self.semaphore = asyncio.Semaphore(max_connections)
        self.active_connections = 0
    
    async def execute_query(self, query):
        """Execute query with connection limit"""
        async with self.semaphore:
            self.active_connections += 1
            print(f"Executing query (active: {self.active_connections})")
            
            # Simulate query execution
            await asyncio.sleep(0.5)
            result = f"Result for: {query}"
            
            self.active_connections -= 1
            return result

async def database_pool_demo():
    pool = DatabasePool(max_connections=3)
    
    # Execute 10 queries, but only 3 run concurrently
    queries = [f"SELECT * FROM table_{i}" for i in range(10)]
    results = await asyncio.gather(*[
        pool.execute_query(query) for query in queries
    ])
    
    print(f"Executed {len(results)} queries")

asyncio.run(database_pool_demo())
```

---

## Resource Pools

Semaphores are perfect for managing limited resources.

### Connection Pool

```python
import asyncio
from typing import List, Optional

class Connection:
    """Simulated connection"""
    def __init__(self, conn_id):
        self.conn_id = conn_id
        self.in_use = False
    
    async def execute(self, query):
        await asyncio.sleep(0.1)
        return f"Result from conn {self.conn_id}: {query}"

class ConnectionPool:
    """Connection pool with semaphore-based limiting"""
    
    def __init__(self, size: int):
        self.size = size
        self.semaphore = asyncio.Semaphore(size)
        self.connections: List[Connection] = [
            Connection(i) for i in range(size)
        ]
        self.lock = asyncio.Lock()
    
    async def acquire(self) -> Connection:
        """Acquire a connection from the pool"""
        # Wait for available slot
        await self.semaphore.acquire()
        
        # Get free connection
        async with self.lock:
            for conn in self.connections:
                if not conn.in_use:
                    conn.in_use = True
                    print(f"Acquired connection {conn.conn_id}")
                    return conn
        
        raise RuntimeError("No connections available")
    
    async def release(self, conn: Connection):
        """Release connection back to pool"""
        async with self.lock:
            conn.in_use = False
            print(f"Released connection {conn.conn_id}")
        
        self.semaphore.release()
    
    async def execute(self, query: str):
        """Execute query using pool"""
        conn = await self.acquire()
        try:
            result = await conn.execute(query)
            return result
        finally:
            await self.release(conn)

async def connection_pool_demo():
    pool = ConnectionPool(size=3)
    
    async def worker(worker_id):
        for i in range(3):
            query = f"SELECT * FROM worker_{worker_id}_query_{i}"
            result = await pool.execute(query)
            print(f"Worker {worker_id}: {result}")
    
    # 5 workers sharing 3 connections
    await asyncio.gather(*[worker(i) for i in range(5)])

asyncio.run(connection_pool_demo())
```

---

## Rate Limiting

Semaphores can implement rate limiting patterns.

### Simple Rate Limiter

```python
import asyncio
import time

class RateLimiter:
    """Rate limiter using semaphore"""
    
    def __init__(self, rate: int, period: float):
        """
        Args:
            rate: Number of operations allowed
            period: Time period in seconds
        """
        self.rate = rate
        self.period = period
        self.semaphore = asyncio.Semaphore(rate)
    
    async def acquire(self):
        """Acquire permission to proceed"""
        await self.semaphore.acquire()
        
        # Schedule release after period
        asyncio.create_task(self._release_after_period())
    
    async def _release_after_period(self):
        """Release semaphore after period"""
        await asyncio.sleep(self.period)
        self.semaphore.release()
    
    async def __aenter__(self):
        await self.acquire()
        return self
    
    async def __aexit__(self, *args):
        pass

async def rate_limiter_demo():
    # Allow 5 requests per 2 seconds
    limiter = RateLimiter(rate=5, period=2.0)
    
    async def make_request(request_id):
        async with limiter:
            print(f"Request {request_id} at {time.time():.2f}")
            await asyncio.sleep(0.1)
    
    # Make 15 requests
    await asyncio.gather(*[make_request(i) for i in range(15)])

asyncio.run(rate_limiter_demo())

# Output shows requests grouped by rate limit:
# Request 0 at 1234567890.00
# Request 1 at 1234567890.01
# Request 2 at 1234567890.02
# Request 3 at 1234567890.03
# Request 4 at 1234567890.04
# (2 second pause)
# Request 5 at 1234567892.00
# ...
```

### Token Bucket Rate Limiter

```python
import asyncio
import time

class TokenBucket:
    """Token bucket rate limiter"""
    
    def __init__(self, rate: float, capacity: int):
        """
        Args:
            rate: Tokens per second
            capacity: Maximum tokens
        """
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.last_update = time.monotonic()
        self.lock = asyncio.Lock()
    
    async def acquire(self, tokens: int = 1):
        """Acquire tokens, wait if not available"""
        while True:
            async with self.lock:
                now = time.monotonic()
                elapsed = now - self.last_update
                
                # Add tokens based on elapsed time
                self.tokens = min(
                    self.capacity,
                    self.tokens + elapsed * self.rate
                )
                self.last_update = now
                
                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return
            
            # Wait before retrying
            await asyncio.sleep(0.1)

async def token_bucket_demo():
    # 10 tokens per second, max 20 tokens
    bucket = TokenBucket(rate=10.0, capacity=20)
    
    async def make_request(request_id):
        await bucket.acquire(tokens=1)
        print(f"Request {request_id} at {time.time():.2f}")
    
    # Burst of 30 requests
    await asyncio.gather(*[make_request(i) for i in range(30)])

asyncio.run(token_bucket_demo())
```

---

## BoundedSemaphore

`BoundedSemaphore` prevents releasing more than acquired.

### BoundedSemaphore vs Semaphore

```python
import asyncio

async def bounded_vs_unbounded():
    # Regular Semaphore: Can release more than acquired
    sem = asyncio.Semaphore(1)
    sem.release()  # OK (value becomes 2)
    sem.release()  # OK (value becomes 3)
    print(f"Semaphore value: {sem._value}")  # 3
    
    # BoundedSemaphore: Cannot release more than initial value
    bounded = asyncio.BoundedSemaphore(1)
    try:
        bounded.release()  # ValueError!
    except ValueError as e:
        print(f"BoundedSemaphore error: {e}")

asyncio.run(bounded_vs_unbounded())
```

### When to Use BoundedSemaphore

```python
import asyncio

class ResourcePool:
    """Resource pool with bounded semaphore for safety"""
    
    def __init__(self, size: int):
        # Use BoundedSemaphore to catch bugs
        self.semaphore = asyncio.BoundedSemaphore(size)
    
    async def acquire(self):
        await self.semaphore.acquire()
    
    async def release(self):
        try:
            self.semaphore.release()
        except ValueError:
            # Catch double-release bugs
            print("ERROR: Attempted to release more than acquired!")
            raise

# Use BoundedSemaphore when:
# - Want to catch double-release bugs
# - Resource count is fixed
# - Safety is more important than flexibility
```

---

## Semaphore vs Queue

Both can manage resources, but have different use cases.

### Comparison

```python
import asyncio

# Semaphore: Limit concurrency
semaphore = asyncio.Semaphore(5)
async with semaphore:
    # Max 5 tasks here
    pass

# Queue: Pass actual resources
queue = asyncio.Queue()
for i in range(5):
    await queue.put(f"resource-{i}")

resource = await queue.get()
try:
    # Use resource
    pass
finally:
    await queue.put(resource)  # Return to pool
```

### When to Use Each

**Use Semaphore when:**
- Just need to limit concurrency
- Resources are interchangeable
- Don't need to track specific resources
- Simpler implementation

**Use Queue when:**
- Need to pass actual resource objects
- Resources have state
- Need FIFO ordering
- Want to track which resource is used

### Example: Semaphore for Simple Limiting

```python
import asyncio

async def semaphore_example():
    # Just limit concurrent API calls
    semaphore = asyncio.Semaphore(10)
    
    async def api_call(url):
        async with semaphore:
            # Make API call
            pass
```

### Example: Queue for Resource Management

```python
import asyncio

async def queue_example():
    # Manage actual connection objects
    pool = asyncio.Queue()
    
    # Add connections
    for i in range(10):
        await pool.put(Connection(i))
    
    # Get specific connection
    conn = await pool.get()
    try:
        await conn.execute("query")
    finally:
        await pool.put(conn)
```

---

## Real-world: API Rate Limiting

Complete API client with rate limiting.

```python
import asyncio
import aiohttp
import time
from typing import List, Dict, Any

class RateLimitedAPIClient:
    """
    API client with rate limiting using semaphore.
    
    Limits concurrent requests and implements retry logic.
    """
    
    def __init__(
        self,
        base_url: str,
        max_concurrent: int = 10,
        requests_per_second: float = 5.0
    ):
        self.base_url = base_url
        self.max_concurrent = max_concurrent
        self.requests_per_second = requests_per_second
        
        # Concurrency limiter
        self.semaphore = asyncio.Semaphore(max_concurrent)
        
        # Rate limiter
        self.rate_limiter = TokenBucket(
            rate=requests_per_second,
            capacity=int(requests_per_second * 2)
        )
        
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()
    
    async def request(
        self,
        method: str,
        endpoint: str,
        **kwargs
    ) -> Dict[str, Any]:
        """Make rate-limited request"""
        # Wait for rate limit
        await self.rate_limiter.acquire()
        
        # Wait for concurrency slot
        async with self.semaphore:
            url = f"{self.base_url}{endpoint}"
            
            print(f"Request: {method} {endpoint}")
            
            async with self.session.request(method, url, **kwargs) as response:
                response.raise_for_status()
                return await response.json()
    
    async def get(self, endpoint: str, **kwargs) -> Dict[str, Any]:
        """GET request"""
        return await self.request("GET", endpoint, **kwargs)
    
    async def post(self, endpoint: str, **kwargs) -> Dict[str, Any]:
        """POST request"""
        return await self.request("POST", endpoint, **kwargs)
    
    async def batch_get(
        self,
        endpoints: List[str]
    ) -> List[Dict[str, Any]]:
        """Fetch multiple endpoints with rate limiting"""
        tasks = [self.get(endpoint) for endpoint in endpoints]
        return await asyncio.gather(*tasks, return_exceptions=True)

async def api_client_demo():
    """Demonstrate rate-limited API client"""
    
    async with RateLimitedAPIClient(
        base_url="https://api.example.com",
        max_concurrent=5,
        requests_per_second=10.0
    ) as client:
        # Make many requests - automatically rate limited
        endpoints = [f"/users/{i}" for i in range(50)]
        
        start = time.time()
        results = await client.batch_get(endpoints)
        elapsed = time.time() - start
        
        print(f"Fetched {len(results)} endpoints in {elapsed:.2f}s")
        print(f"Rate: {len(results)/elapsed:.2f} req/s")

# asyncio.run(api_client_demo())
```

---

## Real-world: Worker Pool with Semaphore

```python
import asyncio
from typing import Callable, Any, List

class WorkerPool:
    """
    Worker pool using semaphore for concurrency control.
    
    Processes tasks with limited concurrency.
    """
    
    def __init__(self, max_workers: int):
        self.max_workers = max_workers
        self.semaphore = asyncio.Semaphore(max_workers)
        self.active_workers = 0
        self.lock = asyncio.Lock()
    
    async def submit(
        self,
        func: Callable,
        *args,
        **kwargs
    ) -> Any:
        """Submit task to worker pool"""
        async with self.semaphore:
            async with self.lock:
                self.active_workers += 1
                print(f"Active workers: {self.active_workers}/{self.max_workers}")
            
            try:
                result = await func(*args, **kwargs)
                return result
            finally:
                async with self.lock:
                    self.active_workers -= 1
    
    async def map(
        self,
        func: Callable,
        items: List[Any]
    ) -> List[Any]:
        """Map function over items with limited concurrency"""
        tasks = [self.submit(func, item) for item in items]
        return await asyncio.gather(*tasks)

async def worker_pool_demo():
    """Demonstrate worker pool"""
    
    async def process_item(item):
        """Simulate processing"""
        print(f"Processing {item}")
        await asyncio.sleep(1.0)
        return f"Processed: {item}"
    
    pool = WorkerPool(max_workers=3)
    
    # Process 10 items with max 3 concurrent
    items = list(range(10))
    results = await pool.map(process_item, items)
    
    print(f"Results: {results}")

asyncio.run(worker_pool_demo())
```

---

## Common Patterns

### Pattern 1: Weighted Semaphore

```python
import asyncio

class WeightedSemaphore:
    """Semaphore with weighted acquire"""
    
    def __init__(self, capacity: int):
        self.capacity = capacity
        self.available = capacity
        self.condition = asyncio.Condition()
    
    async def acquire(self, weight: int = 1):
        """Acquire with weight"""
        async with self.condition:
            while self.available < weight:
                await self.condition.wait()
            
            self.available -= weight
    
    async def release(self, weight: int = 1):
        """Release with weight"""
        async with self.condition:
            self.available += weight
            self.condition.notify_all()

# Use for resources with different sizes
# e.g., memory allocation, bandwidth
```

### Pattern 2: Timeout Semaphore

```python
import asyncio

async def acquire_with_timeout(semaphore, timeout):
    """Acquire semaphore with timeout"""
    try:
        async with asyncio.timeout(timeout):
            await semaphore.acquire()
            return True
    except asyncio.TimeoutError:
        return False

# Usage
sem = asyncio.Semaphore(1)
if await acquire_with_timeout(sem, timeout=5.0):
    try:
        # Do work
        pass
    finally:
        sem.release()
else:
    print("Timeout waiting for semaphore")
```

### Pattern 3: Semaphore with Metrics

```python
import asyncio
import time

class MetricsSemaphore:
    """Semaphore with usage metrics"""
    
    def __init__(self, value: int):
        self.semaphore = asyncio.Semaphore(value)
        self.max_value = value
        self.total_acquires = 0
        self.total_wait_time = 0.0
        self.lock = asyncio.Lock()
    
    async def acquire(self):
        start = time.monotonic()
        await self.semaphore.acquire()
        wait_time = time.monotonic() - start
        
        async with self.lock:
            self.total_acquires += 1
            self.total_wait_time += wait_time
    
    def release(self):
        self.semaphore.release()
    
    async def get_metrics(self):
        async with self.lock:
            return {
                "total_acquires": self.total_acquires,
                "avg_wait_time": (
                    self.total_wait_time / self.total_acquires
                    if self.total_acquires > 0 else 0
                ),
                "current_available": self.semaphore._value
            }
```

---

## Best Practices

### ✅ DO: Use context managers

```python
# Good: Automatic release
async with semaphore:
    await do_work()

# Bad: Manual release (error-prone)
await semaphore.acquire()
await do_work()
semaphore.release()  # Might not execute if error
```

### ✅ DO: Use BoundedSemaphore for fixed resources

```python
# Good: Catches bugs
pool = asyncio.BoundedSemaphore(10)

# Bad: Can release too many times
pool = asyncio.Semaphore(10)
```

### ✅ DO: Choose appropriate limits

```python
# Good: Based on actual constraints
# - CPU cores for CPU-bound work
# - Connection limits for I/O
# - API rate limits
sem = asyncio.Semaphore(os.cpu_count())

# Bad: Arbitrary limits
sem = asyncio.Semaphore(100)  # Why 100?
```

### ❌ DON'T: Use semaphore for mutual exclusion

```python
# Bad: Use Lock instead
sem = asyncio.Semaphore(1)  # Just use Lock!

# Good: Use Lock for mutual exclusion
lock = asyncio.Lock()
```

### ✅ DO: Combine with other primitives

```python
# Good: Semaphore + Queue for resource pool
semaphore = asyncio.Semaphore(10)
resources = asyncio.Queue()

async with semaphore:
    resource = await resources.get()
    try:
        await use_resource(resource)
    finally:
        await resources.put(resource)
```

---

## Summary: Semaphore Mental Model

✅ **Semaphore limits concurrency** - N tasks can proceed

✅ **Perfect for resource pools** - connections, workers, etc.

✅ **Use for rate limiting** - control request rates

✅ **BoundedSemaphore for safety** - catches double-release bugs

✅ **Simpler than Queue** - when you just need limits

✅ **Not for mutual exclusion** - use Lock for that

✅ **Combine with other primitives** - for sophisticated patterns

---

## What's Next?

We've covered all synchronization primitives. Now we tackle cancellation - one of async's most critical concepts.

In [Chapter 13: Cancellation Model](../part5-cancellation/13-cancellation-model.md), we'll cover:
- `task.cancel()` mechanics
- `CancelledError` propagation
- Injection points (where cancellation happens)
- Cooperative cancellation model
- Cancellation vs exceptions
- When and how to cancel tasks

Cancellation is essential for timeouts, cleanup, and graceful shutdown.

---

**Previous:** [← Chapter 11: Condition - Complex Coordination](./11-condition.md)  
**Next:** [Chapter 13: Cancellation Model →](../part5-cancellation/13-cancellation-model.md)