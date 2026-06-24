# Chapter 15: Timeouts

## Introduction

Timeouts prevent operations from running indefinitely. They're essential for building robust systems that fail fast rather than hang. Python 3.11+ introduced `asyncio.timeout()`, a modern context manager for timeouts. This chapter covers both modern and legacy timeout patterns.

By the end of this chapter, you'll understand:
- `asyncio.timeout()` (Python 3.11+)
- `asyncio.wait_for()` (legacy approach)
- Nested timeouts and propagation
- Timeout vs cancellation
- Practical timeout patterns
- Timeout best practices

---

## Modern Timeouts: asyncio.timeout()

Python 3.11+ provides `asyncio.timeout()`, a clean context manager for timeouts.

### Basic Timeout

```python
import asyncio

async def basic_timeout():
    """Basic timeout usage"""
    
    async def slow_operation():
        print("Starting slow operation")
        await asyncio.sleep(10.0)
        print("Completed (never reached)")
        return "result"
    
    try:
        async with asyncio.timeout(2.0):
            result = await slow_operation()
            print(f"Result: {result}")
    except asyncio.TimeoutError:
        print("Operation timed out!")

asyncio.run(basic_timeout())

# Output:
# Starting slow operation
# Operation timed out!
```

**Key points:**
- Clean context manager syntax
- Raises `TimeoutError` on timeout
- Automatically cancels the operation

### Timeout Mechanics

```python
import asyncio
import time

async def timeout_mechanics():
    """Understand what happens during timeout"""
    
    async def operation():
        try:
            print("Operation: Starting")
            await asyncio.sleep(10.0)
            print("Operation: Done (never reached)")
        except asyncio.CancelledError:
            print("Operation: Cancelled by timeout")
            raise
    
    start = time.time()
    
    try:
        async with asyncio.timeout(2.0):
            await operation()
    except asyncio.TimeoutError:
        elapsed = time.time() - start
        print(f"Timed out after {elapsed:.2f}s")

asyncio.run(timeout_mechanics())

# Output:
# Operation: Starting
# Operation: Cancelled by timeout
# Timed out after 2.00s
```

**What happens:**
1. Timeout context manager starts timer
2. Operation begins
3. After 2 seconds, timeout expires
4. `CancelledError` injected into operation
5. `TimeoutError` raised to caller

---

## Legacy Timeouts: wait_for()

Before Python 3.11, `asyncio.wait_for()` was the standard approach.

### Basic wait_for

```python
import asyncio

async def wait_for_demo():
    """Using wait_for for timeouts"""
    
    async def slow_operation():
        await asyncio.sleep(10.0)
        return "result"
    
    try:
        result = await asyncio.wait_for(
            slow_operation(),
            timeout=2.0
        )
        print(f"Result: {result}")
    except asyncio.TimeoutError:
        print("Operation timed out!")

asyncio.run(wait_for_demo())
```

### wait_for vs timeout()

```python
import asyncio

async def comparison():
    async def operation():
        await asyncio.sleep(10.0)
        return "result"
    
    # Modern: asyncio.timeout() (Python 3.11+)
    try:
        async with asyncio.timeout(2.0):
            result = await operation()
    except asyncio.TimeoutError:
        print("Timeout (modern)")
    
    # Legacy: asyncio.wait_for()
    try:
        result = await asyncio.wait_for(operation(), timeout=2.0)
    except asyncio.TimeoutError:
        print("Timeout (legacy)")

# asyncio.run(comparison())
```

**Prefer `asyncio.timeout()`:**
- Cleaner syntax
- Better for multiple operations
- More composable

**Use `wait_for()` when:**
- Python < 3.11
- Timing a single operation
- Need backward compatibility

---

## Nested Timeouts

Timeouts can be nested, with inner timeouts taking precedence.

### Basic Nesting

```python
import asyncio

async def nested_timeouts():
    """Nested timeout behavior"""
    
    async def operation():
        print("Operation: Starting")
        await asyncio.sleep(10.0)
        print("Operation: Done")
    
    try:
        # Outer timeout: 5 seconds
        async with asyncio.timeout(5.0):
            print("Outer timeout: 5s")
            
            try:
                # Inner timeout: 2 seconds (fires first)
                async with asyncio.timeout(2.0):
                    print("Inner timeout: 2s")
                    await operation()
            except asyncio.TimeoutError:
                print("Inner timeout fired!")
                raise  # Propagate to outer
    
    except asyncio.TimeoutError:
        print("Caught at outer level")

asyncio.run(nested_timeouts())

# Output:
# Outer timeout: 5s
# Inner timeout: 2s
# Operation: Starting
# Inner timeout fired!
# Caught at outer level
```

### Timeout Propagation

```python
import asyncio

async def timeout_propagation():
    """How timeouts propagate through call stack"""
    
    async def level_3():
        print("Level 3: Starting")
        await asyncio.sleep(10.0)
        print("Level 3: Done")
    
    async def level_2():
        print("Level 2: Starting")
        await level_3()
        print("Level 2: Done")
    
    async def level_1():
        print("Level 1: Starting")
        await level_2()
        print("Level 1: Done")
    
    try:
        async with asyncio.timeout(2.0):
            await level_1()
    except asyncio.TimeoutError:
        print("Timeout propagated to top")

asyncio.run(timeout_propagation())

# Output:
# Level 1: Starting
# Level 2: Starting
# Level 3: Starting
# Timeout propagated to top
```

---

## Timeout Patterns

### Pattern 1: Per-Operation Timeout

```python
import asyncio

async def per_operation_timeout():
    """Timeout for each operation"""
    
    async def fetch_data(source):
        print(f"Fetching from {source}")
        await asyncio.sleep(1.0)
        return f"data from {source}"
    
    results = []
    
    for source in ["API-1", "API-2", "API-3"]:
        try:
            async with asyncio.timeout(2.0):
                result = await fetch_data(source)
                results.append(result)
        except asyncio.TimeoutError:
            print(f"Timeout fetching from {source}")
            results.append(None)
    
    print(f"Results: {results}")

asyncio.run(per_operation_timeout())
```

### Pattern 2: Total Timeout

```python
import asyncio

async def total_timeout():
    """Timeout for entire sequence"""
    
    async def fetch_data(source):
        await asyncio.sleep(1.0)
        return f"data from {source}"
    
    try:
        # Total time limit: 5 seconds
        async with asyncio.timeout(5.0):
            results = []
            for source in ["API-1", "API-2", "API-3", "API-4", "API-5"]:
                result = await fetch_data(source)
                results.append(result)
            
            print(f"All results: {results}")
    
    except asyncio.TimeoutError:
        print("Total operation timed out")

asyncio.run(total_timeout())
```

### Pattern 3: Retry with Timeout

```python
import asyncio

async def retry_with_timeout():
    """Retry operation with timeout"""
    
    async def unreliable_operation():
        """Simulated unreliable operation"""
        import random
        await asyncio.sleep(random.uniform(0.5, 3.0))
        if random.random() < 0.7:
            raise Exception("Operation failed")
        return "success"
    
    max_retries = 3
    timeout_per_attempt = 2.0
    
    for attempt in range(max_retries):
        try:
            async with asyncio.timeout(timeout_per_attempt):
                result = await unreliable_operation()
                print(f"Success on attempt {attempt + 1}: {result}")
                return result
        
        except asyncio.TimeoutError:
            print(f"Attempt {attempt + 1}: Timeout")
        except Exception as e:
            print(f"Attempt {attempt + 1}: {e}")
        
        if attempt < max_retries - 1:
            await asyncio.sleep(1.0)  # Wait before retry
    
    print("All retries failed")

asyncio.run(retry_with_timeout())
```

### Pattern 4: Timeout with Cleanup

```python
import asyncio

async def timeout_with_cleanup():
    """Ensure cleanup happens on timeout"""
    
    async def operation_with_resources():
        resource = await acquire_resource()
        try:
            async with asyncio.timeout(2.0):
                await use_resource(resource)
        except asyncio.TimeoutError:
            print("Operation timed out, cleaning up")
            raise
        finally:
            await release_resource(resource)
    
    async def acquire_resource():
        print("Acquiring resource")
        await asyncio.sleep(0.1)
        return "resource"
    
    async def use_resource(resource):
        print(f"Using {resource}")
        await asyncio.sleep(10.0)
    
    async def release_resource(resource):
        print(f"Releasing {resource}")
        await asyncio.sleep(0.1)
    
    try:
        await operation_with_resources()
    except asyncio.TimeoutError:
        print("Handled timeout")

asyncio.run(timeout_with_cleanup())

# Output:
# Acquiring resource
# Using resource
# Operation timed out, cleaning up
# Releasing resource
# Handled timeout
```

---

## Real-world: HTTP Client with Timeouts

Complete HTTP client with multiple timeout levels.

```python
import asyncio
import aiohttp
from typing import Optional, Dict, Any

class TimeoutHTTPClient:
    """
    HTTP client with configurable timeouts.
    
    Supports:
    - Connection timeout
    - Read timeout
    - Total request timeout
    """
    
    def __init__(
        self,
        connect_timeout: float = 5.0,
        read_timeout: float = 30.0,
        total_timeout: float = 60.0
    ):
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.total_timeout = total_timeout
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        timeout = aiohttp.ClientTimeout(
            total=self.total_timeout,
            connect=self.connect_timeout,
            sock_read=self.read_timeout
        )
        self.session = aiohttp.ClientSession(timeout=timeout)
        return self
    
    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()
    
    async def get(
        self,
        url: str,
        timeout: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        GET request with optional timeout override.
        
        Args:
            url: URL to fetch
            timeout: Override total timeout for this request
        """
        try:
            # Use custom timeout if provided
            if timeout:
                async with asyncio.timeout(timeout):
                    return await self._do_get(url)
            else:
                return await self._do_get(url)
        
        except asyncio.TimeoutError:
            return {
                "error": "timeout",
                "url": url,
                "message": f"Request timed out after {timeout or self.total_timeout}s"
            }
        except Exception as e:
            return {
                "error": "exception",
                "url": url,
                "message": str(e)
            }
    
    async def _do_get(self, url: str) -> Dict[str, Any]:
        """Perform GET request"""
        async with self.session.get(url) as response:
            return {
                "status": response.status,
                "url": url,
                "data": await response.json()
            }
    
    async def batch_get(
        self,
        urls: list[str],
        per_request_timeout: Optional[float] = None,
        total_timeout: Optional[float] = None
    ) -> list[Dict[str, Any]]:
        """
        Fetch multiple URLs with timeouts.
        
        Args:
            urls: List of URLs to fetch
            per_request_timeout: Timeout for each request
            total_timeout: Timeout for entire batch
        """
        async def fetch_one(url):
            return await self.get(url, timeout=per_request_timeout)
        
        try:
            if total_timeout:
                async with asyncio.timeout(total_timeout):
                    return await asyncio.gather(*[fetch_one(url) for url in urls])
            else:
                return await asyncio.gather(*[fetch_one(url) for url in urls])
        
        except asyncio.TimeoutError:
            return [{
                "error": "batch_timeout",
                "message": f"Batch timed out after {total_timeout}s"
            }]

async def http_client_demo():
    """Demonstrate HTTP client with timeouts"""
    
    async with TimeoutHTTPClient(
        connect_timeout=5.0,
        read_timeout=10.0,
        total_timeout=30.0
    ) as client:
        # Single request with default timeout
        result1 = await client.get("https://httpbin.org/delay/1")
        print(f"Result 1: {result1.get('status')}")
        
        # Single request with custom timeout
        result2 = await client.get(
            "https://httpbin.org/delay/5",
            timeout=2.0  # Will timeout
        )
        print(f"Result 2: {result2}")
        
        # Batch requests
        urls = [
            "https://httpbin.org/delay/1",
            "https://httpbin.org/delay/2",
            "https://httpbin.org/delay/3"
        ]
        results = await client.batch_get(
            urls,
            per_request_timeout=5.0,
            total_timeout=10.0
        )
        print(f"Batch results: {len(results)} responses")

# asyncio.run(http_client_demo())
```

---

## Real-world: Database Query with Timeout

```python
import asyncio
from typing import Any, Optional

class DatabaseClient:
    """Database client with query timeouts"""
    
    def __init__(self, default_timeout: float = 30.0):
        self.default_timeout = default_timeout
        self.connection = None
    
    async def connect(self):
        """Connect to database"""
        print("Connecting to database...")
        await asyncio.sleep(0.5)
        self.connection = "connected"
        print("Connected")
    
    async def execute(
        self,
        query: str,
        timeout: Optional[float] = None
    ) -> Any:
        """
        Execute query with timeout.
        
        Args:
            query: SQL query
            timeout: Query timeout (uses default if None)
        """
        if not self.connection:
            raise RuntimeError("Not connected")
        
        timeout = timeout or self.default_timeout
        
        try:
            async with asyncio.timeout(timeout):
                return await self._execute_query(query)
        
        except asyncio.TimeoutError:
            print(f"Query timed out after {timeout}s: {query}")
            # Cancel query on database side
            await self._cancel_query()
            raise
    
    async def _execute_query(self, query: str) -> Any:
        """Execute query (simulated)"""
        print(f"Executing: {query}")
        
        # Simulate slow query
        if "slow" in query.lower():
            await asyncio.sleep(10.0)
        else:
            await asyncio.sleep(1.0)
        
        return f"Result for: {query}"
    
    async def _cancel_query(self):
        """Cancel running query"""
        print("Cancelling query on database")
        await asyncio.sleep(0.1)
    
    async def transaction(
        self,
        queries: list[str],
        timeout: Optional[float] = None
    ):
        """
        Execute transaction with timeout.
        
        All queries must complete within timeout.
        """
        timeout = timeout or self.default_timeout
        
        try:
            async with asyncio.timeout(timeout):
                await self._begin_transaction()
                
                for query in queries:
                    await self._execute_query(query)
                
                await self._commit_transaction()
        
        except asyncio.TimeoutError:
            print("Transaction timed out, rolling back")
            await self._rollback_transaction()
            raise
        
        except Exception:
            print("Transaction error, rolling back")
            await self._rollback_transaction()
            raise
    
    async def _begin_transaction(self):
        print("BEGIN TRANSACTION")
        await asyncio.sleep(0.1)
    
    async def _commit_transaction(self):
        print("COMMIT")
        await asyncio.sleep(0.1)
    
    async def _rollback_transaction(self):
        print("ROLLBACK")
        await asyncio.sleep(0.1)

async def database_demo():
    """Demonstrate database client with timeouts"""
    
    db = DatabaseClient(default_timeout=5.0)
    await db.connect()
    
    # Fast query
    try:
        result = await db.execute("SELECT * FROM users")
        print(f"Result: {result}")
    except asyncio.TimeoutError:
        print("Query timed out")
    
    # Slow query with custom timeout
    try:
        result = await db.execute(
            "SELECT * FROM slow_table",
            timeout=2.0
        )
        print(f"Result: {result}")
    except asyncio.TimeoutError:
        print("Slow query timed out (expected)")
    
    # Transaction with timeout
    try:
        await db.transaction([
            "INSERT INTO users VALUES (1)",
            "INSERT INTO logs VALUES (1)",
            "UPDATE stats SET count = count + 1"
        ], timeout=5.0)
    except asyncio.TimeoutError:
        print("Transaction timed out")

asyncio.run(database_demo())
```

---

## Timeout vs Cancellation

Understanding the relationship between timeouts and cancellation.

### Timeout Causes Cancellation

```python
import asyncio

async def timeout_causes_cancellation():
    """Timeout internally uses cancellation"""
    
    async def operation():
        try:
            await asyncio.sleep(10.0)
        except asyncio.CancelledError:
            print("Caught CancelledError (from timeout)")
            raise
    
    try:
        async with asyncio.timeout(2.0):
            await operation()
    except asyncio.TimeoutError:
        print("Caught TimeoutError")

asyncio.run(timeout_causes_cancellation())

# Output:
# Caught CancelledError (from timeout)
# Caught TimeoutError
```

**Key insight:** Timeout raises `CancelledError` internally, then converts to `TimeoutError`.

### Distinguishing Timeout from Cancellation

```python
import asyncio

async def distinguish_timeout_cancellation():
    """Distinguish between timeout and explicit cancellation"""
    
    async def operation():
        try:
            await asyncio.sleep(10.0)
        except asyncio.CancelledError:
            # Check if we're in a timeout context
            print("Operation cancelled")
            raise
    
    # Scenario 1: Timeout
    try:
        async with asyncio.timeout(2.0):
            await operation()
    except asyncio.TimeoutError:
        print("Scenario 1: Timeout")
    
    # Scenario 2: Explicit cancellation
    task = asyncio.create_task(operation())
    await asyncio.sleep(0.1)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        print("Scenario 2: Explicit cancellation")

asyncio.run(distinguish_timeout_cancellation())
```

---

## Common Pitfalls

### Pitfall 1: Timeout Too Short

```python
# ❌ BAD: Timeout shorter than operation
async with asyncio.timeout(0.1):
    await asyncio.sleep(1.0)  # Always times out

# ✅ GOOD: Reasonable timeout
async with asyncio.timeout(5.0):
    await asyncio.sleep(1.0)
```

### Pitfall 2: No Timeout

```python
# ❌ BAD: No timeout (can hang forever)
await external_api_call()

# ✅ GOOD: Always use timeout for external calls
async with asyncio.timeout(30.0):
    await external_api_call()
```

### Pitfall 3: Swallowing TimeoutError

```python
# ❌ BAD: Swallowing timeout
try:
    async with asyncio.timeout(5.0):
        await operation()
except asyncio.TimeoutError:
    pass  # Silent failure

# ✅ GOOD: Handle timeout appropriately
try:
    async with asyncio.timeout(5.0):
        await operation()
except asyncio.TimeoutError:
    logger.error("Operation timed out")
    raise  # Or handle appropriately
```

---

## Best Practices

### ✅ DO: Use timeouts for external calls

```python
# Always timeout external operations
async with asyncio.timeout(30.0):
    await api_call()
```

### ✅ DO: Use appropriate timeout values

```python
# Based on expected operation time
async with asyncio.timeout(5.0):  # Fast operation
    await cache_lookup()

async with asyncio.timeout(60.0):  # Slow operation
    await database_query()
```

### ✅ DO: Cleanup on timeout

```python
try:
    async with asyncio.timeout(5.0):
        await operation()
except asyncio.TimeoutError:
    await cleanup()
    raise
```

### ✅ DO: Log timeouts

```python
try:
    async with asyncio.timeout(5.0):
        await operation()
except asyncio.TimeoutError:
    logger.warning("Operation timed out", extra={"timeout": 5.0})
    raise
```

### ❌ DON'T: Use timeouts for flow control

```python
# Bad: Using timeout as flow control
try:
    async with asyncio.timeout(0.1):
        await check_condition()
except asyncio.TimeoutError:
    # Do something else
    pass

# Good: Use proper control flow
if await check_condition():
    # Do something
    pass
```

---

## Summary: Timeout Mental Model

✅ **Always timeout external operations** - prevent hangs

✅ **Use `asyncio.timeout()`** - modern, clean syntax (Python 3.11+)

✅ **Timeouts cause cancellation** - internally uses `CancelledError`

✅ **Cleanup in finally** - ensure resources released

✅ **Nested timeouts work** - inner timeout fires first

✅ **Log timeout events** - for debugging and monitoring

✅ **Choose appropriate values** - based on operation characteristics

---

## What's Next?

We've completed Part 5 on Cancellation. Next, we explore integration with other Python concurrency primitives.

In [Chapter 16: Threads and Async](../part6-integration/16-threads-and-async.md), we'll cover:
- `asyncio.to_thread()` for blocking operations
- `run_in_executor()` for thread pools
- Thread-safe scheduling with `call_soon_threadsafe()`
- Mixing async and sync code
- When to use threads vs async
- Common integration patterns

Understanding thread integration is crucial for real-world applications.

---

**Previous:** [← Chapter 14: Cancellation Safety](./14-cancellation-safety.md)  
**Next:** [Chapter 16: Threads and Async →](../part6-integration/16-threads-and-async.md)