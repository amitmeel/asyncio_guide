# Chapter 24: Performance Optimization

## Overview

Asyncio performance optimization requires understanding the unique characteristics of asynchronous execution. Unlike traditional optimization, async performance depends on:

- **Context switch overhead**: Cost of switching between tasks
- **Event loop efficiency**: How quickly the loop processes ready tasks
- **I/O wait patterns**: How tasks wait for I/O operations
- **Task explosion**: Creating too many concurrent tasks
- **Memory overhead**: Per-task memory consumption
- **Batching opportunities**: Grouping operations for efficiency

This chapter provides systematic approaches to measure, analyze, and optimize asyncio applications.

## Mental Model

Think of async performance like **traffic flow optimization**:

```
┌─────────────────────────────────────────────────────────┐
│                    Event Loop (Highway)                  │
│                                                          │
│  Fast Lane:  [Task1] → [Task2] → [Task3] → ...         │
│  (Ready)                                                 │
│                                                          │
│  Waiting Area: {Task4: I/O, Task5: sleep, ...}         │
│  (Blocked)                                               │
│                                                          │
│  Bottlenecks:                                           │
│  - Too many tasks (traffic jam)                         │
│  - Slow callbacks (roadblock)                           │
│  - Excessive context switches (stop-and-go)             │
└─────────────────────────────────────────────────────────┘
```

**Key performance factors:**

1. **Throughput**: Tasks completed per second
2. **Latency**: Time from task start to completion
3. **Concurrency**: Number of tasks running simultaneously
4. **Overhead**: Cost of task management
5. **Resource usage**: Memory and CPU consumption

## Profiling Async Code

### 1. Basic Timing

```python
import asyncio
import time
from functools import wraps
from typing import Callable, Any

def async_timer(func: Callable) -> Callable:
    """Decorator to time async functions"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        start = time.perf_counter()
        try:
            result = await func(*args, **kwargs)
            return result
        finally:
            elapsed = time.perf_counter() - start
            print(f"{func.__name__} took {elapsed:.4f}s")
    return wrapper

@async_timer
async def slow_operation():
    """Example operation to time"""
    await asyncio.sleep(0.5)
    return "done"

# Usage
async def main():
    await slow_operation()
```

### 2. Detailed Performance Profiler

```python
import asyncio
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from collections import defaultdict

@dataclass
class PerformanceMetrics:
    """Metrics for a single operation"""
    name: str
    count: int = 0
    total_time: float = 0.0
    min_time: float = float('inf')
    max_time: float = 0.0
    times: List[float] = field(default_factory=list)
    
    def record(self, duration: float):
        """Record a timing"""
        self.count += 1
        self.total_time += duration
        self.min_time = min(self.min_time, duration)
        self.max_time = max(self.max_time, duration)
        self.times.append(duration)
    
    @property
    def avg_time(self) -> float:
        """Average time"""
        return self.total_time / self.count if self.count > 0 else 0.0
    
    @property
    def p50(self) -> float:
        """Median time"""
        if not self.times:
            return 0.0
        sorted_times = sorted(self.times)
        return sorted_times[len(sorted_times) // 2]
    
    @property
    def p95(self) -> float:
        """95th percentile"""
        if not self.times:
            return 0.0
        sorted_times = sorted(self.times)
        idx = int(len(sorted_times) * 0.95)
        return sorted_times[idx]
    
    @property
    def p99(self) -> float:
        """99th percentile"""
        if not self.times:
            return 0.0
        sorted_times = sorted(self.times)
        idx = int(len(sorted_times) * 0.99)
        return sorted_times[idx]

class AsyncProfiler:
    """
    Profiler for async operations.
    
    Tracks timing, call counts, and percentiles for async functions.
    """
    
    def __init__(self):
        self.metrics: Dict[str, PerformanceMetrics] = defaultdict(
            lambda: PerformanceMetrics(name="")
        )
        self.enabled = True
    
    def profile(self, name: Optional[str] = None):
        """Decorator to profile async functions"""
        def decorator(func):
            operation_name = name or func.__name__
            
            @wraps(func)
            async def wrapper(*args, **kwargs):
                if not self.enabled:
                    return await func(*args, **kwargs)
                
                start = time.perf_counter()
                try:
                    result = await func(*args, **kwargs)
                    return result
                finally:
                    duration = time.perf_counter() - start
                    self.metrics[operation_name].name = operation_name
                    self.metrics[operation_name].record(duration)
            
            return wrapper
        return decorator
    
    def record_operation(self, name: str, duration: float):
        """Manually record an operation"""
        self.metrics[name].name = name
        self.metrics[name].record(duration)
    
    def print_report(self):
        """Print performance report"""
        print("\n" + "="*80)
        print("PERFORMANCE REPORT")
        print("="*80)
        
        # Sort by total time
        sorted_metrics = sorted(
            self.metrics.values(),
            key=lambda m: m.total_time,
            reverse=True
        )
        
        print(f"\n{'Operation':<30} {'Count':>8} {'Total':>10} {'Avg':>10} {'P50':>10} {'P95':>10} {'P99':>10}")
        print("-"*80)
        
        for metric in sorted_metrics:
            print(
                f"{metric.name:<30} "
                f"{metric.count:>8} "
                f"{metric.total_time:>10.4f}s "
                f"{metric.avg_time:>10.4f}s "
                f"{metric.p50:>10.4f}s "
                f"{metric.p95:>10.4f}s "
                f"{metric.p99:>10.4f}s"
            )
        
        print("="*80)
    
    def get_slowest_operations(self, n: int = 5) -> List[PerformanceMetrics]:
        """Get N slowest operations by average time"""
        return sorted(
            self.metrics.values(),
            key=lambda m: m.avg_time,
            reverse=True
        )[:n]

# Global profiler instance
profiler = AsyncProfiler()

# Example usage
@profiler.profile()
async def fetch_data(url: str):
    """Simulated data fetch"""
    await asyncio.sleep(0.1)
    return f"data from {url}"

@profiler.profile()
async def process_data(data: str):
    """Simulated data processing"""
    await asyncio.sleep(0.05)
    return data.upper()

async def profiling_example():
    """Example showing profiler usage"""
    # Run operations
    for i in range(10):
        data = await fetch_data(f"url-{i}")
        await process_data(data)
    
    # Print report
    profiler.print_report()
```

## Context Switch Optimization

### 1. Reducing Context Switches

```python
import asyncio
from typing import List, Any

# ❌ BAD: Too many context switches
async def inefficient_processing(items: List[Any]):
    """Creates a task for each item - expensive!"""
    tasks = [asyncio.create_task(process_item(item)) for item in items]
    return await asyncio.gather(*tasks)

async def process_item(item: Any):
    """Process single item"""
    await asyncio.sleep(0.001)  # Simulated I/O
    return item * 2

# ✅ GOOD: Batch processing reduces switches
async def efficient_processing(items: List[Any], batch_size: int = 100):
    """Process items in batches"""
    results = []
    
    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]
        batch_results = await asyncio.gather(
            *[process_item(item) for item in batch]
        )
        results.extend(batch_results)
    
    return results

# ✅ BETTER: Sequential processing when appropriate
async def sequential_processing(items: List[Any]):
    """Process items sequentially when I/O is minimal"""
    results = []
    for item in items:
        result = await process_item(item)
        results.append(result)
    return results

async def context_switch_comparison():
    """Compare different approaches"""
    items = list(range(1000))
    
    # Measure inefficient approach
    start = time.perf_counter()
    await inefficient_processing(items)
    inefficient_time = time.perf_counter() - start
    
    # Measure efficient approach
    start = time.perf_counter()
    await efficient_processing(items)
    efficient_time = time.perf_counter() - start
    
    # Measure sequential approach
    start = time.perf_counter()
    await sequential_processing(items)
    sequential_time = time.perf_counter() - start
    
    print(f"Inefficient (1000 tasks): {inefficient_time:.4f}s")
    print(f"Efficient (batched):      {efficient_time:.4f}s")
    print(f"Sequential:               {sequential_time:.4f}s")
```

### 2. Task Pooling

```python
import asyncio
from typing import Callable, Any, Awaitable

class TaskPool:
    """
    Reusable task pool to reduce task creation overhead.
    
    Instead of creating new tasks, reuse existing workers.
    """
    
    def __init__(self, size: int):
        self.size = size
        self.queue: asyncio.Queue = asyncio.Queue()
        self.workers: List[asyncio.Task] = []
        self.results: asyncio.Queue = asyncio.Queue()
    
    async def worker(self):
        """Worker that processes tasks from queue"""
        while True:
            try:
                func, args, kwargs = await self.queue.get()
                
                if func is None:  # Shutdown signal
                    break
                
                try:
                    result = await func(*args, **kwargs)
                    await self.results.put(('success', result))
                except Exception as e:
                    await self.results.put(('error', e))
                finally:
                    self.queue.task_done()
            
            except asyncio.CancelledError:
                break
    
    async def start(self):
        """Start worker pool"""
        self.workers = [
            asyncio.create_task(self.worker())
            for _ in range(self.size)
        ]
    
    async def submit(self, func: Callable[..., Awaitable], *args, **kwargs):
        """Submit task to pool"""
        await self.queue.put((func, args, kwargs))
    
    async def get_result(self):
        """Get next result"""
        return await self.results.get()
    
    async def shutdown(self):
        """Shutdown pool"""
        # Send shutdown signals
        for _ in range(self.size):
            await self.queue.put((None, (), {}))
        
        # Wait for workers
        await asyncio.gather(*self.workers, return_exceptions=True)

# Example usage
async def expensive_task(x: int) -> int:
    """Simulated expensive task"""
    await asyncio.sleep(0.01)
    return x * 2

async def task_pool_example():
    """Compare task pool vs creating tasks"""
    # Without pool
    start = time.perf_counter()
    tasks = [asyncio.create_task(expensive_task(i)) for i in range(100)]
    await asyncio.gather(*tasks)
    without_pool = time.perf_counter() - start
    
    # With pool
    pool = TaskPool(size=10)
    await pool.start()
    
    start = time.perf_counter()
    for i in range(100):
        await pool.submit(expensive_task, i)
    
    results = []
    for _ in range(100):
        status, result = await pool.get_result()
        results.append(result)
    
    with_pool = time.perf_counter() - start
    await pool.shutdown()
    
    print(f"Without pool: {without_pool:.4f}s")
    print(f"With pool:    {with_pool:.4f}s")
    print(f"Speedup:      {without_pool/with_pool:.2f}x")
```

## Batching Operations

### 1. Request Batching

```python
import asyncio
from typing import List, Dict, Any
from dataclasses import dataclass
import time

@dataclass
class BatchRequest:
    """Single request in a batch"""
    id: str
    data: Any
    future: asyncio.Future

class RequestBatcher:
    """
    Batch multiple requests together for efficiency.
    
    Useful when making API calls or database queries where
    batching reduces overhead.
    """
    
    def __init__(self, max_batch_size: int = 100, max_wait_time: float = 0.01):
        self.max_batch_size = max_batch_size
        self.max_wait_time = max_wait_time
        self.pending: List[BatchRequest] = []
        self.lock = asyncio.Lock()
        self.batch_task: Optional[asyncio.Task] = None
    
    async def submit(self, request_id: str, data: Any) -> Any:
        """Submit request and wait for result"""
        future = asyncio.Future()
        request = BatchRequest(id=request_id, data=data, future=future)
        
        async with self.lock:
            self.pending.append(request)
            
            # Start batch timer if not running
            if self.batch_task is None or self.batch_task.done():
                self.batch_task = asyncio.create_task(self._process_batch_after_delay())
            
            # Process immediately if batch is full
            if len(self.pending) >= self.max_batch_size:
                self.batch_task.cancel()
                await self._process_batch()
        
        return await future
    
    async def _process_batch_after_delay(self):
        """Wait for max_wait_time then process batch"""
        try:
            await asyncio.sleep(self.max_wait_time)
            async with self.lock:
                await self._process_batch()
        except asyncio.CancelledError:
            pass
    
    async def _process_batch(self):
        """Process current batch"""
        if not self.pending:
            return
        
        batch = self.pending
        self.pending = []
        
        # Process batch (simulated)
        results = await self._execute_batch([r.data for r in batch])
        
        # Distribute results
        for request, result in zip(batch, results):
            request.future.set_result(result)
    
    async def _execute_batch(self, data_list: List[Any]) -> List[Any]:
        """Execute batch operation"""
        # Simulated batch API call
        await asyncio.sleep(0.05)  # Single API call for entire batch
        return [d * 2 for d in data_list]

async def batching_example():
    """Compare batched vs individual requests"""
    batcher = RequestBatcher(max_batch_size=10, max_wait_time=0.01)
    
    # Individual requests (simulated)
    start = time.perf_counter()
    results = []
    for i in range(100):
        await asyncio.sleep(0.05)  # Individual API call
        results.append(i * 2)
    individual_time = time.perf_counter() - start
    
    # Batched requests
    start = time.perf_counter()
    tasks = [batcher.submit(f"req-{i}", i) for i in range(100)]
    results = await asyncio.gather(*tasks)
    batched_time = time.perf_counter() - start
    
    print(f"Individual requests: {individual_time:.4f}s")
    print(f"Batched requests:    {batched_time:.4f}s")
    print(f"Speedup:             {individual_time/batched_time:.2f}x")
```

### 2. Write Batching

```python
import asyncio
from typing import List, Any
from collections import deque

class WriteBatcher:
    """
    Batch writes to reduce I/O operations.
    
    Useful for logging, database writes, or file operations.
    """
    
    def __init__(self, flush_size: int = 100, flush_interval: float = 1.0):
        self.flush_size = flush_size
        self.flush_interval = flush_interval
        self.buffer: deque = deque()
        self.lock = asyncio.Lock()
        self.flush_task: Optional[asyncio.Task] = None
        self.running = False
    
    async def start(self):
        """Start background flusher"""
        self.running = True
        self.flush_task = asyncio.create_task(self._periodic_flush())
    
    async def stop(self):
        """Stop and flush remaining data"""
        self.running = False
        if self.flush_task:
            self.flush_task.cancel()
            await asyncio.gather(self.flush_task, return_exceptions=True)
        await self.flush()
    
    async def write(self, data: Any):
        """Add data to buffer"""
        async with self.lock:
            self.buffer.append(data)
            
            # Flush if buffer is full
            if len(self.buffer) >= self.flush_size:
                await self._flush_buffer()
    
    async def flush(self):
        """Manually flush buffer"""
        async with self.lock:
            await self._flush_buffer()
    
    async def _flush_buffer(self):
        """Flush current buffer"""
        if not self.buffer:
            return
        
        # Get all data
        data = list(self.buffer)
        self.buffer.clear()
        
        # Write batch (simulated)
        await self._write_batch(data)
    
    async def _write_batch(self, data: List[Any]):
        """Write batch to storage"""
        # Simulated batch write
        await asyncio.sleep(0.01)
        print(f"Wrote batch of {len(data)} items")
    
    async def _periodic_flush(self):
        """Periodically flush buffer"""
        while self.running:
            try:
                await asyncio.sleep(self.flush_interval)
                await self.flush()
            except asyncio.CancelledError:
                break

async def write_batching_example():
    """Compare batched vs individual writes"""
    # Individual writes
    start = time.perf_counter()
    for i in range(1000):
        await asyncio.sleep(0.001)  # Individual write
    individual_time = time.perf_counter() - start
    
    # Batched writes
    batcher = WriteBatcher(flush_size=100, flush_interval=0.1)
    await batcher.start()
    
    start = time.perf_counter()
    for i in range(1000):
        await batcher.write(i)
    await batcher.stop()
    batched_time = time.perf_counter() - start
    
    print(f"Individual writes: {individual_time:.4f}s")
    print(f"Batched writes:    {batched_time:.4f}s")
    print(f"Speedup:           {individual_time/batched_time:.2f}x")
```

## Memory Optimization

### 1. Task Memory Overhead

```python
import asyncio
import sys
from typing import List

async def measure_task_overhead():
    """Measure memory overhead per task"""
    import gc
    gc.collect()
    
    # Baseline memory
    baseline = sys.getsizeof(asyncio.all_tasks())
    
    # Create tasks
    tasks = []
    for i in range(1000):
        task = asyncio.create_task(asyncio.sleep(10))
        tasks.append(task)
    
    # Measure memory
    task_memory = sys.getsizeof(asyncio.all_tasks())
    overhead = (task_memory - baseline) / 1000
    
    print(f"Memory per task: ~{overhead:.2f} bytes")
    
    # Cleanup
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

# ✅ GOOD: Limit concurrent tasks
async def memory_efficient_processing(items: List[Any], max_concurrent: int = 100):
    """Process items with limited concurrency"""
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async def process_with_limit(item):
        async with semaphore:
            return await process_item(item)
    
    tasks = [process_with_limit(item) for item in items]
    return await asyncio.gather(*tasks)
```

### 2. Generator-Based Processing

```python
import asyncio
from typing import AsyncIterator, List

# ❌ BAD: Load everything into memory
async def load_all_data() -> List[str]:
    """Loads all data at once - memory intensive"""
    data = []
    for i in range(10000):
        await asyncio.sleep(0.001)
        data.append(f"item-{i}")
    return data

# ✅ GOOD: Stream data with async generator
async def stream_data() -> AsyncIterator[str]:
    """Stream data one item at a time"""
    for i in range(10000):
        await asyncio.sleep(0.001)
        yield f"item-{i}"

async def process_streamed_data():
    """Process data as it streams"""
    count = 0
    async for item in stream_data():
        # Process item
        count += 1
    print(f"Processed {count} items")
```

## Throughput Optimization

### 1. Connection Pooling

```python
import asyncio
from typing import Optional, Callable, Any
from collections import deque

class ConnectionPool:
    """
    Connection pool for reusing expensive resources.
    
    Avoids overhead of creating/destroying connections.
    """
    
    def __init__(
        self,
        create_connection: Callable[[], Awaitable[Any]],
        max_size: int = 10,
        min_size: int = 2
    ):
        self.create_connection = create_connection
        self.max_size = max_size
        self.min_size = min_size
        self.pool: deque = deque()
        self.size = 0
        self.lock = asyncio.Lock()
        self.available = asyncio.Condition(self.lock)
    
    async def initialize(self):
        """Create minimum connections"""
        for _ in range(self.min_size):
            conn = await self.create_connection()
            self.pool.append(conn)
            self.size += 1
    
    async def acquire(self) -> Any:
        """Acquire connection from pool"""
        async with self.available:
            while True:
                # Try to get existing connection
                if self.pool:
                    return self.pool.popleft()
                
                # Create new connection if under limit
                if self.size < self.max_size:
                    conn = await self.create_connection()
                    self.size += 1
                    return conn
                
                # Wait for connection to be released
                await self.available.wait()
    
    async def release(self, conn: Any):
        """Release connection back to pool"""
        async with self.available:
            self.pool.append(conn)
            self.available.notify()
    
    async def close(self):
        """Close all connections"""
        async with self.lock:
            while self.pool:
                conn = self.pool.popleft()
                # Close connection (implementation specific)
                await conn.close()
            self.size = 0

# Example usage
class MockConnection:
    """Mock connection for demonstration"""
    def __init__(self, id: int):
        self.id = id
    
    async def query(self, sql: str):
        await asyncio.sleep(0.01)
        return f"Result from conn {self.id}"
    
    async def close(self):
        pass

async def create_mock_connection():
    """Create mock connection"""
    await asyncio.sleep(0.1)  # Simulated connection overhead
    return MockConnection(id=id(asyncio.current_task()))

async def connection_pool_example():
    """Compare with and without connection pooling"""
    # Without pooling
    start = time.perf_counter()
    for i in range(100):
        conn = await create_mock_connection()
        await conn.query("SELECT * FROM table")
        await conn.close()
    without_pool = time.perf_counter() - start
    
    # With pooling
    pool = ConnectionPool(create_mock_connection, max_size=10)
    await pool.initialize()
    
    start = time.perf_counter()
    for i in range(100):
        conn = await pool.acquire()
        await conn.query("SELECT * FROM table")
        await pool.release(conn)
    with_pool = time.perf_counter() - start
    
    await pool.close()
    
    print(f"Without pool: {without_pool:.4f}s")
    print(f"With pool:    {with_pool:.4f}s")
    print(f"Speedup:      {without_pool/with_pool:.2f}x")
```

### 2. Caching

```python
import asyncio
from typing import Any, Optional, Callable
from functools import wraps
import time

class AsyncCache:
    """
    Simple async cache with TTL.
    
    Reduces redundant expensive operations.
    """
    
    def __init__(self, ttl: float = 60.0):
        self.ttl = ttl
        self.cache: Dict[str, tuple[Any, float]] = {}
        self.lock = asyncio.Lock()
    
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache"""
        async with self.lock:
            if key in self.cache:
                value, timestamp = self.cache[key]
                if time.time() - timestamp < self.ttl:
                    return value
                else:
                    del self.cache[key]
        return None
    
    async def set(self, key: str, value: Any):
        """Set value in cache"""
        async with self.lock:
            self.cache[key] = (value, time.time())
    
    async def clear(self):
        """Clear cache"""
        async with self.lock:
            self.cache.clear()

def cached(cache: AsyncCache, key_func: Optional[Callable] = None):
    """Decorator for caching async function results"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Generate cache key
            if key_func:
                key = key_func(*args, **kwargs)
            else:
                key = f"{func.__name__}:{args}:{kwargs}"
            
            # Check cache
            cached_value = await cache.get(key)
            if cached_value is not None:
                return cached_value
            
            # Compute and cache
            result = await func(*args, **kwargs)
            await cache.set(key, result)
            return result
        
        return wrapper
    return decorator

# Example usage
cache = AsyncCache(ttl=5.0)

@cached(cache)
async def expensive_computation(x: int) -> int:
    """Expensive computation that benefits from caching"""
    await asyncio.sleep(1.0)  # Simulated expensive operation
    return x * x

async def caching_example():
    """Compare with and without caching"""
    # First call (cache miss)
    start = time.perf_counter()
    result1 = await expensive_computation(5)
    first_call = time.perf_counter() - start
    
    # Second call (cache hit)
    start = time.perf_counter()
    result2 = await expensive_computation(5)
    second_call = time.perf_counter() - start
    
    print(f"First call (miss):  {first_call:.4f}s")
    print(f"Second call (hit):  {second_call:.4f}s")
    print(f"Speedup:            {first_call/second_call:.2f}x")
```

## Event Loop Tuning

### 1. Custom Event Loop Policy

```python
import asyncio
import uvloop  # High-performance event loop

# Use uvloop for better performance
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

async def main():
    # Your async code here
    pass

# Run with uvloop
asyncio.run(main())
```

### 2. Slow Callback Threshold

```python
import asyncio

# Set slow callback threshold
loop = asyncio.get_event_loop()
loop.slow_callback_duration = 0.05  # 50ms

# Now callbacks taking >50ms will be logged
```

## Benchmarking Framework

```python
import asyncio
import time
from typing import Callable, Awaitable, List
from dataclasses import dataclass

@dataclass
class BenchmarkResult:
    """Result of a benchmark run"""
    name: str
    iterations: int
    total_time: float
    avg_time: float
    ops_per_sec: float
    
    def __str__(self):
        return (
            f"{self.name}:\n"
            f"  Iterations: {self.iterations}\n"
            f"  Total time: {self.total_time:.4f}s\n"
            f"  Avg time:   {self.avg_time:.6f}s\n"
            f"  Ops/sec:    {self.ops_per_sec:.2f}"
        )

async def benchmark(
    name: str,
    func: Callable[[], Awaitable],
    iterations: int = 1000
) -> BenchmarkResult:
    """Benchmark an async function"""
    # Warmup
    for _ in range(min(10, iterations // 10)):
        await func()
    
    # Benchmark
    start = time.perf_counter()
    for _ in range(iterations):
        await func()
    total_time = time.perf_counter() - start
    
    avg_time = total_time / iterations
    ops_per_sec = iterations / total_time
    
    return BenchmarkResult(
        name=name,
        iterations=iterations,
        total_time=total_time,
        avg_time=avg_time,
        ops_per_sec=ops_per_sec
    )

async def compare_benchmarks(benchmarks: List[tuple[str, Callable]]):
    """Run and compare multiple benchmarks"""
    results = []
    
    for name, func in benchmarks:
        result = await benchmark(name, func)
        results.append(result)
        print(f"\n{result}")
    
    # Print comparison
    if len(results) > 1:
        print("\n" + "="*60)
        print("COMPARISON")
        print("="*60)
        baseline = results[0]
        for result in results[1:]:
            speedup = result.ops_per_sec / baseline.ops_per_sec
            print(f"{result.name} vs {baseline.name}: {speedup:.2f}x")

# Example usage
async def method_a():
    await asyncio.sleep(0.001)

async def method_b():
    await asyncio.sleep(0.0005)

async def benchmarking_example():
    await compare_benchmarks([
        ("Method A", method_a),
        ("Method B", method_b)
    ])
```

## Mental Model Summary

**Async performance optimization focuses on:**

1. **Reducing overhead**: Minimize task creation and context switches
2. **Batching**: Group operations for efficiency
3. **Pooling**: Reuse expensive resources
4. **Caching**: Avoid redundant computations
5. **Streaming**: Process data incrementally
6. **Profiling**: Measure before optimizing

**Key principles:**

- **Measure first**: Profile before optimizing
- **Batch operations**: Reduce per-operation overhead
- **Limit concurrency**: Too many tasks hurt performance
- **Reuse resources**: Connection pools, task pools
- **Stream data**: Don't load everything into memory
- **Cache results**: Avoid redundant work

**Common optimizations:**

- Use batching for API calls and database queries
- Implement connection pooling for databases
- Limit concurrent tasks with semaphores
- Use async generators for large datasets
- Cache expensive computations
- Profile to find bottlenecks

**Performance anti-patterns:**

- Creating thousands of tasks unnecessarily
- Not batching operations
- Loading large datasets into memory
- Creating new connections for each request
- Not caching repeated computations
- Blocking the event loop with sync code

Async performance is about finding the right balance between concurrency and overhead. Too little concurrency wastes I/O wait time; too much creates excessive overhead. Profile, measure, and optimize based on real bottlenecks.