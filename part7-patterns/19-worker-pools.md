# Chapter 19: Worker Pools

## Introduction

Worker pools manage concurrent task execution efficiently. They limit concurrency, distribute work, and provide clean abstractions for parallel processing. This chapter covers various worker pool patterns from simple to sophisticated.

By the end of this chapter, you'll understand:
- Fixed worker pools
- Dynamic worker pools
- Bounded worker pools
- Work stealing patterns
- Load balancing strategies
- Practical worker pool implementations

---

## Why Worker Pools?

Worker pools solve several problems:

1. **Concurrency limiting** - Prevent resource exhaustion
2. **Work distribution** - Distribute tasks across workers
3. **Resource management** - Reuse expensive resources
4. **Backpressure** - Handle work overflow gracefully
5. **Monitoring** - Track worker utilization

### The Problem Without Pools

```python
import asyncio

async def naive_approach():
    """Naive approach: Create task for each item"""
    
    async def process_item(item):
        await asyncio.sleep(1.0)
        return f"Processed {item}"
    
    # Problem: Creates 10,000 concurrent tasks!
    items = range(10_000)
    results = await asyncio.gather(*[
        process_item(item) for item in items
    ])
    
    return results

# This overwhelms the system with too many concurrent tasks
```

### The Solution: Worker Pool

```python
import asyncio

async def worker_pool_approach():
    """Worker pool: Limit concurrency"""
    
    async def process_item(item):
        await asyncio.sleep(1.0)
        return f"Processed {item}"
    
    # Solution: Use semaphore to limit concurrency
    semaphore = asyncio.Semaphore(100)  # Max 100 concurrent
    
    async def limited_process(item):
        async with semaphore:
            return await process_item(item)
    
    items = range(10_000)
    results = await asyncio.gather(*[
        limited_process(item) for item in items
    ])
    
    return results

# Only 100 tasks run concurrently
```

---

## Fixed Worker Pool

Fixed number of workers processing tasks from a queue.

### Basic Fixed Pool

```python
import asyncio
from typing import Callable, Any, List

class FixedWorkerPool:
    """
    Fixed worker pool with task queue.
    
    Features:
    - Fixed number of workers
    - Task queue
    - Graceful shutdown
    """
    
    def __init__(self, num_workers: int):
        self.num_workers = num_workers
        self.queue = asyncio.Queue()
        self.workers: List[asyncio.Task] = []
        self.running = False
    
    async def worker(self, worker_id: int):
        """Worker that processes tasks from queue"""
        print(f"Worker {worker_id}: Started")
        
        while self.running:
            try:
                # Get task with timeout
                task_func, args, future = await asyncio.wait_for(
                    self.queue.get(),
                    timeout=1.0
                )
                
                try:
                    # Execute task
                    result = await task_func(*args)
                    future.set_result(result)
                
                except Exception as e:
                    future.set_exception(e)
                
                finally:
                    self.queue.task_done()
            
            except asyncio.TimeoutError:
                continue
        
        print(f"Worker {worker_id}: Stopped")
    
    async def start(self):
        """Start worker pool"""
        self.running = True
        self.workers = [
            asyncio.create_task(self.worker(i))
            for i in range(self.num_workers)
        ]
        print(f"Started {self.num_workers} workers")
    
    async def submit(self, func: Callable, *args) -> Any:
        """Submit task to pool"""
        if not self.running:
            raise RuntimeError("Pool not started")
        
        future = asyncio.Future()
        await self.queue.put((func, args, future))
        return await future
    
    async def shutdown(self, wait: bool = True):
        """Shutdown worker pool"""
        print("Shutting down worker pool...")
        
        if wait:
            # Wait for queue to empty
            await self.queue.join()
        
        # Stop workers
        self.running = False
        
        # Wait for workers to finish
        await asyncio.gather(*self.workers, return_exceptions=True)
        
        print("Worker pool shutdown complete")

async def fixed_pool_demo():
    """Demonstrate fixed worker pool"""
    
    async def process_item(item_id):
        """Simulate processing"""
        await asyncio.sleep(0.5)
        return f"Result {item_id}"
    
    pool = FixedWorkerPool(num_workers=5)
    await pool.start()
    
    try:
        # Submit tasks
        tasks = [
            pool.submit(process_item, i)
            for i in range(20)
        ]
        
        # Wait for results
        results = await asyncio.gather(*tasks)
        
        print(f"Processed {len(results)} items")
    
    finally:
        await pool.shutdown()

asyncio.run(fixed_pool_demo())
```

---

## Dynamic Worker Pool

Adjusts worker count based on load.

### Adaptive Worker Pool

```python
import asyncio
from typing import Callable, Any
import time

class DynamicWorkerPool:
    """
    Dynamic worker pool that scales based on load.
    
    Features:
    - Scales up when queue is full
    - Scales down when idle
    - Min/max worker limits
    """
    
    def __init__(
        self,
        min_workers: int = 2,
        max_workers: int = 10,
        scale_up_threshold: int = 10,
        scale_down_timeout: float = 5.0
    ):
        self.min_workers = min_workers
        self.max_workers = max_workers
        self.scale_up_threshold = scale_up_threshold
        self.scale_down_timeout = scale_down_timeout
        
        self.queue = asyncio.Queue()
        self.workers: List[asyncio.Task] = []
        self.running = False
        self.worker_id_counter = 0
        self.lock = asyncio.Lock()
    
    async def worker(self, worker_id: int):
        """Worker that processes tasks"""
        print(f"Worker {worker_id}: Started")
        last_work_time = time.time()
        
        while self.running:
            try:
                # Get task with timeout
                task_func, args, future = await asyncio.wait_for(
                    self.queue.get(),
                    timeout=1.0
                )
                
                last_work_time = time.time()
                
                try:
                    result = await task_func(*args)
                    future.set_result(result)
                except Exception as e:
                    future.set_exception(e)
                finally:
                    self.queue.task_done()
            
            except asyncio.TimeoutError:
                # Check if should scale down
                idle_time = time.time() - last_work_time
                
                async with self.lock:
                    if (idle_time > self.scale_down_timeout and
                        len(self.workers) > self.min_workers):
                        print(f"Worker {worker_id}: Scaling down (idle)")
                        return
        
        print(f"Worker {worker_id}: Stopped")
    
    async def scale_up(self):
        """Add a worker"""
        async with self.lock:
            if len(self.workers) < self.max_workers:
                worker_id = self.worker_id_counter
                self.worker_id_counter += 1
                
                worker_task = asyncio.create_task(self.worker(worker_id))
                self.workers.append(worker_task)
                
                print(f"Scaled up to {len(self.workers)} workers")
    
    async def monitor(self):
        """Monitor queue and scale as needed"""
        while self.running:
            await asyncio.sleep(1.0)
            
            queue_size = self.queue.qsize()
            
            # Scale up if queue is large
            if queue_size > self.scale_up_threshold:
                await self.scale_up()
            
            # Remove finished workers
            async with self.lock:
                self.workers = [w for w in self.workers if not w.done()]
    
    async def start(self):
        """Start worker pool"""
        self.running = True
        
        # Start minimum workers
        for _ in range(self.min_workers):
            worker_id = self.worker_id_counter
            self.worker_id_counter += 1
            worker_task = asyncio.create_task(self.worker(worker_id))
            self.workers.append(worker_task)
        
        # Start monitor
        self.monitor_task = asyncio.create_task(self.monitor())
        
        print(f"Started dynamic pool with {self.min_workers} workers")
    
    async def submit(self, func: Callable, *args) -> Any:
        """Submit task to pool"""
        if not self.running:
            raise RuntimeError("Pool not started")
        
        future = asyncio.Future()
        await self.queue.put((func, args, future))
        return await future
    
    async def shutdown(self):
        """Shutdown worker pool"""
        print("Shutting down dynamic pool...")
        
        # Wait for queue
        await self.queue.join()
        
        # Stop workers
        self.running = False
        
        # Wait for workers and monitor
        await asyncio.gather(
            *self.workers,
            self.monitor_task,
            return_exceptions=True
        )
        
        print("Dynamic pool shutdown complete")

async def dynamic_pool_demo():
    """Demonstrate dynamic worker pool"""
    
    async def process_item(item_id):
        await asyncio.sleep(0.5)
        return f"Result {item_id}"
    
    pool = DynamicWorkerPool(
        min_workers=2,
        max_workers=10,
        scale_up_threshold=5
    )
    
    await pool.start()
    
    try:
        # Submit burst of tasks
        print("Submitting burst of 50 tasks...")
        tasks = [pool.submit(process_item, i) for i in range(50)]
        
        results = await asyncio.gather(*tasks)
        print(f"Processed {len(results)} items")
        
        # Wait to see scale down
        await asyncio.sleep(10.0)
    
    finally:
        await pool.shutdown()

asyncio.run(dynamic_pool_demo())
```

---

## Bounded Worker Pool

Limits both workers and queue size for backpressure.

### Bounded Pool with Backpressure

```python
import asyncio
from typing import Callable, Any, Optional

class BoundedWorkerPool:
    """
    Bounded worker pool with queue size limit.
    
    Features:
    - Limited queue size
    - Backpressure when full
    - Timeout support
    """
    
    def __init__(
        self,
        num_workers: int,
        max_queue_size: int = 100
    ):
        self.num_workers = num_workers
        self.max_queue_size = max_queue_size
        self.queue = asyncio.Queue(maxsize=max_queue_size)
        self.workers: List[asyncio.Task] = []
        self.running = False
    
    async def worker(self, worker_id: int):
        """Worker that processes tasks"""
        while self.running:
            try:
                task_func, args, future = await asyncio.wait_for(
                    self.queue.get(),
                    timeout=1.0
                )
                
                try:
                    result = await task_func(*args)
                    future.set_result(result)
                except Exception as e:
                    future.set_exception(e)
                finally:
                    self.queue.task_done()
            
            except asyncio.TimeoutError:
                continue
    
    async def start(self):
        """Start worker pool"""
        self.running = True
        self.workers = [
            asyncio.create_task(self.worker(i))
            for i in range(self.num_workers)
        ]
    
    async def submit(
        self,
        func: Callable,
        *args,
        timeout: Optional[float] = None
    ) -> Any:
        """
        Submit task to pool.
        
        Blocks if queue is full (backpressure).
        """
        if not self.running:
            raise RuntimeError("Pool not started")
        
        future = asyncio.Future()
        
        # This blocks if queue is full (backpressure)
        if timeout:
            async with asyncio.timeout(timeout):
                await self.queue.put((func, args, future))
        else:
            await self.queue.put((func, args, future))
        
        return await future
    
    async def try_submit(
        self,
        func: Callable,
        *args
    ) -> Optional[asyncio.Future]:
        """
        Try to submit task without blocking.
        
        Returns None if queue is full.
        """
        if not self.running:
            raise RuntimeError("Pool not started")
        
        try:
            future = asyncio.Future()
            self.queue.put_nowait((func, args, future))
            return future
        except asyncio.QueueFull:
            return None
    
    async def shutdown(self):
        """Shutdown worker pool"""
        await self.queue.join()
        self.running = False
        await asyncio.gather(*self.workers, return_exceptions=True)

async def bounded_pool_demo():
    """Demonstrate bounded worker pool"""
    
    async def process_item(item_id):
        await asyncio.sleep(0.5)
        return f"Result {item_id}"
    
    pool = BoundedWorkerPool(
        num_workers=5,
        max_queue_size=10
    )
    
    await pool.start()
    
    try:
        # Try to submit many tasks
        submitted = 0
        rejected = 0
        
        for i in range(50):
            future = await pool.try_submit(process_item, i)
            if future:
                submitted += 1
            else:
                rejected += 1
                print(f"Task {i} rejected (queue full)")
        
        print(f"Submitted: {submitted}, Rejected: {rejected}")
        
        # Wait for completion
        await pool.queue.join()
    
    finally:
        await pool.shutdown()

asyncio.run(bounded_pool_demo())
```

---

## Real-world: Task Processor

Complete task processor with priorities and retries.

```python
import asyncio
from typing import Callable, Any, Optional
from dataclasses import dataclass, field
from enum import Enum
import time

class Priority(Enum):
    LOW = 3
    NORMAL = 2
    HIGH = 1

@dataclass(order=True)
class Task:
    """Task with priority"""
    priority: int
    func: Callable = field(compare=False)
    args: tuple = field(compare=False)
    future: asyncio.Future = field(compare=False)
    retries: int = field(default=0, compare=False)
    max_retries: int = field(default=3, compare=False)

class TaskProcessor:
    """
    Advanced task processor with priorities and retries.
    
    Features:
    - Priority queue
    - Automatic retries
    - Timeout support
    - Statistics tracking
    """
    
    def __init__(self, num_workers: int = 5):
        self.num_workers = num_workers
        self.queue = asyncio.PriorityQueue()
        self.workers: List[asyncio.Task] = []
        self.running = False
        
        # Statistics
        self.stats = {
            'processed': 0,
            'failed': 0,
            'retried': 0
        }
        self.stats_lock = asyncio.Lock()
    
    async def worker(self, worker_id: int):
        """Worker that processes tasks"""
        print(f"Worker {worker_id}: Started")
        
        while self.running:
            try:
                task = await asyncio.wait_for(
                    self.queue.get(),
                    timeout=1.0
                )
                
                try:
                    # Execute task
                    result = await task.func(*task.args)
                    task.future.set_result(result)
                    
                    async with self.stats_lock:
                        self.stats['processed'] += 1
                
                except Exception as e:
                    # Retry logic
                    if task.retries < task.max_retries:
                        task.retries += 1
                        print(f"Worker {worker_id}: Retrying task (attempt {task.retries})")
                        
                        await self.queue.put(task)
                        
                        async with self.stats_lock:
                            self.stats['retried'] += 1
                    else:
                        task.future.set_exception(e)
                        
                        async with self.stats_lock:
                            self.stats['failed'] += 1
                
                finally:
                    self.queue.task_done()
            
            except asyncio.TimeoutError:
                continue
        
        print(f"Worker {worker_id}: Stopped")
    
    async def start(self):
        """Start task processor"""
        self.running = True
        self.workers = [
            asyncio.create_task(self.worker(i))
            for i in range(self.num_workers)
        ]
        print(f"Started {self.num_workers} workers")
    
    async def submit(
        self,
        func: Callable,
        *args,
        priority: Priority = Priority.NORMAL,
        max_retries: int = 3
    ) -> Any:
        """Submit task with priority"""
        if not self.running:
            raise RuntimeError("Processor not started")
        
        future = asyncio.Future()
        task = Task(
            priority=priority.value,
            func=func,
            args=args,
            future=future,
            max_retries=max_retries
        )
        
        await self.queue.put(task)
        return await future
    
    async def get_stats(self):
        """Get processing statistics"""
        async with self.stats_lock:
            return dict(self.stats)
    
    async def shutdown(self):
        """Shutdown task processor"""
        print("Shutting down task processor...")
        
        await self.queue.join()
        self.running = False
        await asyncio.gather(*self.workers, return_exceptions=True)
        
        stats = await self.get_stats()
        print(f"Final stats: {stats}")

async def task_processor_demo():
    """Demonstrate task processor"""
    
    async def reliable_task(task_id):
        """Task that always succeeds"""
        await asyncio.sleep(0.5)
        return f"Success {task_id}"
    
    async def unreliable_task(task_id):
        """Task that sometimes fails"""
        await asyncio.sleep(0.5)
        if task_id % 3 == 0:
            raise Exception(f"Task {task_id} failed")
        return f"Success {task_id}"
    
    processor = TaskProcessor(num_workers=5)
    await processor.start()
    
    try:
        # Submit tasks with different priorities
        tasks = []
        
        # High priority tasks
        for i in range(5):
            tasks.append(
                processor.submit(
                    reliable_task, i,
                    priority=Priority.HIGH
                )
            )
        
        # Normal priority tasks (some fail)
        for i in range(10):
            tasks.append(
                processor.submit(
                    unreliable_task, i,
                    priority=Priority.NORMAL,
                    max_retries=2
                )
            )
        
        # Low priority tasks
        for i in range(5):
            tasks.append(
                processor.submit(
                    reliable_task, i + 100,
                    priority=Priority.LOW
                )
            )
        
        # Wait for results
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        successes = sum(1 for r in results if not isinstance(r, Exception))
        failures = sum(1 for r in results if isinstance(r, Exception))
        
        print(f"\nResults: {successes} successes, {failures} failures")
        
        stats = await processor.get_stats()
        print(f"Stats: {stats}")
    
    finally:
        await processor.shutdown()

asyncio.run(task_processor_demo())
```

---

## Common Patterns

### Pattern 1: Map-Reduce

```python
import asyncio

async def map_reduce_pattern():
    """Map-reduce using worker pool"""
    
    async def map_func(item):
        """Map function"""
        await asyncio.sleep(0.1)
        return item * 2
    
    def reduce_func(results):
        """Reduce function"""
        return sum(results)
    
    # Map phase (parallel)
    pool = FixedWorkerPool(num_workers=10)
    await pool.start()
    
    try:
        items = range(100)
        mapped = await asyncio.gather(*[
            pool.submit(map_func, item)
            for item in items
        ])
        
        # Reduce phase (sequential)
        result = reduce_func(mapped)
        print(f"Result: {result}")
    
    finally:
        await pool.shutdown()
```

### Pattern 2: Batch Processing

```python
import asyncio

async def batch_processing():
    """Process items in batches"""
    
    async def process_batch(batch):
        """Process batch of items"""
        await asyncio.sleep(1.0)
        return [f"Processed {item}" for item in batch]
    
    # Create batches
    items = range(100)
    batch_size = 10
    batches = [
        list(items)[i:i+batch_size]
        for i in range(0, len(items), batch_size)
    ]
    
    # Process batches in parallel
    pool = FixedWorkerPool(num_workers=5)
    await pool.start()
    
    try:
        results = await asyncio.gather(*[
            pool.submit(process_batch, batch)
            for batch in batches
        ])
        
        # Flatten results
        all_results = [item for batch in results for item in batch]
        print(f"Processed {len(all_results)} items")
    
    finally:
        await pool.shutdown()
```

---

## Best Practices

### ✅ DO: Choose appropriate pool size

```python
# Good: Based on workload
# I/O-bound: More workers
pool = FixedWorkerPool(num_workers=100)

# CPU-bound: Fewer workers (use multiprocessing instead)
pool = FixedWorkerPool(num_workers=os.cpu_count())
```

### ✅ DO: Handle worker failures

```python
# Good: Catch exceptions in workers
try:
    result = await task_func(*args)
    future.set_result(result)
except Exception as e:
    future.set_exception(e)
```

### ✅ DO: Implement graceful shutdown

```python
# Good: Wait for queue to empty
await queue.join()
running = False
await asyncio.gather(*workers)
```

### ❌ DON'T: Create unbounded pools

```python
# Bad: No limit on concurrency
tasks = [asyncio.create_task(process(item)) for item in items]

# Good: Use pool to limit
pool = FixedWorkerPool(num_workers=10)
```

---

## Summary: Worker Pool Mental Model

✅ **Limit concurrency** - prevent resource exhaustion

✅ **Use queues** - distribute work to workers

✅ **Fixed pools** - simple and predictable

✅ **Dynamic pools** - adapt to load

✅ **Bounded pools** - implement backpressure

✅ **Priority queues** - handle urgent tasks first

✅ **Graceful shutdown** - wait for completion

---

## What's Next?

Worker pools process independent tasks. For sequential processing with transformations, we need pipelines.

In [Chapter 20: Pipelines](./20-pipelines.md), we'll cover:
- Producer → Transform → Sink patterns
- Multi-stage pipelines
- Backpressure propagation
- Pipeline composition
- Error handling in pipelines
- Practical pipeline examples

Pipelines are essential for stream processing and data transformation.

---

**Previous:** [← Chapter 18: Networking](../part6-integration/18-networking.md)  
**Next:** [Chapter 20: Pipelines →](./20-pipelines.md)