# Chapter 7: Queues

## Introduction

Queues are fundamental for communication between async tasks. They enable producer-consumer patterns, work distribution, and backpressure management. Understanding queues is essential for building scalable async systems.

By the end of this chapter, you'll understand:
- Queue types (Queue, PriorityQueue, LifoQueue)
- Producer-consumer patterns
- Bounded queues and backpressure
- `put_nowait()`, `get_nowait()`, `join()`, `task_done()`
- Building work distribution systems
- Real-world queue implementations

---

## What is an Async Queue?

An async queue is a thread-safe, coroutine-safe data structure for passing data between tasks.

### Basic Queue Operations

```python
import asyncio

async def basic_queue_demo():
    # Create a queue
    queue = asyncio.Queue()
    
    # Put items (async)
    await queue.put("item1")
    await queue.put("item2")
    await queue.put("item3")
    
    print(f"Queue size: {queue.qsize()}")
    
    # Get items (async)
    item1 = await queue.get()
    item2 = await queue.get()
    item3 = await queue.get()
    
    print(f"Got: {item1}, {item2}, {item3}")

asyncio.run(basic_queue_demo())

# Output:
# Queue size: 3
# Got: item1, item2, item3
```

**Key characteristics:**
- FIFO (First In, First Out) by default
- Async operations (`await queue.put()`, `await queue.get()`)
- Thread-safe and coroutine-safe
- Supports backpressure with bounded queues

---

## Queue Types

Asyncio provides three queue types for different use cases.

### 1. Queue (FIFO)

Standard first-in, first-out queue.

```python
import asyncio

async def fifo_demo():
    queue = asyncio.Queue()
    
    # Add items
    for i in range(5):
        await queue.put(f"item-{i}")
    
    # Retrieve in FIFO order
    while not queue.empty():
        item = await queue.get()
        print(item)

asyncio.run(fifo_demo())

# Output:
# item-0
# item-1
# item-2
# item-3
# item-4
```

### 2. PriorityQueue

Items are retrieved in priority order (lowest priority number first).

```python
import asyncio

async def priority_demo():
    queue = asyncio.PriorityQueue()
    
    # Add items with priorities (priority, data)
    await queue.put((3, "Low priority"))
    await queue.put((1, "High priority"))
    await queue.put((2, "Medium priority"))
    await queue.put((1, "Also high priority"))
    
    # Retrieve in priority order
    while not queue.empty():
        priority, item = await queue.get()
        print(f"[Priority {priority}] {item}")

asyncio.run(priority_demo())

# Output:
# [Priority 1] High priority
# [Priority 1] Also high priority
# [Priority 2] Medium priority
# [Priority 3] Low priority
```

**Use cases:**
- Task scheduling by importance
- Event processing by urgency
- Resource allocation

### 3. LifoQueue (Stack)

Last-in, first-out queue (stack behavior).

```python
import asyncio

async def lifo_demo():
    queue = asyncio.LifoQueue()
    
    # Add items
    for i in range(5):
        await queue.put(f"item-{i}")
    
    # Retrieve in LIFO order (stack)
    while not queue.empty():
        item = await queue.get()
        print(item)

asyncio.run(lifo_demo())

# Output:
# item-4
# item-3
# item-2
# item-1
# item-0
```

**Use cases:**
- Undo/redo operations
- Depth-first traversal
- Backtracking algorithms

---

## Producer-Consumer Pattern

The classic pattern for work distribution.

### Basic Producer-Consumer

```python
import asyncio
import random

async def producer(queue, producer_id):
    """Produce items and put them in the queue"""
    for i in range(5):
        item = f"Producer-{producer_id}-Item-{i}"
        await queue.put(item)
        print(f"Produced: {item}")
        await asyncio.sleep(random.uniform(0.1, 0.5))
    
    print(f"Producer-{producer_id} done")

async def consumer(queue, consumer_id):
    """Consume items from the queue"""
    while True:
        item = await queue.get()
        
        # Process item
        print(f"Consumer-{consumer_id} processing: {item}")
        await asyncio.sleep(random.uniform(0.2, 0.6))
        
        # Mark as done
        queue.task_done()

async def main():
    queue = asyncio.Queue()
    
    # Create producers and consumers
    producers = [
        asyncio.create_task(producer(queue, i))
        for i in range(2)
    ]
    
    consumers = [
        asyncio.create_task(consumer(queue, i))
        for i in range(3)
    ]
    
    # Wait for all producers to finish
    await asyncio.gather(*producers)
    
    # Wait for queue to be empty
    await queue.join()
    
    # Cancel consumers (they run forever)
    for c in consumers:
        c.cancel()
    
    await asyncio.gather(*consumers, return_exceptions=True)
    print("All work complete")

asyncio.run(main())
```

**Output (example):**
```
Produced: Producer-0-Item-0
Produced: Producer-1-Item-0
Consumer-0 processing: Producer-0-Item-0
Consumer-1 processing: Producer-1-Item-0
Produced: Producer-0-Item-1
Consumer-2 processing: Producer-0-Item-1
...
All work complete
```

---

## Bounded Queues and Backpressure

Bounded queues limit the number of items, providing natural backpressure.

### Unbounded Queue (Default)

```python
import asyncio

async def unbounded_demo():
    queue = asyncio.Queue()  # No size limit
    
    # Can add unlimited items
    for i in range(1000):
        await queue.put(i)  # Never blocks
    
    print(f"Added 1000 items, queue size: {queue.qsize()}")

asyncio.run(unbounded_demo())

# Output:
# Added 1000 items, queue size: 1000
```

**Problem:** Memory can grow unbounded if producers are faster than consumers.

### Bounded Queue with Backpressure

```python
import asyncio

async def fast_producer(queue):
    """Producer that tries to produce quickly"""
    for i in range(10):
        print(f"Producer: Trying to add item {i}")
        await queue.put(i)  # Blocks when queue is full
        print(f"Producer: Added item {i}")

async def slow_consumer(queue):
    """Consumer that processes slowly"""
    while True:
        item = await queue.get()
        print(f"Consumer: Processing item {item}")
        await asyncio.sleep(1.0)  # Slow processing
        queue.task_done()

async def main():
    # Bounded queue with max size 3
    queue = asyncio.Queue(maxsize=3)
    
    producer_task = asyncio.create_task(fast_producer(queue))
    consumer_task = asyncio.create_task(slow_consumer(queue))
    
    # Wait for producer to finish
    await producer_task
    
    # Wait for queue to be empty
    await queue.join()
    
    # Cancel consumer
    consumer_task.cancel()
    await asyncio.gather(consumer_task, return_exceptions=True)

asyncio.run(main())
```

**Output:**
```
Producer: Trying to add item 0
Producer: Added item 0
Producer: Trying to add item 1
Producer: Added item 1
Producer: Trying to add item 2
Producer: Added item 2
Producer: Trying to add item 3
Consumer: Processing item 0
Producer: Added item 3
Producer: Trying to add item 4
Consumer: Processing item 1
Producer: Added item 4
...
```

**Key insight:** `put()` blocks when queue is full, providing automatic backpressure.

---

## Non-blocking Operations

For cases where you don't want to wait.

### `put_nowait()` and `get_nowait()`

```python
import asyncio

async def nowait_demo():
    queue = asyncio.Queue(maxsize=2)
    
    # put_nowait() - doesn't block
    try:
        queue.put_nowait("item1")
        queue.put_nowait("item2")
        queue.put_nowait("item3")  # Queue full!
    except asyncio.QueueFull:
        print("Queue is full!")
    
    # get_nowait() - doesn't block
    try:
        print(queue.get_nowait())
        print(queue.get_nowait())
        print(queue.get_nowait())  # Queue empty!
    except asyncio.QueueEmpty:
        print("Queue is empty!")

asyncio.run(nowait_demo())

# Output:
# Queue is full!
# item1
# item2
# Queue is empty!
```

**Use cases:**
- Polling without blocking
- Try-and-fail patterns
- Performance-critical paths

---

## `join()` and `task_done()`

These methods coordinate producer-consumer completion.

### Understanding `join()` and `task_done()`

```python
import asyncio

async def worker(queue, worker_id):
    while True:
        item = await queue.get()
        
        print(f"Worker-{worker_id}: Processing {item}")
        await asyncio.sleep(0.5)
        
        # CRITICAL: Mark item as done
        queue.task_done()
        print(f"Worker-{worker_id}: Done with {item}")

async def main():
    queue = asyncio.Queue()
    
    # Add work items
    for i in range(5):
        await queue.put(f"task-{i}")
    
    # Start workers
    workers = [
        asyncio.create_task(worker(queue, i))
        for i in range(2)
    ]
    
    # Wait for all items to be processed
    print("Waiting for all tasks to complete...")
    await queue.join()
    print("All tasks complete!")
    
    # Cancel workers
    for w in workers:
        w.cancel()
    await asyncio.gather(*workers, return_exceptions=True)

asyncio.run(main())
```

**How it works:**

```
queue.put(item)     → Internal counter += 1
queue.get()         → Returns item (counter unchanged)
queue.task_done()   → Internal counter -= 1
queue.join()        → Waits until counter == 0
```

### Common Mistake: Forgetting `task_done()`

```python
async def bad_worker(queue):
    while True:
        item = await queue.get()
        await process(item)
        # BUG: Forgot queue.task_done()!

async def main():
    queue = asyncio.Queue()
    await queue.put("item")
    
    worker = asyncio.create_task(bad_worker(queue))
    
    # This will hang forever!
    await queue.join()  # Never completes
```

**Always call `task_done()` after processing each item.**

---

## Real-world Example: Task Queue System

```python
import asyncio
import time
from typing import Callable, Any
from dataclasses import dataclass
from enum import Enum

class TaskPriority(Enum):
    HIGH = 1
    MEDIUM = 2
    LOW = 3

@dataclass
class Task:
    priority: TaskPriority
    func: Callable
    args: tuple
    kwargs: dict
    task_id: str
    
    def __lt__(self, other):
        # For PriorityQueue comparison
        return self.priority.value < other.priority.value

class TaskQueue:
    def __init__(self, num_workers: int = 3, max_queue_size: int = 100):
        self.queue = asyncio.PriorityQueue(maxsize=max_queue_size)
        self.num_workers = num_workers
        self.workers = []
        self.stats = {
            "completed": 0,
            "failed": 0,
            "total_time": 0.0
        }
    
    async def add_task(
        self,
        func: Callable,
        *args,
        priority: TaskPriority = TaskPriority.MEDIUM,
        task_id: str = None,
        **kwargs
    ):
        """Add a task to the queue"""
        if task_id is None:
            task_id = f"task-{time.time()}"
        
        task = Task(
            priority=priority,
            func=func,
            args=args,
            kwargs=kwargs,
            task_id=task_id
        )
        
        await self.queue.put(task)
        print(f"📥 Queued: {task_id} (Priority: {priority.name})")
    
    async def worker(self, worker_id: int):
        """Worker that processes tasks from the queue"""
        while True:
            task = await self.queue.get()
            
            print(f"🔧 Worker-{worker_id}: Starting {task.task_id}")
            start_time = time.time()
            
            try:
                # Execute the task
                if asyncio.iscoroutinefunction(task.func):
                    result = await task.func(*task.args, **task.kwargs)
                else:
                    result = task.func(*task.args, **task.kwargs)
                
                elapsed = time.time() - start_time
                self.stats["completed"] += 1
                self.stats["total_time"] += elapsed
                
                print(f"✅ Worker-{worker_id}: Completed {task.task_id} in {elapsed:.2f}s")
                
            except Exception as e:
                self.stats["failed"] += 1
                print(f"❌ Worker-{worker_id}: Failed {task.task_id}: {e}")
            
            finally:
                self.queue.task_done()
    
    async def start(self):
        """Start all workers"""
        self.workers = [
            asyncio.create_task(self.worker(i))
            for i in range(self.num_workers)
        ]
        print(f"🚀 Started {self.num_workers} workers")
    
    async def wait_completion(self):
        """Wait for all tasks to complete"""
        await self.queue.join()
        print("✨ All tasks completed")
    
    async def stop(self):
        """Stop all workers"""
        for worker in self.workers:
            worker.cancel()
        await asyncio.gather(*self.workers, return_exceptions=True)
        print("🛑 All workers stopped")
    
    def get_stats(self):
        """Get queue statistics"""
        return {
            **self.stats,
            "queue_size": self.queue.qsize(),
            "avg_time": (
                self.stats["total_time"] / self.stats["completed"]
                if self.stats["completed"] > 0
                else 0
            )
        }

# Example tasks
async def fetch_data(url: str):
    """Simulate fetching data"""
    await asyncio.sleep(1.0)
    return f"Data from {url}"

async def process_data(data: str):
    """Simulate processing data"""
    await asyncio.sleep(0.5)
    return f"Processed: {data}"

def compute(x: int, y: int):
    """Synchronous computation"""
    time.sleep(0.3)
    return x + y

async def main():
    # Create task queue
    task_queue = TaskQueue(num_workers=3, max_queue_size=50)
    await task_queue.start()
    
    # Add various tasks with different priorities
    await task_queue.add_task(
        fetch_data,
        "https://api.example.com/urgent",
        priority=TaskPriority.HIGH,
        task_id="urgent-fetch"
    )
    
    await task_queue.add_task(
        process_data,
        "some data",
        priority=TaskPriority.MEDIUM,
        task_id="process-1"
    )
    
    await task_queue.add_task(
        compute,
        10, 20,
        priority=TaskPriority.LOW,
        task_id="compute-1"
    )
    
    # Add more tasks
    for i in range(5):
        await task_queue.add_task(
            fetch_data,
            f"https://api.example.com/data/{i}",
            priority=TaskPriority.MEDIUM,
            task_id=f"fetch-{i}"
        )
    
    # Wait for completion
    await task_queue.wait_completion()
    
    # Print statistics
    stats = task_queue.get_stats()
    print(f"\n📊 Statistics:")
    print(f"   Completed: {stats['completed']}")
    print(f"   Failed: {stats['failed']}")
    print(f"   Avg time: {stats['avg_time']:.2f}s")
    
    # Stop workers
    await task_queue.stop()

asyncio.run(main())
```

---

## Advanced Queue Patterns

### Pattern 1: Rate-Limited Queue

```python
import asyncio
import time

class RateLimitedQueue:
    def __init__(self, rate_limit: float):
        """
        rate_limit: minimum seconds between get() calls
        """
        self.queue = asyncio.Queue()
        self.rate_limit = rate_limit
        self.last_get = 0.0
    
    async def put(self, item):
        await self.queue.put(item)
    
    async def get(self):
        # Enforce rate limit
        now = time.time()
        elapsed = now - self.last_get
        
        if elapsed < self.rate_limit:
            await asyncio.sleep(self.rate_limit - elapsed)
        
        item = await self.queue.get()
        self.last_get = time.time()
        return item
    
    def task_done(self):
        self.queue.task_done()
    
    async def join(self):
        await self.queue.join()

async def demo_rate_limited():
    queue = RateLimitedQueue(rate_limit=0.5)  # Max 2 items/second
    
    # Add items
    for i in range(5):
        await queue.put(f"item-{i}")
    
    # Get items (rate limited)
    start = time.time()
    for _ in range(5):
        item = await queue.get()
        elapsed = time.time() - start
        print(f"[{elapsed:.2f}s] Got: {item}")
        queue.task_done()

asyncio.run(demo_rate_limited())

# Output:
# [0.00s] Got: item-0
# [0.50s] Got: item-1
# [1.00s] Got: item-2
# [1.50s] Got: item-3
# [2.00s] Got: item-4
```

### Pattern 2: Batching Queue

```python
import asyncio
from typing import List

class BatchingQueue:
    def __init__(self, batch_size: int, timeout: float):
        self.queue = asyncio.Queue()
        self.batch_size = batch_size
        self.timeout = timeout
    
    async def put(self, item):
        await self.queue.put(item)
    
    async def get_batch(self) -> List:
        """Get a batch of items"""
        batch = []
        
        try:
            # Get first item (wait if needed)
            batch.append(await self.queue.get())
            
            # Try to get more items up to batch_size
            while len(batch) < self.batch_size:
                try:
                    # Use timeout to avoid waiting too long
                    item = await asyncio.wait_for(
                        self.queue.get(),
                        timeout=self.timeout
                    )
                    batch.append(item)
                except asyncio.TimeoutError:
                    break
        
        except asyncio.QueueEmpty:
            pass
        
        return batch
    
    def task_done(self, count: int = 1):
        for _ in range(count):
            self.queue.task_done()

async def demo_batching():
    queue = BatchingQueue(batch_size=3, timeout=0.5)
    
    # Producer
    async def producer():
        for i in range(10):
            await queue.put(f"item-{i}")
            await asyncio.sleep(0.2)
    
    # Consumer
    async def consumer():
        while True:
            batch = await queue.get_batch()
            if not batch:
                break
            print(f"Processing batch of {len(batch)}: {batch}")
            queue.task_done(len(batch))
            await asyncio.sleep(1.0)
    
    producer_task = asyncio.create_task(producer())
    consumer_task = asyncio.create_task(consumer())
    
    await producer_task
    await asyncio.sleep(2.0)  # Let consumer finish
    consumer_task.cancel()

asyncio.run(demo_batching())
```

### Pattern 3: Priority-based Work Stealing

```python
import asyncio
from typing import Dict

class WorkStealingQueue:
    def __init__(self, num_workers: int):
        # Each worker has its own queue
        self.queues: Dict[int, asyncio.Queue] = {
            i: asyncio.Queue()
            for i in range(num_workers)
        }
        self.num_workers = num_workers
    
    async def put(self, item, worker_id: int = None):
        """Put item in specific worker's queue or least loaded"""
        if worker_id is None:
            # Find least loaded queue
            worker_id = min(
                self.queues.keys(),
                key=lambda k: self.queues[k].qsize()
            )
        
        await self.queues[worker_id].put(item)
    
    async def get(self, worker_id: int):
        """Get from own queue, or steal from others"""
        # Try own queue first
        if not self.queues[worker_id].empty():
            return await self.queues[worker_id].get()
        
        # Try to steal from others
        for other_id, queue in self.queues.items():
            if other_id != worker_id and not queue.empty():
                print(f"Worker-{worker_id} stealing from Worker-{other_id}")
                return await queue.get()
        
        # Wait on own queue
        return await self.queues[worker_id].get()

# Demo work stealing
async def demo_work_stealing():
    queue = WorkStealingQueue(num_workers=3)
    
    # Add work unevenly
    for i in range(10):
        await queue.put(f"task-{i}", worker_id=0)  # All to worker 0
    
    async def worker(worker_id):
        for _ in range(4):
            task = await queue.get(worker_id)
            print(f"Worker-{worker_id}: {task}")
            await asyncio.sleep(0.5)
    
    workers = [
        asyncio.create_task(worker(i))
        for i in range(3)
    ]
    
    await asyncio.gather(*workers)

asyncio.run(demo_work_stealing())
```

---

## Best Practices

### ✅ DO: Use bounded queues for backpressure

```python
# Good: Prevents memory issues
queue = asyncio.Queue(maxsize=100)
```

### ✅ DO: Always call `task_done()`

```python
async def worker(queue):
    while True:
        item = await queue.get()
        await process(item)
        queue.task_done()  # ✅ Always call this
```

### ✅ DO: Use `join()` to wait for completion

```python
# Good: Wait for all work to complete
await queue.join()
```

### ❌ DON'T: Forget to cancel infinite workers

```python
# BAD: Workers run forever
workers = [asyncio.create_task(worker(queue)) for _ in range(5)]
await queue.join()
# Workers still running!

# GOOD: Cancel workers
await queue.join()
for w in workers:
    w.cancel()
await asyncio.gather(*workers, return_exceptions=True)
```

### ✅ DO: Handle QueueFull and QueueEmpty

```python
try:
    queue.put_nowait(item)
except asyncio.QueueFull:
    # Handle full queue
    pass

try:
    item = queue.get_nowait()
except asyncio.QueueEmpty:
    # Handle empty queue
    pass
```

---

## Summary: Queue Mental Model

✅ **Queues enable task communication** - producer-consumer pattern

✅ **Three types:** Queue (FIFO), PriorityQueue, LifoQueue (LIFO)

✅ **Bounded queues provide backpressure** - prevent memory issues

✅ **`put()` and `get()` are async** - can block

✅ **`put_nowait()` and `get_nowait()`** - non-blocking alternatives

✅ **`task_done()` and `join()`** - coordinate completion

✅ **Always cancel infinite workers** after `join()`

---

## What's Next?

Now that you understand queues, we'll explore shared state and race conditions.

In [Chapter 8: Shared State and Race Conditions](./08-shared-state-races.md), we'll cover:
- Why race conditions happen in async
- Shared mutable state problems
- Atomicity misconceptions
- When synchronization is needed
- Practical examples and solutions

Understanding these issues is critical before learning synchronization primitives.

---

**Previous:** [← Chapter 6: Waiting Primitives](../part2-orchestration/06-waiting-primitives.md)  
**Next:** [Chapter 8: Shared State and Race Conditions →](./08-shared-state-races.md)