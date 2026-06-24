# Chapter 11: Condition - Complex Coordination

## Introduction

`asyncio.Condition` combines a lock with event-like waiting, enabling complex coordination patterns. While Events signal simple state changes, Conditions allow tasks to wait for arbitrary predicates (conditions) to become true.

By the end of this chapter, you'll understand:
- Lock + wait model
- `notify()` vs `notify_all()`
- Why predicate loops are necessary
- Producer-consumer with conditions
- Bounded buffer implementation
- Blackboard pattern with Conditions (more powerful than Events)
- When to use Condition vs Event vs Queue

---

## What is a Condition?

A `Condition` is a synchronization primitive that combines:
1. **A lock** - for protecting shared state
2. **A wait mechanism** - for suspending tasks until notified

### The Problem Conditions Solve

Events are too simple for complex coordination:

```python
# Problem: Event doesn't know WHAT changed
event = asyncio.Event()

# Consumer doesn't know if queue has items or is full
await event.wait()  # What happened? Items added? Space available?
```

Conditions solve this by letting you check predicates while holding a lock:

```python
# Solution: Check predicate under lock
async with condition:
    while not predicate():
        await condition.wait()
    # Predicate is true, proceed safely
```

---

## Basic Condition Usage

```python
import asyncio

async def basic_condition_demo():
    condition = asyncio.Condition()
    data = []
    
    async def consumer():
        async with condition:
            print("Consumer: Waiting for data")
            await condition.wait()  # Release lock and wait
            print(f"Consumer: Got data: {data}")
    
    async def producer():
        await asyncio.sleep(1.0)
        
        async with condition:
            data.append("item")
            print("Producer: Added item, notifying")
            condition.notify()  # Wake one waiter
    
    await asyncio.gather(consumer(), producer())

asyncio.run(basic_condition_demo())

# Output:
# Consumer: Waiting for data
# Producer: Added item, notifying
# Consumer: Got data: ['item']
```

**Key points:**
- Must acquire lock before `wait()` or `notify()`
- `wait()` releases lock and suspends
- `notify()` wakes one waiting task
- Woken task reacquires lock before continuing

---

## Condition Operations

### Core Operations

```python
import asyncio

condition = asyncio.Condition()

# Acquire lock (usually via context manager)
async with condition:
    # Wait for notification (releases lock while waiting)
    await condition.wait()
    
    # Notify one waiting task
    condition.notify()
    
    # Notify all waiting tasks
    condition.notify_all()

# Can also use explicit acquire/release
await condition.acquire()
try:
    await condition.wait()
finally:
    condition.release()
```

### Wait Semantics

```python
import asyncio

async def wait_semantics():
    condition = asyncio.Condition()
    
    async with condition:
        print("Before wait (lock held)")
        
        # wait() releases lock and suspends
        await condition.wait()
        
        print("After wait (lock reacquired)")
```

**What happens during `wait()`:**
1. Release the lock
2. Suspend task (add to wait queue)
3. When notified: reacquire lock
4. Return to caller

---

## The Predicate Loop Pattern

**Critical concept:** Always use a `while` loop, never `if`.

### Why Predicate Loops Are Necessary

```python
import asyncio

# ❌ WRONG: Using if
async def wrong_consumer(condition, queue):
    async with condition:
        if len(queue) == 0:  # ❌ BAD
            await condition.wait()
        item = queue.pop(0)
    return item

# ✅ CORRECT: Using while
async def correct_consumer(condition, queue):
    async with condition:
        while len(queue) == 0:  # ✅ GOOD
            await condition.wait()
        item = queue.pop(0)
    return item
```

### Why `while` Not `if`?

**Three reasons:**

1. **Spurious wakeups** - Task might wake without notification
2. **Multiple consumers** - Another task might consume the item first
3. **Condition might change** - State could change between notification and reacquisition

### Demonstration of the Problem

```python
import asyncio

async def demonstrate_if_problem():
    condition = asyncio.Condition()
    queue = []
    
    async def bad_consumer(consumer_id):
        async with condition:
            if len(queue) == 0:  # ❌ Using if
                print(f"Consumer {consumer_id}: Waiting")
                await condition.wait()
            
            # BUG: Queue might be empty here!
            item = queue.pop(0)  # Could raise IndexError
            print(f"Consumer {consumer_id}: Got {item}")
    
    async def producer():
        await asyncio.sleep(0.1)
        async with condition:
            queue.append("item")
            print("Producer: Added item")
            condition.notify_all()  # Wake both consumers
    
    # Start two consumers and one producer
    await asyncio.gather(
        bad_consumer(1),
        bad_consumer(2),
        producer(),
        return_exceptions=True
    )

asyncio.run(demonstrate_if_problem())

# Output (race condition):
# Consumer 1: Waiting
# Consumer 2: Waiting
# Producer: Added item
# Consumer 1: Got item
# Consumer 2: IndexError!  ← BUG
```

### Correct Implementation

```python
import asyncio

async def demonstrate_while_solution():
    condition = asyncio.Condition()
    queue = []
    
    async def good_consumer(consumer_id):
        async with condition:
            while len(queue) == 0:  # ✅ Using while
                print(f"Consumer {consumer_id}: Waiting")
                await condition.wait()
            
            item = queue.pop(0)
            print(f"Consumer {consumer_id}: Got {item}")
    
    async def producer():
        await asyncio.sleep(0.1)
        async with condition:
            queue.append("item")
            print("Producer: Added item")
            condition.notify_all()
    
    await asyncio.gather(
        good_consumer(1),
        good_consumer(2),
        producer()
    )

asyncio.run(demonstrate_while_solution())

# Output (correct):
# Consumer 1: Waiting
# Consumer 2: Waiting
# Producer: Added item
# Consumer 1: Got item
# Consumer 2: Waiting  ← Correctly waits again
```

---

## notify() vs notify_all()

### notify() - Wake One Task

```python
import asyncio

async def notify_one_demo():
    condition = asyncio.Condition()
    counter = 0
    
    async def worker(worker_id):
        async with condition:
            while counter < worker_id:
                await condition.wait()
            print(f"Worker {worker_id} activated")
    
    async def coordinator():
        nonlocal counter
        for i in range(1, 4):
            await asyncio.sleep(0.5)
            async with condition:
                counter = i
                print(f"Coordinator: counter = {counter}")
                condition.notify()  # Wake ONE worker
    
    await asyncio.gather(
        worker(1),
        worker(2),
        worker(3),
        coordinator()
    )

asyncio.run(notify_one_demo())

# Output:
# Coordinator: counter = 1
# Worker 1 activated
# Coordinator: counter = 2
# Worker 2 activated
# Coordinator: counter = 3
# Worker 3 activated
```

### notify_all() - Wake All Tasks

```python
import asyncio

async def notify_all_demo():
    condition = asyncio.Condition()
    ready = False
    
    async def worker(worker_id):
        async with condition:
            while not ready:
                await condition.wait()
            print(f"Worker {worker_id} starting")
    
    async def coordinator():
        nonlocal ready
        await asyncio.sleep(1.0)
        async with condition:
            ready = True
            print("Coordinator: Broadcasting ready")
            condition.notify_all()  # Wake ALL workers
    
    await asyncio.gather(
        *[worker(i) for i in range(5)],
        coordinator()
    )

asyncio.run(notify_all_demo())

# Output:
# Coordinator: Broadcasting ready
# Worker 0 starting
# Worker 1 starting
# Worker 2 starting
# Worker 3 starting
# Worker 4 starting
```

### When to Use Each

**Use `notify()`:**
- Only one task should proceed
- Work items are independent
- Example: Single item added to queue

**Use `notify_all()`:**
- All tasks should check condition
- Broadcast state change
- Example: Service ready, shutdown signal

---

## Producer-Consumer with Condition

Classic pattern using Condition for coordination.

```python
import asyncio
from collections import deque

class BoundedQueue:
    """Thread-safe bounded queue using Condition"""
    
    def __init__(self, maxsize):
        self.maxsize = maxsize
        self.queue = deque()
        self.condition = asyncio.Condition()
    
    async def put(self, item):
        """Add item, wait if full"""
        async with self.condition:
            # Wait while queue is full
            while len(self.queue) >= self.maxsize:
                await self.condition.wait()
            
            self.queue.append(item)
            print(f"  Produced: {item} (size: {len(self.queue)})")
            
            # Notify consumers
            self.condition.notify()
    
    async def get(self):
        """Remove item, wait if empty"""
        async with self.condition:
            # Wait while queue is empty
            while len(self.queue) == 0:
                await self.condition.wait()
            
            item = self.queue.popleft()
            print(f"  Consumed: {item} (size: {len(self.queue)})")
            
            # Notify producers
            self.condition.notify()
            
            return item

async def producer_consumer_demo():
    queue = BoundedQueue(maxsize=3)
    
    async def producer(producer_id):
        for i in range(5):
            item = f"P{producer_id}-Item{i}"
            await queue.put(item)
            await asyncio.sleep(0.3)
    
    async def consumer(consumer_id):
        for _ in range(5):
            item = await queue.get()
            await asyncio.sleep(0.5)
    
    await asyncio.gather(
        producer(1),
        consumer(1),
        consumer(2)
    )

asyncio.run(producer_consumer_demo())
```

---

## Real-world: Bounded Buffer

Complete bounded buffer implementation with proper coordination.

```python
import asyncio
from collections import deque
from typing import TypeVar, Generic

T = TypeVar('T')

class BoundedBuffer(Generic[T]):
    """
    Bounded buffer with producer-consumer coordination.
    
    Producers wait when buffer is full.
    Consumers wait when buffer is empty.
    """
    
    def __init__(self, capacity: int):
        self.capacity = capacity
        self.buffer = deque()
        self.condition = asyncio.Condition()
        self.closed = False
    
    async def put(self, item: T):
        """Put item in buffer, wait if full"""
        async with self.condition:
            # Wait while buffer is full
            while len(self.buffer) >= self.capacity and not self.closed:
                await self.condition.wait()
            
            if self.closed:
                raise RuntimeError("Buffer is closed")
            
            self.buffer.append(item)
            
            # Notify one consumer
            self.condition.notify()
    
    async def get(self) -> T:
        """Get item from buffer, wait if empty"""
        async with self.condition:
            # Wait while buffer is empty
            while len(self.buffer) == 0 and not self.closed:
                await self.condition.wait()
            
            if len(self.buffer) == 0 and self.closed:
                raise RuntimeError("Buffer is closed and empty")
            
            item = self.buffer.popleft()
            
            # Notify one producer
            self.condition.notify()
            
            return item
    
    async def close(self):
        """Close buffer and wake all waiters"""
        async with self.condition:
            self.closed = True
            self.condition.notify_all()
    
    def __len__(self):
        return len(self.buffer)

async def bounded_buffer_demo():
    buffer = BoundedBuffer(capacity=5)
    
    async def producer(producer_id, count):
        try:
            for i in range(count):
                item = f"P{producer_id}-{i}"
                await buffer.put(item)
                print(f"Producer {producer_id}: Put {item}")
                await asyncio.sleep(0.1)
        except RuntimeError as e:
            print(f"Producer {producer_id}: {e}")
    
    async def consumer(consumer_id, count):
        try:
            for _ in range(count):
                item = await buffer.get()
                print(f"Consumer {consumer_id}: Got {item}")
                await asyncio.sleep(0.2)
        except RuntimeError as e:
            print(f"Consumer {consumer_id}: {e}")
    
    # Start producers and consumers
    await asyncio.gather(
        producer(1, 10),
        producer(2, 10),
        consumer(1, 10),
        consumer(2, 10)
    )
    
    await buffer.close()

asyncio.run(bounded_buffer_demo())
```

---

## Blackboard Pattern with Conditions

Conditions enable more sophisticated blackboard patterns than Events.

```python
import asyncio
from typing import Dict, Any, Callable, List
from dataclasses import dataclass
from enum import Enum

class EventType(Enum):
    DATA_UPDATED = "data_updated"
    ANALYSIS_COMPLETE = "analysis_complete"
    DECISION_MADE = "decision_made"

@dataclass
class BlackboardEvent:
    event_type: EventType
    data: Any
    source: str

class ConditionBasedBlackboard:
    """
    Blackboard using Conditions for sophisticated coordination.
    
    Advantages over Event-based:
    - Can wait for specific predicates
    - Multiple conditions can be checked atomically
    - Better for complex state dependencies
    """
    
    def __init__(self):
        self.condition = asyncio.Condition()
        self.events: List[BlackboardEvent] = []
        self.data: Dict[str, Any] = {}
        self.closed = False
    
    async def post_event(self, event: BlackboardEvent):
        """Post event and notify all waiters"""
        async with self.condition:
            if self.closed:
                raise RuntimeError("Blackboard is closed")
            
            print(f"📢 Posted: {event.event_type.value} from {event.source}")
            self.events.append(event)
            
            # Notify all waiters to check their predicates
            self.condition.notify_all()
    
    async def wait_for_event(
        self,
        predicate: Callable[[List[BlackboardEvent]], bool],
        subscriber_name: str
    ) -> BlackboardEvent:
        """
        Wait for an event matching the predicate.
        
        Predicate receives list of events and returns True if condition met.
        """
        async with self.condition:
            print(f"👂 {subscriber_name}: Waiting for event")
            
            # Wait while predicate is false
            while not predicate(self.events) and not self.closed:
                await self.condition.wait()
            
            if self.closed:
                raise RuntimeError("Blackboard closed")
            
            # Find matching event
            for event in self.events:
                if predicate([event]):
                    print(f"✅ {subscriber_name}: Got {event.event_type.value}")
                    return event
    
    async def wait_for_condition(
        self,
        predicate: Callable[[], bool],
        subscriber_name: str
    ):
        """Wait for arbitrary condition on blackboard state"""
        async with self.condition:
            print(f"👂 {subscriber_name}: Waiting for condition")
            
            while not predicate() and not self.closed:
                await self.condition.wait()
            
            if self.closed:
                raise RuntimeError("Blackboard closed")
            
            print(f"✅ {subscriber_name}: Condition met")
    
    async def write_data(self, key: str, value: Any):
        """Write data and notify waiters"""
        async with self.condition:
            self.data[key] = value
            self.condition.notify_all()
    
    async def read_data(self, key: str) -> Any:
        """Read data"""
        async with self.condition:
            return self.data.get(key)
    
    async def close(self):
        """Close blackboard"""
        async with self.condition:
            self.closed = True
            self.condition.notify_all()

# Knowledge Sources
class DataCollector:
    def __init__(self, blackboard: ConditionBasedBlackboard):
        self.blackboard = blackboard
        self.name = "DataCollector"
    
    async def run(self):
        for i in range(3):
            await asyncio.sleep(1.0)
            
            data = {"reading": i * 10, "timestamp": i}
            await self.blackboard.write_data("sensor_data", data)
            
            await self.blackboard.post_event(
                BlackboardEvent(
                    event_type=EventType.DATA_UPDATED,
                    data=data,
                    source=self.name
                )
            )

class DataAnalyzer:
    def __init__(self, blackboard: ConditionBasedBlackboard):
        self.blackboard = blackboard
        self.name = "DataAnalyzer"
    
    async def run(self):
        while True:
            try:
                # Wait for DATA_UPDATED events
                event = await self.blackboard.wait_for_event(
                    lambda events: any(
                        e.event_type == EventType.DATA_UPDATED
                        for e in events
                    ),
                    self.name
                )
                
                print(f"🔍 {self.name}: Analyzing {event.data}")
                await asyncio.sleep(0.5)
                
                analysis = {"trend": "increasing"}
                await self.blackboard.write_data("analysis", analysis)
                
                await self.blackboard.post_event(
                    BlackboardEvent(
                        event_type=EventType.ANALYSIS_COMPLETE,
                        data=analysis,
                        source=self.name
                    )
                )
            except RuntimeError:
                break

class DecisionMaker:
    def __init__(self, blackboard: ConditionBasedBlackboard):
        self.blackboard = blackboard
        self.name = "DecisionMaker"
    
    async def run(self):
        while True:
            try:
                # Wait for BOTH data and analysis to be available
                await self.blackboard.wait_for_condition(
                    lambda: (
                        self.blackboard.data.get("sensor_data") is not None
                        and self.blackboard.data.get("analysis") is not None
                    ),
                    self.name
                )
                
                sensor_data = await self.blackboard.read_data("sensor_data")
                analysis = await self.blackboard.read_data("analysis")
                
                print(f"🎯 {self.name}: Making decision")
                print(f"   Data: {sensor_data}, Analysis: {analysis}")
                
                decision = {"action": "adjust"}
                
                await self.blackboard.post_event(
                    BlackboardEvent(
                        event_type=EventType.DECISION_MADE,
                        data=decision,
                        source=self.name
                    )
                )
                
                # Clear for next iteration
                await self.blackboard.write_data("sensor_data", None)
                await self.blackboard.write_data("analysis", None)
                
            except RuntimeError:
                break

async def demo_condition_blackboard():
    blackboard = ConditionBasedBlackboard()
    
    collector = DataCollector(blackboard)
    analyzer = DataAnalyzer(blackboard)
    decision_maker = DecisionMaker(blackboard)
    
    # Run for limited time
    async def run_limited():
        await asyncio.gather(
            collector.run(),
            analyzer.run(),
            decision_maker.run(),
            return_exceptions=True
        )
    
    task = asyncio.create_task(run_limited())
    
    await asyncio.sleep(5.0)
    await blackboard.close()
    
    try:
        await task
    except RuntimeError:
        pass

asyncio.run(demo_condition_blackboard())
```

---

## Condition vs Event vs Queue

### When to Use Each

**Use `Condition` when:**
- Need to wait for complex predicates
- Multiple conditions must be checked atomically
- Need fine-grained control over notifications
- Implementing custom synchronization patterns

**Use `Event` when:**
- Simple binary state (set/not set)
- Broadcasting to all waiters
- No shared state to protect
- Startup/shutdown coordination

**Use `Queue` when:**
- Passing data between tasks
- Producer-consumer pattern
- Built-in backpressure needed
- Don't need custom predicates

### Comparison

```python
# Condition: Complex coordination
async with condition:
    while not complex_predicate():
        await condition.wait()
    # Proceed

# Event: Simple signaling
await event.wait()

# Queue: Data passing
item = await queue.get()
```

---

## Common Patterns

### Pattern 1: Wait for Multiple Conditions

```python
import asyncio

class MultiConditionWaiter:
    def __init__(self):
        self.condition = asyncio.Condition()
        self.flag_a = False
        self.flag_b = False
    
    async def wait_for_both(self):
        """Wait for both flags to be set"""
        async with self.condition:
            while not (self.flag_a and self.flag_b):
                await self.condition.wait()
            print("Both conditions met!")
    
    async def set_flag_a(self):
        async with self.condition:
            self.flag_a = True
            self.condition.notify_all()
    
    async def set_flag_b(self):
        async with self.condition:
            self.flag_b = True
            self.condition.notify_all()
```

### Pattern 2: Countdown Latch

```python
import asyncio

class CountdownLatch:
    """Wait for N events to occur"""
    
    def __init__(self, count):
        self.count = count
        self.condition = asyncio.Condition()
    
    async def count_down(self):
        """Decrement count"""
        async with self.condition:
            self.count -= 1
            if self.count <= 0:
                self.condition.notify_all()
    
    async def wait(self):
        """Wait for count to reach zero"""
        async with self.condition:
            while self.count > 0:
                await self.condition.wait()
```

### Pattern 3: Read-Write Lock

```python
import asyncio

class ReadWriteLock:
    """Allow multiple readers or one writer"""
    
    def __init__(self):
        self.condition = asyncio.Condition()
        self.readers = 0
        self.writer = False
    
    async def acquire_read(self):
        async with self.condition:
            while self.writer:
                await self.condition.wait()
            self.readers += 1
    
    async def release_read(self):
        async with self.condition:
            self.readers -= 1
            if self.readers == 0:
                self.condition.notify_all()
    
    async def acquire_write(self):
        async with self.condition:
            while self.writer or self.readers > 0:
                await self.condition.wait()
            self.writer = True
    
    async def release_write(self):
        async with self.condition:
            self.writer = False
            self.condition.notify_all()
```

---

## Best Practices

### ✅ DO: Always use while loops

```python
# Good: while loop
async with condition:
    while not predicate():
        await condition.wait()
    # Safe to proceed

# Bad: if statement
async with condition:
    if not predicate():  # ❌ Race condition!
        await condition.wait()
```

### ✅ DO: Hold lock when checking predicates

```python
# Good: Check under lock
async with condition:
    while len(queue) == 0:
        await condition.wait()
    item = queue.pop()

# Bad: Check without lock
if len(queue) == 0:  # ❌ Race condition!
    async with condition:
        await condition.wait()
```

### ✅ DO: Notify after state changes

```python
# Good: Notify after change
async with condition:
    queue.append(item)
    condition.notify()

# Bad: Notify before change
async with condition:
    condition.notify()  # ❌ Waiters wake to unchanged state
    queue.append(item)
```

### ✅ DO: Use notify_all() when unsure

```python
# Safe: Wake all waiters
async with condition:
    state_changed = True
    condition.notify_all()  # All waiters check predicate
```

### ❌ DON'T: Call wait() without holding lock

```python
# Bad: wait() without lock
await condition.wait()  # ❌ RuntimeError!

# Good: wait() with lock
async with condition:
    await condition.wait()
```

---

## Summary: Condition Mental Model

✅ **Condition = Lock + Wait** - protect state and wait for changes

✅ **Always use `while` loops** - never `if` for predicates

✅ **`wait()` releases lock** - other tasks can modify state

✅ **`notify()` wakes one** - `notify_all()` wakes all

✅ **Check predicates under lock** - avoid race conditions

✅ **Use for complex coordination** - when Event is too simple

✅ **Perfect for bounded buffers** - producer-consumer patterns

---

## What's Next?

Conditions coordinate complex state. For limiting concurrency, we need Semaphores.

In [Chapter 12: Semaphore - Concurrency Limiting](./12-semaphore.md), we'll cover:
- Concurrency limiting
- Resource pools
- Rate limiting
- Semaphore vs Queue for resource management
- Connection pools
- Practical patterns for limiting parallel operations

Semaphores are simpler than Conditions but perfect for resource management.

---

**Previous:** [← Chapter 10: Event - Signaling and Broadcasting](./10-event.md)  
**Next:** [Chapter 12: Semaphore - Concurrency Limiting →](./12-semaphore.md)