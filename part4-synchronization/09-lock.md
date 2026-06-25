# Chapter 9: Lock - Mutual Exclusion

## Introduction

`asyncio.Lock` is the fundamental synchronization primitive for protecting shared state in async code. It ensures that only one task can access a critical section at a time, preventing race conditions.

By the end of this chapter, you'll understand:
- What mutual exclusion means
- How `asyncio.Lock` works internally
- Fairness guarantees
- Deadlock scenarios and prevention
- Lock granularity strategies
- Common patterns and anti-patterns
- Real-world locking examples

---

## What is Mutual Exclusion?

**Mutual exclusion** means ensuring that only one task executes a critical section of code at a time.

### The Problem Without Locks

```python
import asyncio

balance = 1000

async def withdraw(amount):
    global balance
    
    # Critical section (not protected)
    current = balance
    await asyncio.sleep(0.1)  # Simulate processing
    balance = current - amount
    print(f"Withdrew {amount}, balance: {balance}")

async def main():
    await asyncio.gather(
        withdraw(600),
        withdraw(600)
    )
    print(f"Final balance: {balance}")

asyncio.run(main())

# Output:
# Withdrew 600, balance: 400
# Withdrew 600, balance: -200  # ❌ Negative balance!
# Final balance: -200
```

### The Solution With Locks

```python
import asyncio

balance = 1000
lock = asyncio.Lock()

async def withdraw(amount):
    global balance
    
    # Critical section (protected by lock)
    async with lock:
        current = balance
        await asyncio.sleep(0.1)
        balance = current - amount
        print(f"Withdrew {amount}, balance: {balance}")

async def main():
    await asyncio.gather(
        withdraw(600),
        withdraw(600)
    )
    print(f"Final balance: {balance}")

asyncio.run(main())

# Output:
# Withdrew 600, balance: 400
# Withdrew 600, balance: -200  # Still happens but...
# Final balance: -200           # ...sequentially!
```

**Wait, still negative?** Yes, because we need to check balance inside the lock:

```python
async def withdraw_correct(amount):
    global balance
    
    async with lock:
        if balance >= amount:
            current = balance
            await asyncio.sleep(0.1)
            balance = current - amount
            print(f"Withdrew {amount}, balance: {balance}")
            return True
        else:
            print(f"Insufficient funds for {amount}")
            return False

# Now works correctly!
```

---

## How Lock Works

### Basic Lock Usage

```python
import asyncio

lock = asyncio.Lock()

async def critical_section(task_id):
    print(f"Task {task_id}: Waiting for lock")
    
    async with lock:
        print(f"Task {task_id}: Acquired lock")
        await asyncio.sleep(1.0)
        print(f"Task {task_id}: Releasing lock")
    
    print(f"Task {task_id}: Lock released")

async def main():
    await asyncio.gather(
        critical_section(1),
        critical_section(2),
        critical_section(3)
    )

asyncio.run(main())

# Output:
# Task 1: Waiting for lock
# Task 1: Acquired lock
# Task 2: Waiting for lock
# Task 3: Waiting for lock
# Task 1: Releasing lock
# Task 1: Lock released
# Task 2: Acquired lock
# Task 2: Releasing lock
# Task 2: Lock released
# Task 3: Acquired lock
# Task 3: Releasing lock
# Task 3: Lock released
```

**Key observations:**
- Only one task holds the lock at a time
- Other tasks wait in queue
- Lock is released automatically when exiting `async with`

### Manual Lock Management

```python
import asyncio

async def manual_lock_usage():
    lock = asyncio.Lock()
    
    # Acquire lock
    await lock.acquire()
    
    try:
        # Critical section
        print("In critical section")
        await asyncio.sleep(1.0)
    finally:
        # Always release in finally
        lock.release()
        print("Lock released")

asyncio.run(manual_lock_usage())
```

**⚠️ Warning:** Always use `async with` instead of manual acquire/release. It's safer and handles exceptions automatically.

---

## Lock Internals

Understanding how locks work internally helps debug issues.

### Lock State

```python
import asyncio

async def inspect_lock():
    lock = asyncio.Lock()
    
    print(f"Locked: {lock.locked()}")  # False
    
    async with lock:
        print(f"Locked: {lock.locked()}")  # True
        
        # Try to acquire again (will block)
        # await lock.acquire()  # Would deadlock!
    
    print(f"Locked: {lock.locked()}")  # False

asyncio.run(inspect_lock())
```

### Waiting Queue

```python
import asyncio

async def show_waiters():
    lock = asyncio.Lock()
    
    async def waiter(task_id):
        async with lock:
            print(f"Task {task_id} acquired lock")
            await asyncio.sleep(0.5)
    
    # Start multiple tasks
    tasks = [asyncio.create_task(waiter(i)) for i in range(3)]
    
    # Let them start
    await asyncio.sleep(0.1)
    
    # Check internal state (implementation detail)
    print(f"Lock held: {lock.locked()}")
    
    await asyncio.gather(*tasks)

asyncio.run(show_waiters())
```

---

## Fairness Guarantees

`asyncio.Lock` provides **FIFO fairness** - tasks acquire the lock in the order they requested it.

### Demonstrating Fairness

```python
import asyncio
import time

async def fair_lock_demo():
    lock = asyncio.Lock()
    order = []
    
    async def worker(worker_id):
        start = time.time()
        print(f"[{time.time()-start:.2f}s] Worker {worker_id}: Requesting lock")
        
        async with lock:
            elapsed = time.time() - start
            print(f"[{elapsed:.2f}s] Worker {worker_id}: Acquired lock")
            order.append(worker_id)
            await asyncio.sleep(0.5)
    
    start = time.time()
    
    # Start workers
    tasks = [asyncio.create_task(worker(i)) for i in range(5)]
    await asyncio.gather(*tasks)
    
    print(f"\nAcquisition order: {order}")
    # Output: [0, 1, 2, 3, 4] - FIFO order

asyncio.run(fair_lock_demo())
```

**Key insight:** Lock is fair - no task starvation.

---

## Deadlocks

Deadlocks occur when tasks wait for each other in a cycle.

### Classic Deadlock Example

```python
import asyncio

lock_a = asyncio.Lock()
lock_b = asyncio.Lock()

async def task1():
    async with lock_a:
        print("Task 1: Acquired lock A")
        await asyncio.sleep(0.1)
        
        print("Task 1: Waiting for lock B")
        async with lock_b:  # ❌ Deadlock!
            print("Task 1: Acquired lock B")

async def task2():
    async with lock_b:
        print("Task 2: Acquired lock B")
        await asyncio.sleep(0.1)
        
        print("Task 2: Waiting for lock A")
        async with lock_a:  # ❌ Deadlock!
            print("Task 2: Acquired lock A")

async def main():
    await asyncio.gather(task1(), task2())

# This will hang forever!
# asyncio.run(main())
```

**What happens:**
```
Task 1: Holds lock_a, waits for lock_b
Task 2: Holds lock_b, waits for lock_a
→ Deadlock!
```

### Preventing Deadlocks

**Solution 1: Lock Ordering**

```python
import asyncio

lock_a = asyncio.Lock()
lock_b = asyncio.Lock()

async def task1():
    # Always acquire in same order: A then B
    async with lock_a:
        print("Task 1: Acquired lock A")
        async with lock_b:
            print("Task 1: Acquired lock B")

async def task2():
    # Same order: A then B
    async with lock_a:
        print("Task 2: Acquired lock A")
        async with lock_b:
            print("Task 2: Acquired lock B")

async def main():
    await asyncio.gather(task1(), task2())

asyncio.run(main())
# No deadlock!
```

**Solution 2: Timeout**

```python
import asyncio

async def task_with_timeout():
    try:
        async with asyncio.timeout(5.0):
            async with lock_a:
                await asyncio.sleep(0.1)
                async with lock_b:
                    print("Acquired both locks")
    except asyncio.TimeoutError:
        print("Timeout - possible deadlock")
```

**Solution 3: Try-Lock Pattern**

```python
import asyncio

async def try_acquire_both():
    # Try to acquire lock_a
    if lock_a.locked():
        return False
    
    async with lock_a:
        # Try to acquire lock_b
        if lock_b.locked():
            return False
        
        async with lock_b:
            # Got both locks
            print("Acquired both locks")
            return True
```

---

## Lock Granularity

Choosing the right lock granularity is crucial for performance.

### Coarse-Grained Locking (One Big Lock)

```python
import asyncio

class CoarseGrainedCache:
    def __init__(self):
        self.cache = {}
        self.lock = asyncio.Lock()  # One lock for everything
    
    async def get(self, key):
        async with self.lock:
            return self.cache.get(key)
    
    async def set(self, key, value):
        async with self.lock:
            self.cache[key] = value
    
    async def delete(self, key):
        async with self.lock:
            self.cache.pop(key, None)
```

**Pros:**
- Simple
- No deadlocks
- Easy to reason about

**Cons:**
- Poor concurrency
- All operations serialized
- Bottleneck under load

### Fine-Grained Locking (Lock Per Item)

```python
import asyncio
from collections import defaultdict

class FineGrainedCache:
    def __init__(self):
        self.cache = {}
        self.locks = defaultdict(asyncio.Lock)
        self.main_lock = asyncio.Lock()
    
    async def get(self, key):
        # Get lock for this specific key
        async with self.main_lock:
            key_lock = self.locks[key]
        
        async with key_lock:
            return self.cache.get(key)
    
    async def set(self, key, value):
        async with self.main_lock:
            key_lock = self.locks[key]
        
        async with key_lock:
            self.cache[key] = value
    
    async def delete(self, key):
        async with self.main_lock:
            key_lock = self.locks[key]
        
        async with key_lock:
            self.cache.pop(key, None)
```

**Pros:**
- Better concurrency
- Different keys can be accessed simultaneously
- Scales better

**Cons:**
- More complex
- More memory (lock per key)
- Potential for deadlocks if not careful

### Choosing Granularity

```python
# Coarse-grained: Good for
# - Simple cases
# - Low contention
# - Small critical sections

# Fine-grained: Good for
# - High contention
# - Independent resources
# - Need maximum concurrency
```

---

## Common Patterns

### Pattern 1: Double-Checked Locking

```python
import asyncio

class LazyInitializer:
    def __init__(self):
        self._resource = None
        self._lock = asyncio.Lock()
    
    async def get_resource(self):
        # First check (no lock)
        if self._resource is not None:
            return self._resource
        
        # Acquire lock
        async with self._lock:
            # Second check (with lock)
            if self._resource is None:
                self._resource = await self._initialize()
            return self._resource
    
    async def _initialize(self):
        print("Initializing resource...")
        await asyncio.sleep(1.0)
        return "initialized resource"

async def demo():
    initializer = LazyInitializer()
    
    # Multiple concurrent calls
    results = await asyncio.gather(*[
        initializer.get_resource()
        for _ in range(10)
    ])
    
    print(f"All got: {results[0]}")
    # Only initializes once!

asyncio.run(demo())
```

### Pattern 2: Read-Write Lock (Simplified)

```python
import asyncio

class SimpleRWLock:
    def __init__(self):
        self._lock = asyncio.Lock()
    
    async def read_lock(self):
        """For now, same as write lock (simplified)"""
        return self._lock
    
    async def write_lock(self):
        return self._lock

class DataStore:
    def __init__(self):
        self.data = {}
        self.lock = SimpleRWLock()
    
    async def read(self, key):
        async with await self.lock.read_lock():
            return self.data.get(key)
    
    async def write(self, key, value):
        async with await self.lock.write_lock():
            self.data[key] = value
```

### Pattern 3: Lock with Timeout

```python
import asyncio

async def acquire_with_timeout(lock, timeout=5.0):
    """Try to acquire lock with timeout"""
    try:
        async with asyncio.timeout(timeout):
            async with lock:
                # Critical section
                await do_work()
                return True
    except asyncio.TimeoutError:
        print("Failed to acquire lock within timeout")
        return False
```

### Pattern 4: Conditional Locking

```python
import asyncio

class ConditionalLock:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.enabled = True
    
    async def __aenter__(self):
        if self.enabled:
            await self.lock.acquire()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.enabled and self.lock.locked():
            self.lock.release()

# Usage
lock = ConditionalLock()

async def maybe_locked_operation():
    async with lock:
        # Only locked if lock.enabled is True
        await do_work()
```

---

## Real-world Example: Thread-Safe Counter

```python
import asyncio
from typing import Dict

class SafeCounter:
    """Thread-safe and coroutine-safe counter with multiple operations"""
    
    def __init__(self):
        self._counts: Dict[str, int] = {}
        self._lock = asyncio.Lock()
    
    async def increment(self, key: str, amount: int = 1):
        """Increment counter for key"""
        async with self._lock:
            self._counts[key] = self._counts.get(key, 0) + amount
    
    async def decrement(self, key: str, amount: int = 1):
        """Decrement counter for key"""
        async with self._lock:
            self._counts[key] = self._counts.get(key, 0) - amount
    
    async def get(self, key: str) -> int:
        """Get current count"""
        async with self._lock:
            return self._counts.get(key, 0)
    
    async def get_all(self) -> Dict[str, int]:
        """Get all counts"""
        async with self._lock:
            return self._counts.copy()
    
    async def reset(self, key: str):
        """Reset counter for key"""
        async with self._lock:
            self._counts[key] = 0
    
    async def increment_if_below(self, key: str, threshold: int) -> bool:
        """Increment only if below threshold"""
        async with self._lock:
            current = self._counts.get(key, 0)
            if current < threshold:
                self._counts[key] = current + 1
                return True
            return False

async def demo_counter():
    counter = SafeCounter()
    
    # Concurrent increments
    async def worker(worker_id):
        for i in range(100):
            await counter.increment(f"worker-{worker_id}")
            await asyncio.sleep(0.001)
    
    # Run 10 workers
    await asyncio.gather(*[worker(i) for i in range(10)])
    
    # Get results
    counts = await counter.get_all()
    print(f"Counts: {counts}")
    print(f"Total: {sum(counts.values())}")

asyncio.run(demo_counter())
```

---

## Real-world Example: Resource Pool

```python
import asyncio
from typing import List, Optional

class ResourcePool:
    """Pool of resources with lock-protected access"""
    
    def __init__(self, resources: List[str]):
        self._available = list(resources)
        self._in_use = set()
        self._lock = asyncio.Lock()
    
    async def acquire(self, timeout: Optional[float] = None) -> str:
        """Acquire a resource from the pool"""
        start = asyncio.get_event_loop().time()
        
        while True:
            async with self._lock:
                if self._available:
                    resource = self._available.pop()
                    self._in_use.add(resource)
                    print(f"Acquired: {resource}")
                    return resource
            
            # Check timeout
            if timeout is not None:
                elapsed = asyncio.get_event_loop().time() - start
                if elapsed >= timeout:
                    raise TimeoutError("No resource available")
            
            # Wait a bit before retrying
            await asyncio.sleep(0.1)
    
    async def release(self, resource: str):
        """Release a resource back to the pool"""
        async with self._lock:
            if resource in self._in_use:
                self._in_use.remove(resource)
                self._available.append(resource)
                print(f"Released: {resource}")
            else:
                raise ValueError(f"Resource {resource} not in use")
    
    async def get_stats(self) -> dict:
        """Get pool statistics"""
        async with self._lock:
            return {
                "available": len(self._available),
                "in_use": len(self._in_use),
                "total": len(self._available) + len(self._in_use)
            }

async def demo_pool():
    pool = ResourcePool(["resource-1", "resource-2", "resource-3"])
    
    async def worker(worker_id):
        # Acquire resource
        resource = await pool.acquire(timeout=5.0)
        
        try:
            # Use resource
            print(f"Worker {worker_id} using {resource}")
            await asyncio.sleep(1.0)
        finally:
            # Always release
            await pool.release(resource)
    
    # Run 5 workers (more than resources)
    await asyncio.gather(*[worker(i) for i in range(5)])
    
    stats = await pool.get_stats()
    print(f"Final stats: {stats}")

asyncio.run(demo_pool())
```

---

## Best Practices

### ✅ DO: Use `async with` for locks

```python
# Good
async with lock:
    # Critical section
    pass
```

### ✅ DO: Keep critical sections small

```python
# Good: Minimize lock duration
data = await fetch_data()  # Outside lock
async with lock:
    cache[key] = data  # Only lock for update

# Bad: Holding lock during I/O
async with lock:
    data = await fetch_data()  # ❌ Blocks other tasks
    cache[key] = data
```

### ✅ DO: Use consistent lock ordering

```python
# Good: Always acquire in same order
async with lock_a:
    async with lock_b:
        pass

# Bad: Inconsistent ordering causes deadlocks
async with lock_b:
    async with lock_a:  # ❌ Different order
        pass
```

### ❌ DON'T: Acquire same lock twice

```python
# Bad: Deadlock!
async with lock:
    async with lock:  # ❌ Deadlock
        pass
```

### ❌ DON'T: Hold locks across await points unnecessarily

```python
# Bad
async with lock:
    result = await long_operation()  # ❌ Holds lock too long

# Good
result = await long_operation()
async with lock:
    update_state(result)  # ✅ Minimal lock time
```

### ✅ DO: Document locking requirements

```python
class SharedResource:
    """
    Thread-safe resource.
    
    All public methods are protected by internal lock.
    Do not call public methods while holding external locks
    to avoid deadlocks.
    """
    def __init__(self):
        self._lock = asyncio.Lock()
        self._data = {}
    
    async def update(self, key, value):
        """Update data (thread-safe)"""
        async with self._lock:
            self._data[key] = value
```

---

## Summary: Lock Mental Model

✅ **Lock provides mutual exclusion** - only one task in critical section

✅ **Use `async with lock`** - automatic acquire/release

✅ **Locks are fair** - FIFO ordering prevents starvation

✅ **Deadlocks happen** with circular lock dependencies

✅ **Prevent deadlocks** with consistent lock ordering

✅ **Minimize lock duration** - don't hold during I/O

✅ **Choose granularity** based on contention and complexity

✅ **Document locking** requirements clearly

---

## What's Next?

Locks are for mutual exclusion. For signaling between tasks, we need Events.

In [Chapter 10: Event - Signaling and Broadcasting](./10-event.md), we'll cover:
- Signaling between tasks
- Broadcasting to multiple waiters
- Wakeup semantics
- Startup/shutdown coordination
- Real-world: Service readiness signaling
- Real-world: Blackboard pattern with Events

Events are essential for coordination without shared state.

---

**Previous:** [← Chapter 8: Shared State and Race Conditions](../part3-communication/08-shared-state-races.md)  
**Next:** [Chapter 10: Event - Signaling and Broadcasting →](./10-event.md)
