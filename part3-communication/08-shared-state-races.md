# Chapter 8: Shared State and Race Conditions

## Introduction

A common misconception is that async code doesn't have race conditions because it's single-threaded. This is **false**. Race conditions can and do occur in async code when tasks share mutable state. Understanding when and why is critical for writing correct async programs.

By the end of this chapter, you'll understand:
- Why race conditions happen in single-threaded async code
- Shared mutable state problems
- Atomicity misconceptions
- When synchronization is needed
- Practical examples and solutions
- When you DON'T need locks

---

## The Single-Threaded Misconception

Many developers assume: "Async is single-threaded, so I don't need locks." This is **partially true** but dangerously misleading.

### What's True

```python
import asyncio

counter = 0

async def increment():
    global counter
    counter += 1  # This single line IS atomic

async def main():
    tasks = [increment() for _ in range(1000)]
    await asyncio.gather(*tasks)
    print(f"Counter: {counter}")

asyncio.run(main())

# Output: Counter: 1000 (correct!)
```

**Why this works:** The line `counter += 1` executes atomically - no other task can run during this single Python bytecode operation.

### What's False

```python
import asyncio

counter = 0

async def increment_with_await():
    global counter
    temp = counter
    await asyncio.sleep(0)  # Suspension point!
    counter = temp + 1

async def main():
    tasks = [increment_with_await() for _ in range(1000)]
    await asyncio.gather(*tasks)
    print(f"Counter: {counter}")

asyncio.run(main())

# Output: Counter: 1 (WRONG! Should be 1000)
```

**Why this fails:** The `await` creates a suspension point where other tasks can run, causing a race condition.

---

## Understanding Race Conditions in Async

A race condition occurs when the correctness of your program depends on the timing or ordering of uncontrollable events.

### Anatomy of a Race Condition

```python
import asyncio

balance = 1000

async def withdraw(amount):
    global balance
    
    # Step 1: Check balance
    if balance >= amount:
        print(f"Withdrawing {amount}, balance: {balance}")
        
        # Step 2: Simulate processing delay
        await asyncio.sleep(0.1)
        
        # Step 3: Deduct amount
        balance -= amount
        print(f"Withdrew {amount}, new balance: {balance}")
    else:
        print(f"Insufficient funds for {amount}")

async def main():
    # Two concurrent withdrawals
    await asyncio.gather(
        withdraw(600),
        withdraw(600)
    )
    
    print(f"Final balance: {balance}")

asyncio.run(main())

# Output:
# Withdrawing 600, balance: 1000
# Withdrawing 600, balance: 1000
# Withdrew 600, new balance: 400
# Withdrew 600, new balance: -200
# Final balance: -200
```

**What happened:**

```
Time | Task 1                    | Task 2                    | Balance
-----|---------------------------|---------------------------|--------
0.0  | Check: 1000 >= 600 ✓      |                           | 1000
0.0  | await sleep(0.1)          |                           | 1000
0.0  |                           | Check: 1000 >= 600 ✓      | 1000
0.0  |                           | await sleep(0.1)          | 1000
0.1  | balance = 1000 - 600      |                           | 400
0.1  |                           | balance = 1000 - 600      | 400
```

**Both tasks saw balance = 1000, both withdrew, resulting in negative balance!**

---

## When Race Conditions Occur

Race conditions happen when:
1. Multiple tasks access shared mutable state
2. At least one task modifies the state
3. There's a suspension point (`await`) between read and write

### Example 1: Counter with Async Operation

```python
import asyncio

class Counter:
    def __init__(self):
        self.value = 0
    
    async def increment(self):
        # Read
        current = self.value
        
        # Simulate async work
        await asyncio.sleep(0.001)
        
        # Write
        self.value = current + 1

async def main():
    counter = Counter()
    
    # 100 concurrent increments
    tasks = [counter.increment() for _ in range(100)]
    await asyncio.gather(*tasks)
    
    print(f"Expected: 100, Got: {counter.value}")

asyncio.run(main())

# Output: Expected: 100, Got: 1 (or some small number)
```

**Problem:** All tasks read `value = 0`, then all write `value = 1`.

### Example 2: List Modification

```python
import asyncio

shared_list = []

async def add_items(start, count):
    for i in range(start, start + count):
        # Read list length
        length = len(shared_list)
        
        # Simulate async work
        await asyncio.sleep(0.001)
        
        # Append based on old length
        shared_list.append(f"item-{length}")

async def main():
    await asyncio.gather(
        add_items(0, 5),
        add_items(5, 5)
    )
    
    print(f"List: {shared_list}")
    print(f"Length: {len(shared_list)}")

asyncio.run(main())

# Output: List has duplicate indices!
# ['item-0', 'item-0', 'item-1', 'item-1', ...]
```

---

## Atomicity Misconceptions

Understanding what operations are atomic is crucial.

### Atomic Operations (Safe Without Locks)

```python
import asyncio

# These are atomic in async (single bytecode operation)
counter = 0
counter += 1          # ✅ Atomic
counter -= 1          # ✅ Atomic

my_list = []
my_list.append(x)     # ✅ Atomic
x = my_list.pop()     # ✅ Atomic

my_dict = {}
my_dict[key] = value  # ✅ Atomic
value = my_dict[key]  # ✅ Atomic
```

**Important:** These are only atomic if there's no `await` between operations!

### Non-Atomic Operations (Need Synchronization)

```python
import asyncio

# These are NOT atomic
async def non_atomic_operations():
    # Read-modify-write with await
    temp = counter
    await some_async_operation()
    counter = temp + 1  # ❌ Race condition
    
    # Check-then-act with await
    if len(queue) > 0:
        await asyncio.sleep(0)
        item = queue.pop()  # ❌ Queue might be empty now
    
    # Multiple operations with await
    balance = account.get_balance()
    await validate_transaction()
    account.set_balance(balance - amount)  # ❌ Balance might have changed
```

---

## When You DON'T Need Locks

Not all shared state requires synchronization.

### Case 1: Read-Only Shared State

```python
import asyncio

# Shared configuration (read-only)
CONFIG = {
    "api_url": "https://api.example.com",
    "timeout": 30,
    "max_retries": 3
}

async def fetch_data(endpoint):
    # Safe: Only reading CONFIG
    url = f"{CONFIG['api_url']}/{endpoint}"
    timeout = CONFIG['timeout']
    # ... fetch data ...

async def main():
    # Multiple tasks reading CONFIG - no problem
    tasks = [fetch_data(f"endpoint-{i}") for i in range(100)]
    await asyncio.gather(*tasks)
```

**Safe because:** No task modifies CONFIG.

### Case 2: No Suspension Points

```python
import asyncio

counter = 0

async def increment_atomic():
    global counter
    # No await between read and write
    counter += 1  # ✅ Safe

async def main():
    tasks = [increment_atomic() for _ in range(1000)]
    await asyncio.gather(*tasks)
    print(f"Counter: {counter}")  # Always 1000

asyncio.run(main())
```

**Safe because:** No suspension point between operations.

### Case 3: Task-Local State

```python
import asyncio

async def worker(worker_id):
    # Each task has its own local state
    local_counter = 0
    
    for i in range(100):
        local_counter += 1
        await asyncio.sleep(0.001)
    
    return local_counter

async def main():
    results = await asyncio.gather(*[worker(i) for i in range(10)])
    print(f"Results: {results}")  # Each is 100

asyncio.run(main())
```

**Safe because:** Each task has independent state.

---

## When You DO Need Synchronization

### Case 1: Read-Modify-Write with Await

```python
import asyncio

balance = 1000

async def transfer_unsafe(amount):
    global balance
    
    # Read
    current = balance
    
    # Async operation (suspension point)
    await asyncio.sleep(0.1)
    
    # Write
    balance = current - amount  # ❌ Race condition

# Solution: Use Lock (covered in next chapter)
lock = asyncio.Lock()

async def transfer_safe(amount):
    global balance
    
    async with lock:
        current = balance
        await asyncio.sleep(0.1)
        balance = current - amount  # ✅ Protected
```

### Case 2: Check-Then-Act Pattern

```python
import asyncio

cache = {}

async def get_or_compute_unsafe(key):
    # Check
    if key not in cache:
        # Compute (suspension point)
        value = await expensive_computation(key)
        # Act
        cache[key] = value  # ❌ Multiple tasks might compute same key
    
    return cache[key]

# Solution: Use Lock
lock = asyncio.Lock()

async def get_or_compute_safe(key):
    async with lock:
        if key not in cache:
            value = await expensive_computation(key)
            cache[key] = value
        return cache[key]  # ✅ Protected
```

### Case 3: Coordinated State Changes

```python
import asyncio

class BankAccount:
    def __init__(self, balance):
        self.balance = balance
        self.lock = asyncio.Lock()
    
    async def transfer_to(self, other, amount):
        # Need to lock BOTH accounts
        async with self.lock:
            async with other.lock:
                if self.balance >= amount:
                    await asyncio.sleep(0.1)  # Simulate processing
                    self.balance -= amount
                    other.balance += amount
                    return True
        return False
```

---

## Real-world Example: Cache with Race Conditions

### Buggy Implementation

```python
import asyncio
from typing import Dict, Any

class BuggyCache:
    def __init__(self):
        self.cache: Dict[str, Any] = {}
    
    async def get(self, key: str):
        # Check if in cache
        if key in self.cache:
            return self.cache[key]
        
        # Not in cache, compute
        print(f"Computing {key}...")
        value = await self._expensive_computation(key)
        
        # Store in cache
        self.cache[key] = value
        return value
    
    async def _expensive_computation(self, key: str):
        await asyncio.sleep(1.0)  # Simulate expensive operation
        return f"value-for-{key}"

async def test_buggy_cache():
    cache = BuggyCache()
    
    # Multiple tasks request same key simultaneously
    results = await asyncio.gather(*[
        cache.get("expensive-key")
        for _ in range(5)
    ])
    
    print(f"Results: {results}")

asyncio.run(test_buggy_cache())

# Output:
# Computing expensive-key...
# Computing expensive-key...
# Computing expensive-key...
# Computing expensive-key...
# Computing expensive-key...
# Results: ['value-for-expensive-key', ...]
```

**Problem:** All 5 tasks see cache miss and compute the same value!

### Fixed Implementation

```python
import asyncio
from typing import Dict, Any

class SafeCache:
    def __init__(self):
        self.cache: Dict[str, Any] = {}
        self.locks: Dict[str, asyncio.Lock] = {}
        self.main_lock = asyncio.Lock()
    
    async def get(self, key: str):
        # Get or create lock for this key
        async with self.main_lock:
            if key not in self.locks:
                self.locks[key] = asyncio.Lock()
            key_lock = self.locks[key]
        
        # Lock for this specific key
        async with key_lock:
            # Double-check cache (might have been filled while waiting)
            if key in self.cache:
                return self.cache[key]
            
            # Compute and cache
            print(f"Computing {key}...")
            value = await self._expensive_computation(key)
            self.cache[key] = value
            return value
    
    async def _expensive_computation(self, key: str):
        await asyncio.sleep(1.0)
        return f"value-for-{key}"

async def test_safe_cache():
    cache = SafeCache()
    
    results = await asyncio.gather(*[
        cache.get("expensive-key")
        for _ in range(5)
    ])
    
    print(f"Results: {results}")

asyncio.run(test_safe_cache())

# Output:
# Computing expensive-key...
# Results: ['value-for-expensive-key', ...]
# (Only computed once!)
```

---

## Coordination Problems

Sometimes you need tasks to coordinate their actions.

### Problem: Producer-Consumer Without Coordination

```python
import asyncio

buffer = []
MAX_SIZE = 5

async def producer():
    for i in range(10):
        # BUG: No coordination with consumer
        if len(buffer) < MAX_SIZE:
            buffer.append(i)
            print(f"Produced: {i}")
        await asyncio.sleep(0.1)

async def consumer():
    while True:
        # BUG: No coordination with producer
        if buffer:
            item = buffer.pop(0)
            print(f"Consumed: {item}")
        await asyncio.sleep(0.2)

# This has race conditions and coordination issues!
```

**Problems:**
1. Check-then-act race conditions
2. No signaling between producer and consumer
3. Consumer might miss items

**Solution:** Use `asyncio.Queue` (covered in Chapter 7) or synchronization primitives (next chapters).

---

## Detecting Race Conditions

### Technique 1: Add Delays

```python
import asyncio

async def test_with_delays():
    # Add random delays to expose race conditions
    import random
    
    counter = 0
    
    async def increment():
        nonlocal counter
        temp = counter
        await asyncio.sleep(random.uniform(0, 0.01))  # Random delay
        counter = temp + 1
    
    tasks = [increment() for _ in range(100)]
    await asyncio.gather(*tasks)
    
    print(f"Counter: {counter}")
    # If < 100, you have a race condition

asyncio.run(test_with_delays())
```

### Technique 2: Increase Concurrency

```python
# Run with high concurrency to expose races
tasks = [risky_operation() for _ in range(10000)]
await asyncio.gather(*tasks)
```

### Technique 3: Assertions

```python
async def withdraw(amount):
    async with lock:
        assert balance >= 0, "Balance should never be negative"
        # ... withdrawal logic ...
        assert balance >= 0, "Balance corrupted!"
```

---

## Common Patterns and Solutions

### Pattern 1: Lazy Initialization

```python
import asyncio

class LazyResource:
    def __init__(self):
        self._resource = None
        self._lock = asyncio.Lock()
    
    async def get_resource(self):
        if self._resource is None:
            async with self._lock:
                # Double-check pattern
                if self._resource is None:
                    self._resource = await self._initialize()
        return self._resource
    
    async def _initialize(self):
        await asyncio.sleep(1.0)  # Expensive initialization
        return "initialized resource"
```

### Pattern 2: Reference Counting

```python
import asyncio

class ResourcePool:
    def __init__(self):
        self.active_count = 0
        self.lock = asyncio.Lock()
    
    async def acquire(self):
        async with self.lock:
            self.active_count += 1
            print(f"Acquired, active: {self.active_count}")
    
    async def release(self):
        async with self.lock:
            self.active_count -= 1
            print(f"Released, active: {self.active_count}")
```

### Pattern 3: State Machine

```python
import asyncio
from enum import Enum

class State(Enum):
    IDLE = 1
    PROCESSING = 2
    DONE = 3

class StateMachine:
    def __init__(self):
        self.state = State.IDLE
        self.lock = asyncio.Lock()
    
    async def transition(self, new_state):
        async with self.lock:
            print(f"Transition: {self.state} -> {new_state}")
            self.state = new_state
    
    async def process(self):
        async with self.lock:
            if self.state != State.IDLE:
                raise ValueError("Already processing")
            self.state = State.PROCESSING
        
        # Do work without holding lock
        await asyncio.sleep(1.0)
        
        async with self.lock:
            self.state = State.DONE
```

---

## Best Practices

### ✅ DO: Minimize shared mutable state

```python
# Good: Pass data through function arguments
async def process(data):
    result = await transform(data)
    return result

# Better than: Modifying global state
global_data = None
async def process_global():
    global global_data
    global_data = await transform(global_data)
```

### ✅ DO: Use immutable data structures

```python
from typing import NamedTuple

class Config(NamedTuple):
    url: str
    timeout: int

# Immutable - safe to share
config = Config(url="https://api.example.com", timeout=30)
```

### ✅ DO: Document synchronization requirements

```python
class SharedResource:
    """
    Thread-safe and coroutine-safe resource.
    
    All methods are protected by an internal lock.
    """
    def __init__(self):
        self._lock = asyncio.Lock()
        self._data = {}
    
    async def update(self, key, value):
        """Update data (synchronized)"""
        async with self._lock:
            self._data[key] = value
```

### ❌ DON'T: Assume operations are atomic

```python
# BAD: Assuming this is atomic
if key in cache:
    await process()
    value = cache[key]  # ❌ Key might be deleted

# GOOD: Protect the entire sequence
async with lock:
    if key in cache:
        await process()
        value = cache[key]  # ✅ Protected
```

### ❌ DON'T: Hold locks during I/O

```python
# BAD: Holding lock during I/O
async with lock:
    data = await fetch_from_network()  # ❌ Blocks other tasks

# GOOD: Minimize lock duration
data = await fetch_from_network()
async with lock:
    cache[key] = data  # ✅ Only lock for state update
```

---

## Summary: Race Condition Mental Model

✅ **Async is single-threaded** but still has race conditions

✅ **Race conditions occur** when tasks share mutable state with suspension points

✅ **Atomic operations** are safe without locks (if no await)

✅ **Read-modify-write** with await needs synchronization

✅ **Check-then-act** patterns need synchronization

✅ **Minimize shared state** - prefer passing data

✅ **Use immutable data** when possible

✅ **Document synchronization** requirements clearly

---

## What's Next?

Now that you understand when race conditions occur, we'll learn how to prevent them.

In [Chapter 9: Lock - Mutual Exclusion](../part4-synchronization/09-lock.md), we'll cover:
- What mutual exclusion means
- How `asyncio.Lock` works internally
- Fairness guarantees
- Deadlock scenarios
- Lock granularity strategies
- Practical locking patterns

Locks are the fundamental synchronization primitive for protecting shared state.

---

**Previous:** [← Chapter 7: Queues](./07-queues.md)  
**Next:** [Chapter 9: Lock - Mutual Exclusion →](../part4-synchronization/09-lock.md)