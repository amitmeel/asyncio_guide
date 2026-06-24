# Chapter 14: Cancellation Safety

## Introduction

Writing cancellation-safe code is one of the most challenging aspects of async programming. When a task is cancelled, you must ensure resources are released, locks are freed, and state remains consistent. This chapter teaches you how to write robust code that handles cancellation correctly.

By the end of this chapter, you'll understand:
- Cleanup patterns with `finally`
- Resource release during cancellation
- Lock release safety
- Partial state recovery
- Atomic operations under cancellation
- Context managers for safety
- Testing cancellation safety

---

## The Cancellation Safety Problem

When cancellation occurs, your code might be interrupted at any `await` point, potentially leaving resources in an inconsistent state.

### The Problem

```python
import asyncio

async def unsafe_operation():
    """Unsafe: Resources may leak on cancellation"""
    
    # Acquire resource
    file = await open_file("data.txt")
    
    # Process (cancellation could happen here!)
    await process_data(file)
    
    # Release resource (might never execute!)
    await close_file(file)

# If cancelled during process_data(), file never closes!
```

### The Solution

```python
import asyncio

async def safe_operation():
    """Safe: Resources always released"""
    
    file = await open_file("data.txt")
    try:
        await process_data(file)
    finally:
        # Always executes, even on cancellation
        await close_file(file)
```

---

## Cleanup with finally

`finally` blocks execute even when `CancelledError` is raised.

### Basic finally Pattern

```python
import asyncio

async def finally_demo():
    async def worker():
        print("1. Starting")
        try:
            print("2. Working")
            await asyncio.sleep(10.0)
            print("3. Done (never reached)")
        except asyncio.CancelledError:
            print("4. Cancelled!")
            raise
        finally:
            print("5. Cleanup (always runs)")
    
    task = asyncio.create_task(worker())
    await asyncio.sleep(0.1)
    
    task.cancel()
    
    try:
        await task
    except asyncio.CancelledError:
        print("6. Task cancelled")

asyncio.run(finally_demo())

# Output:
# 1. Starting
# 2. Working
# 4. Cancelled!
# 5. Cleanup (always runs)
# 6. Task cancelled
```

**Key insight:** `finally` runs before `CancelledError` propagates.

### Multiple Resources

```python
import asyncio

async def multiple_resources():
    """Managing multiple resources safely"""
    
    resource1 = None
    resource2 = None
    resource3 = None
    
    try:
        resource1 = await acquire_resource1()
        resource2 = await acquire_resource2()
        resource3 = await acquire_resource3()
        
        await do_work(resource1, resource2, resource3)
        
    finally:
        # Release in reverse order
        if resource3:
            await release_resource3(resource3)
        if resource2:
            await release_resource2(resource2)
        if resource1:
            await release_resource1(resource1)
```

---

## Context Managers for Safety

Context managers (`async with`) guarantee cleanup.

### Basic Context Manager

```python
import asyncio

class Resource:
    """Resource with automatic cleanup"""
    
    def __init__(self, name):
        self.name = name
        self.acquired = False
    
    async def __aenter__(self):
        print(f"{self.name}: Acquiring")
        await asyncio.sleep(0.1)
        self.acquired = True
        print(f"{self.name}: Acquired")
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        print(f"{self.name}: Releasing")
        await asyncio.sleep(0.1)
        self.acquired = False
        print(f"{self.name}: Released")
        return False  # Don't suppress exceptions

async def context_manager_demo():
    async def worker():
        async with Resource("DB Connection") as conn:
            print("Working with resource")
            await asyncio.sleep(10.0)
            print("Done (never reached)")
    
    task = asyncio.create_task(worker())
    await asyncio.sleep(0.5)
    
    task.cancel()
    
    try:
        await task
    except asyncio.CancelledError:
        print("Task cancelled")

asyncio.run(context_manager_demo())

# Output:
# DB Connection: Acquiring
# DB Connection: Acquired
# Working with resource
# DB Connection: Releasing
# DB Connection: Released
# Task cancelled
```

**Key insight:** `__aexit__` always runs, even on cancellation.

### Nested Context Managers

```python
import asyncio

async def nested_context_managers():
    """Multiple context managers"""
    
    async with Resource("Database") as db:
        async with Resource("Cache") as cache:
            async with Resource("Lock") as lock:
                # All resources acquired
                await do_work(db, cache, lock)
                # All resources released in reverse order
```

---

## Lock Release Safety

Locks must be released even on cancellation.

### Unsafe Lock Usage

```python
import asyncio

async def unsafe_lock():
    """❌ BAD: Lock might not be released"""
    lock = asyncio.Lock()
    
    await lock.acquire()
    try:
        await do_work()  # Cancellation here!
    except Exception:
        pass
    
    lock.release()  # Might not execute!
```

### Safe Lock Usage

```python
import asyncio

async def safe_lock():
    """✅ GOOD: Lock always released"""
    lock = asyncio.Lock()
    
    await lock.acquire()
    try:
        await do_work()
    finally:
        lock.release()  # Always executes

# Even better: Use context manager
async def best_lock():
    """✅ BEST: Context manager"""
    lock = asyncio.Lock()
    
    async with lock:
        await do_work()
    # Lock automatically released
```

### Lock with Timeout

```python
import asyncio

async def lock_with_timeout():
    """Lock acquisition with timeout"""
    lock = asyncio.Lock()
    
    try:
        async with asyncio.timeout(5.0):
            async with lock:
                await do_work()
    except asyncio.TimeoutError:
        print("Operation timed out")
        # Lock still released properly
```

---

## Partial State Recovery

When cancelled mid-operation, you may need to recover partial state.

### Transaction Pattern

```python
import asyncio

class Transaction:
    """Transaction with rollback on cancellation"""
    
    def __init__(self):
        self.operations = []
        self.committed = False
    
    async def add_operation(self, op):
        """Add operation to transaction"""
        self.operations.append(op)
        await op.execute()
    
    async def commit(self):
        """Commit transaction"""
        self.committed = True
        print("Transaction committed")
    
    async def rollback(self):
        """Rollback all operations"""
        print("Rolling back transaction")
        for op in reversed(self.operations):
            await op.undo()
        print("Rollback complete")
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is asyncio.CancelledError:
            # Rollback on cancellation
            await self.rollback()
        elif exc_type is not None:
            # Rollback on error
            await self.rollback()
        elif not self.committed:
            # Auto-commit if not explicitly committed
            await self.commit()
        
        return False

class Operation:
    def __init__(self, name):
        self.name = name
    
    async def execute(self):
        print(f"Executing: {self.name}")
        await asyncio.sleep(0.1)
    
    async def undo(self):
        print(f"Undoing: {self.name}")
        await asyncio.sleep(0.1)

async def transaction_demo():
    async def worker():
        async with Transaction() as txn:
            await txn.add_operation(Operation("Create user"))
            await txn.add_operation(Operation("Send email"))
            await txn.add_operation(Operation("Update database"))
            
            # Simulate long operation
            await asyncio.sleep(10.0)
            
            await txn.commit()
    
    task = asyncio.create_task(worker())
    await asyncio.sleep(0.5)
    
    # Cancel during transaction
    task.cancel()
    
    try:
        await task
    except asyncio.CancelledError:
        print("Transaction cancelled and rolled back")

asyncio.run(transaction_demo())

# Output:
# Executing: Create user
# Executing: Send email
# Executing: Update database
# Rolling back transaction
# Undoing: Update database
# Undoing: Send email
# Undoing: Create user
# Rollback complete
# Transaction cancelled and rolled back
```

---

## Atomic Operations

Some operations must complete atomically, even during cancellation.

### Shield Pattern

```python
import asyncio

async def shield_critical_operation():
    """Protect critical operation from cancellation"""
    
    async def critical_save():
        print("Saving critical data...")
        await asyncio.sleep(2.0)
        print("Critical data saved")
    
    async def worker():
        try:
            print("Working...")
            await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            print("Cancellation requested")
            
            # Shield critical operation
            print("Completing critical save...")
            await asyncio.shield(critical_save())
            
            print("Critical save complete, now cancelling")
            raise
    
    task = asyncio.create_task(worker())
    await asyncio.sleep(0.5)
    
    task.cancel()
    
    try:
        await task
    except asyncio.CancelledError:
        print("Task cancelled after critical save")

asyncio.run(shield_critical_operation())

# Output:
# Working...
# Cancellation requested
# Completing critical save...
# Saving critical data...
# Critical data saved
# Critical save complete, now cancelling
# Task cancelled after critical save
```

### Two-Phase Commit

```python
import asyncio

class TwoPhaseCommit:
    """Two-phase commit protocol"""
    
    def __init__(self):
        self.prepared = False
        self.committed = False
    
    async def prepare(self):
        """Phase 1: Prepare (can be cancelled)"""
        print("Phase 1: Preparing...")
        await asyncio.sleep(1.0)
        self.prepared = True
        print("Phase 1: Prepared")
    
    async def commit(self):
        """Phase 2: Commit (must complete atomically)"""
        if not self.prepared:
            raise RuntimeError("Not prepared")
        
        print("Phase 2: Committing (atomic)...")
        await asyncio.sleep(1.0)
        self.committed = True
        print("Phase 2: Committed")
    
    async def execute(self):
        """Execute two-phase commit"""
        try:
            # Phase 1: Can be cancelled
            await self.prepare()
            
            # Phase 2: Must complete atomically
            await asyncio.shield(self.commit())
            
        except asyncio.CancelledError:
            if self.prepared and not self.committed:
                # Prepared but not committed - must rollback
                print("Rolling back prepared transaction")
                await self.rollback()
            raise
    
    async def rollback(self):
        """Rollback prepared transaction"""
        print("Rollback: Undoing prepared changes")
        await asyncio.sleep(0.5)
        self.prepared = False
        print("Rollback: Complete")

async def two_phase_demo():
    async def worker():
        tpc = TwoPhaseCommit()
        await tpc.execute()
    
    task = asyncio.create_task(worker())
    await asyncio.sleep(1.5)  # Cancel after prepare, before commit
    
    task.cancel()
    
    try:
        await task
    except asyncio.CancelledError:
        print("Transaction cancelled")

asyncio.run(two_phase_demo())
```

---

## Real-world: Database Connection Pool

Complete connection pool with cancellation safety.

```python
import asyncio
from typing import Optional
from contextlib import asynccontextmanager

class Connection:
    """Database connection"""
    
    def __init__(self, conn_id: int):
        self.conn_id = conn_id
        self.in_use = False
        self.transaction_active = False
    
    async def begin_transaction(self):
        """Begin transaction"""
        print(f"Conn {self.conn_id}: BEGIN")
        await asyncio.sleep(0.1)
        self.transaction_active = True
    
    async def commit(self):
        """Commit transaction"""
        print(f"Conn {self.conn_id}: COMMIT")
        await asyncio.sleep(0.1)
        self.transaction_active = False
    
    async def rollback(self):
        """Rollback transaction"""
        print(f"Conn {self.conn_id}: ROLLBACK")
        await asyncio.sleep(0.1)
        self.transaction_active = False
    
    async def execute(self, query: str):
        """Execute query"""
        print(f"Conn {self.conn_id}: {query}")
        await asyncio.sleep(0.2)
        return f"Result from conn {self.conn_id}"

class ConnectionPool:
    """
    Connection pool with cancellation safety.
    
    Ensures:
    - Connections always returned to pool
    - Transactions rolled back on cancellation
    - No connection leaks
    """
    
    def __init__(self, size: int):
        self.size = size
        self.connections = [Connection(i) for i in range(size)]
        self.available = asyncio.Queue()
        self.semaphore = asyncio.Semaphore(size)
        
        # Initialize available connections
        for conn in self.connections:
            self.available.put_nowait(conn)
    
    @asynccontextmanager
    async def acquire(self):
        """
        Acquire connection from pool.
        
        Guarantees connection is returned even on cancellation.
        """
        # Wait for available connection
        await self.semaphore.acquire()
        
        conn = await self.available.get()
        conn.in_use = True
        
        print(f"Acquired connection {conn.conn_id}")
        
        try:
            yield conn
        finally:
            # Always return connection
            print(f"Releasing connection {conn.conn_id}")
            
            # Rollback any active transaction
            if conn.transaction_active:
                print(f"Rolling back active transaction on conn {conn.conn_id}")
                await conn.rollback()
            
            conn.in_use = False
            await self.available.put(conn)
            self.semaphore.release()
    
    @asynccontextmanager
    async def transaction(self):
        """
        Execute transaction with automatic rollback on cancellation.
        """
        async with self.acquire() as conn:
            await conn.begin_transaction()
            
            try:
                yield conn
                # Commit if no exception
                await conn.commit()
            except asyncio.CancelledError:
                # Rollback on cancellation
                print(f"Transaction cancelled, rolling back")
                await conn.rollback()
                raise
            except Exception:
                # Rollback on error
                print(f"Transaction error, rolling back")
                await conn.rollback()
                raise

async def connection_pool_demo():
    """Demonstrate cancellation-safe connection pool"""
    
    pool = ConnectionPool(size=3)
    
    async def worker(worker_id: int):
        """Worker that uses connection pool"""
        try:
            async with pool.transaction() as conn:
                await conn.execute(f"INSERT INTO users VALUES ({worker_id})")
                await conn.execute(f"UPDATE stats SET count = count + 1")
                
                # Simulate long operation
                await asyncio.sleep(10.0)
                
                await conn.execute(f"INSERT INTO logs VALUES ({worker_id})")
        
        except asyncio.CancelledError:
            print(f"Worker {worker_id}: Cancelled")
            raise
    
    # Start workers
    tasks = [
        asyncio.create_task(worker(i))
        for i in range(5)
    ]
    
    # Let them start
    await asyncio.sleep(0.5)
    
    # Cancel all workers
    print("\n=== Cancelling all workers ===\n")
    for task in tasks:
        task.cancel()
    
    # Wait for cancellation
    await asyncio.gather(*tasks, return_exceptions=True)
    
    print("\n=== All workers cancelled, connections returned ===")

asyncio.run(connection_pool_demo())
```

---

## Testing Cancellation Safety

How to test that your code handles cancellation correctly.

### Test Pattern 1: Cancel at Different Points

```python
import asyncio
import pytest

async def operation_with_steps():
    """Operation with multiple steps"""
    await step1()
    await step2()
    await step3()

async def step1():
    await asyncio.sleep(0.1)

async def step2():
    await asyncio.sleep(0.1)

async def step3():
    await asyncio.sleep(0.1)

@pytest.mark.asyncio
async def test_cancel_at_step1():
    """Test cancellation during step 1"""
    task = asyncio.create_task(operation_with_steps())
    await asyncio.sleep(0.05)  # Cancel during step1
    task.cancel()
    
    with pytest.raises(asyncio.CancelledError):
        await task

@pytest.mark.asyncio
async def test_cancel_at_step2():
    """Test cancellation during step 2"""
    task = asyncio.create_task(operation_with_steps())
    await asyncio.sleep(0.15)  # Cancel during step2
    task.cancel()
    
    with pytest.raises(asyncio.CancelledError):
        await task

@pytest.mark.asyncio
async def test_cancel_at_step3():
    """Test cancellation during step 3"""
    task = asyncio.create_task(operation_with_steps())
    await asyncio.sleep(0.25)  # Cancel during step3
    task.cancel()
    
    with pytest.raises(asyncio.CancelledError):
        await task
```

### Test Pattern 2: Verify Cleanup

```python
import asyncio
import pytest

class ResourceTracker:
    """Track resource acquisition/release"""
    def __init__(self):
        self.acquired = 0
        self.released = 0
    
    async def acquire(self):
        self.acquired += 1
        await asyncio.sleep(0.1)
    
    async def release(self):
        self.released += 1
        await asyncio.sleep(0.1)
    
    def is_balanced(self):
        return self.acquired == self.released

@pytest.mark.asyncio
async def test_resources_released_on_cancel():
    """Test that resources are released on cancellation"""
    tracker = ResourceTracker()
    
    async def operation():
        await tracker.acquire()
        try:
            await asyncio.sleep(10.0)
        finally:
            await tracker.release()
    
    task = asyncio.create_task(operation())
    await asyncio.sleep(0.2)
    
    task.cancel()
    
    try:
        await task
    except asyncio.CancelledError:
        pass
    
    # Verify resources released
    assert tracker.is_balanced()
```

### Test Pattern 3: Stress Test

```python
import asyncio
import pytest

@pytest.mark.asyncio
async def test_cancellation_stress():
    """Stress test cancellation safety"""
    
    async def operation(op_id):
        try:
            for i in range(100):
                await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            # Verify cleanup
            raise
    
    # Start many tasks
    tasks = [
        asyncio.create_task(operation(i))
        for i in range(100)
    ]
    
    # Cancel at random times
    await asyncio.sleep(0.5)
    
    for task in tasks:
        task.cancel()
    
    # All should cancel cleanly
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    cancelled_count = sum(
        1 for r in results
        if isinstance(r, asyncio.CancelledError)
    )
    
    assert cancelled_count == 100
```

---

## Common Pitfalls

### Pitfall 1: Forgetting finally

```python
# ❌ BAD
async def bad():
    resource = await acquire()
    await use(resource)
    await release(resource)  # Might not execute!

# ✅ GOOD
async def good():
    resource = await acquire()
    try:
        await use(resource)
    finally:
        await release(resource)  # Always executes
```

### Pitfall 2: Cleanup in except

```python
# ❌ BAD
async def bad():
    resource = await acquire()
    try:
        await use(resource)
    except asyncio.CancelledError:
        await release(resource)  # Only on cancellation
        raise

# ✅ GOOD
async def good():
    resource = await acquire()
    try:
        await use(resource)
    finally:
        await release(resource)  # On all exits
```

### Pitfall 3: Not Using Context Managers

```python
# ❌ BAD
async def bad():
    lock = asyncio.Lock()
    await lock.acquire()
    try:
        await work()
    finally:
        lock.release()

# ✅ GOOD
async def good():
    lock = asyncio.Lock()
    async with lock:
        await work()
```

---

## Best Practices

### ✅ DO: Use finally for cleanup

```python
try:
    await operation()
finally:
    await cleanup()
```

### ✅ DO: Use context managers

```python
async with resource:
    await use(resource)
```

### ✅ DO: Shield critical operations

```python
try:
    await work()
except asyncio.CancelledError:
    await asyncio.shield(critical_save())
    raise
```

### ✅ DO: Test cancellation paths

```python
@pytest.mark.asyncio
async def test_cancel():
    task = asyncio.create_task(operation())
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
```

### ❌ DON'T: Assume operations complete

```python
# Bad: Assumes both execute
await operation1()
await operation2()  # Might not execute if cancelled

# Good: Ensure cleanup
try:
    await operation1()
    await operation2()
finally:
    await cleanup()
```

---

## Summary: Cancellation Safety Mental Model

✅ **Use finally for cleanup** - guaranteed execution

✅ **Context managers are safer** - automatic cleanup

✅ **Shield critical operations** - when they must complete

✅ **Test cancellation paths** - verify cleanup works

✅ **Rollback transactions** - on cancellation

✅ **Return resources to pools** - prevent leaks

✅ **Assume cancellation anywhere** - at any await point

---

## What's Next?

Cancellation safety often involves timeouts. Understanding timeout mechanisms is crucial for robust async code.

In [Chapter 15: Timeouts](./15-timeouts.md), we'll cover:
- `asyncio.timeout()` (Python 3.11+)
- `wait_for()` for older Python
- Nested timeouts
- Timeout propagation
- Timeout vs cancellation
- Practical timeout patterns

Timeouts are essential for preventing hung operations.

---

**Previous:** [← Chapter 13: Cancellation Model](./13-cancellation-model.md)  
**Next:** [Chapter 15: Timeouts →](./15-timeouts.md)