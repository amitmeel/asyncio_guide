# Chapter 23: Debugging Async Code

## Overview

Debugging asynchronous code is fundamentally different from debugging synchronous code. The non-linear execution flow, concurrent tasks, and event loop mechanics create unique challenges:

- **Task leaks**: Tasks that never complete
- **Pending tasks at shutdown**: Unfinished work
- **Deadlocks**: Tasks waiting on each other
- **Race conditions**: Timing-dependent bugs
- **Stack traces**: Incomplete or confusing
- **Event loop blocking**: Synchronous code blocking async execution

This chapter provides systematic approaches to identify, diagnose, and fix these issues.

## Mental Model

Think of debugging async code like **debugging a distributed system**:

```
┌─────────────────────────────────────────────────────────┐
│                    Event Loop                            │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐             │
│  │ Task 1   │  │ Task 2   │  │ Task 3   │  ...        │
│  │ (running)│  │(waiting) │  │(pending) │             │
│  └──────────┘  └──────────┘  └──────────┘             │
│                                                          │
│  Ready Queue: [Task 4, Task 5]                          │
│  Waiting: {Task 2 → socket, Task 6 → timeout}          │
└─────────────────────────────────────────────────────────┘
```

**Key debugging challenges:**

1. **Non-determinism**: Execution order varies
2. **Concurrency**: Multiple things happening "at once"
3. **Suspension points**: Execution pauses at `await`
4. **Hidden state**: Event loop internals
5. **Timing issues**: Race conditions, deadlocks

## Debug Mode

Python's asyncio has a built-in debug mode that catches common issues:

```python
import asyncio
import warnings

# Enable debug mode
asyncio.run(main(), debug=True)

# Or set environment variable
# PYTHONASYNCIODEBUG=1 python script.py

# Or programmatically
loop = asyncio.get_event_loop()
loop.set_debug(True)
```

**What debug mode catches:**

1. **Coroutines that were never awaited**
2. **Callbacks taking too long** (>100ms by default)
3. **Tasks destroyed while pending**
4. **Exceptions in callbacks**

### Example: Catching Unawaited Coroutines

```python
import asyncio

async def fetch_data():
    await asyncio.sleep(0.1)
    return "data"

async def buggy_code():
    # BUG: Created coroutine but never awaited it
    fetch_data()  # Missing await!
    
    # Correct version:
    # result = await fetch_data()

async def main():
    await buggy_code()

# With debug=True, you'll see:
# RuntimeWarning: coroutine 'fetch_data' was never awaited
asyncio.run(main(), debug=True)
```

## Task Inspection

### 1. Finding All Tasks

```python
import asyncio
from typing import Set

def get_all_tasks() -> Set[asyncio.Task]:
    """Get all currently running tasks"""
    return asyncio.all_tasks()

def get_current_task() -> asyncio.Task:
    """Get the currently executing task"""
    return asyncio.current_task()

async def inspect_tasks():
    """Inspect all running tasks"""
    current = asyncio.current_task()
    all_tasks = asyncio.all_tasks()
    
    print(f"Current task: {current.get_name()}")
    print(f"Total tasks: {len(all_tasks)}")
    
    for task in all_tasks:
        print(f"  - {task.get_name()}: {task._state}")
        print(f"    Coro: {task.get_coro()}")
        if not task.done():
            print(f"    Stack: {task.get_stack()}")
```

### 2. Task Naming for Debugging

```python
async def worker(worker_id: int):
    """Worker task"""
    await asyncio.sleep(1)
    return f"Worker {worker_id} done"

async def main():
    # Name tasks for easier debugging
    tasks = [
        asyncio.create_task(worker(i), name=f"worker-{i}")
        for i in range(5)
    ]
    
    # Now tasks have meaningful names
    for task in asyncio.all_tasks():
        print(f"Task: {task.get_name()}")
    
    await asyncio.gather(*tasks)

asyncio.run(main())
```

### 3. Task State Inspection

```python
import asyncio
from enum import Enum

class TaskState(Enum):
    """Task states for debugging"""
    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    CANCELLED = "cancelled"

def get_task_state(task: asyncio.Task) -> TaskState:
    """Get human-readable task state"""
    if task.cancelled():
        return TaskState.CANCELLED
    elif task.done():
        return TaskState.DONE
    else:
        # Check if it's the current task
        if task == asyncio.current_task():
            return TaskState.RUNNING
        return TaskState.PENDING

async def debug_task_states():
    """Debug helper to show all task states"""
    tasks = asyncio.all_tasks()
    
    print("\n=== Task States ===")
    for task in tasks:
        state = get_task_state(task)
        print(f"{task.get_name():20} {state.value:10}")
        
        # Show what it's waiting for
        if state == TaskState.PENDING:
            coro = task.get_coro()
            print(f"  Waiting in: {coro.cr_code.co_name}")
            print(f"  At line: {coro.cr_frame.f_lineno if coro.cr_frame else 'N/A'}")
```

## Detecting Task Leaks

Task leaks occur when tasks are created but never complete or get cancelled:

```python
import asyncio
import weakref
from typing import Set, Dict
import time

class TaskTracker:
    """
    Track task creation and completion to detect leaks.
    
    Usage:
        tracker = TaskTracker()
        tracker.start()
        # ... run your code ...
        tracker.report()
    """
    
    def __init__(self):
        self.created_tasks: Dict[int, dict] = {}
        self.completed_tasks: Set[int] = set()
        self.tracking = False
    
    def start(self):
        """Start tracking tasks"""
        self.tracking = True
        self._original_create_task = asyncio.create_task
        asyncio.create_task = self._tracked_create_task
    
    def stop(self):
        """Stop tracking tasks"""
        self.tracking = False
        asyncio.create_task = self._original_create_task
    
    def _tracked_create_task(self, coro, *, name=None):
        """Wrapper around create_task that tracks creation"""
        task = self._original_create_task(coro, name=name)
        
        task_id = id(task)
        self.created_tasks[task_id] = {
            'name': task.get_name(),
            'coro': task.get_coro().__name__,
            'created_at': time.time(),
            'task': weakref.ref(task)
        }
        
        # Add callback to track completion
        task.add_done_callback(lambda t: self.completed_tasks.add(id(t)))
        
        return task
    
    def get_leaked_tasks(self) -> list:
        """Get tasks that were created but never completed"""
        leaked = []
        
        for task_id, info in self.created_tasks.items():
            if task_id not in self.completed_tasks:
                task_ref = info['task']
                task = task_ref()
                
                if task is not None and not task.done():
                    leaked.append({
                        'name': info['name'],
                        'coro': info['coro'],
                        'age': time.time() - info['created_at'],
                        'task': task
                    })
        
        return leaked
    
    def report(self):
        """Print leak report"""
        leaked = self.get_leaked_tasks()
        
        print("\n=== Task Leak Report ===")
        print(f"Created: {len(self.created_tasks)}")
        print(f"Completed: {len(self.completed_tasks)}")
        print(f"Leaked: {len(leaked)}")
        
        if leaked:
            print("\nLeaked tasks:")
            for info in leaked:
                print(f"  - {info['name']} ({info['coro']})")
                print(f"    Age: {info['age']:.2f}s")
                
                # Show stack trace
                task = info['task']
                if task and not task.done():
                    stack = task.get_stack()
                    if stack:
                        print(f"    Stack: {stack[-1].f_code.co_filename}:{stack[-1].f_lineno}")

# Example usage
async def leaky_function():
    """Function that leaks tasks"""
    # BUG: Create task but don't await or store reference
    asyncio.create_task(asyncio.sleep(10), name="leaked-task")
    
    # Correct version:
    # task = asyncio.create_task(asyncio.sleep(10))
    # await task

async def main():
    tracker = TaskTracker()
    tracker.start()
    
    await leaky_function()
    await asyncio.sleep(0.1)
    
    tracker.stop()
    tracker.report()

asyncio.run(main())
```

## Stack Trace Inspection

### 1. Getting Task Stack Traces

```python
import asyncio
import traceback
from typing import List

def print_task_stack(task: asyncio.Task):
    """Print stack trace for a task"""
    print(f"\n=== Stack for {task.get_name()} ===")
    
    if task.done():
        print("Task is done")
        if task.cancelled():
            print("Task was cancelled")
        elif task.exception():
            print(f"Task raised: {task.exception()}")
            traceback.print_exception(
                type(task.exception()),
                task.exception(),
                task.exception().__traceback__
            )
        return
    
    # Get stack frames
    stack = task.get_stack()
    if not stack:
        print("No stack available")
        return
    
    # Print each frame
    for frame in stack:
        filename = frame.f_code.co_filename
        lineno = frame.f_lineno
        func_name = frame.f_code.co_name
        print(f"  File \"{filename}\", line {lineno}, in {func_name}")
        
        # Show local variables
        if frame.f_locals:
            print(f"    Locals: {list(frame.f_locals.keys())}")

async def print_all_stacks():
    """Print stacks for all tasks"""
    for task in asyncio.all_tasks():
        print_task_stack(task)
```

### 2. Deadlock Detection

```python
import asyncio
from typing import Dict, Set, Optional
import time

class DeadlockDetector:
    """
    Detect potential deadlocks by analyzing task dependencies.
    
    A deadlock occurs when tasks are waiting on each other in a cycle.
    """
    
    def __init__(self):
        self.waiting_for: Dict[asyncio.Task, Set[asyncio.Task]] = {}
        self.last_check = time.time()
    
    def record_wait(self, waiter: asyncio.Task, waited_on: asyncio.Task):
        """Record that one task is waiting for another"""
        if waiter not in self.waiting_for:
            self.waiting_for[waiter] = set()
        self.waiting_for[waiter].add(waited_on)
    
    def clear_wait(self, waiter: asyncio.Task):
        """Clear waiting record for a task"""
        self.waiting_for.pop(waiter, None)
    
    def find_cycle(self, start: asyncio.Task, visited: Optional[Set] = None) -> Optional[List]:
        """Find cycle in wait graph using DFS"""
        if visited is None:
            visited = set()
        
        if start in visited:
            return [start]  # Found cycle
        
        visited.add(start)
        
        for next_task in self.waiting_for.get(start, []):
            cycle = self.find_cycle(next_task, visited.copy())
            if cycle:
                return [start] + cycle
        
        return None
    
    def detect_deadlocks(self) -> List[List[asyncio.Task]]:
        """Detect all deadlock cycles"""
        deadlocks = []
        checked = set()
        
        for task in self.waiting_for:
            if task not in checked:
                cycle = self.find_cycle(task)
                if cycle:
                    # Normalize cycle (start from smallest id)
                    min_idx = cycle.index(min(cycle, key=id))
                    normalized = cycle[min_idx:] + cycle[:min_idx]
                    
                    # Check if we've seen this cycle
                    if normalized not in deadlocks:
                        deadlocks.append(normalized)
                        checked.update(cycle)
        
        return deadlocks
    
    def report(self):
        """Print deadlock report"""
        deadlocks = self.detect_deadlocks()
        
        print("\n=== Deadlock Detection ===")
        if not deadlocks:
            print("No deadlocks detected")
            return
        
        print(f"Found {len(deadlocks)} deadlock(s):")
        for i, cycle in enumerate(deadlocks, 1):
            print(f"\nDeadlock {i}:")
            for task in cycle:
                print(f"  {task.get_name()} waiting for...")
            print(f"  (cycle back to {cycle[0].get_name()})")

# Example: Detecting a deadlock
async def task_a(detector, event_a, event_b):
    """Task A waits for B"""
    print("Task A: waiting for event B")
    detector.record_wait(asyncio.current_task(), asyncio.current_task())
    await event_b.wait()
    event_a.set()

async def task_b(detector, event_a, event_b):
    """Task B waits for A - creates deadlock!"""
    print("Task B: waiting for event A")
    detector.record_wait(asyncio.current_task(), asyncio.current_task())
    await event_a.wait()
    event_b.set()

async def deadlock_example():
    detector = DeadlockDetector()
    
    event_a = asyncio.Event()
    event_b = asyncio.Event()
    
    # Create tasks that wait on each other
    t_a = asyncio.create_task(task_a(detector, event_a, event_b), name="TaskA")
    t_b = asyncio.create_task(task_b(detector, event_a, event_b), name="TaskB")
    
    await asyncio.sleep(0.1)
    
    # Check for deadlocks
    detector.report()
    
    # Cleanup
    t_a.cancel()
    t_b.cancel()
    await asyncio.gather(t_a, t_b, return_exceptions=True)
```

## Logging and Tracing

### 1. Structured Logging for Async

```python
import asyncio
import logging
from contextvars import ContextVar
import uuid

# Context variable for request ID
request_id: ContextVar[str] = ContextVar('request_id', default='no-request')

class AsyncLogger:
    """Logger that includes async context"""
    
    def __init__(self, name: str):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)
        
        # Add handler with custom format
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '[%(asctime)s] [%(request_id)s] [%(task_name)s] %(levelname)s: %(message)s'
        )
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
    
    def _get_extra(self):
        """Get extra context for logging"""
        task = asyncio.current_task()
        return {
            'request_id': request_id.get(),
            'task_name': task.get_name() if task else 'no-task'
        }
    
    def debug(self, msg, *args, **kwargs):
        self.logger.debug(msg, *args, extra=self._get_extra(), **kwargs)
    
    def info(self, msg, *args, **kwargs):
        self.logger.info(msg, *args, extra=self._get_extra(), **kwargs)
    
    def warning(self, msg, *args, **kwargs):
        self.logger.warning(msg, *args, extra=self._get_extra(), **kwargs)
    
    def error(self, msg, *args, **kwargs):
        self.logger.error(msg, *args, extra=self._get_extra(), **kwargs)

# Example usage
logger = AsyncLogger(__name__)

async def process_request(data):
    """Process request with logging"""
    # Set request ID for this context
    req_id = str(uuid.uuid4())[:8]
    request_id.set(req_id)
    
    logger.info(f"Processing request: {data}")
    await asyncio.sleep(0.1)
    logger.info("Request complete")

async def main():
    await asyncio.gather(
        process_request("data1"),
        process_request("data2"),
        process_request("data3")
    )
```

### 2. Task Execution Tracer

```python
import asyncio
import time
from typing import Dict, List
from dataclasses import dataclass

@dataclass
class TaskEvent:
    """Event in task execution"""
    task_name: str
    event_type: str  # 'created', 'started', 'suspended', 'resumed', 'completed'
    timestamp: float
    location: str = ""

class TaskTracer:
    """
    Trace task execution for debugging.
    Records when tasks are created, suspended, resumed, and completed.
    """
    
    def __init__(self):
        self.events: List[TaskEvent] = []
        self.task_start_times: Dict[int, float] = {}
    
    def record_event(self, event_type: str, task: asyncio.Task = None, location: str = ""):
        """Record a task event"""
        if task is None:
            task = asyncio.current_task()
        
        if task is None:
            return
        
        event = TaskEvent(
            task_name=task.get_name(),
            event_type=event_type,
            timestamp=time.time(),
            location=location
        )
        self.events.append(event)
        
        if event_type == 'started':
            self.task_start_times[id(task)] = event.timestamp
    
    def get_task_timeline(self, task_name: str) -> List[TaskEvent]:
        """Get timeline for specific task"""
        return [e for e in self.events if e.task_name == task_name]
    
    def get_task_duration(self, task: asyncio.Task) -> float:
        """Get total execution time for task"""
        task_id = id(task)
        if task_id in self.task_start_times:
            return time.time() - self.task_start_times[task_id]
        return 0.0
    
    def print_timeline(self):
        """Print execution timeline"""
        if not self.events:
            print("No events recorded")
            return
        
        print("\n=== Task Execution Timeline ===")
        start_time = self.events[0].timestamp
        
        for event in self.events:
            elapsed = (event.timestamp - start_time) * 1000  # ms
            print(f"[{elapsed:7.2f}ms] {event.task_name:20} {event.event_type:10} {event.location}")
    
    def print_summary(self):
        """Print execution summary"""
        print("\n=== Execution Summary ===")
        
        # Group by task
        tasks = {}
        for event in self.events:
            if event.task_name not in tasks:
                tasks[event.task_name] = []
            tasks[event.task_name].append(event)
        
        for task_name, events in tasks.items():
            print(f"\n{task_name}:")
            print(f"  Events: {len(events)}")
            
            # Calculate time between first and last event
            if len(events) >= 2:
                duration = (events[-1].timestamp - events[0].timestamp) * 1000
                print(f"  Duration: {duration:.2f}ms")
            
            # Count suspensions
            suspensions = sum(1 for e in events if e.event_type == 'suspended')
            print(f"  Suspensions: {suspensions}")

# Example usage with tracer
tracer = TaskTracer()

async def traced_task(name: str, duration: float):
    """Task that records trace events"""
    task = asyncio.current_task()
    tracer.record_event('started', task)
    
    for i in range(3):
        tracer.record_event('suspended', task, f"sleep {i+1}")
        await asyncio.sleep(duration)
        tracer.record_event('resumed', task, f"after sleep {i+1}")
    
    tracer.record_event('completed', task)

async def tracing_example():
    tasks = [
        asyncio.create_task(traced_task(f"Task-{i}", 0.1), name=f"Task-{i}")
        for i in range(3)
    ]
    
    await asyncio.gather(*tasks)
    
    tracer.print_timeline()
    tracer.print_summary()
```

## Debugging Tools

### 1. Event Loop Slow Callback Detection

```python
import asyncio
import time
import warnings

class SlowCallbackDetector:
    """
    Detect callbacks that take too long.
    Helps find blocking code in async functions.
    """
    
    def __init__(self, threshold: float = 0.1):
        self.threshold = threshold  # seconds
        self.slow_callbacks: List[dict] = []
    
    def wrap_callback(self, callback, *args):
        """Wrap callback to measure execution time"""
        def wrapper():
            start = time.perf_counter()
            try:
                result = callback(*args)
                return result
            finally:
                duration = time.perf_counter() - start
                if duration > self.threshold:
                    self.slow_callbacks.append({
                        'callback': callback.__name__,
                        'duration': duration,
                        'timestamp': time.time()
                    })
                    warnings.warn(
                        f"Slow callback: {callback.__name__} took {duration:.3f}s",
                        RuntimeWarning
                    )
        return wrapper
    
    def report(self):
        """Print slow callback report"""
        print("\n=== Slow Callbacks ===")
        if not self.slow_callbacks:
            print("No slow callbacks detected")
            return
        
        print(f"Found {len(self.slow_callbacks)} slow callback(s):")
        for cb in self.slow_callbacks:
            print(f"  {cb['callback']}: {cb['duration']:.3f}s")
```

### 2. Memory Leak Detection

```python
import asyncio
import gc
import sys
from typing import Dict, Any

class MemoryLeakDetector:
    """
    Detect memory leaks in async code.
    Tracks object counts and identifies growing collections.
    """
    
    def __init__(self):
        self.snapshots: List[Dict[str, int]] = []
    
    def take_snapshot(self):
        """Take memory snapshot"""
        gc.collect()  # Force garbage collection
        
        # Count objects by type
        counts = {}
        for obj in gc.get_objects():
            obj_type = type(obj).__name__
            counts[obj_type] = counts.get(obj_type, 0) + 1
        
        self.snapshots.append(counts)
    
    def compare_snapshots(self, idx1: int = -2, idx2: int = -1) -> Dict[str, int]:
        """Compare two snapshots"""
        if len(self.snapshots) < 2:
            return {}
        
        snap1 = self.snapshots[idx1]
        snap2 = self.snapshots[idx2]
        
        # Find differences
        diff = {}
        all_types = set(snap1.keys()) | set(snap2.keys())
        
        for obj_type in all_types:
            count1 = snap1.get(obj_type, 0)
            count2 = snap2.get(obj_type, 0)
            delta = count2 - count1
            if delta != 0:
                diff[obj_type] = delta
        
        return diff
    
    def find_leaks(self, threshold: int = 100) -> Dict[str, int]:
        """Find types with growing object counts"""
        if len(self.snapshots) < 2:
            return {}
        
        diff = self.compare_snapshots()
        
        # Filter for significant growth
        leaks = {
            obj_type: count
            for obj_type, count in diff.items()
            if count > threshold
        }
        
        return leaks
    
    def report(self):
        """Print leak report"""
        print("\n=== Memory Leak Report ===")
        
        if len(self.snapshots) < 2:
            print("Need at least 2 snapshots")
            return
        
        leaks = self.find_leaks()
        
        if not leaks:
            print("No significant leaks detected")
            return
        
        print(f"Potential leaks (objects growing by >100):")
        for obj_type, count in sorted(leaks.items(), key=lambda x: x[1], reverse=True):
            print(f"  {obj_type}: +{count}")

# Example usage
async def leaky_code():
    """Code that leaks memory"""
    leaked_list = []
    
    for i in range(1000):
        # BUG: Accumulating objects without cleanup
        leaked_list.append([0] * 1000)
        await asyncio.sleep(0.001)

async def memory_leak_example():
    detector = MemoryLeakDetector()
    
    detector.take_snapshot()
    await leaky_code()
    detector.take_snapshot()
    
    detector.report()
```

## Debugging Patterns

### 1. Timeout Wrapper for Debugging

```python
import asyncio
from functools import wraps

def debug_timeout(seconds: float):
    """
    Decorator that adds timeout and debugging info.
    Helps identify where code is hanging.
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                return await asyncio.wait_for(
                    func(*args, **kwargs),
                    timeout=seconds
                )
            except asyncio.TimeoutError:
                task = asyncio.current_task()
                print(f"\n!!! TIMEOUT in {func.__name__} !!!")
                print(f"Task: {task.get_name()}")
                print(f"Timeout: {seconds}s")
                
                # Print stack
                stack = task.get_stack()
                if stack:
                    print("Stack trace:")
                    for frame in stack:
                        print(f"  {frame.f_code.co_filename}:{frame.f_lineno} in {frame.f_code.co_name}")
                
                raise
        return wrapper
    return decorator

# Example usage
@debug_timeout(1.0)
async def slow_function():
    """Function that might hang"""
    await asyncio.sleep(2.0)  # Will timeout!

async def timeout_debug_example():
    try:
        await slow_function()
    except asyncio.TimeoutError:
        print("Caught timeout with debug info")
```

### 2. Assertion Helpers

```python
import asyncio

async def assert_completes_within(coro, seconds: float, message: str = ""):
    """Assert that coroutine completes within time limit"""
    try:
        await asyncio.wait_for(coro, timeout=seconds)
    except asyncio.TimeoutError:
        raise AssertionError(
            f"Coroutine did not complete within {seconds}s. {message}"
        )

async def assert_task_count(expected: int, message: str = ""):
    """Assert expected number of tasks"""
    actual = len(asyncio.all_tasks())
    if actual != expected:
        tasks = asyncio.all_tasks()
        task_names = [t.get_name() for t in tasks]
        raise AssertionError(
            f"Expected {expected} tasks, found {actual}. {message}\n"
            f"Tasks: {task_names}"
        )

async def assert_no_pending_tasks(exclude_current: bool = True):
    """Assert no tasks are pending"""
    tasks = asyncio.all_tasks()
    
    if exclude_current:
        current = asyncio.current_task()
        tasks = {t for t in tasks if t != current}
    
    pending = {t for t in tasks if not t.done()}
    
    if pending:
        task_info = [f"{t.get_name()} ({t.get_coro().__name__})" for t in pending]
        raise AssertionError(
            f"Found {len(pending)} pending task(s):\n" +
            "\n".join(f"  - {info}" for info in task_info)
        )
```

## Mental Model Summary

**Debugging async code requires different tools:**

1. **Debug mode**: Catches common mistakes automatically
2. **Task inspection**: See what tasks are doing
3. **Stack traces**: Understand where tasks are suspended
4. **Leak detection**: Find tasks that never complete
5. **Deadlock detection**: Identify circular waits
6. **Tracing**: Record execution timeline
7. **Logging**: Add context to log messages

**Common debugging workflow:**

1. **Enable debug mode**: Catch obvious issues
2. **Name your tasks**: Make logs readable
3. **Add logging**: Track execution flow
4. **Inspect task states**: See what's running/waiting
5. **Check for leaks**: Find orphaned tasks
6. **Analyze stack traces**: Understand suspension points
7. **Use timeouts**: Prevent infinite waits

**Best practices:**

- Always name tasks with meaningful names
- Use structured logging with context
- Add timeouts to all waits
- Check for pending tasks at shutdown
- Use debug mode during development
- Write assertions about task counts
- Monitor memory usage
- Profile slow callbacks

Debugging async code is challenging, but with the right tools and systematic approach, you can identify and fix issues efficiently.