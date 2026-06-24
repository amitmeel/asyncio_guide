# Chapter 2: Event Loop Internals

## Introduction

The event loop is the beating heart of asyncio. Understanding its internals is crucial for mastering async programming. This chapter builds a precise mental model of how the event loop works, from scheduling to I/O multiplexing.

By the end, you'll understand:
- What the event loop actually is
- How tasks are scheduled and executed
- The role of selectors (epoll, kqueue, IOCP)
- What happens when you `await`
- Why async is single-threaded yet concurrent

---

## What is the Event Loop?

At its core, the event loop is a **while loop** that:
1. Checks for ready tasks
2. Executes one ready task
3. Checks for I/O events
4. Repeats

Here's a simplified conceptual implementation:

```python
# Simplified event loop (conceptual)
class SimpleEventLoop:
    def __init__(self):
        self.ready_queue = []      # Tasks ready to run
        self.waiting_tasks = {}    # Tasks waiting for I/O
        self.selector = Selector() # I/O multiplexer
    
    def run_forever(self):
        while True:
            # 1. Run all ready tasks
            while self.ready_queue:
                task = self.ready_queue.pop(0)
                task.step()  # Execute one step of the task
            
            # 2. Wait for I/O events (with timeout)
            events = self.selector.select(timeout=0)
            
            # 3. Wake up tasks that have I/O ready
            for key, mask in events:
                task = self.waiting_tasks[key.fd]
                self.ready_queue.append(task)
            
            # 4. Check for timers, callbacks, etc.
            self.process_timers()
```

**Key insight:** The loop never blocks indefinitely. It either runs tasks or checks for I/O, then repeats.

---

## The Event Loop Architecture

Let's visualize the complete architecture:

```
┌─────────────────────────────────────────────────────────────┐
│                      Event Loop                              │
│                                                              │
│  ┌────────────────┐      ┌──────────────┐                  │
│  │  Ready Queue   │      │ Selector     │                  │
│  │  (deque)       │      │ (epoll/      │                  │
│  │                │      │  kqueue/     │                  │
│  │  [task1]       │      │  IOCP)       │                  │
│  │  [task2]       │      │              │                  │
│  │  [task3]       │      │  Monitors:   │                  │
│  └────────────────┘      │  - sockets   │                  │
│                          │  - files     │                  │
│  ┌────────────────┐      │  - pipes     │                  │
│  │  Scheduled     │      └──────────────┘                  │
│  │  Callbacks     │                                         │
│  │                │      ┌──────────────┐                  │
│  │  [(time, cb)]  │      │ Waiting      │                  │
│  │  [(time, cb)]  │      │ Tasks        │                  │
│  └────────────────┘      │              │                  │
│                          │  {fd: task}  │                  │
│                          │  {fd: task}  │                  │
│                          └──────────────┘                  │
└─────────────────────────────────────────────────────────────┘
```

### Components Explained

**1. Ready Queue (deque)**
- Tasks that are ready to execute immediately
- FIFO (First In, First Out) ordering
- Tasks are popped and executed one at a time

**2. Selector (I/O Multiplexer)**
- Monitors file descriptors for I/O readiness
- Platform-specific: epoll (Linux), kqueue (BSD/macOS), IOCP (Windows)
- Returns which file descriptors have data ready

**3. Waiting Tasks**
- Tasks suspended on I/O operations
- Mapped by file descriptor
- Moved to ready queue when I/O is ready

**4. Scheduled Callbacks**
- Time-based callbacks (timers)
- Sorted by execution time
- Executed when their time arrives

---

## Cooperative Multitasking

Asyncio uses **cooperative multitasking**, not preemptive multitasking like threads.

### Preemptive (Threads)

```python
import threading
import time

def task1():
    while True:
        print("Task 1")
        time.sleep(0.1)  # OS can interrupt here

def task2():
    while True:
        print("Task 2")
        time.sleep(0.1)  # OS can interrupt here

# OS scheduler decides when to switch
t1 = threading.Thread(target=task1)
t2 = threading.Thread(target=task2)
t1.start()
t2.start()
```

**Preemptive:** The OS can interrupt a thread at any time and switch to another.

### Cooperative (Asyncio)

```python
import asyncio

async def task1():
    while True:
        print("Task 1")
        await asyncio.sleep(0.1)  # Explicitly yields control

async def task2():
    while True:
        print("Task 2")
        await asyncio.sleep(0.1)  # Explicitly yields control

# Tasks voluntarily yield control
async def main():
    await asyncio.gather(task1(), task2())

asyncio.run(main())
```

**Cooperative:** Tasks must explicitly yield control with `await`. The event loop can only switch tasks at `await` points.

### Why Cooperative?

**Advantages:**
1. **No race conditions:** Only one task runs at a time
2. **Predictable switching:** Only at `await` points
3. **Lightweight:** No OS context switching overhead
4. **Deterministic:** Easier to reason about

**Disadvantages:**
1. **Blocking code breaks everything:** One blocking call freezes all tasks
2. **Requires discipline:** Must use `await` appropriately
3. **CPU-bound work blocks:** No automatic preemption

---

## The Ready Queue: Task Scheduling

The ready queue is a simple FIFO queue, but understanding its behavior is crucial.

### Example: Task Scheduling Order

```python
import asyncio

async def task_a():
    print("A: Start")
    await asyncio.sleep(0)  # Yield control
    print("A: Resume")

async def task_b():
    print("B: Start")
    await asyncio.sleep(0)  # Yield control
    print("B: Resume")

async def task_c():
    print("C: Start")
    await asyncio.sleep(0)  # Yield control
    print("C: Resume")

async def main():
    # Create all tasks
    await asyncio.gather(task_a(), task_b(), task_c())

asyncio.run(main())

# Output:
# A: Start
# B: Start
# C: Start
# A: Resume
# B: Resume
# C: Resume
```

**What happened?**

```
Time  | Ready Queue        | Action
------|-------------------|----------------------------------
0     | [A, B, C]         | Initial tasks
1     | [B, C]            | Run A → "A: Start" → await → A suspended
2     | [C]               | Run B → "B: Start" → await → B suspended
3     | []                | Run C → "C: Start" → await → C suspended
4     | [A, B, C]         | sleep(0) completes → all ready
5     | [B, C]            | Run A → "A: Resume" → complete
6     | [C]               | Run B → "B: Resume" → complete
7     | []                | Run C → "C: Resume" → complete
```

**Key insight:** Tasks are scheduled in the order they become ready, not the order they were created.

### Immediate Scheduling with `create_task()`

```python
import asyncio

async def background_task():
    print("Background: Start")
    await asyncio.sleep(1)
    print("Background: Done")

async def main():
    print("Main: Before create_task")
    
    # Create task - it's immediately scheduled
    task = asyncio.create_task(background_task())
    
    print("Main: After create_task")
    
    # Task hasn't run yet! We need to yield control
    await asyncio.sleep(0)  # Yield to let background_task start
    
    print("Main: After yield")
    
    await task  # Wait for completion

asyncio.run(main())

# Output:
# Main: Before create_task
# Main: After create_task
# Background: Start
# Main: After yield
# Background: Done
```

**Timeline:**

```
Ready Queue: [main]
→ Run main: "Main: Before create_task"
→ create_task(background_task) → adds to ready queue
Ready Queue: [main, background_task]
→ Continue main: "Main: After create_task"
→ await sleep(0) → main yields
Ready Queue: [background_task, main]
→ Run background_task: "Background: Start"
→ await sleep(1) → background_task suspended
Ready Queue: [main]
→ Run main: "Main: After yield"
→ await task → main waits for background_task
... (1 second later)
Ready Queue: [background_task]
→ Run background_task: "Background: Done"
→ background_task completes → main resumes
```

---

## Selectors: I/O Multiplexing

Selectors are the magic that makes async I/O efficient. They allow monitoring multiple file descriptors with a single system call.

### Without Selectors (Polling)

```python
import socket

# BAD: Polling approach
sockets = [sock1, sock2, sock3]

while True:
    for sock in sockets:
        try:
            data = sock.recv(1024, socket.MSG_DONTWAIT)
            if data:
                process(data)
        except BlockingIOError:
            pass  # No data yet
    # This loops constantly, wasting CPU!
```

**Problem:** Constantly checking each socket wastes CPU cycles.

### With Selectors (Event-driven)

```python
import selectors
import socket

# GOOD: Selector approach
selector = selectors.DefaultSelector()

# Register sockets
selector.register(sock1, selectors.EVENT_READ)
selector.register(sock2, selectors.EVENT_READ)
selector.register(sock3, selectors.EVENT_READ)

while True:
    # Block until at least one socket has data
    events = selector.select(timeout=1.0)
    
    for key, mask in events:
        sock = key.fileobj
        data = sock.recv(1024)
        process(data)
```

**Advantage:** Single system call monitors all sockets. CPU sleeps until data arrives.

### Platform-Specific Selectors

Different operating systems use different I/O multiplexing mechanisms:

**Linux: epoll**
```python
# Conceptual - asyncio uses this automatically on Linux
import select

epoll = select.epoll()
epoll.register(fd, select.EPOLLIN)

while True:
    events = epoll.poll(timeout=1.0)
    for fd, event in events:
        # Handle ready file descriptor
        pass
```

**macOS/BSD: kqueue**
```python
# Conceptual - asyncio uses this automatically on macOS
import select

kq = select.kqueue()
kevent = select.kevent(fd, filter=select.KQ_FILTER_READ)
kq.control([kevent], 0)

while True:
    events = kq.control(None, 1, timeout=1.0)
    for event in events:
        # Handle ready file descriptor
        pass
```

**Windows: IOCP (I/O Completion Ports)**
```python
# Conceptual - asyncio uses ProactorEventLoop on Windows
# IOCP is more complex - completion-based, not readiness-based
```

**Good news:** Asyncio abstracts all this! You don't need to worry about platform differences.

### How Asyncio Uses Selectors

```python
import asyncio
import socket

async def handle_client(reader, writer):
    # Internally, asyncio:
    # 1. Registers socket with selector
    # 2. Suspends this coroutine
    # 3. Waits for selector to signal data ready
    # 4. Resumes this coroutine
    
    data = await reader.read(1024)  # Non-blocking!
    writer.write(data)
    await writer.drain()

async def main():
    server = await asyncio.start_server(
        handle_client, '127.0.0.1', 8888
    )
    await server.serve_forever()

asyncio.run(main())
```

**Under the hood:**

```
1. await reader.read(1024)
   ↓
2. Check if data available in buffer
   ↓
3. If not: Register socket with selector
   ↓
4. Suspend coroutine (yield control)
   ↓
5. Event loop checks selector
   ↓
6. When data ready: Selector returns event
   ↓
7. Resume coroutine with data
```

---

## What Happens After `await`

This is the most important section for building your mental model.

### The `await` Mechanism

```python
import asyncio

async def fetch_data():
    print("1. Before await")
    result = await asyncio.sleep(1)  # Suspension point
    print("2. After await")
    return result

async def main():
    print("A. Before calling fetch_data")
    data = await fetch_data()
    print("B. After fetch_data returns")

asyncio.run(main())

# Output:
# A. Before calling fetch_data
# 1. Before await
# 2. After await
# B. After fetch_data returns
```

**Step-by-step execution:**

```
Step 1: main() starts
  Ready Queue: [main]
  → Execute: print("A. Before calling fetch_data")

Step 2: await fetch_data()
  → Call fetch_data() - creates coroutine
  → Enter fetch_data()
  Ready Queue: [main (suspended in fetch_data)]
  → Execute: print("1. Before await")

Step 3: await asyncio.sleep(1)
  → asyncio.sleep(1) returns a Future
  → Future is not ready yet
  → Suspend fetch_data()
  → Schedule callback for 1 second later
  → Return control to event loop
  Ready Queue: []
  Scheduled: [(time.now + 1s, resume_fetch_data)]

Step 4: Event loop waits
  → No ready tasks
  → Check scheduled callbacks
  → Sleep until next callback (1 second)

Step 5: Timer fires (1 second later)
  → Callback executes
  → Marks Future as complete
  → Adds fetch_data to ready queue
  Ready Queue: [fetch_data]

Step 6: Resume fetch_data()
  → Execute: print("2. After await")
  → Return from fetch_data()
  → Resume main()
  Ready Queue: [main]

Step 7: Resume main()
  → Execute: print("B. After fetch_data returns")
  → main() completes
```

### The Suspension Point

`await` is a **suspension point** - the only place where control can be yielded.

```python
async def example():
    print("This runs immediately")
    x = 1 + 1  # No suspension
    y = compute(x)  # No suspension
    
    # SUSPENSION POINT - control can be yielded here
    result = await async_operation()
    
    print("This runs after resumption")
```

**Critical rules:**

1. **Only `await` suspends:** Regular code runs to completion
2. **Suspension is voluntary:** Task must hit `await` to yield
3. **Resumption is automatic:** Event loop resumes when ready

### Multiple Awaits

```python
import asyncio

async def multi_await():
    print("Start")
    
    await asyncio.sleep(0.1)  # Suspend 1
    print("After first await")
    
    await asyncio.sleep(0.1)  # Suspend 2
    print("After second await")
    
    await asyncio.sleep(0.1)  # Suspend 3
    print("Done")

asyncio.run(multi_await())
```

**Execution timeline:**

```
Time  | State           | Action
------|-----------------|--------------------------------
0.0s  | Running         | print("Start")
0.0s  | Suspended       | await sleep(0.1) → suspend
0.1s  | Running         | print("After first await")
0.1s  | Suspended       | await sleep(0.1) → suspend
0.2s  | Running         | print("After second await")
0.2s  | Suspended       | await sleep(0.1) → suspend
0.3s  | Running         | print("Done")
0.3s  | Complete        | Function returns
```

---

## Callback Registration and Task Wakeups

Understanding how tasks are woken up is key to understanding async performance.

### Callback-based Wakeup

```python
import asyncio

# Low-level: How asyncio works internally
async def low_level_example():
    loop = asyncio.get_event_loop()
    future = loop.create_future()
    
    # Register callback to wake up this task
    def wakeup_callback():
        future.set_result("Data ready!")
    
    # Schedule callback for later
    loop.call_later(1.0, wakeup_callback)
    
    # Suspend until callback fires
    result = await future
    print(result)

asyncio.run(low_level_example())
```

**What happens:**

```
1. Create Future (represents future result)
2. Register callback to set Future result
3. await Future → suspend task
4. Event loop continues
5. After 1 second: callback fires
6. Callback sets Future result
7. Future wakes up waiting task
8. Task resumes with result
```

### I/O-based Wakeup

```python
import asyncio

async def read_socket(reader):
    # Internally:
    # 1. Check if data in buffer
    # 2. If not: register socket with selector
    # 3. Suspend task
    # 4. When selector signals ready: resume task
    
    data = await reader.read(1024)
    return data
```

**Selector integration:**

```
1. await reader.read(1024)
   ↓
2. No data in buffer
   ↓
3. selector.register(socket_fd, EVENT_READ, callback=resume_task)
   ↓
4. Suspend task
   ↓
5. Event loop: events = selector.select()
   ↓
6. Socket has data → selector returns event
   ↓
7. Call registered callback
   ↓
8. Resume task with data
```

---

## Building a Mental Model: The Complete Picture

Let's trace a complete example to solidify understanding:

```python
import asyncio

async def task_a():
    print("A1")
    await asyncio.sleep(0.2)
    print("A2")

async def task_b():
    print("B1")
    await asyncio.sleep(0.1)
    print("B2")

async def main():
    print("Main: Start")
    await asyncio.gather(task_a(), task_b())
    print("Main: Done")

asyncio.run(main())
```

**Complete execution trace:**

```
Time   | Ready Queue      | Scheduled Callbacks        | Action
-------|------------------|----------------------------|------------------
0.00s  | [main]           | []                         | Start
0.00s  | []               | []                         | main: print("Main: Start")
0.00s  | [task_a, task_b] | []                         | gather() creates tasks
0.00s  | [task_b]         | []                         | task_a: print("A1")
0.00s  | [task_b]         | [(0.20s, wake_task_a)]     | task_a: await sleep(0.2)
0.00s  | []               | [(0.20s, wake_task_a)]     | task_b: print("B1")
0.00s  | []               | [(0.10s, wake_task_b),     | task_b: await sleep(0.1)
       |                  |  (0.20s, wake_task_a)]     |
0.10s  | [task_b]         | [(0.20s, wake_task_a)]     | Timer: wake_task_b
0.10s  | []               | [(0.20s, wake_task_a)]     | task_b: print("B2")
0.10s  | []               | [(0.20s, wake_task_a)]     | task_b: complete
0.20s  | [task_a]         | []                         | Timer: wake_task_a
0.20s  | []               | []                         | task_a: print("A2")
0.20s  | []               | []                         | task_a: complete
0.20s  | [main]           | []                         | gather() complete
0.20s  | []               | []                         | main: print("Main: Done")
```

**Output:**
```
Main: Start
A1
B1
B2
A2
Main: Done
```

**Key observations:**

1. Tasks run in order they're added to ready queue
2. `await` suspends and schedules wakeup
3. Timers fire in chronological order
4. Event loop is never blocked

---

## Event Loop Lifecycle

Understanding the event loop lifecycle helps debug issues.

### Loop States

```python
import asyncio

async def main():
    loop = asyncio.get_running_loop()
    
    print(f"Is running: {loop.is_running()}")  # True
    print(f"Is closed: {loop.is_closed()}")    # False

asyncio.run(main())
# After asyncio.run() completes, loop is closed
```

### Loop Creation and Cleanup

```python
import asyncio

# Method 1: asyncio.run() (recommended)
async def main():
    print("Running")

asyncio.run(main())  # Creates loop, runs, closes loop

# Method 2: Manual loop management (advanced)
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
try:
    loop.run_until_complete(main())
finally:
    loop.close()
```

### Loop Policies

```python
import asyncio

# Get current event loop policy
policy = asyncio.get_event_loop_policy()

# Create new loop
loop = policy.new_event_loop()

# Set as current loop
policy.set_event_loop(loop)
```

---

## Real-world Example: HTTP Server

Let's see how the event loop handles a real HTTP server:

```python
import asyncio

async def handle_request(reader, writer):
    # Read HTTP request
    request = await reader.read(1024)
    print(f"Received: {request[:50]}")
    
    # Simulate processing
    await asyncio.sleep(0.1)
    
    # Send response
    response = b"HTTP/1.1 200 OK\r\n\r\nHello, World!"
    writer.write(response)
    await writer.drain()
    
    writer.close()
    await writer.wait_closed()

async def main():
    server = await asyncio.start_server(
        handle_request, '127.0.0.1', 8888
    )
    
    print("Server started on port 8888")
    async with server:
        await server.serve_forever()

asyncio.run(main())
```

**What happens with 3 concurrent requests:**

```
Time   | Ready Queue              | Selector Monitoring      | Action
-------|--------------------------|--------------------------|------------------
0.00s  | [server]                 | [server_socket]          | Server listening
0.10s  | [server, client1]        | [server_socket, sock1]   | Client 1 connects
0.10s  | [client1]                | [server_socket, sock1]   | Accept connection
0.10s  | [client1]                | [server_socket, sock1]   | await reader.read()
0.15s  | [client1, client2]       | [server_socket,          | Client 2 connects
       |                          |  sock1, sock2]           |
0.15s  | [client2]                | [server_socket,          | Accept connection
       |                          |  sock1, sock2]           |
0.15s  | [client2]                | [server_socket,          | await reader.read()
       |                          |  sock1, sock2]           |
0.20s  | [client2, client1]       | [server_socket, sock2]   | sock1 data ready
0.20s  | [client1]                | [server_socket, sock2]   | client1 processes
0.20s  | [client1]                | [server_socket, sock2]   | await sleep(0.1)
0.25s  | [client1, client2]       | [server_socket]          | sock2 data ready
0.25s  | [client2]                | [server_socket]          | client2 processes
0.25s  | [client2]                | [server_socket]          | await sleep(0.1)
0.30s  | [client2, client1]       | [server_socket]          | client1 sleep done
0.30s  | [client1]                | [server_socket]          | client1 sends response
0.35s  | [client1, client2]       | [server_socket]          | client2 sleep done
0.35s  | [client2]                | [server_socket]          | client2 sends response
```

**Key insight:** Single thread handles multiple clients concurrently by switching at `await` points.

---

## Performance Characteristics

Understanding event loop performance helps optimize applications.

### Task Switching Overhead

```python
import asyncio
import time

async def empty_task():
    await asyncio.sleep(0)  # Immediate yield

async def benchmark_switching():
    start = time.perf_counter()
    
    # Create 10,000 tasks that just yield
    tasks = [empty_task() for _ in range(10000)]
    await asyncio.gather(*tasks)
    
    duration = time.perf_counter() - start
    print(f"10,000 task switches: {duration:.3f}s")
    print(f"Per switch: {duration/10000*1000:.3f}ms")

asyncio.run(benchmark_switching())

# Typical output:
# 10,000 task switches: 0.045s
# Per switch: 0.0045ms
```

**Comparison:**
- Thread context switch: ~1-10 microseconds
- Asyncio task switch: ~0.5-5 microseconds
- **Asyncio is 2-10x faster than threads**

### Selector Performance

```python
import asyncio
import time

async def benchmark_selector():
    # Create many sockets
    readers = []
    writers = []
    
    for _ in range(1000):
        reader, writer = await asyncio.open_connection('127.0.0.1', 8888)
        readers.append(reader)
        writers.append(writer)
    
    start = time.perf_counter()
    
    # Selector monitors all 1000 sockets efficiently
    tasks = [reader.read(1) for reader in readers]
    await asyncio.gather(*tasks)
    
    duration = time.perf_counter() - start
    print(f"1000 concurrent reads: {duration:.3f}s")

# Selector scales to thousands of connections
```

---

## Common Pitfalls

### Pitfall 1: Blocking the Event Loop

```python
import asyncio
import time

async def bad_example():
    print("Start")
    time.sleep(1)  # ❌ BLOCKS THE ENTIRE EVENT LOOP!
    print("End")

async def good_example():
    print("Start")
    await asyncio.sleep(1)  # ✅ Yields control
    print("End")
```

**Why it's bad:** `time.sleep()` blocks the thread, preventing all other tasks from running.

### Pitfall 2: Forgetting to Await

```python
async def fetch_data():
    await asyncio.sleep(1)
    return "data"

async def bad_example():
    result = fetch_data()  # ❌ Returns coroutine, doesn't execute!
    print(result)  # Prints: <coroutine object fetch_data>

async def good_example():
    result = await fetch_data()  # ✅ Executes and waits
    print(result)  # Prints: data
```

### Pitfall 3: Creating Tasks Without Awaiting

```python
async def background_work():
    await asyncio.sleep(1)
    print("Done")

async def bad_example():
    asyncio.create_task(background_work())
    # Task is created but we don't wait for it!
    # It might not complete before main() exits

async def good_example():
    task = asyncio.create_task(background_work())
    await task  # Wait for completion
```

---

## Summary: Mental Model Checklist

✅ **Event loop is a while loop** that runs tasks and checks I/O

✅ **Ready queue** holds tasks ready to execute (FIFO)

✅ **Selector** monitors file descriptors for I/O readiness

✅ **Cooperative multitasking** - tasks yield at `await` points

✅ **`await` suspends** the current task and returns control to the loop

✅ **Callbacks wake tasks** when I/O is ready or timers fire

✅ **Single-threaded** by default - no parallelism, only concurrency

✅ **Scales efficiently** - thousands of tasks with minimal overhead

---

## What's Next?

Now that you understand the event loop's internals, we'll explore coroutines in depth.

In [Chapter 3: Coroutines Deep Dive](./03-coroutines.md), we'll cover:
- What coroutines actually are
- The difference between coroutine objects and execution
- Suspension and resumption mechanics
- Stack unwinding and exception propagation

This will complete your foundational understanding of asyncio's execution model.

---

**Previous:** [← Chapter 1: Why Async Exists](./01-why-async-exists.md)  
**Next:** [Chapter 3: Coroutines Deep Dive →](./03-coroutines.md)