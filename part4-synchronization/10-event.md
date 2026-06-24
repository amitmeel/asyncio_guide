# Chapter 10: Event - Signaling and Broadcasting

## Introduction

`asyncio.Event` is a synchronization primitive for signaling between tasks. Unlike locks that protect shared state, events coordinate task execution through notifications. Events are essential for implementing pub-sub patterns, service coordination, and blackboard systems.

By the end of this chapter, you'll understand:
- Signaling between tasks
- Broadcasting to multiple waiters
- Wakeup semantics and state management
- Startup/shutdown coordination
- Service readiness patterns
- **Blackboard pattern with Events** (event-driven coordination)
- When to use Event vs other primitives

---

## What is an Event?

An `asyncio.Event` is a boolean flag that tasks can wait on. When set, all waiting tasks are woken up.

### Basic Event Usage

```python
import asyncio

async def basic_event_demo():
    event = asyncio.Event()
    
    async def waiter(name):
        print(f"{name}: Waiting for event")
        await event.wait()  # Blocks until event is set
        print(f"{name}: Event received!")
    
    async def setter():
        print("Setter: Doing some work...")
        await asyncio.sleep(2.0)
        print("Setter: Setting event")
        event.set()  # Wake up all waiters
    
    # Start waiters and setter
    await asyncio.gather(
        waiter("Task-1"),
        waiter("Task-2"),
        waiter("Task-3"),
        setter()
    )

asyncio.run(basic_event_demo())

# Output:
# Task-1: Waiting for event
# Task-2: Waiting for event
# Task-3: Waiting for event
# Setter: Doing some work...
# Setter: Setting event
# Task-1: Event received!
# Task-2: Event received!
# Task-3: Event received!
```

**Key characteristics:**
- Multiple tasks can wait on the same event
- `set()` wakes up **all** waiting tasks (broadcast)
- Once set, event stays set until `clear()`

---

## Event States and Operations

### Event State Management

```python
import asyncio

async def event_states():
    event = asyncio.Event()
    
    # Initial state: not set
    print(f"Is set: {event.is_set()}")  # False
    
    # Set the event
    event.set()
    print(f"Is set: {event.is_set()}")  # True
    
    # Wait on set event (returns immediately)
    await event.wait()
    print("Wait returned immediately")
    
    # Clear the event
    event.clear()
    print(f"Is set: {event.is_set()}")  # False
    
    # Now wait would block
    # await event.wait()  # Would block forever

asyncio.run(event_states())
```

### Event Operations

```python
import asyncio

event = asyncio.Event()

# Check if set
if event.is_set():
    print("Event is set")

# Set the event (wake all waiters)
event.set()

# Clear the event (reset to unset state)
event.clear()

# Wait for event (blocks if not set)
await event.wait()
```

---

## Broadcasting to Multiple Waiters

Events excel at broadcasting signals to multiple tasks.

### One-to-Many Notification

```python
import asyncio

async def broadcast_demo():
    ready_event = asyncio.Event()
    
    async def worker(worker_id):
        print(f"Worker {worker_id}: Waiting for ready signal")
        await ready_event.wait()
        print(f"Worker {worker_id}: Starting work")
        await asyncio.sleep(1.0)
        print(f"Worker {worker_id}: Done")
    
    async def coordinator():
        print("Coordinator: Preparing...")
        await asyncio.sleep(2.0)
        
        print("Coordinator: Broadcasting ready signal")
        ready_event.set()  # All workers start simultaneously
    
    # Start workers and coordinator
    await asyncio.gather(
        *[worker(i) for i in range(5)],
        coordinator()
    )

asyncio.run(broadcast_demo())

# Output:
# Worker 0: Waiting for ready signal
# Worker 1: Waiting for ready signal
# Worker 2: Waiting for ready signal
# Worker 3: Waiting for ready signal
# Worker 4: Waiting for ready signal
# Coordinator: Preparing...
# Coordinator: Broadcasting ready signal
# Worker 0: Starting work
# Worker 1: Starting work
# Worker 2: Starting work
# Worker 3: Starting work
# Worker 4: Starting work
# (all workers start together)
```

---

## Wakeup Semantics

Understanding when and how tasks are woken is crucial.

### Immediate Wakeup for Set Events

```python
import asyncio

async def immediate_wakeup():
    event = asyncio.Event()
    event.set()  # Set before waiting
    
    # Wait returns immediately
    print("Before wait")
    await event.wait()
    print("After wait (immediate)")

asyncio.run(immediate_wakeup())

# Output:
# Before wait
# After wait (immediate)
```

### All Waiters Wake Up

```python
import asyncio

async def all_waiters_wake():
    event = asyncio.Event()
    wakeup_count = 0
    
    async def waiter(waiter_id):
        nonlocal wakeup_count
        await event.wait()
        wakeup_count += 1
        print(f"Waiter {waiter_id} woke up")
    
    # Start 100 waiters
    waiters = [asyncio.create_task(waiter(i)) for i in range(100)]
    
    await asyncio.sleep(0.1)  # Let them all wait
    
    # Set event - wakes ALL waiters
    event.set()
    
    await asyncio.gather(*waiters)
    print(f"Total woken: {wakeup_count}")

asyncio.run(all_waiters_wake())

# Output: Total woken: 100
```

### Event Stays Set

```python
import asyncio

async def event_stays_set():
    event = asyncio.Event()
    
    # Set event
    event.set()
    
    # Multiple waits all return immediately
    await event.wait()
    print("First wait")
    
    await event.wait()
    print("Second wait")
    
    await event.wait()
    print("Third wait")
    
    # Event is still set
    print(f"Still set: {event.is_set()}")

asyncio.run(event_stays_set())

# Output:
# First wait
# Second wait
# Third wait
# Still set: True
```

---

## Startup/Shutdown Coordination

Events are perfect for coordinating service lifecycle.

### Service Readiness Pattern

```python
import asyncio

class Service:
    def __init__(self, name):
        self.name = name
        self.ready = asyncio.Event()
        self.shutdown = asyncio.Event()
    
    async def start(self):
        """Start service and signal readiness"""
        print(f"{self.name}: Starting...")
        
        # Initialization
        await asyncio.sleep(1.0)
        
        print(f"{self.name}: Ready")
        self.ready.set()  # Signal ready
        
        # Wait for shutdown signal
        await self.shutdown.wait()
        
        print(f"{self.name}: Shutting down")
    
    async def wait_ready(self):
        """Wait for service to be ready"""
        await self.ready.wait()
    
    async def stop(self):
        """Signal shutdown"""
        self.shutdown.set()

async def main():
    # Create services
    db = Service("Database")
    cache = Service("Cache")
    api = Service("API")
    
    # Start services
    service_tasks = [
        asyncio.create_task(db.start()),
        asyncio.create_task(cache.start()),
        asyncio.create_task(api.start())
    ]
    
    # Wait for all services to be ready
    print("Main: Waiting for services...")
    await asyncio.gather(
        db.wait_ready(),
        cache.wait_ready(),
        api.wait_ready()
    )
    print("Main: All services ready!")
    
    # Run for a while
    await asyncio.sleep(2.0)
    
    # Shutdown
    print("Main: Initiating shutdown")
    await asyncio.gather(
        db.stop(),
        cache.stop(),
        api.stop()
    )
    
    # Wait for services to stop
    await asyncio.gather(*service_tasks)
    print("Main: All services stopped")

asyncio.run(main())
```

---

## Real-world: Blackboard Pattern with Events

The blackboard pattern is an AI architecture where multiple knowledge sources (subscribers) react to events posted on a shared blackboard.

### Basic Blackboard with Event Notifications

```python
import asyncio
from typing import Dict, Set, Callable, Any
from dataclasses import dataclass
from enum import Enum

class EventType(Enum):
    DATA_UPDATED = "data_updated"
    ANALYSIS_COMPLETE = "analysis_complete"
    DECISION_MADE = "decision_made"
    ERROR_OCCURRED = "error_occurred"

@dataclass
class BlackboardEvent:
    event_type: EventType
    data: Any
    source: str

class Blackboard:
    """
    Blackboard pattern with event-driven notifications.
    
    Multiple subscribers can wait for specific event types.
    When an event is posted, all subscribers for that type are notified.
    """
    
    def __init__(self):
        # Event objects for each event type
        self._events: Dict[EventType, asyncio.Event] = {
            event_type: asyncio.Event()
            for event_type in EventType
        }
        
        # Store latest event data
        self._event_data: Dict[EventType, BlackboardEvent] = {}
        
        # Subscribers waiting for events
        self._subscribers: Dict[EventType, Set[str]] = {
            event_type: set()
            for event_type in EventType
        }
        
        # Shared data store
        self._data: Dict[str, Any] = {}
        self._lock = asyncio.Lock()
    
    async def post_event(self, event: BlackboardEvent):
        """Post an event to the blackboard"""
        async with self._lock:
            print(f"📢 Blackboard: Event posted - {event.event_type.value} from {event.source}")
            
            # Store event data
            self._event_data[event.event_type] = event
            
            # Set the event (wake all waiters)
            self._events[event.event_type].set()
            
            # Clear immediately so next wait blocks
            # (This creates a pulse, not a persistent state)
            await asyncio.sleep(0)  # Let waiters wake up
            self._events[event.event_type].clear()
    
    async def wait_for_event(self, event_type: EventType, subscriber_name: str) -> BlackboardEvent:
        """Wait for a specific event type"""
        # Register subscriber
        self._subscribers[event_type].add(subscriber_name)
        
        print(f"👂 {subscriber_name}: Waiting for {event_type.value}")
        
        # Wait for event
        await self._events[event_type].wait()
        
        # Get event data
        async with self._lock:
            event_data = self._event_data.get(event_type)
        
        print(f"✅ {subscriber_name}: Received {event_type.value}")
        return event_data
    
    async def write_data(self, key: str, value: Any):
        """Write data to blackboard"""
        async with self._lock:
            self._data[key] = value
    
    async def read_data(self, key: str) -> Any:
        """Read data from blackboard"""
        async with self._lock:
            return self._data.get(key)

# Knowledge Sources (Subscribers)
class DataCollector:
    """Collects data and posts to blackboard"""
    
    def __init__(self, blackboard: Blackboard):
        self.blackboard = blackboard
        self.name = "DataCollector"
    
    async def run(self):
        """Collect data periodically"""
        for i in range(3):
            await asyncio.sleep(1.0)
            
            # Collect data
            data = {"sensor_reading": i * 10, "timestamp": i}
            await self.blackboard.write_data("sensor_data", data)
            
            # Post event
            await self.blackboard.post_event(
                BlackboardEvent(
                    event_type=EventType.DATA_UPDATED,
                    data=data,
                    source=self.name
                )
            )

class DataAnalyzer:
    """Analyzes data when DATA_UPDATED event occurs"""
    
    def __init__(self, blackboard: Blackboard):
        self.blackboard = blackboard
        self.name = "DataAnalyzer"
    
    async def run(self):
        """Wait for data updates and analyze"""
        while True:
            # Wait for DATA_UPDATED event
            event = await self.blackboard.wait_for_event(
                EventType.DATA_UPDATED,
                self.name
            )
            
            # Analyze data
            print(f"🔍 {self.name}: Analyzing {event.data}")
            await asyncio.sleep(0.5)
            
            # Post analysis result
            analysis = {"trend": "increasing", "confidence": 0.95}
            await self.blackboard.write_data("analysis", analysis)
            
            await self.blackboard.post_event(
                BlackboardEvent(
                    event_type=EventType.ANALYSIS_COMPLETE,
                    data=analysis,
                    source=self.name
                )
            )

class DecisionMaker:
    """Makes decisions based on analysis"""
    
    def __init__(self, blackboard: Blackboard):
        self.blackboard = blackboard
        self.name = "DecisionMaker"
    
    async def run(self):
        """Wait for analysis and make decisions"""
        while True:
            # Wait for ANALYSIS_COMPLETE event
            event = await self.blackboard.wait_for_event(
                EventType.ANALYSIS_COMPLETE,
                self.name
            )
            
            # Make decision
            print(f"🎯 {self.name}: Making decision based on {event.data}")
            await asyncio.sleep(0.3)
            
            decision = {"action": "increase_threshold", "priority": "high"}
            
            await self.blackboard.post_event(
                BlackboardEvent(
                    event_type=EventType.DECISION_MADE,
                    data=decision,
                    source=self.name
                )
            )

class ActionExecutor:
    """Executes actions based on decisions"""
    
    def __init__(self, blackboard: Blackboard):
        self.blackboard = blackboard
        self.name = "ActionExecutor"
    
    async def run(self):
        """Wait for decisions and execute"""
        while True:
            # Wait for DECISION_MADE event
            event = await self.blackboard.wait_for_event(
                EventType.DECISION_MADE,
                self.name
            )
            
            # Execute action
            print(f"⚡ {self.name}: Executing {event.data}")
            await asyncio.sleep(0.2)

async def demo_blackboard():
    """Demonstrate blackboard pattern with events"""
    blackboard = Blackboard()
    
    # Create knowledge sources
    collector = DataCollector(blackboard)
    analyzer = DataAnalyzer(blackboard)
    decision_maker = DecisionMaker(blackboard)
    executor = ActionExecutor(blackboard)
    
    # Run all knowledge sources concurrently
    await asyncio.gather(
        collector.run(),
        analyzer.run(),
        decision_maker.run(),
        executor.run(),
        return_exceptions=True
    )

asyncio.run(demo_blackboard())
```

**Output:**
```
👂 DataAnalyzer: Waiting for data_updated
👂 DecisionMaker: Waiting for analysis_complete
👂 ActionExecutor: Waiting for decision_made
📢 Blackboard: Event posted - data_updated from DataCollector
✅ DataAnalyzer: Received data_updated
🔍 DataAnalyzer: Analyzing {'sensor_reading': 0, 'timestamp': 0}
📢 Blackboard: Event posted - analysis_complete from DataAnalyzer
✅ DecisionMaker: Received analysis_complete
🎯 DecisionMaker: Making decision based on {'trend': 'increasing', 'confidence': 0.95}
📢 Blackboard: Event posted - decision_made from DecisionMaker
✅ ActionExecutor: Received decision_made
⚡ ActionExecutor: Executing {'action': 'increase_threshold', 'priority': 'high'}
...
```

---

## Advanced Blackboard: Multiple Event Types

```python
import asyncio
from typing import Dict, List
from enum import Enum

class EventType(Enum):
    SENSOR_DATA = "sensor_data"
    ANOMALY_DETECTED = "anomaly_detected"
    ALERT_TRIGGERED = "alert_triggered"

class MultiEventSubscriber:
    """Subscriber that waits for multiple event types"""
    
    def __init__(self, blackboard: Blackboard, name: str):
        self.blackboard = blackboard
        self.name = name
    
    async def wait_for_any(self, event_types: List[EventType]) -> BlackboardEvent:
        """Wait for any of the specified event types"""
        # Create tasks for each event type
        tasks = [
            asyncio.create_task(
                self.blackboard.wait_for_event(event_type, self.name)
            )
            for event_type in event_types
        ]
        
        # Wait for first to complete
        done, pending = await asyncio.wait(
            tasks,
            return_when=asyncio.FIRST_COMPLETED
        )
        
        # Cancel pending tasks
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        
        # Return first result
        return list(done)[0].result()
    
    async def run(self):
        """Wait for multiple event types"""
        while True:
            event = await self.wait_for_any([
                EventType.SENSOR_DATA,
                EventType.ANOMALY_DETECTED
            ])
            
            print(f"📨 {self.name}: Received {event.event_type.value}")
            
            # Process based on event type
            if event.event_type == EventType.SENSOR_DATA:
                await self.process_sensor_data(event)
            elif event.event_type == EventType.ANOMALY_DETECTED:
                await self.process_anomaly(event)
    
    async def process_sensor_data(self, event):
        print(f"  Processing sensor data: {event.data}")
    
    async def process_anomaly(self, event):
        print(f"  ⚠️  Processing anomaly: {event.data}")
```

---

## Event vs Other Primitives

### When to Use Event

**✅ Use Event when:**
- Broadcasting signals to multiple tasks
- Coordinating startup/shutdown
- Implementing pub-sub patterns
- Signaling state changes
- Blackboard/event-driven architectures

**❌ Don't use Event when:**
- Need to protect shared data → Use `Lock`
- Need to pass data between tasks → Use `Queue`
- Need to limit concurrency → Use `Semaphore`
- Need complex waiting conditions → Use `Condition`

### Comparison

```python
# Event: One-to-many signaling
event = asyncio.Event()
await event.wait()  # Multiple tasks can wait
event.set()         # Wakes all waiters

# Lock: Mutual exclusion
lock = asyncio.Lock()
async with lock:    # Only one task at a time
    # Critical section
    pass

# Queue: Data passing
queue = asyncio.Queue()
await queue.put(data)   # Producer
data = await queue.get()  # Consumer

# Semaphore: Concurrency limiting
sem = asyncio.Semaphore(3)
async with sem:     # Max 3 tasks simultaneously
    pass
```

---

## Common Patterns

### Pattern 1: Barrier (Wait for All)

```python
import asyncio

class Barrier:
    """Wait for N tasks to reach a point"""
    
    def __init__(self, n):
        self.n = n
        self.count = 0
        self.event = asyncio.Event()
        self.lock = asyncio.Lock()
    
    async def wait(self):
        async with self.lock:
            self.count += 1
            if self.count >= self.n:
                self.event.set()
        
        await self.event.wait()

async def demo_barrier():
    barrier = Barrier(3)
    
    async def worker(worker_id, delay):
        print(f"Worker {worker_id}: Working...")
        await asyncio.sleep(delay)
        print(f"Worker {worker_id}: Reached barrier")
        await barrier.wait()
        print(f"Worker {worker_id}: Passed barrier")
    
    await asyncio.gather(
        worker(1, 1.0),
        worker(2, 2.0),
        worker(3, 3.0)
    )

asyncio.run(demo_barrier())
```

### Pattern 2: One-Shot Event

```python
import asyncio

class OneShotEvent:
    """Event that can only be set once"""
    
    def __init__(self):
        self.event = asyncio.Event()
        self._set = False
    
    def set(self):
        if not self._set:
            self._set = True
            self.event.set()
    
    async def wait(self):
        await self.event.wait()
    
    def is_set(self):
        return self._set
```

### Pattern 3: Pulsing Event

```python
import asyncio

class PulsingEvent:
    """Event that pulses (set then immediately clear)"""
    
    def __init__(self):
        self.event = asyncio.Event()
    
    async def pulse(self):
        """Send a pulse to all waiters"""
        self.event.set()
        await asyncio.sleep(0)  # Let waiters wake
        self.event.clear()
    
    async def wait(self):
        await self.event.wait()
```

---

## Best Practices

### ✅ DO: Use events for coordination, not data passing

```python
# Good: Signal coordination
ready_event = asyncio.Event()
await ready_event.wait()

# Bad: Trying to pass data through events
# Use Queue instead
```

### ✅ DO: Clear events when appropriate

```python
# Pattern: Reusable event
event = asyncio.Event()

# Use
event.set()
await process()
event.clear()  # Reset for next use
```

### ✅ DO: Use events for broadcast notifications

```python
# Good: Notify all subscribers
shutdown_event = asyncio.Event()
shutdown_event.set()  # All tasks notified
```

### ❌ DON'T: Forget that events stay set

```python
# Bad: Assuming event auto-clears
event.set()
await event.wait()  # Returns immediately
await event.wait()  # Still returns immediately!

# Good: Clear when needed
event.set()
await event.wait()
event.clear()  # Reset
```

### ✅ DO: Document event semantics

```python
class Service:
    """
    Service with lifecycle events.
    
    Events:
        ready: Set when service is ready to accept requests
        shutdown: Set when service should shut down
    """
    def __init__(self):
        self.ready = asyncio.Event()
        self.shutdown = asyncio.Event()
```

---

## Summary: Event Mental Model

✅ **Events signal between tasks** - coordination without data

✅ **`set()` broadcasts** to all waiting tasks

✅ **Events stay set** until explicitly cleared

✅ **Perfect for pub-sub** and blackboard patterns

✅ **Use for coordination** - startup, shutdown, state changes

✅ **Not for data passing** - use Queue instead

✅ **Not for mutual exclusion** - use Lock instead

---

## What's Next?

Events are for simple signaling. For complex coordination with predicates, we need Conditions.

In [Chapter 11: Condition - Complex Coordination](./11-condition.md), we'll cover:
- Lock + wait model
- `notify()` vs `notify_all()`
- Predicate loops and why they're necessary
- Producer-consumer with conditions
- Bounded buffer implementation
- Blackboard pattern with Conditions (more powerful than Events)

Conditions combine locks and events for sophisticated coordination.

---

**Previous:** [← Chapter 9: Lock - Mutual Exclusion](./09-lock.md)  
**Next:** [Chapter 11: Condition - Complex Coordination →](./11-condition.md)