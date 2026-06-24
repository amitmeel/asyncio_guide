# Chapter 21: Blackboard Systems

## Overview

The **blackboard pattern** is a powerful architectural pattern for building event-driven systems where multiple independent components (knowledge sources) collaborate by reading from and writing to a shared data structure (the blackboard). This pattern excels at problems requiring:

- **Opportunistic problem solving**: Components react to changes as they occur
- **Heterogeneous expertise**: Different specialists contribute their knowledge
- **Dynamic coordination**: No fixed control flow; components self-organize
- **Incremental refinement**: Solutions emerge through iterative contributions

In asyncio, blackboard systems leverage queues, events, and conditions to coordinate multiple concurrent workers that subscribe to specific event types and react accordingly.

## Mental Model

Think of a blackboard system like a **collaborative workspace**:

```
┌─────────────────────────────────────────────────────────────┐
│                      BLACKBOARD                              │
│  ┌────────────────────────────────────────────────────┐    │
│  │  Shared State: Events, Data, Hypotheses            │    │
│  └────────────────────────────────────────────────────┘    │
│                           ↕                                  │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  │
│  │ Worker 1 │  │ Worker 2 │  │ Worker 3 │  │ Worker N │  │
│  │ (Parser) │  │(Analyzer)│  │(Planner) │  │ (Critic) │  │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘  │
│       ↓              ↓              ↓              ↓        │
│  Subscribe to   Subscribe to   Subscribe to   Subscribe to │
│  "parse"        "analyze"      "plan"         "critique"   │
└─────────────────────────────────────────────────────────────┘
```

**Key concepts:**

1. **Blackboard**: Shared data structure holding current state
2. **Knowledge Sources (KS)**: Independent workers that read/write blackboard
3. **Control Component**: Decides which KS to activate (can be implicit)
4. **Event Bus**: Notification mechanism for state changes

## Core Components

### 1. Event Types and Messages

```python
from dataclasses import dataclass
from typing import Any, Dict
from enum import Enum, auto

class EventType(Enum):
    """Types of events that can be posted to blackboard"""
    DATA_RECEIVED = auto()
    ANALYSIS_COMPLETE = auto()
    HYPOTHESIS_FORMED = auto()
    PLAN_READY = auto()
    CRITIQUE_AVAILABLE = auto()
    SOLUTION_FOUND = auto()
    ERROR_OCCURRED = auto()

@dataclass
class BlackboardEvent:
    """Event posted to blackboard"""
    event_type: EventType
    data: Dict[str, Any]
    source: str  # Which component posted this
    timestamp: float
    
    def __repr__(self):
        return f"Event({self.event_type.name}, from={self.source})"
```

### 2. Basic Blackboard with Queue

The simplest blackboard uses a queue for event distribution:

```python
import asyncio
from typing import Dict, Set, Callable, Awaitable
from collections import defaultdict
import time

class QueueBasedBlackboard:
    """
    Blackboard using queues for event distribution.
    
    Each subscriber gets its own queue. When an event is posted,
    it's copied to all queues of subscribers interested in that event type.
    """
    
    def __init__(self):
        self.state: Dict[str, Any] = {}  # Shared state
        self.subscribers: Dict[EventType, Set[asyncio.Queue]] = defaultdict(set)
        self._lock = asyncio.Lock()
    
    async def subscribe(self, event_type: EventType) -> asyncio.Queue:
        """
        Subscribe to specific event type.
        Returns a queue that will receive matching events.
        """
        queue = asyncio.Queue()
        async with self._lock:
            self.subscribers[event_type].add(queue)
        return queue
    
    async def unsubscribe(self, event_type: EventType, queue: asyncio.Queue):
        """Unsubscribe from event type"""
        async with self._lock:
            self.subscribers[event_type].discard(queue)
    
    async def post_event(self, event: BlackboardEvent):
        """
        Post event to blackboard.
        All subscribers to this event type will receive it.
        """
        async with self._lock:
            # Update shared state if needed
            if 'state_update' in event.data:
                self.state.update(event.data['state_update'])
            
            # Distribute to subscribers
            queues = self.subscribers[event.event_type]
            for queue in queues:
                await queue.put(event)
    
    async def get_state(self, key: str) -> Any:
        """Read from shared state"""
        async with self._lock:
            return self.state.get(key)
    
    async def update_state(self, updates: Dict[str, Any]):
        """Update shared state"""
        async with self._lock:
            self.state.update(updates)
```

### 3. Knowledge Source (Worker) Pattern

```python
class KnowledgeSource:
    """
    Base class for knowledge sources (workers).
    
    Each KS:
    1. Subscribes to specific event types
    2. Waits for events in a loop
    3. Processes events and posts results back
    """
    
    def __init__(self, name: str, blackboard: QueueBasedBlackboard):
        self.name = name
        self.blackboard = blackboard
        self.subscribed_events: Set[EventType] = set()
        self.queues: Dict[EventType, asyncio.Queue] = {}
    
    async def subscribe(self, *event_types: EventType):
        """Subscribe to one or more event types"""
        for event_type in event_types:
            if event_type not in self.subscribed_events:
                queue = await self.blackboard.subscribe(event_type)
                self.queues[event_type] = queue
                self.subscribed_events.add(event_type)
    
    async def wait_for_event(self) -> BlackboardEvent:
        """
        Wait for any subscribed event.
        Returns the first event received from any queue.
        """
        if not self.queues:
            raise RuntimeError(f"{self.name}: No subscriptions")
        
        # Wait for first event from any queue
        pending = [queue.get() for queue in self.queues.values()]
        done, pending = await asyncio.wait(
            pending,
            return_when=asyncio.FIRST_COMPLETED
        )
        
        # Cancel remaining waits
        for task in pending:
            task.cancel()
        
        # Return the event
        return done.pop().result()
    
    async def post_event(self, event_type: EventType, data: Dict[str, Any]):
        """Post event to blackboard"""
        event = BlackboardEvent(
            event_type=event_type,
            data=data,
            source=self.name,
            timestamp=time.time()
        )
        await self.blackboard.post_event(event)
    
    async def process_event(self, event: BlackboardEvent):
        """Override this to implement KS logic"""
        raise NotImplementedError
    
    async def run(self):
        """Main loop: wait for events and process them"""
        print(f"[{self.name}] Starting, subscribed to: {[e.name for e in self.subscribed_events]}")
        
        try:
            while True:
                event = await self.wait_for_event()
                print(f"[{self.name}] Received {event}")
                await self.process_event(event)
        except asyncio.CancelledError:
            print(f"[{self.name}] Shutting down")
            raise
```

## Example 1: Data Processing Pipeline

A realistic example showing how multiple workers collaborate to process data:

```python
class DataParser(KnowledgeSource):
    """Parses raw data and posts structured data"""
    
    async def process_event(self, event: BlackboardEvent):
        if event.event_type == EventType.DATA_RECEIVED:
            raw_data = event.data['raw']
            
            # Simulate parsing
            await asyncio.sleep(0.1)
            
            parsed = {
                'values': [int(x) for x in raw_data.split(',')],
                'count': len(raw_data.split(','))
            }
            
            await self.post_event(
                EventType.ANALYSIS_COMPLETE,
                {'parsed': parsed}
            )

class DataAnalyzer(KnowledgeSource):
    """Analyzes parsed data and forms hypotheses"""
    
    async def process_event(self, event: BlackboardEvent):
        if event.event_type == EventType.ANALYSIS_COMPLETE:
            parsed = event.data['parsed']
            values = parsed['values']
            
            # Simulate analysis
            await asyncio.sleep(0.15)
            
            hypothesis = {
                'mean': sum(values) / len(values),
                'max': max(values),
                'min': min(values),
                'trend': 'increasing' if values[-1] > values[0] else 'decreasing'
            }
            
            await self.post_event(
                EventType.HYPOTHESIS_FORMED,
                {'hypothesis': hypothesis}
            )

class Planner(KnowledgeSource):
    """Creates action plans based on hypotheses"""
    
    async def process_event(self, event: BlackboardEvent):
        if event.event_type == EventType.HYPOTHESIS_FORMED:
            hypothesis = event.data['hypothesis']
            
            # Simulate planning
            await asyncio.sleep(0.1)
            
            plan = {
                'action': 'scale_up' if hypothesis['trend'] == 'increasing' else 'scale_down',
                'confidence': 0.85,
                'reason': f"Trend is {hypothesis['trend']}, mean={hypothesis['mean']:.2f}"
            }
            
            await self.post_event(
                EventType.PLAN_READY,
                {'plan': plan}
            )

class Critic(KnowledgeSource):
    """Evaluates plans and provides feedback"""
    
    async def process_event(self, event: BlackboardEvent):
        if event.event_type == EventType.PLAN_READY:
            plan = event.data['plan']
            
            # Simulate critique
            await asyncio.sleep(0.05)
            
            critique = {
                'approved': plan['confidence'] > 0.8,
                'feedback': 'Plan looks good' if plan['confidence'] > 0.8 else 'Needs revision'
            }
            
            if critique['approved']:
                await self.post_event(
                    EventType.SOLUTION_FOUND,
                    {'solution': plan, 'critique': critique}
                )
            else:
                await self.post_event(
                    EventType.CRITIQUE_AVAILABLE,
                    {'critique': critique, 'original_plan': plan}
                )

async def run_pipeline_example():
    """Run the data processing pipeline"""
    blackboard = QueueBasedBlackboard()
    
    # Create knowledge sources
    parser = DataParser("Parser", blackboard)
    await parser.subscribe(EventType.DATA_RECEIVED)
    
    analyzer = DataAnalyzer("Analyzer", blackboard)
    await analyzer.subscribe(EventType.ANALYSIS_COMPLETE)
    
    planner = Planner("Planner", blackboard)
    await planner.subscribe(EventType.HYPOTHESIS_FORMED)
    
    critic = Critic("Critic", blackboard)
    await critic.subscribe(EventType.PLAN_READY)
    
    # Start all workers
    async with asyncio.TaskGroup() as tg:
        tg.create_task(parser.run())
        tg.create_task(analyzer.run())
        tg.create_task(planner.run())
        tg.create_task(critic.run())
        
        # Inject initial data
        await asyncio.sleep(0.1)
        await blackboard.post_event(BlackboardEvent(
            event_type=EventType.DATA_RECEIVED,
            data={'raw': '10,15,20,25,30'},
            source='external',
            timestamp=time.time()
        ))
        
        # Let it run for a bit
        await asyncio.sleep(2)

# Output shows the flow:
# [Parser] Starting, subscribed to: ['DATA_RECEIVED']
# [Analyzer] Starting, subscribed to: ['ANALYSIS_COMPLETE']
# [Planner] Starting, subscribed to: ['HYPOTHESIS_FORMED']
# [Critic] Starting, subscribed to: ['PLAN_READY']
# [Parser] Received Event(DATA_RECEIVED, from=external)
# [Analyzer] Received Event(ANALYSIS_COMPLETE, from=Parser)
# [Planner] Received Event(HYPOTHESIS_FORMED, from=Analyzer)
# [Critic] Received Event(PLAN_READY, from=Planner)
```

## Event-Based Blackboard with asyncio.Event

For simpler notification patterns, use `asyncio.Event`:

```python
class EventBasedBlackboard:
    """
    Blackboard using asyncio.Event for notifications.
    
    Subscribers wait on events, then check if new data matches their interest.
    More efficient than queues when events are frequent.
    """
    
    def __init__(self):
        self.state: Dict[str, Any] = {}
        self.events: Dict[EventType, asyncio.Event] = {
            event_type: asyncio.Event() for event_type in EventType
        }
        self.event_history: list[BlackboardEvent] = []
        self._lock = asyncio.Lock()
    
    async def post_event(self, event: BlackboardEvent):
        """Post event and notify all waiters"""
        async with self._lock:
            self.event_history.append(event)
            if 'state_update' in event.data:
                self.state.update(event.data['state_update'])
        
        # Notify all waiters for this event type
        self.events[event.event_type].set()
        
        # Clear immediately so next wait() will block
        await asyncio.sleep(0)  # Let waiters wake up
        self.events[event.event_type].clear()
    
    async def wait_for_event(self, event_type: EventType) -> BlackboardEvent:
        """
        Wait for specific event type.
        Returns the most recent event of that type.
        """
        await self.events[event_type].wait()
        
        # Find most recent event of this type
        async with self._lock:
            for event in reversed(self.event_history):
                if event.event_type == event_type:
                    return event
        
        raise RuntimeError("Event disappeared")
    
    async def wait_for_any(self, *event_types: EventType) -> BlackboardEvent:
        """Wait for any of the specified event types"""
        events = [self.events[et] for et in event_types]
        
        # Wait for any event
        done, pending = await asyncio.wait(
            [asyncio.create_task(e.wait()) for e in events],
            return_when=asyncio.FIRST_COMPLETED
        )
        
        # Cancel pending
        for task in pending:
            task.cancel()
        
        # Find which event fired
        async with self._lock:
            for event in reversed(self.event_history):
                if event.event_type in event_types:
                    return event
        
        raise RuntimeError("Event disappeared")
```

### Example: Event-Based Monitoring System

```python
class MetricsCollector(KnowledgeSource):
    """Collects system metrics periodically"""
    
    async def run(self):
        print(f"[{self.name}] Starting metrics collection")
        try:
            while True:
                # Simulate collecting metrics
                await asyncio.sleep(1)
                
                metrics = {
                    'cpu': 45.2,
                    'memory': 78.5,
                    'disk': 62.1
                }
                
                await self.post_event(
                    EventType.DATA_RECEIVED,
                    {'metrics': metrics}
                )
        except asyncio.CancelledError:
            print(f"[{self.name}] Shutting down")
            raise

class AnomalyDetector(KnowledgeSource):
    """Detects anomalies in metrics"""
    
    async def process_event(self, event: BlackboardEvent):
        if event.event_type == EventType.DATA_RECEIVED:
            metrics = event.data['metrics']
            
            # Check for anomalies
            anomalies = []
            if metrics['cpu'] > 80:
                anomalies.append('High CPU usage')
            if metrics['memory'] > 90:
                anomalies.append('High memory usage')
            
            if anomalies:
                await self.post_event(
                    EventType.ERROR_OCCURRED,
                    {'anomalies': anomalies, 'metrics': metrics}
                )

class AlertManager(KnowledgeSource):
    """Sends alerts for errors"""
    
    async def process_event(self, event: BlackboardEvent):
        if event.event_type == EventType.ERROR_OCCURRED:
            anomalies = event.data['anomalies']
            print(f"[{self.name}] ALERT: {', '.join(anomalies)}")
            
            # In real system, would send email/SMS/Slack
            await asyncio.sleep(0.1)
```

## Condition-Based Blackboard (Most Powerful)

For complex coordination, use `asyncio.Condition`:

```python
class ConditionBasedBlackboard:
    """
    Blackboard using asyncio.Condition for fine-grained coordination.
    
    Allows subscribers to wait for specific predicates on the blackboard state.
    Most flexible but requires careful predicate design.
    """
    
    def __init__(self):
        self.state: Dict[str, Any] = {}
        self.event_history: list[BlackboardEvent] = []
        self.condition = asyncio.Condition()
    
    async def post_event(self, event: BlackboardEvent):
        """Post event and notify all waiters"""
        async with self.condition:
            self.event_history.append(event)
            if 'state_update' in event.data:
                self.state.update(event.data['state_update'])
            
            # Notify all waiters to check their predicates
            self.condition.notify_all()
    
    async def wait_for_predicate(self, predicate: Callable[[], bool], timeout: float = None):
        """
        Wait until predicate becomes true.
        
        Predicate is called with no arguments and should check self.state.
        """
        async with self.condition:
            await asyncio.wait_for(
                self.condition.wait_for(predicate),
                timeout=timeout
            )
    
    async def wait_for_event_type(self, event_type: EventType) -> BlackboardEvent:
        """Wait for specific event type"""
        async with self.condition:
            # Check if event already exists
            for event in reversed(self.event_history):
                if event.event_type == event_type:
                    return event
            
            # Wait for new event
            initial_len = len(self.event_history)
            
            def predicate():
                if len(self.event_history) > initial_len:
                    return self.event_history[-1].event_type == event_type
                return False
            
            await self.condition.wait_for(predicate)
            return self.event_history[-1]
    
    async def wait_for_state(self, key: str, value: Any, timeout: float = None):
        """Wait until state[key] == value"""
        def predicate():
            return self.state.get(key) == value
        
        await self.wait_for_predicate(predicate, timeout)
    
    async def get_state_when(self, key: str, predicate: Callable[[Any], bool]) -> Any:
        """Get state value when predicate is satisfied"""
        async with self.condition:
            def check():
                val = self.state.get(key)
                return val is not None and predicate(val)
            
            await self.condition.wait_for(check)
            return self.state[key]
```

## Example 2: Multi-Agent Planning System

A sophisticated example showing how multiple agents collaborate on complex planning:

```python
from typing import List, Optional
from dataclasses import dataclass, field

@dataclass
class Task:
    """Represents a task to be planned"""
    id: str
    description: str
    dependencies: List[str] = field(default_factory=list)
    assigned_to: Optional[str] = None
    status: str = "pending"  # pending, planned, executing, complete

class TaskDecomposer(KnowledgeSource):
    """Breaks down complex tasks into subtasks"""
    
    async def process_event(self, event: BlackboardEvent):
        if event.event_type == EventType.DATA_RECEIVED:
            task_desc = event.data.get('task_description')
            if task_desc:
                # Simulate task decomposition
                await asyncio.sleep(0.2)
                
                subtasks = [
                    Task(id="t1", description="Gather requirements"),
                    Task(id="t2", description="Design solution", dependencies=["t1"]),
                    Task(id="t3", description="Implement", dependencies=["t2"]),
                    Task(id="t4", description="Test", dependencies=["t3"]),
                ]
                
                await self.post_event(
                    EventType.ANALYSIS_COMPLETE,
                    {
                        'subtasks': subtasks,
                        'state_update': {'tasks': {t.id: t for t in subtasks}}
                    }
                )

class ResourceAllocator(KnowledgeSource):
    """Assigns tasks to available resources"""
    
    def __init__(self, name: str, blackboard: ConditionBasedBlackboard):
        super().__init__(name, blackboard)
        self.available_agents = ["agent1", "agent2", "agent3"]
    
    async def process_event(self, event: BlackboardEvent):
        if event.event_type == EventType.ANALYSIS_COMPLETE:
            subtasks = event.data['subtasks']
            
            # Simulate resource allocation
            await asyncio.sleep(0.15)
            
            assignments = {}
            for i, task in enumerate(subtasks):
                agent = self.available_agents[i % len(self.available_agents)]
                task.assigned_to = agent
                assignments[task.id] = agent
            
            await self.post_event(
                EventType.PLAN_READY,
                {
                    'assignments': assignments,
                    'state_update': {'assignments': assignments}
                }
            )

class DependencyChecker(KnowledgeSource):
    """Validates task dependencies and ordering"""
    
    async def process_event(self, event: BlackboardEvent):
        if event.event_type == EventType.PLAN_READY:
            # Get current tasks from blackboard
            tasks_dict = await self.blackboard.get_state('tasks')
            if not tasks_dict:
                return
            
            # Check for circular dependencies
            await asyncio.sleep(0.1)
            
            # Simple validation (in real system, would use topological sort)
            valid = True
            issues = []
            
            for task_id, task in tasks_dict.items():
                for dep_id in task.dependencies:
                    if dep_id not in tasks_dict:
                        valid = False
                        issues.append(f"Task {task_id} depends on non-existent {dep_id}")
            
            if valid:
                await self.post_event(
                    EventType.HYPOTHESIS_FORMED,
                    {'validation': 'passed', 'message': 'All dependencies valid'}
                )
            else:
                await self.post_event(
                    EventType.ERROR_OCCURRED,
                    {'validation': 'failed', 'issues': issues}
                )

class ExecutionCoordinator(KnowledgeSource):
    """Coordinates task execution based on dependencies"""
    
    async def process_event(self, event: BlackboardEvent):
        if event.event_type == EventType.HYPOTHESIS_FORMED:
            if event.data.get('validation') == 'passed':
                # Get tasks and start execution
                tasks_dict = await self.blackboard.get_state('tasks')
                
                # Find tasks with no dependencies (can start immediately)
                ready_tasks = [
                    task for task in tasks_dict.values()
                    if not task.dependencies and task.status == "pending"
                ]
                
                if ready_tasks:
                    await self.post_event(
                        EventType.SOLUTION_FOUND,
                        {
                            'ready_tasks': [t.id for t in ready_tasks],
                            'message': f'Ready to execute {len(ready_tasks)} tasks'
                        }
                    )

async def run_planning_system():
    """Run the multi-agent planning system"""
    blackboard = ConditionBasedBlackboard()
    
    # Create agents
    decomposer = TaskDecomposer("Decomposer", blackboard)
    await decomposer.subscribe(EventType.DATA_RECEIVED)
    
    allocator = ResourceAllocator("Allocator", blackboard)
    await allocator.subscribe(EventType.ANALYSIS_COMPLETE)
    
    checker = DependencyChecker("Checker", blackboard)
    await checker.subscribe(EventType.PLAN_READY)
    
    coordinator = ExecutionCoordinator("Coordinator", blackboard)
    await coordinator.subscribe(EventType.HYPOTHESIS_FORMED)
    
    # Start all agents
    async with asyncio.TaskGroup() as tg:
        tg.create_task(decomposer.run())
        tg.create_task(allocator.run())
        tg.create_task(checker.run())
        tg.create_task(coordinator.run())
        
        # Inject planning request
        await asyncio.sleep(0.1)
        await blackboard.post_event(BlackboardEvent(
            event_type=EventType.DATA_RECEIVED,
            data={'task_description': 'Build new feature'},
            source='user',
            timestamp=time.time()
        ))
        
        # Wait for solution
        await blackboard.wait_for_event_type(EventType.SOLUTION_FOUND)
        print("\n✓ Planning complete!")
        
        await asyncio.sleep(0.5)
```

## Example 3: Real-Time Collaborative Editor

Shows how blackboard pattern handles concurrent edits:

```python
from dataclasses import dataclass
from typing import Tuple

@dataclass
class Edit:
    """Represents a text edit operation"""
    user: str
    position: int
    operation: str  # 'insert' or 'delete'
    text: str
    timestamp: float

class CollaborativeDocument:
    """Shared document on blackboard"""
    
    def __init__(self):
        self.content: str = ""
        self.version: int = 0
        self.edit_history: List[Edit] = []
    
    def apply_edit(self, edit: Edit) -> bool:
        """Apply edit to document"""
        try:
            if edit.operation == 'insert':
                self.content = (
                    self.content[:edit.position] +
                    edit.text +
                    self.content[edit.position:]
                )
            elif edit.operation == 'delete':
                end = edit.position + len(edit.text)
                self.content = (
                    self.content[:edit.position] +
                    self.content[end:]
                )
            
            self.version += 1
            self.edit_history.append(edit)
            return True
        except Exception:
            return False

class EditorBlackboard(ConditionBasedBlackboard):
    """Specialized blackboard for collaborative editing"""
    
    def __init__(self):
        super().__init__()
        self.state['document'] = CollaborativeDocument()
    
    async def apply_edit(self, edit: Edit) -> bool:
        """Apply edit with conflict detection"""
        async with self.condition:
            doc = self.state['document']
            success = doc.apply_edit(edit)
            
            if success:
                # Notify all editors
                await self.post_event(BlackboardEvent(
                    event_type=EventType.DATA_RECEIVED,
                    data={'edit': edit, 'version': doc.version},
                    source=edit.user,
                    timestamp=edit.timestamp
                ))
            
            return success
    
    async def get_document_version(self) -> int:
        """Get current document version"""
        async with self.condition:
            return self.state['document'].version
    
    async def wait_for_version(self, version: int):
        """Wait until document reaches specific version"""
        async with self.condition:
            def predicate():
                return self.state['document'].version >= version
            await self.condition.wait_for(predicate)

class EditorClient(KnowledgeSource):
    """Represents a user editing the document"""
    
    def __init__(self, name: str, blackboard: EditorBlackboard):
        super().__init__(name, blackboard)
        self.local_version = 0
    
    async def make_edit(self, position: int, operation: str, text: str):
        """Make an edit to the document"""
        edit = Edit(
            user=self.name,
            position=position,
            operation=operation,
            text=text,
            timestamp=time.time()
        )
        
        success = await self.blackboard.apply_edit(edit)
        if success:
            self.local_version += 1
            print(f"[{self.name}] Applied edit: {operation} '{text}' at {position}")
        else:
            print(f"[{self.name}] Edit failed!")
    
    async def process_event(self, event: BlackboardEvent):
        """Receive edits from other users"""
        if event.event_type == EventType.DATA_RECEIVED:
            edit = event.data['edit']
            if edit.user != self.name:
                print(f"[{self.name}] Received edit from {edit.user}: {edit.operation} '{edit.text}'")
                self.local_version = event.data['version']

async def run_collaborative_editor():
    """Run collaborative editor simulation"""
    blackboard = EditorBlackboard()
    
    # Create editor clients
    alice = EditorClient("Alice", blackboard)
    await alice.subscribe(EventType.DATA_RECEIVED)
    
    bob = EditorClient("Bob", blackboard)
    await bob.subscribe(EventType.DATA_RECEIVED)
    
    carol = EditorClient("Carol", blackboard)
    await carol.subscribe(EventType.DATA_RECEIVED)
    
    # Start all clients
    async with asyncio.TaskGroup() as tg:
        tg.create_task(alice.run())
        tg.create_task(bob.run())
        tg.create_task(carol.run())
        
        # Simulate concurrent edits
        await asyncio.sleep(0.1)
        
        await alice.make_edit(0, 'insert', 'Hello ')
        await asyncio.sleep(0.05)
        
        await bob.make_edit(6, 'insert', 'World')
        await asyncio.sleep(0.05)
        
        await carol.make_edit(11, 'insert', '!')
        await asyncio.sleep(0.05)
        
        await alice.make_edit(6, 'insert', 'Beautiful ')
        
        # Wait for all edits to propagate
        await blackboard.wait_for_version(4)
        
        # Show final document
        doc = blackboard.state['document']
        print(f"\nFinal document: '{doc.content}'")
        print(f"Version: {doc.version}")
        
        await asyncio.sleep(0.5)
```

## Advanced Pattern: Priority-Based Blackboard

For systems where some events are more urgent:

```python
import heapq
from typing import Tuple

class PriorityBlackboard:
    """Blackboard with priority-based event processing"""
    
    def __init__(self):
        self.state: Dict[str, Any] = {}
        self.priority_queue: List[Tuple[int, float, BlackboardEvent]] = []
        self.condition = asyncio.Condition()
        self.event_counter = 0  # For stable sorting
    
    async def post_event(self, event: BlackboardEvent, priority: int = 5):
        """
        Post event with priority (lower number = higher priority).
        Priority 0 = critical, 5 = normal, 10 = low
        """
        async with self.condition:
            # Use counter for stable sort (FIFO within same priority)
            heapq.heappush(
                self.priority_queue,
                (priority, self.event_counter, event)
            )
            self.event_counter += 1
            
            if 'state_update' in event.data:
                self.state.update(event.data['state_update'])
            
            self.condition.notify_all()
    
    async def get_next_event(self, timeout: float = None) -> Tuple[int, BlackboardEvent]:
        """Get highest priority event"""
        async with self.condition:
            while not self.priority_queue:
                await asyncio.wait_for(
                    self.condition.wait(),
                    timeout=timeout
                )
            
            priority, _, event = heapq.heappop(self.priority_queue)
            return priority, event
    
    async def peek_priority(self) -> Optional[int]:
        """Check priority of next event without removing it"""
        async with self.condition:
            if self.priority_queue:
                return self.priority_queue[0][0]
            return None

class PriorityWorker(KnowledgeSource):
    """Worker that processes events by priority"""
    
    def __init__(self, name: str, blackboard: PriorityBlackboard):
        super().__init__(name, blackboard)
        self.blackboard: PriorityBlackboard = blackboard
    
    async def run(self):
        print(f"[{self.name}] Starting priority-based processing")
        try:
            while True:
                priority, event = await self.blackboard.get_next_event()
                print(f"[{self.name}] Processing priority {priority}: {event}")
                await self.process_event(event)
        except asyncio.CancelledError:
            print(f"[{self.name}] Shutting down")
            raise
```

## When to Use Blackboard Pattern

### ✅ Use When:

1. **Multiple independent solvers**: Different components contribute different expertise
2. **Opportunistic problem solving**: No fixed algorithm; solution emerges
3. **Incremental refinement**: Solution built up through iterations
4. **Dynamic coordination**: Components self-organize based on current state
5. **Heterogeneous data**: Different types of information need integration
6. **Exploratory problems**: Solution path not known in advance

**Examples:**
- Speech recognition (acoustic, linguistic, semantic analysis)
- Image understanding (edge detection, object recognition, scene interpretation)
- Planning systems (task decomposition, resource allocation, scheduling)
- Collaborative editing (concurrent edits, conflict resolution)
- Diagnostic systems (symptom analysis, hypothesis formation, testing)

### ❌ Avoid When:

1. **Simple linear workflows**: Use pipelines instead
2. **Fixed control flow**: Use state machines
3. **Request-response**: Use worker pools
4. **Strict ordering required**: Use queues with sequential processing
5. **Low latency critical**: Blackboard adds coordination overhead

## Performance Considerations

### Queue-Based vs Event-Based vs Condition-Based

```python
# Queue-Based: Best for high-throughput, independent events
# - Each subscriber gets own queue
# - No contention between subscribers
# - Higher memory usage (event copied to each queue)
# - Best when: Many events, few subscribers per event type

# Event-Based: Best for simple notifications
# - Single event per type
# - All waiters wake up simultaneously
# - Lower memory usage
# - Best when: Infrequent events, many subscribers

# Condition-Based: Best for complex predicates
# - Most flexible
# - Can wait for arbitrary conditions
# - Higher CPU usage (all waiters check predicate)
# - Best when: Complex coordination, state-dependent waiting
```

### Optimization Strategies

```python
class OptimizedBlackboard:
    """Blackboard with performance optimizations"""
    
    def __init__(self, max_history: int = 1000):
        self.state: Dict[str, Any] = {}
        self.subscribers: Dict[EventType, Set[asyncio.Queue]] = defaultdict(set)
        self.event_history: deque = deque(maxlen=max_history)  # Bounded history
        self._lock = asyncio.Lock()
        self._stats = {
            'events_posted': 0,
            'events_delivered': 0,
            'subscribers_count': 0
        }
    
    async def post_event_batch(self, events: List[BlackboardEvent]):
        """Post multiple events atomically"""
        async with self._lock:
            for event in events:
                self.event_history.append(event)
                if 'state_update' in event.data:
                    self.state.update(event.data['state_update'])
                
                # Batch delivery
                queues = self.subscribers[event.event_type]
                for queue in queues:
                    await queue.put(event)
                
                self._stats['events_posted'] += 1
                self._stats['events_delivered'] += len(queues)
    
    async def get_stats(self) -> Dict[str, int]:
        """Get performance statistics"""
        async with self._lock:
            return self._stats.copy()
```

## Testing Blackboard Systems

```python
import pytest

class TestBlackboard:
    """Test suite for blackboard systems"""
    
    @pytest.mark.asyncio
    async def test_event_delivery(self):
        """Test that events are delivered to subscribers"""
        blackboard = QueueBasedBlackboard()
        
        # Subscribe
        queue = await blackboard.subscribe(EventType.DATA_RECEIVED)
        
        # Post event
        event = BlackboardEvent(
            event_type=EventType.DATA_RECEIVED,
            data={'test': 'data'},
            source='test',
            timestamp=time.time()
        )
        await blackboard.post_event(event)
        
        # Verify delivery
        received = await asyncio.wait_for(queue.get(), timeout=1.0)
        assert received.event_type == EventType.DATA_RECEIVED
        assert received.data['test'] == 'data'
    
    @pytest.mark.asyncio
    async def test_multiple_subscribers(self):
        """Test that all subscribers receive events"""
        blackboard = QueueBasedBlackboard()
        
        # Multiple subscribers
        queue1 = await blackboard.subscribe(EventType.DATA_RECEIVED)
        queue2 = await blackboard.subscribe(EventType.DATA_RECEIVED)
        queue3 = await blackboard.subscribe(EventType.DATA_RECEIVED)
        
        # Post event
        event = BlackboardEvent(
            event_type=EventType.DATA_RECEIVED,
            data={'test': 'broadcast'},
            source='test',
            timestamp=time.time()
        )
        await blackboard.post_event(event)
        
        # All should receive
        received1 = await asyncio.wait_for(queue1.get(), timeout=1.0)
        received2 = await asyncio.wait_for(queue2.get(), timeout=1.0)
        received3 = await asyncio.wait_for(queue3.get(), timeout=1.0)
        
        assert received1.data['test'] == 'broadcast'
        assert received2.data['test'] == 'broadcast'
        assert received3.data['test'] == 'broadcast'
    
    @pytest.mark.asyncio
    async def test_condition_predicate(self):
        """Test condition-based waiting"""
        blackboard = ConditionBasedBlackboard()
        
        # Start waiter
        async def waiter():
            await blackboard.wait_for_state('status', 'ready', timeout=2.0)
            return True
        
        waiter_task = asyncio.create_task(waiter())
        
        # Update state
        await asyncio.sleep(0.1)
        await blackboard.post_event(BlackboardEvent(
            event_type=EventType.DATA_RECEIVED,
            data={'state_update': {'status': 'ready'}},
            source='test',
            timestamp=time.time()
        ))
        
        # Waiter should complete
        result = await asyncio.wait_for(waiter_task, timeout=3.0)
        assert result is True
```

## Mental Model Summary

**Blackboard pattern is like a collaborative workspace:**

1. **Shared blackboard**: Central data structure visible to all
2. **Knowledge sources**: Independent workers with specialized skills
3. **Event bus**: Notification mechanism for changes
4. **Opportunistic execution**: Workers activate when they can contribute

**Key insights:**

- **No central controller**: System self-organizes
- **Incremental progress**: Solution emerges through contributions
- **Flexible coordination**: Workers decide when to act
- **Heterogeneous expertise**: Different workers, different capabilities

**Choose implementation based on needs:**
- **Queue-based**: High throughput, independent events
- **Event-based**: Simple notifications, many subscribers
- **Condition-based**: Complex predicates, state-dependent waiting

**Common pitfalls:**
- **Event storms**: Too many events overwhelm system
- **Starvation**: Some workers never get to contribute
- **Deadlock**: Circular dependencies in waiting
- **Memory leaks**: Unbounded event history

**Best practices:**
- Bound event history
- Use priorities for critical events
- Implement timeouts on waits
- Monitor subscriber counts
- Test with concurrent load

The blackboard pattern excels at problems requiring flexible, opportunistic collaboration between heterogeneous components. It trades deterministic control flow for emergent, adaptive behavior.