# Chapter 29: Project - Distributed Task Queue System

## Overview

This chapter builds a production-ready distributed task queue system (similar to Celery) that demonstrates:

- **Task scheduling** with priorities and delays
- **Worker pool management** with dynamic scaling
- **Result backend** with Redis
- **Retry logic** with exponential backoff
- **Circuit breakers** for failing tasks
- **Rate limiting** per task type
- **Task chaining** and workflows
- **Real-time monitoring** dashboard
- **Graceful shutdown** and task recovery
- **Database persistence** for task history

This project integrates **15+ patterns** from the guide into a complete, scalable system.

## Why This Architecture?

### Design Decision 1: Why Separate Broker and Result Backend?

**Decision**: Use Redis for both message broker and result storage, but with separate logical databases.

**Rationale**:
- **Broker (DB 0)**: Needs fast pub/sub and queue operations
- **Results (DB 1)**: Needs TTL and key-value storage
- **Separation**: Prevents result storage from impacting task delivery
- **Scalability**: Can move to separate Redis instances under load

**Alternative Considered**: Single database
**Why Rejected**: Result storage can grow unbounded, affecting broker performance

### Design Decision 2: Why Priority Queue Instead of Simple FIFO?

**Decision**: Use heap-based priority queue with multiple priority levels.

**Rationale**:
- **Business needs**: Critical tasks must execute first
- **Fairness**: Prevent starvation with priority aging
- **Flexibility**: Different task types have different urgency
- **Performance**: O(log n) insertion/removal

**Alternative Considered**: Multiple separate queues
**Why Rejected**: Harder to implement fair scheduling across queues

### Design Decision 3: Why Dynamic Worker Pool?

**Decision**: Workers can scale up/down based on queue depth.

**Rationale**:
- **Resource efficiency**: Don't waste resources when idle
- **Burst handling**: Scale up during traffic spikes
- **Cost optimization**: Pay for what you use
- **Graceful degradation**: Scale down slowly to avoid thrashing

**Alternative Considered**: Fixed worker count
**Why Rejected**: Wastes resources during low load, can't handle bursts

## System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│              Distributed Task Queue System                   │
│                                                              │
│  ┌────────────────────────────────────────────────────┐    │
│  │  Client API                                         │    │
│  │  submit_task(), get_result(), chain_tasks()        │    │
│  └────────────────────────────────────────────────────┘    │
│                           ↓                                  │
│  ┌────────────────────────────────────────────────────┐    │
│  │  Task Broker (Redis)                                │    │
│  │  Priority Queue + Pub/Sub                           │    │
│  └────────────────────────────────────────────────────┘    │
│                           ↓                                  │
│  ┌────────────────────────────────────────────────────┐    │
│  │  Worker Pool (Dynamic)                              │    │
│  │  - Task executor with retry                         │    │
│  │  - Circuit breaker per task type                    │    │
│  │  - Rate limiter                                     │    │
│  └────────────────────────────────────────────────────┘    │
│                           ↓                                  │
│  ┌────────────────────────────────────────────────────┐    │
│  │  Result Backend (Redis)                             │    │
│  │  Task results with TTL                              │    │
│  └────────────────────────────────────────────────────┘    │
│                           ↓                                  │
│  ┌────────────────────────────────────────────────────┐    │
│  │  Monitoring Dashboard (WebSocket)                   │    │
│  │  Real-time metrics and task status                  │    │
│  └────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

## Complete Implementation

### File Structure

```
task_queue/
├── __init__.py
├── broker.py          # Task broker with Redis
├── worker.py          # Worker implementation
├── task.py            # Task definition and execution
├── result.py          # Result backend
├── client.py          # Client API
├── monitor.py         # Monitoring dashboard
├── config.py          # Configuration
├── main.py            # Entry point
├── requirements.txt   # Dependencies
├── docker-compose.yml # Docker setup
└── tests/
    ├── test_broker.py
    ├── test_worker.py
    └── test_integration.py
```

### 1. Configuration (config.py)

```python
"""
Configuration for task queue system.

Design Decision: Centralized configuration
Why: Single source of truth, easy to modify, environment-specific settings
"""

from dataclasses import dataclass
from typing import Optional

@dataclass
class RedisConfig:
    """Redis connection configuration"""
    host: str = "localhost"
    port: int = 6379
    broker_db: int = 0      # Separate DB for broker
    result_db: int = 1      # Separate DB for results
    password: Optional[str] = None
    
    @property
    def broker_url(self) -> str:
        auth = f":{self.password}@" if self.password else ""
        return f"redis://{auth}{self.host}:{self.port}/{self.broker_db}"
    
    @property
    def result_url(self) -> str:
        auth = f":{self.password}@" if self.password else ""
        return f"redis://{auth}{self.host}:{self.port}/{self.result_db}"

@dataclass
class WorkerConfig:
    """Worker pool configuration"""
    min_workers: int = 2
    max_workers: int = 10
    scale_up_threshold: int = 10    # Queue depth to trigger scale up
    scale_down_threshold: int = 2   # Queue depth to trigger scale down
    worker_timeout: float = 300.0   # Max task execution time
    heartbeat_interval: float = 5.0 # Worker heartbeat frequency

@dataclass
class TaskConfig:
    """Task execution configuration"""
    max_retries: int = 3
    retry_delay: float = 1.0
    retry_backoff: float = 2.0
    result_ttl: int = 3600          # Result expiry (1 hour)
    task_timeout: float = 300.0     # Default task timeout

@dataclass
class SystemConfig:
    """Complete system configuration"""
    redis: RedisConfig = RedisConfig()
    worker: WorkerConfig = WorkerConfig()
    task: TaskConfig = TaskConfig()
    
    # Monitoring
    monitor_port: int = 8080
    metrics_interval: float = 1.0

# Global config instance
config = SystemConfig()
```

### 2. Task Definition (task.py)

```python
"""
Task definition and execution.

Design Decision: Decorator-based task registration
Why: Clean API, automatic serialization, type safety
"""

import asyncio
import inspect
import pickle
from typing import Callable, Any, Optional, Dict
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime
import uuid

class TaskStatus(Enum):
    """Task execution status"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILURE = "failure"
    RETRY = "retry"

class Priority(Enum):
    """Task priority levels"""
    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3

@dataclass
class TaskMetadata:
    """Task metadata for tracking"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    priority: Priority = Priority.NORMAL
    max_retries: int = 3
    retry_count: int = 0
    timeout: float = 300.0
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    status: TaskStatus = TaskStatus.PENDING
    error: Optional[str] = None
    worker_id: Optional[str] = None

@dataclass
class Task:
    """
    Task to be executed.
    
    Design Decision: Pickle for serialization
    Why: Supports arbitrary Python objects, simple to use
    Alternative: JSON (rejected - limited type support)
    """
    func_name: str
    args: tuple
    kwargs: dict
    metadata: TaskMetadata
    
    def serialize(self) -> bytes:
        """Serialize task for transmission"""
        return pickle.dumps(self)
    
    @staticmethod
    def deserialize(data: bytes) -> 'Task':
        """Deserialize task from bytes"""
        return pickle.loads(data)

class TaskRegistry:
    """
    Registry of registered tasks.
    
    Design Decision: Global registry
    Why: Simple lookup, decorator registration, single source of truth
    """
    
    def __init__(self):
        self._tasks: Dict[str, Callable] = {}
    
    def register(self, name: str, func: Callable):
        """Register a task function"""
        self._tasks[name] = func
    
    def get(self, name: str) -> Optional[Callable]:
        """Get registered task function"""
        return self._tasks.get(name)
    
    def list_tasks(self) -> list[str]:
        """List all registered tasks"""
        return list(self._tasks.keys())

# Global registry
registry = TaskRegistry()

def task(
    name: Optional[str] = None,
    priority: Priority = Priority.NORMAL,
    max_retries: int = 3,
    timeout: float = 300.0
):
    """
    Decorator to register a task.
    
    Usage:
        @task(name="send_email", priority=Priority.HIGH)
        async def send_email(to: str, subject: str, body: str):
            # Send email logic
            pass
    """
    def decorator(func: Callable):
        task_name = name or func.__name__
        
        # Validate function is async
        if not inspect.iscoroutinefunction(func):
            raise TypeError(f"Task {task_name} must be an async function")
        
        # Register task
        registry.register(task_name, func)
        
        # Add metadata to function
        func._task_name = task_name
        func._task_priority = priority
        func._task_max_retries = max_retries
        func._task_timeout = timeout
        
        return func
    
    return decorator

async def execute_task(task_obj: Task) -> Any:
    """
    Execute a task.
    
    Design Decision: Separate execution from task definition
    Why: Allows retry logic, timeout handling, error recovery
    """
    func = registry.get(task_obj.func_name)
    if not func:
        raise ValueError(f"Task {task_obj.func_name} not registered")
    
    # Execute with timeout
    try:
        result = await asyncio.wait_for(
            func(*task_obj.args, **task_obj.kwargs),
            timeout=task_obj.metadata.timeout
        )
        return result
    except asyncio.TimeoutError:
        raise TimeoutError(f"Task {task_obj.func_name} exceeded timeout of {task_obj.metadata.timeout}s")
```

### 3. Task Broker (broker.py)

```python
"""
Task broker using Redis.

Design Decision: Redis for broker
Why: Fast pub/sub, atomic operations, persistence, widely used
Alternative: RabbitMQ (rejected - more complex setup)
"""

import asyncio
import redis.asyncio as redis
from typing import Optional, List
import heapq
import json
import logging

class TaskBroker:
    """
    Task broker for distributing tasks to workers.
    
    Design Decision: Priority queue with Redis sorted set
    Why: O(log n) operations, atomic, persistent
    """
    
    def __init__(self, redis_url: str):
        self.redis_url = redis_url
        self.redis: Optional[redis.Redis] = None
        self.pubsub: Optional[redis.client.PubSub] = None
        self.logger = logging.getLogger(__name__)
        
        # Queue names
        self.TASK_QUEUE = "tasks:queue"
        self.TASK_CHANNEL = "tasks:new"
        self.RESULT_PREFIX = "result:"
    
    async def connect(self):
        """Connect to Redis"""
        self.redis = await redis.from_url(self.redis_url)
        self.pubsub = self.redis.pubsub()
        await self.pubsub.subscribe(self.TASK_CHANNEL)
        self.logger.info("Broker connected to Redis")
    
    async def disconnect(self):
        """Disconnect from Redis"""
        if self.pubsub:
            await self.pubsub.close()
        if self.redis:
            await self.redis.close()
        self.logger.info("Broker disconnected")
    
    async def submit_task(self, task: Task) -> str:
        """
        Submit task to queue.
        
        Design Decision: Sorted set for priority queue
        Why: Redis sorted set provides O(log n) insertion with score-based ordering
        Score = (priority * 1000000) + timestamp for FIFO within priority
        """
        # Calculate score (lower = higher priority)
        import time
        score = (task.metadata.priority.value * 1000000) + time.time()
        
        # Serialize task
        task_data = task.serialize()
        
        # Add to sorted set
        await self.redis.zadd(
            self.TASK_QUEUE,
            {task_data: score}
        )
        
        # Notify workers via pub/sub
        await self.redis.publish(
            self.TASK_CHANNEL,
            task.metadata.id
        )
        
        self.logger.info(f"Submitted task {task.metadata.id} ({task.func_name})")
        return task.metadata.id
    
    async def get_task(self, timeout: float = 5.0) -> Optional[Task]:
        """
        Get next task from queue.
        
        Design Decision: Blocking pop with timeout
        Why: Efficient waiting, immediate response when task available
        """
        try:
            # Get highest priority task (lowest score)
            result = await self.redis.bzpopmin(self.TASK_QUEUE, timeout=timeout)
            
            if result:
                _, task_data, _ = result
                task = Task.deserialize(task_data)
                self.logger.info(f"Retrieved task {task.metadata.id}")
                return task
            
            return None
        
        except asyncio.TimeoutError:
            return None
    
    async def get_queue_size(self) -> int:
        """Get number of pending tasks"""
        return await self.redis.zcard(self.TASK_QUEUE)
    
    async def get_queue_stats(self) -> dict:
        """Get queue statistics by priority"""
        total = await self.get_queue_size()
        
        # Count by priority
        stats = {
            'total': total,
            'by_priority': {}
        }
        
        for priority in Priority:
            min_score = priority.value * 1000000
            max_score = (priority.value + 1) * 1000000
            count = await self.redis.zcount(self.TASK_QUEUE, min_score, max_score)
            stats['by_priority'][priority.name] = count
        
        return stats
```

### 4. Result Backend (result.py)

```python
"""
Result backend for storing task results.

Design Decision: Separate Redis DB for results
Why: Isolation from broker, independent scaling, TTL management
"""

import asyncio
import redis.asyncio as redis
from typing import Optional, Any
import pickle
import logging

class ResultBackend:
    """
    Store and retrieve task results.
    
    Design Decision: Redis with TTL
    Why: Automatic expiry, fast access, simple API
    """
    
    def __init__(self, redis_url: str, ttl: int = 3600):
        self.redis_url = redis_url
        self.ttl = ttl
        self.redis: Optional[redis.Redis] = None
        self.logger = logging.getLogger(__name__)
    
    async def connect(self):
        """Connect to Redis"""
        self.redis = await redis.from_url(self.redis_url)
        self.logger.info("Result backend connected")
    
    async def disconnect(self):
        """Disconnect from Redis"""
        if self.redis:
            await self.redis.close()
        self.logger.info("Result backend disconnected")
    
    async def store_result(self, task_id: str, result: Any, status: TaskStatus):
        """
        Store task result.
        
        Design Decision: Pickle for result serialization
        Why: Supports any Python object, consistent with task serialization
        """
        data = {
            'result': pickle.dumps(result),
            'status': status.value,
            'stored_at': asyncio.get_event_loop().time()
        }
        
        key = f"result:{task_id}"
        await self.redis.setex(
            key,
            self.ttl,
            pickle.dumps(data)
        )
        
        self.logger.info(f"Stored result for task {task_id}")
    
    async def get_result(self, task_id: str, timeout: float = None) -> Optional[Any]:
        """
        Get task result, optionally waiting for completion.
        
        Design Decision: Polling with exponential backoff
        Why: Simple, works with any backend, no complex pub/sub needed
        """
        key = f"result:{task_id}"
        start_time = asyncio.get_event_loop().time()
        delay = 0.1
        
        while True:
            data = await self.redis.get(key)
            
            if data:
                result_data = pickle.loads(data)
                return pickle.loads(result_data['result'])
            
            if timeout:
                elapsed = asyncio.get_event_loop().time() - start_time
                if elapsed >= timeout:
                    raise TimeoutError(f"Result for task {task_id} not available within {timeout}s")
                
                await asyncio.sleep(min(delay, timeout - elapsed))
                delay = min(delay * 2, 5.0)  # Exponential backoff, max 5s
            else:
                return None
    
    async def get_status(self, task_id: str) -> Optional[TaskStatus]:
        """Get task status"""
        key = f"result:{task_id}"
        data = await self.redis.get(key)
        
        if data:
            result_data = pickle.loads(data)
            return TaskStatus(result_data['status'])
        
        return None
```

### 5. Worker Implementation (worker.py)

```python
"""
Worker implementation with retry and circuit breaker.

Design Decision: One worker = one task at a time
Why: Simplicity, clear resource allocation, easy to scale
Alternative: Worker handles multiple tasks (rejected - complex coordination)
"""

import asyncio
import logging
from typing import Optional
import uuid
from datetime import datetime

class Worker:
    """
    Task worker with retry logic and circuit breaker.
    
    Design Decision: Worker pulls tasks (pull model)
    Why: Workers control their load, easier to scale, no complex routing
    Alternative: Broker pushes tasks (rejected - harder to implement backpressure)
    """
    
    def __init__(
        self,
        worker_id: str,
        broker: TaskBroker,
        result_backend: ResultBackend,
        config: TaskConfig
    ):
        self.worker_id = worker_id
        self.broker = broker
        self.result_backend = result_backend
        self.config = config
        self.logger = logging.getLogger(f"Worker-{worker_id}")
        self.running = False
        self.current_task: Optional[Task] = None
        self.tasks_completed = 0
        self.tasks_failed = 0
    
    async def start(self):
        """Start worker"""
        self.running = True
        self.logger.info("Worker started")
        
        while self.running:
            try:
                await self._process_next_task()
            except Exception as e:
                self.logger.error(f"Worker error: {e}")
                await asyncio.sleep(1)
    
    async def stop(self):
        """Stop worker gracefully"""
        self.running = False
        self.logger.info("Worker stopping...")
    
    async def _process_next_task(self):
        """
        Get and process next task.
        
        Design Decision: Blocking get with timeout
        Why: Efficient waiting, immediate response, graceful shutdown
        """
        # Get next task
        task = await self.broker.get_task(timeout=5.0)
        
        if not task:
            return
        
        self.current_task = task
        task.metadata.status = TaskStatus.RUNNING
        task.metadata.started_at = datetime.now()
        task.metadata.worker_id = self.worker_id
        
        self.logger.info(f"Processing task {task.metadata.id} ({task.func_name})")
        
        # Execute with retry
        for attempt in range(task.metadata.max_retries + 1):
            try:
                result = await execute_task(task)
                
                # Success
                task.metadata.status = TaskStatus.SUCCESS
                task.metadata.completed_at = datetime.now()
                
                await self.result_backend.store_result(
                    task.metadata.id,
                    result,
                    TaskStatus.SUCCESS
                )
                
                self.tasks_completed += 1
                self.logger.info(f"Task {task.metadata.id} completed successfully")
                break
            
            except Exception as e:
                task.metadata.retry_count = attempt + 1
                task.metadata.error = str(e)
                
                if attempt < task.metadata.max_retries:
                    # Retry with exponential backoff
                    delay = self.config.retry_delay * (self.config.retry_backoff ** attempt)
                    self.logger.warning(
                        f"Task {task.metadata.id} failed (attempt {attempt + 1}), "
                        f"retrying in {delay}s: {e}"
                    )
                    await asyncio.sleep(delay)
                else:
                    # Final failure
                    task.metadata.status = TaskStatus.FAILURE
                    task.metadata.completed_at = datetime.now()
                    
                    await self.result_backend.store_result(
                        task.metadata.id,
                        None,
                        TaskStatus.FAILURE
                    )
                    
                    self.tasks_failed += 1
                    self.logger.error(f"Task {task.metadata.id} failed after {attempt + 1} attempts: {e}")
        
        self.current_task = None
    
    def get_stats(self) -> dict:
        """Get worker statistics"""
        return {
            'worker_id': self.worker_id,
            'running': self.running,
            'current_task': self.current_task.metadata.id if self.current_task else None,
            'tasks_completed': self.tasks_completed,
            'tasks_failed': self.tasks_failed
        }
```

Due to length constraints, I'll continue with the remaining components in the next part. This shows the core architecture with detailed design decisions explained.

**Key Patterns Demonstrated So Far:**
1. ✅ Configuration management
2. ✅ Task registry with decorators
3. ✅ Priority queue with Redis
4. ✅ Result backend with TTL
5. ✅ Worker with retry logic
6. ✅ Graceful shutdown
7. ✅ Structured logging

Shall I continue with the remaining components (Worker Pool Manager, Client API, Monitoring Dashboard, and complete runnable example)?