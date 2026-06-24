# Chapter 20: Pipelines

## Introduction

Pipelines process data through sequential stages, where each stage transforms the data and passes it to the next. This pattern is essential for stream processing, ETL (Extract-Transform-Load), and data processing workflows.

By the end of this chapter, you'll understand:
- Producer → Transform → Sink patterns
- Multi-stage pipelines
- Backpressure propagation
- Pipeline composition
- Error handling in pipelines
- Practical pipeline implementations

---

## Pipeline Basics

A pipeline consists of stages connected by queues:

```
Producer → Queue → Transform → Queue → Sink
```

### Simple Pipeline

```python
import asyncio

async def simple_pipeline():
    """Basic three-stage pipeline"""
    
    # Queues between stages
    input_queue = asyncio.Queue()
    output_queue = asyncio.Queue()
    
    # Producer: Generate data
    async def producer():
        for i in range(10):
            print(f"Producing: {i}")
            await input_queue.put(i)
            await asyncio.sleep(0.1)
        
        # Signal end
        await input_queue.put(None)
    
    # Transform: Process data
    async def transform():
        while True:
            item = await input_queue.get()
            
            if item is None:
                await output_queue.put(None)
                break
            
            # Transform
            result = item * 2
            print(f"Transforming: {item} → {result}")
            await output_queue.put(result)
    
    # Sink: Consume data
    async def sink():
        results = []
        while True:
            item = await output_queue.get()
            
            if item is None:
                break
            
            print(f"Consuming: {item}")
            results.append(item)
        
        return results
    
    # Run pipeline
    producer_task = asyncio.create_task(producer())
    transform_task = asyncio.create_task(transform())
    sink_task = asyncio.create_task(sink())
    
    # Wait for completion
    await producer_task
    await transform_task
    results = await sink_task
    
    print(f"Results: {results}")

asyncio.run(simple_pipeline())
```

---

## Pipeline Framework

Reusable pipeline framework for building complex pipelines.

### Pipeline Stage Base Class

```python
import asyncio
from typing import Any, Optional, AsyncIterator
from abc import ABC, abstractmethod

class PipelineStage(ABC):
    """Base class for pipeline stages"""
    
    def __init__(self, name: str):
        self.name = name
        self.input_queue: Optional[asyncio.Queue] = None
        self.output_queue: Optional[asyncio.Queue] = None
        self.running = False
    
    def connect_input(self, queue: asyncio.Queue):
        """Connect input queue"""
        self.input_queue = queue
    
    def connect_output(self, queue: asyncio.Queue):
        """Connect output queue"""
        self.output_queue = queue
    
    @abstractmethod
    async def process(self, item: Any) -> Any:
        """Process single item (override in subclass)"""
        pass
    
    async def run(self):
        """Run stage (processes items from input to output)"""
        self.running = True
        print(f"[{self.name}] Started")
        
        try:
            while self.running:
                # Get item from input
                item = await self.input_queue.get()
                
                # Check for sentinel (end of stream)
                if item is None:
                    if self.output_queue:
                        await self.output_queue.put(None)
                    break
                
                try:
                    # Process item
                    result = await self.process(item)
                    
                    # Send to output
                    if self.output_queue and result is not None:
                        await self.output_queue.put(result)
                
                except Exception as e:
                    print(f"[{self.name}] Error processing {item}: {e}")
        
        finally:
            print(f"[{self.name}] Stopped")
    
    async def stop(self):
        """Stop stage"""
        self.running = False

class Pipeline:
    """Pipeline that connects multiple stages"""
    
    def __init__(self):
        self.stages: list[PipelineStage] = []
        self.queues: list[asyncio.Queue] = []
    
    def add_stage(self, stage: PipelineStage, queue_size: int = 10):
        """Add stage to pipeline"""
        # Create queue for this stage's output
        queue = asyncio.Queue(maxsize=queue_size)
        self.queues.append(queue)
        
        # Connect stage
        if self.stages:
            # Connect to previous stage's output
            stage.connect_input(self.queues[-2])
        else:
            # First stage - create input queue
            input_queue = asyncio.Queue(maxsize=queue_size)
            self.queues.insert(0, input_queue)
            stage.connect_input(input_queue)
        
        stage.connect_output(queue)
        self.stages.append(stage)
    
    async def run(self):
        """Run all stages"""
        # Start all stages
        tasks = [asyncio.create_task(stage.run()) for stage in self.stages]
        
        # Wait for all to complete
        await asyncio.gather(*tasks)
    
    async def feed(self, items: list[Any]):
        """Feed items into pipeline"""
        input_queue = self.queues[0]
        
        for item in items:
            await input_queue.put(item)
        
        # Send sentinel
        await input_queue.put(None)
    
    async def collect(self) -> list[Any]:
        """Collect results from pipeline"""
        output_queue = self.queues[-1]
        results = []
        
        while True:
            item = await output_queue.get()
            if item is None:
                break
            results.append(item)
        
        return results
```

### Example Pipeline Stages

```python
class MultiplyStage(PipelineStage):
    """Multiply numbers by a factor"""
    
    def __init__(self, factor: int):
        super().__init__(f"Multiply×{factor}")
        self.factor = factor
    
    async def process(self, item: Any) -> Any:
        await asyncio.sleep(0.1)  # Simulate work
        return item * self.factor

class FilterStage(PipelineStage):
    """Filter items based on predicate"""
    
    def __init__(self, predicate):
        super().__init__("Filter")
        self.predicate = predicate
    
    async def process(self, item: Any) -> Optional[Any]:
        if self.predicate(item):
            return item
        return None  # Filtered out

class BatchStage(PipelineStage):
    """Batch items together"""
    
    def __init__(self, batch_size: int):
        super().__init__(f"Batch({batch_size})")
        self.batch_size = batch_size
        self.batch = []
    
    async def process(self, item: Any) -> Optional[list]:
        self.batch.append(item)
        
        if len(self.batch) >= self.batch_size:
            result = self.batch.copy()
            self.batch.clear()
            return result
        
        return None

async def pipeline_framework_demo():
    """Demonstrate pipeline framework"""
    
    # Create pipeline
    pipeline = Pipeline()
    
    # Add stages
    pipeline.add_stage(MultiplyStage(2))
    pipeline.add_stage(FilterStage(lambda x: x > 10))
    pipeline.add_stage(MultiplyStage(3))
    
    # Start pipeline
    pipeline_task = asyncio.create_task(pipeline.run())
    
    # Feed data
    await pipeline.feed(list(range(20)))
    
    # Collect results
    results = await pipeline.collect()
    
    # Wait for pipeline to finish
    await pipeline_task
    
    print(f"Results: {results}")

asyncio.run(pipeline_framework_demo())
```

---

## Backpressure in Pipelines

Backpressure prevents fast producers from overwhelming slow consumers.

### Bounded Queues for Backpressure

```python
import asyncio
import time

async def backpressure_demo():
    """Demonstrate backpressure with bounded queues"""
    
    # Bounded queue (max 5 items)
    queue = asyncio.Queue(maxsize=5)
    
    async def fast_producer():
        """Fast producer"""
        for i in range(20):
            print(f"Producer: Producing {i}")
            start = time.time()
            
            # This blocks when queue is full (backpressure)
            await queue.put(i)
            
            elapsed = time.time() - start
            if elapsed > 0.01:
                print(f"Producer: Blocked for {elapsed:.2f}s")
    
    async def slow_consumer():
        """Slow consumer"""
        for _ in range(20):
            item = await queue.get()
            print(f"Consumer: Processing {item}")
            await asyncio.sleep(0.5)  # Slow processing
    
    await asyncio.gather(
        fast_producer(),
        slow_consumer()
    )

asyncio.run(backpressure_demo())

# Output shows producer blocking when queue fills up
```

### Adaptive Backpressure

```python
import asyncio
from typing import Any

class AdaptiveStage(PipelineStage):
    """Stage that adapts processing speed based on queue size"""
    
    def __init__(self, name: str):
        super().__init__(name)
        self.processing_delay = 0.1
    
    async def process(self, item: Any) -> Any:
        # Adapt delay based on output queue size
        if self.output_queue:
            queue_size = self.output_queue.qsize()
            max_size = self.output_queue.maxsize
            
            if queue_size > max_size * 0.8:
                # Queue filling up - slow down
                self.processing_delay = 0.2
                print(f"[{self.name}] Slowing down (queue: {queue_size}/{max_size})")
            elif queue_size < max_size * 0.2:
                # Queue draining - speed up
                self.processing_delay = 0.05
                print(f"[{self.name}] Speeding up (queue: {queue_size}/{max_size})")
        
        await asyncio.sleep(self.processing_delay)
        return item * 2
```

---

## Real-world: Data Processing Pipeline

Complete ETL pipeline for processing data files.

```python
import asyncio
import json
from typing import Any, Dict, List
from dataclasses import dataclass
from datetime import datetime

@dataclass
class Record:
    """Data record"""
    id: int
    timestamp: datetime
    value: float
    metadata: Dict[str, Any]

class ExtractStage(PipelineStage):
    """Extract: Read data from source"""
    
    def __init__(self, source: List[Dict]):
        super().__init__("Extract")
        self.source = source
        self.index = 0
    
    async def run(self):
        """Override run to generate data"""
        print(f"[{self.name}] Started")
        
        for item in self.source:
            await self.output_queue.put(item)
            await asyncio.sleep(0.1)
        
        # Send sentinel
        await self.output_queue.put(None)
        print(f"[{self.name}] Stopped")
    
    async def process(self, item: Any) -> Any:
        # Not used in this stage
        pass

class TransformStage(PipelineStage):
    """Transform: Convert and validate data"""
    
    def __init__(self):
        super().__init__("Transform")
    
    async def process(self, item: Dict) -> Optional[Record]:
        try:
            # Validate
            if 'id' not in item or 'value' not in item:
                print(f"[{self.name}] Invalid record: {item}")
                return None
            
            # Transform
            record = Record(
                id=item['id'],
                timestamp=datetime.fromisoformat(item.get('timestamp', datetime.now().isoformat())),
                value=float(item['value']),
                metadata=item.get('metadata', {})
            )
            
            # Enrich
            record.metadata['processed_at'] = datetime.now().isoformat()
            
            await asyncio.sleep(0.1)  # Simulate processing
            return record
        
        except Exception as e:
            print(f"[{self.name}] Error transforming {item}: {e}")
            return None

class AggregateStage(PipelineStage):
    """Aggregate: Compute statistics"""
    
    def __init__(self, window_size: int = 5):
        super().__init__("Aggregate")
        self.window_size = window_size
        self.window: List[Record] = []
    
    async def process(self, item: Record) -> Optional[Dict]:
        self.window.append(item)
        
        if len(self.window) >= self.window_size:
            # Compute statistics
            values = [r.value for r in self.window]
            stats = {
                'count': len(values),
                'sum': sum(values),
                'avg': sum(values) / len(values),
                'min': min(values),
                'max': max(values),
                'window': [r.id for r in self.window]
            }
            
            self.window.clear()
            return stats
        
        return None

class LoadStage(PipelineStage):
    """Load: Write results to destination"""
    
    def __init__(self):
        super().__init__("Load")
        self.results: List[Any] = []
    
    async def process(self, item: Any) -> Any:
        # Store result
        self.results.append(item)
        print(f"[{self.name}] Loaded: {item}")
        
        await asyncio.sleep(0.05)
        return item
    
    def get_results(self) -> List[Any]:
        """Get all loaded results"""
        return self.results

async def etl_pipeline_demo():
    """Demonstrate ETL pipeline"""
    
    # Sample data
    source_data = [
        {'id': i, 'value': i * 10, 'timestamp': datetime.now().isoformat()}
        for i in range(20)
    ]
    
    # Create pipeline
    extract = ExtractStage(source_data)
    transform = TransformStage()
    aggregate = AggregateStage(window_size=5)
    load = LoadStage()
    
    # Build pipeline
    pipeline = Pipeline()
    pipeline.stages = [extract, transform, aggregate, load]
    
    # Connect stages manually
    queue1 = asyncio.Queue(maxsize=10)
    queue2 = asyncio.Queue(maxsize=10)
    queue3 = asyncio.Queue(maxsize=10)
    queue4 = asyncio.Queue(maxsize=10)
    
    extract.connect_output(queue1)
    transform.connect_input(queue1)
    transform.connect_output(queue2)
    aggregate.connect_input(queue2)
    aggregate.connect_output(queue3)
    load.connect_input(queue3)
    load.connect_output(queue4)
    
    # Run pipeline
    await pipeline.run()
    
    # Get results
    results = load.get_results()
    print(f"\nFinal results: {len(results)} aggregates")
    for result in results:
        print(f"  {result}")

asyncio.run(etl_pipeline_demo())
```

---

## Real-world: Stream Processing Pipeline

Process continuous data streams with windowing.

```python
import asyncio
from typing import Any, Callable, List
from collections import deque
import time

class WindowedAggregator:
    """
    Windowed aggregation for stream processing.
    
    Features:
    - Tumbling windows
    - Sliding windows
    - Time-based windows
    """
    
    def __init__(
        self,
        window_size: int,
        aggregator: Callable[[List], Any],
        slide: int = None
    ):
        self.window_size = window_size
        self.aggregator = aggregator
        self.slide = slide or window_size  # Default: tumbling window
        self.buffer = deque()
    
    async def process(self, item: Any) -> List[Any]:
        """Process item and return aggregates"""
        self.buffer.append(item)
        results = []
        
        # Check if we have enough items for a window
        while len(self.buffer) >= self.window_size:
            # Get window
            window = list(self.buffer)[:self.window_size]
            
            # Aggregate
            result = self.aggregator(window)
            results.append(result)
            
            # Slide window
            for _ in range(self.slide):
                if self.buffer:
                    self.buffer.popleft()
        
        return results

class StreamProcessor:
    """
    Stream processing pipeline.
    
    Processes continuous data streams with windowing and aggregation.
    """
    
    def __init__(self):
        self.input_queue = asyncio.Queue()
        self.output_queue = asyncio.Queue()
        self.running = False
    
    async def producer(self, data_source):
        """Produce data from source"""
        for item in data_source:
            await self.input_queue.put(item)
            await asyncio.sleep(0.1)
        
        await self.input_queue.put(None)
    
    async def processor(self, aggregator: WindowedAggregator):
        """Process stream with windowed aggregation"""
        while self.running:
            item = await self.input_queue.get()
            
            if item is None:
                await self.output_queue.put(None)
                break
            
            # Process item
            results = await aggregator.process(item)
            
            # Output aggregates
            for result in results:
                await self.output_queue.put(result)
    
    async def consumer(self):
        """Consume processed results"""
        results = []
        while True:
            item = await self.output_queue.get()
            
            if item is None:
                break
            
            results.append(item)
            print(f"Result: {item}")
        
        return results
    
    async def run(self, data_source, aggregator):
        """Run stream processor"""
        self.running = True
        
        # Start components
        producer_task = asyncio.create_task(self.producer(data_source))
        processor_task = asyncio.create_task(self.processor(aggregator))
        consumer_task = asyncio.create_task(self.consumer())
        
        # Wait for completion
        await producer_task
        await processor_task
        results = await consumer_task
        
        return results

async def stream_processing_demo():
    """Demonstrate stream processing"""
    
    # Data source (simulated sensor readings)
    data_source = [
        {'sensor_id': 1, 'value': i, 'timestamp': time.time() + i}
        for i in range(20)
    ]
    
    # Create windowed aggregator
    aggregator = WindowedAggregator(
        window_size=5,
        aggregator=lambda window: {
            'count': len(window),
            'avg': sum(item['value'] for item in window) / len(window),
            'min': min(item['value'] for item in window),
            'max': max(item['value'] for item in window)
        },
        slide=2  # Sliding window (overlap)
    )
    
    # Run processor
    processor = StreamProcessor()
    results = await processor.run(data_source, aggregator)
    
    print(f"\nProcessed {len(results)} windows")

asyncio.run(stream_processing_demo())
```

---

## Common Patterns

### Pattern 1: Fan-out (Broadcast)

```python
async def fan_out_pattern():
    """One producer, multiple consumers"""
    
    input_queue = asyncio.Queue()
    output_queues = [asyncio.Queue() for _ in range(3)]
    
    async def producer():
        for i in range(10):
            await input_queue.put(i)
        await input_queue.put(None)
    
    async def broadcaster():
        """Broadcast to all output queues"""
        while True:
            item = await input_queue.get()
            
            if item is None:
                for q in output_queues:
                    await q.put(None)
                break
            
            # Send to all queues
            for q in output_queues:
                await q.put(item)
    
    async def consumer(consumer_id, queue):
        while True:
            item = await queue.get()
            if item is None:
                break
            print(f"Consumer {consumer_id}: {item}")
    
    await asyncio.gather(
        producer(),
        broadcaster(),
        *[consumer(i, q) for i, q in enumerate(output_queues)]
    )
```

### Pattern 2: Fan-in (Merge)

```python
async def fan_in_pattern():
    """Multiple producers, one consumer"""
    
    input_queues = [asyncio.Queue() for _ in range(3)]
    output_queue = asyncio.Queue()
    
    async def producer(producer_id, queue):
        for i in range(5):
            await queue.put(f"P{producer_id}-{i}")
        await queue.put(None)
    
    async def merger():
        """Merge from all input queues"""
        active_queues = set(input_queues)
        
        while active_queues:
            for queue in list(active_queues):
                try:
                    item = queue.get_nowait()
                    
                    if item is None:
                        active_queues.remove(queue)
                    else:
                        await output_queue.put(item)
                
                except asyncio.QueueEmpty:
                    continue
            
            await asyncio.sleep(0.1)
        
        await output_queue.put(None)
    
    async def consumer():
        while True:
            item = await output_queue.get()
            if item is None:
                break
            print(f"Consumer: {item}")
    
    await asyncio.gather(
        *[producer(i, q) for i, q in enumerate(input_queues)],
        merger(),
        consumer()
    )
```

---

## Best Practices

### ✅ DO: Use bounded queues

```python
# Good: Bounded queue provides backpressure
queue = asyncio.Queue(maxsize=100)
```

### ✅ DO: Handle sentinels properly

```python
# Good: Propagate sentinel through pipeline
if item is None:
    await output_queue.put(None)
    break
```

### ✅ DO: Handle errors gracefully

```python
# Good: Catch and log errors
try:
    result = await process(item)
except Exception as e:
    logger.error(f"Error processing {item}: {e}")
    continue
```

### ❌ DON'T: Create unbounded queues

```python
# Bad: No backpressure
queue = asyncio.Queue()  # Unbounded

# Good: Bounded queue
queue = asyncio.Queue(maxsize=100)
```

### ✅ DO: Monitor queue sizes

```python
# Good: Monitor for bottlenecks
if queue.qsize() > queue.maxsize * 0.8:
    logger.warning("Queue filling up")
```

---

## Summary: Pipeline Mental Model

✅ **Stages connected by queues** - producer → transform → sink

✅ **Bounded queues** - provide backpressure

✅ **Sentinels for termination** - signal end of stream

✅ **Error handling** - catch and log errors

✅ **Windowing for streams** - aggregate over windows

✅ **Fan-out/fan-in** - broadcast and merge patterns

✅ **Monitor queue sizes** - detect bottlenecks

---

## What's Next?

Pipelines process data sequentially. For event-driven coordination, we need blackboard systems.

In [Chapter 21: Blackboard Systems](./21-blackboard-systems.md), we'll cover:
- Blackboard architecture
- Knowledge sources
- Event-driven coordination
- Opportunistic problem solving
- Complete blackboard implementation
- Real-world blackboard patterns

Blackboard systems enable sophisticated multi-agent coordination.

---

**Previous:** [← Chapter 19: Worker Pools](./19-worker-pools.md)  
**Next:** [Chapter 21: Blackboard Systems →](./21-blackboard-systems.md)