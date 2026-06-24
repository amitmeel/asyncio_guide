# Chapter 26: Retry Patterns and Circuit Breakers

## Overview

In distributed systems and async applications, failures are inevitable. Network issues, service outages, and transient errors require robust error handling strategies:

- **Retry patterns**: Automatically retry failed operations
- **Circuit breakers**: Prevent cascading failures
- **Backoff strategies**: Space out retry attempts
- **Fallback mechanisms**: Provide alternative responses
- **Bulkheads**: Isolate failures to prevent system-wide impact
- **Timeout management**: Prevent indefinite waits

These patterns are essential for building resilient, production-ready async applications.

## Mental Model

Think of resilience patterns like **electrical safety systems**:

```
┌─────────────────────────────────────────────────────────┐
│                    Service Call                          │
│                                                          │
│  ┌──────────────────────────────────────────────────┐  │
│  │  Circuit Breaker (Safety Switch)                  │  │
│  │  States: CLOSED → OPEN → HALF_OPEN               │  │
│  └──────────────────────────────────────────────────┘  │
│                           ↓                              │
│  ┌──────────────────────────────────────────────────┐  │
│  │  Retry Logic (Automatic Reset)                    │  │
│  │  Attempts: 1 → 2 → 3 (with backoff)              │  │
│  └──────────────────────────────────────────────────┘  │
│                           ↓                              │
│  ┌──────────────────────────────────────────────────┐  │
│  │  Timeout (Emergency Cutoff)                       │  │
│  │  Max wait: 30s                                    │  │
│  └──────────────────────────────────────────────────┘  │
│                           ↓                              │
│  ┌──────────────────────────────────────────────────┐  │
│  │  Fallback (Backup Power)                          │  │
│  │  Return cached/default value                      │  │
│  └──────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

**Key concepts:**

1. **Transient failures**: Temporary issues that resolve themselves
2. **Permanent failures**: Issues requiring intervention
3. **Cascading failures**: One failure triggering others
4. **Graceful degradation**: Reduced functionality instead of total failure
5. **Fast failure**: Fail quickly rather than waiting indefinitely

## Retry Patterns

### 1. Basic Retry with Exponential Backoff

```python
import asyncio
import random
from typing import Callable, TypeVar, Optional, Type, Tuple
from functools import wraps
import time

T = TypeVar('T')

class RetryConfig:
    """Configuration for retry behavior"""
    
    def __init__(
        self,
        max_attempts: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_base: float = 2.0,
        jitter: bool = True,
        retryable_exceptions: Tuple[Type[Exception], ...] = (Exception,)
    ):
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter
        self.retryable_exceptions = retryable_exceptions
    
    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay for given attempt"""
        # Exponential backoff
        delay = min(
            self.base_delay * (self.exponential_base ** attempt),
            self.max_delay
        )
        
        # Add jitter to prevent thundering herd
        if self.jitter:
            delay = delay * (0.5 + random.random() * 0.5)
        
        return delay

async def retry(
    func: Callable[..., Awaitable[T]],
    *args,
    config: Optional[RetryConfig] = None,
    **kwargs
) -> T:
    """
    Retry async function with exponential backoff.
    
    Args:
        func: Async function to retry
        config: Retry configuration
        *args, **kwargs: Arguments to pass to func
    
    Returns:
        Result from successful function call
    
    Raises:
        Last exception if all retries fail
    """
    if config is None:
        config = RetryConfig()
    
    last_exception = None
    
    for attempt in range(config.max_attempts):
        try:
            return await func(*args, **kwargs)
        
        except config.retryable_exceptions as e:
            last_exception = e
            
            if attempt < config.max_attempts - 1:
                delay = config.calculate_delay(attempt)
                print(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay:.2f}s...")
                await asyncio.sleep(delay)
            else:
                print(f"All {config.max_attempts} attempts failed")
    
    raise last_exception

# Decorator version
def with_retry(config: Optional[RetryConfig] = None):
    """Decorator to add retry logic to async functions"""
    def decorator(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> T:
            return await retry(func, *args, config=config, **kwargs)
        return wrapper
    return decorator

# Example usage
@with_retry(RetryConfig(max_attempts=3, base_delay=1.0))
async def unreliable_api_call(url: str) -> dict:
    """Simulated API call that sometimes fails"""
    if random.random() < 0.7:  # 70% failure rate
        raise ConnectionError("Network error")
    return {"status": "success", "data": "result"}

async def retry_example():
    """Demonstrate retry pattern"""
    try:
        result = await unreliable_api_call("https://api.example.com/data")
        print(f"Success: {result}")
    except Exception as e:
        print(f"Failed after all retries: {e}")
```

### 2. Advanced Retry with Callbacks

```python
import asyncio
from typing import Callable, Awaitable, Optional
from dataclasses import dataclass
import time

@dataclass
class RetryContext:
    """Context information for retry callbacks"""
    attempt: int
    max_attempts: int
    exception: Exception
    elapsed_time: float
    next_delay: float

class AdvancedRetry:
    """
    Advanced retry with callbacks and metrics.
    
    Provides hooks for monitoring and custom behavior.
    """
    
    def __init__(self, config: RetryConfig):
        self.config = config
        self.on_retry: Optional[Callable[[RetryContext], Awaitable[None]]] = None
        self.on_success: Optional[Callable[[int, float], Awaitable[None]]] = None
        self.on_failure: Optional[Callable[[Exception, int], Awaitable[None]]] = None
    
    async def execute(
        self,
        func: Callable[..., Awaitable[T]],
        *args,
        **kwargs
    ) -> T:
        """Execute function with retry logic"""
        start_time = time.time()
        last_exception = None
        
        for attempt in range(self.config.max_attempts):
            try:
                result = await func(*args, **kwargs)
                
                # Success callback
                if self.on_success:
                    elapsed = time.time() - start_time
                    await self.on_success(attempt + 1, elapsed)
                
                return result
            
            except self.config.retryable_exceptions as e:
                last_exception = e
                
                if attempt < self.config.max_attempts - 1:
                    delay = self.config.calculate_delay(attempt)
                    elapsed = time.time() - start_time
                    
                    # Retry callback
                    if self.on_retry:
                        context = RetryContext(
                            attempt=attempt + 1,
                            max_attempts=self.config.max_attempts,
                            exception=e,
                            elapsed_time=elapsed,
                            next_delay=delay
                        )
                        await self.on_retry(context)
                    
                    await asyncio.sleep(delay)
        
        # Failure callback
        if self.on_failure:
            await self.on_failure(last_exception, self.config.max_attempts)
        
        raise last_exception

# Example with callbacks
async def retry_with_callbacks_example():
    """Demonstrate retry with callbacks"""
    config = RetryConfig(max_attempts=3, base_delay=1.0)
    retry_handler = AdvancedRetry(config)
    
    # Set up callbacks
    async def on_retry(ctx: RetryContext):
        print(f"Retry {ctx.attempt}/{ctx.max_attempts}: {ctx.exception}")
        print(f"Waiting {ctx.next_delay:.2f}s before next attempt")
    
    async def on_success(attempts: int, elapsed: float):
        print(f"Success after {attempts} attempt(s) in {elapsed:.2f}s")
    
    async def on_failure(exception: Exception, attempts: int):
        print(f"Failed after {attempts} attempts: {exception}")
    
    retry_handler.on_retry = on_retry
    retry_handler.on_success = on_success
    retry_handler.on_failure = on_failure
    
    # Execute with retry
    try:
        result = await retry_handler.execute(unreliable_api_call, "https://api.example.com")
        print(f"Result: {result}")
    except Exception as e:
        print(f"Final failure: {e}")
```

## Circuit Breaker Pattern

### 1. Basic Circuit Breaker

```python
import asyncio
from enum import Enum, auto
from typing import Callable, Awaitable, Optional
import time
from dataclasses import dataclass

class CircuitState(Enum):
    """Circuit breaker states"""
    CLOSED = auto()    # Normal operation
    OPEN = auto()      # Failing, reject requests
    HALF_OPEN = auto() # Testing if service recovered

@dataclass
class CircuitBreakerConfig:
    """Circuit breaker configuration"""
    failure_threshold: int = 5          # Failures before opening
    success_threshold: int = 2          # Successes to close from half-open
    timeout: float = 60.0               # Seconds before trying half-open
    expected_exception: Type[Exception] = Exception

class CircuitBreakerError(Exception):
    """Raised when circuit is open"""
    pass

class CircuitBreaker:
    """
    Circuit breaker implementation.
    
    Prevents cascading failures by stopping requests to failing services.
    
    States:
    - CLOSED: Normal operation, requests pass through
    - OPEN: Service failing, requests rejected immediately
    - HALF_OPEN: Testing recovery, limited requests allowed
    """
    
    def __init__(self, name: str, config: Optional[CircuitBreakerConfig] = None):
        self.name = name
        self.config = config or CircuitBreakerConfig()
        
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: Optional[float] = None
        self.lock = asyncio.Lock()
    
    async def call(
        self,
        func: Callable[..., Awaitable[T]],
        *args,
        **kwargs
    ) -> T:
        """
        Execute function through circuit breaker.
        
        Raises CircuitBreakerError if circuit is open.
        """
        async with self.lock:
            # Check if we should transition from OPEN to HALF_OPEN
            if self.state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    print(f"[{self.name}] Transitioning to HALF_OPEN")
                    self.state = CircuitState.HALF_OPEN
                    self.success_count = 0
                else:
                    raise CircuitBreakerError(
                        f"Circuit breaker '{self.name}' is OPEN"
                    )
        
        # Execute function
        try:
            result = await func(*args, **kwargs)
            await self._on_success()
            return result
        
        except self.config.expected_exception as e:
            await self._on_failure()
            raise
    
    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to try half-open"""
        if self.last_failure_time is None:
            return False
        return time.time() - self.last_failure_time >= self.config.timeout
    
    async def _on_success(self):
        """Handle successful call"""
        async with self.lock:
            self.failure_count = 0
            
            if self.state == CircuitState.HALF_OPEN:
                self.success_count += 1
                print(f"[{self.name}] Success in HALF_OPEN: {self.success_count}/{self.config.success_threshold}")
                
                if self.success_count >= self.config.success_threshold:
                    print(f"[{self.name}] Transitioning to CLOSED")
                    self.state = CircuitState.CLOSED
                    self.success_count = 0
    
    async def _on_failure(self):
        """Handle failed call"""
        async with self.lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.state == CircuitState.HALF_OPEN:
                print(f"[{self.name}] Failure in HALF_OPEN, transitioning to OPEN")
                self.state = CircuitState.OPEN
                self.success_count = 0
            
            elif self.state == CircuitState.CLOSED:
                print(f"[{self.name}] Failure count: {self.failure_count}/{self.config.failure_threshold}")
                
                if self.failure_count >= self.config.failure_threshold:
                    print(f"[{self.name}] Threshold reached, transitioning to OPEN")
                    self.state = CircuitState.OPEN
    
    async def get_state(self) -> CircuitState:
        """Get current state"""
        async with self.lock:
            return self.state
    
    async def reset(self):
        """Manually reset circuit breaker"""
        async with self.lock:
            self.state = CircuitState.CLOSED
            self.failure_count = 0
            self.success_count = 0
            self.last_failure_time = None
            print(f"[{self.name}] Manually reset to CLOSED")

# Example usage
async def flaky_service(fail: bool = False):
    """Simulated service that can fail"""
    await asyncio.sleep(0.1)
    if fail:
        raise ConnectionError("Service unavailable")
    return "success"

async def circuit_breaker_example():
    """Demonstrate circuit breaker"""
    config = CircuitBreakerConfig(
        failure_threshold=3,
        success_threshold=2,
        timeout=5.0
    )
    breaker = CircuitBreaker("api-service", config)
    
    # Cause failures to open circuit
    print("=== Causing failures ===")
    for i in range(5):
        try:
            result = await breaker.call(flaky_service, fail=True)
        except (ConnectionError, CircuitBreakerError) as e:
            print(f"Call {i+1}: {type(e).__name__}")
    
    # Circuit should be open now
    state = await breaker.get_state()
    print(f"\nCircuit state: {state.name}")
    
    # Wait for timeout
    print(f"\nWaiting {config.timeout}s for circuit to try half-open...")
    await asyncio.sleep(config.timeout)
    
    # Try successful calls to close circuit
    print("\n=== Successful calls ===")
    for i in range(3):
        try:
            result = await breaker.call(flaky_service, fail=False)
            print(f"Call {i+1}: {result}")
        except CircuitBreakerError as e:
            print(f"Call {i+1}: {e}")
    
    # Check final state
    state = await breaker.get_state()
    print(f"\nFinal circuit state: {state.name}")
```

### 2. Circuit Breaker with Metrics

```python
import asyncio
from typing import Dict, Any
from dataclasses import dataclass, field
import time

@dataclass
class CircuitBreakerMetrics:
    """Metrics for circuit breaker"""
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    rejected_calls: int = 0
    state_transitions: Dict[str, int] = field(default_factory=lambda: {
        'CLOSED->OPEN': 0,
        'OPEN->HALF_OPEN': 0,
        'HALF_OPEN->CLOSED': 0,
        'HALF_OPEN->OPEN': 0
    })
    last_state_change: Optional[float] = None
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate"""
        total = self.successful_calls + self.failed_calls
        return self.successful_calls / total if total > 0 else 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'total_calls': self.total_calls,
            'successful_calls': self.successful_calls,
            'failed_calls': self.failed_calls,
            'rejected_calls': self.rejected_calls,
            'success_rate': self.success_rate,
            'state_transitions': self.state_transitions,
            'last_state_change': self.last_state_change
        }

class MonitoredCircuitBreaker(CircuitBreaker):
    """Circuit breaker with metrics tracking"""
    
    def __init__(self, name: str, config: Optional[CircuitBreakerConfig] = None):
        super().__init__(name, config)
        self.metrics = CircuitBreakerMetrics()
    
    async def call(self, func: Callable[..., Awaitable[T]], *args, **kwargs) -> T:
        """Execute with metrics tracking"""
        self.metrics.total_calls += 1
        
        try:
            result = await super().call(func, *args, **kwargs)
            self.metrics.successful_calls += 1
            return result
        
        except CircuitBreakerError:
            self.metrics.rejected_calls += 1
            raise
        
        except Exception:
            self.metrics.failed_calls += 1
            raise
    
    async def _on_success(self):
        """Track state transitions on success"""
        old_state = self.state
        await super()._on_success()
        await self._track_transition(old_state, self.state)
    
    async def _on_failure(self):
        """Track state transitions on failure"""
        old_state = self.state
        await super()._on_failure()
        await self._track_transition(old_state, self.state)
    
    async def _track_transition(self, old_state: CircuitState, new_state: CircuitState):
        """Track state transition"""
        if old_state != new_state:
            transition = f"{old_state.name}->{new_state.name}"
            self.metrics.state_transitions[transition] = \
                self.metrics.state_transitions.get(transition, 0) + 1
            self.metrics.last_state_change = time.time()
    
    async def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics"""
        async with self.lock:
            return self.metrics.to_dict()
```

## Combining Retry and Circuit Breaker

```python
import asyncio
from typing import Callable, Awaitable, Optional

class ResilientClient:
    """
    Client combining retry logic and circuit breaker.
    
    Provides comprehensive resilience for service calls.
    """
    
    def __init__(
        self,
        name: str,
        retry_config: Optional[RetryConfig] = None,
        circuit_config: Optional[CircuitBreakerConfig] = None
    ):
        self.name = name
        self.retry_config = retry_config or RetryConfig()
        self.circuit_breaker = MonitoredCircuitBreaker(name, circuit_config)
    
    async def call(
        self,
        func: Callable[..., Awaitable[T]],
        *args,
        **kwargs
    ) -> T:
        """
        Execute function with retry and circuit breaker.
        
        Circuit breaker wraps retry logic to prevent
        retrying when service is known to be down.
        """
        async def retry_wrapper():
            return await retry(func, *args, config=self.retry_config, **kwargs)
        
        return await self.circuit_breaker.call(retry_wrapper)
    
    async def get_health(self) -> Dict[str, Any]:
        """Get health status"""
        state = await self.circuit_breaker.get_state()
        metrics = await self.circuit_breaker.get_metrics()
        
        return {
            'name': self.name,
            'state': state.name,
            'healthy': state == CircuitState.CLOSED,
            'metrics': metrics
        }

# Example usage
async def api_call(endpoint: str, fail_rate: float = 0.3):
    """Simulated API call"""
    await asyncio.sleep(0.1)
    if random.random() < fail_rate:
        raise ConnectionError(f"Failed to reach {endpoint}")
    return {"status": "success", "endpoint": endpoint}

async def resilient_client_example():
    """Demonstrate resilient client"""
    client = ResilientClient(
        "payment-service",
        retry_config=RetryConfig(max_attempts=3, base_delay=0.5),
        circuit_config=CircuitBreakerConfig(failure_threshold=5, timeout=3.0)
    )
    
    # Make multiple calls
    for i in range(20):
        try:
            result = await client.call(api_call, f"/api/payment/{i}")
            print(f"Call {i+1}: Success")
        except (ConnectionError, CircuitBreakerError) as e:
            print(f"Call {i+1}: {type(e).__name__}")
        
        await asyncio.sleep(0.2)
    
    # Check health
    health = await client.get_health()
    print(f"\nService health: {health}")
```

## Fallback Patterns

### 1. Fallback with Cache

```python
import asyncio
from typing import Optional, Callable, Awaitable, Any
import time

class CachedFallback:
    """
    Provide fallback using cached values.
    
    Returns cached data when service is unavailable.
    """
    
    def __init__(self, ttl: float = 300.0):
        self.cache: Dict[str, tuple[Any, float]] = {}
        self.ttl = ttl
        self.lock = asyncio.Lock()
    
    async def call_with_fallback(
        self,
        key: str,
        func: Callable[..., Awaitable[T]],
        *args,
        **kwargs
    ) -> T:
        """
        Call function with cache fallback.
        
        On success, updates cache. On failure, returns cached value.
        """
        try:
            # Try to get fresh data
            result = await func(*args, **kwargs)
            
            # Update cache
            async with self.lock:
                self.cache[key] = (result, time.time())
            
            return result
        
        except Exception as e:
            # Try to use cached value
            async with self.lock:
                if key in self.cache:
                    value, timestamp = self.cache[key]
                    age = time.time() - timestamp
                    
                    if age < self.ttl:
                        print(f"Using cached value (age: {age:.1f}s)")
                        return value
                    else:
                        print(f"Cache expired (age: {age:.1f}s)")
            
            # No valid cache, re-raise exception
            raise

# Example usage
cache_fallback = CachedFallback(ttl=60.0)

async def fetch_user_profile(user_id: int, fail: bool = False):
    """Fetch user profile"""
    await asyncio.sleep(0.1)
    if fail:
        raise ConnectionError("Service unavailable")
    return {"id": user_id, "name": f"User {user_id}"}

async def fallback_example():
    """Demonstrate fallback pattern"""
    # First call succeeds and caches
    profile = await cache_fallback.call_with_fallback(
        "user:1",
        fetch_user_profile,
        1,
        fail=False
    )
    print(f"Fresh data: {profile}")
    
    # Second call fails but returns cached data
    try:
        profile = await cache_fallback.call_with_fallback(
            "user:1",
            fetch_user_profile,
            1,
            fail=True
        )
        print(f"Cached data: {profile}")
    except ConnectionError as e:
        print(f"Failed: {e}")
```

### 2. Fallback Chain

```python
import asyncio
from typing import List, Callable, Awaitable, Optional

class FallbackChain:
    """
    Try multiple fallback strategies in sequence.
    
    Attempts each strategy until one succeeds.
    """
    
    def __init__(self):
        self.strategies: List[Callable[..., Awaitable[Any]]] = []
    
    def add_strategy(self, func: Callable[..., Awaitable[Any]]):
        """Add fallback strategy"""
        self.strategies.append(func)
        return self
    
    async def execute(self, *args, **kwargs) -> Any:
        """Execute strategies until one succeeds"""
        last_exception = None
        
        for i, strategy in enumerate(self.strategies):
            try:
                print(f"Trying strategy {i+1}/{len(self.strategies)}")
                result = await strategy(*args, **kwargs)
                print(f"Strategy {i+1} succeeded")
                return result
            
            except Exception as e:
                print(f"Strategy {i+1} failed: {e}")
                last_exception = e
                continue
        
        raise RuntimeError(
            f"All {len(self.strategies)} fallback strategies failed"
        ) from last_exception

# Example usage
async def primary_service(data: str):
    """Primary service (fails)"""
    raise ConnectionError("Primary service down")

async def secondary_service(data: str):
    """Secondary service (fails)"""
    raise ConnectionError("Secondary service down")

async def cached_service(data: str):
    """Cached service (succeeds)"""
    return f"Cached: {data}"

async def fallback_chain_example():
    """Demonstrate fallback chain"""
    chain = FallbackChain()
    chain.add_strategy(primary_service)
    chain.add_strategy(secondary_service)
    chain.add_strategy(cached_service)
    
    result = await chain.execute("test data")
    print(f"Final result: {result}")
```

## Bulkhead Pattern

```python
import asyncio
from typing import Callable, Awaitable, Optional

class Bulkhead:
    """
    Isolate resources to prevent cascading failures.
    
    Limits concurrent operations to protect system resources.
    """
    
    def __init__(self, name: str, max_concurrent: int, queue_size: int = 0):
        self.name = name
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.queue_size = queue_size
        self.active_count = 0
        self.rejected_count = 0
        self.lock = asyncio.Lock()
    
    async def execute(
        self,
        func: Callable[..., Awaitable[T]],
        *args,
        **kwargs
    ) -> T:
        """Execute function within bulkhead limits"""
        # Check if we can accept more work
        async with self.lock:
            if self.queue_size > 0:
                waiting = self.semaphore._value
                if waiting >= self.queue_size:
                    self.rejected_count += 1
                    raise RuntimeError(
                        f"Bulkhead '{self.name}' queue full "
                        f"({waiting}/{self.queue_size})"
                    )
        
        async with self.semaphore:
            async with self.lock:
                self.active_count += 1
            
            try:
                return await func(*args, **kwargs)
            finally:
                async with self.lock:
                    self.active_count -= 1
    
    async def get_stats(self) -> Dict[str, int]:
        """Get bulkhead statistics"""
        async with self.lock:
            return {
                'active': self.active_count,
                'available': self.semaphore._value,
                'rejected': self.rejected_count
            }

# Example: Separate bulkheads for different services
async def critical_service():
    """Critical service with dedicated resources"""
    await asyncio.sleep(0.5)
    return "critical result"

async def non_critical_service():
    """Non-critical service with limited resources"""
    await asyncio.sleep(0.5)
    return "non-critical result"

async def bulkhead_example():
    """Demonstrate bulkhead pattern"""
    # Separate resource pools
    critical_bulkhead = Bulkhead("critical", max_concurrent=10)
    non_critical_bulkhead = Bulkhead("non-critical", max_concurrent=2)
    
    # Flood both services
    critical_tasks = [
        critical_bulkhead.execute(critical_service)
        for _ in range(20)
    ]
    
    non_critical_tasks = [
        non_critical_bulkhead.execute(non_critical_service)
        for _ in range(20)
    ]
    
    # Critical service has more resources
    results = await asyncio.gather(
        *critical_tasks,
        *non_critical_tasks,
        return_exceptions=True
    )
    
    # Check stats
    critical_stats = await critical_bulkhead.get_stats()
    non_critical_stats = await non_critical_bulkhead.get_stats()
    
    print(f"Critical bulkhead: {critical_stats}")
    print(f"Non-critical bulkhead: {non_critical_stats}")
```

## Complete Resilience Framework

```python
import asyncio
from typing import Callable, Awaitable, Optional, Any, Dict
from dataclasses import dataclass

@dataclass
class ResilienceConfig:
    """Complete resilience configuration"""
    retry_config: RetryConfig
    circuit_config: CircuitBreakerConfig
    timeout: float = 30.0
    bulkhead_size: int = 100
    enable_fallback: bool = True

class ResilientService:
    """
    Complete resilience framework.
    
    Combines all resilience patterns:
    - Retry with backoff
    - Circuit breaker
    - Timeout
    - Bulkhead
    - Fallback
    """
    
    def __init__(self, name: str, config: ResilienceConfig):
        self.name = name
        self.config = config
        
        self.client = ResilientClient(
            name,
            retry_config=config.retry_config,
            circuit_config=config.circuit_config
        )
        self.bulkhead = Bulkhead(name, max_concurrent=config.bulkhead_size)
        self.fallback_cache = CachedFallback() if config.enable_fallback else None
    
    async def call(
        self,
        func: Callable[..., Awaitable[T]],
        *args,
        fallback_key: Optional[str] = None,
        **kwargs
    ) -> T:
        """Execute with full resilience"""
        async def execute():
            # Bulkhead isolation
            async def bulkhead_wrapper():
                # Timeout protection
                async def timeout_wrapper():
                    return await self.client.call(func, *args, **kwargs)
                
                return await asyncio.wait_for(
                    timeout_wrapper(),
                    timeout=self.config.timeout
                )
            
            return await self.bulkhead.execute(bulkhead_wrapper)
        
        # Fallback if enabled
        if self.fallback_cache and fallback_key:
            return await self.fallback_cache.call_with_fallback(
                fallback_key,
                execute
            )
        else:
            return await execute()
    
    async def get_health(self) -> Dict[str, Any]:
        """Get comprehensive health status"""
        client_health = await self.client.get_health()
        bulkhead_stats = await self.bulkhead.get_stats()
        
        return {
            **client_health,
            'bulkhead': bulkhead_stats,
            'timeout': self.config.timeout
        }

# Example: Production-ready service
async def production_example():
    """Demonstrate production-ready resilient service"""
    config = ResilienceConfig(
        retry_config=RetryConfig(max_attempts=3, base_delay=1.0),
        circuit_config=CircuitBreakerConfig(failure_threshold=5, timeout=30.0),
        timeout=10.0,
        bulkhead_size=50,
        enable_fallback=True
    )
    
    service = ResilientService("payment-api", config)
    
    # Make resilient calls
    for i in range(10):
        try:
            result = await service.call(
                api_call,
                f"/payment/{i}",
                fallback_key=f"payment:{i}"
            )
            print(f"Payment {i}: Success")
        except Exception as e:
            print(f"Payment {i}: {type(e).__name__}")
    
    # Check health
    health = await service.get_health()
    print(f"\nService health: {health}")
```

## Mental Model Summary

**Resilience patterns protect against failures:**

1. **Retry**: Automatically retry transient failures
2. **Circuit breaker**: Stop calling failing services
3. **Timeout**: Prevent indefinite waits
4. **Bulkhead**: Isolate resources
5. **Fallback**: Provide alternative responses

**Key principles:**

- **Fail fast**: Don't wait for doomed operations
- **Isolate failures**: Prevent cascading
- **Degrade gracefully**: Reduced functionality > total failure
- **Monitor health**: Track metrics and state
- **Combine patterns**: Use multiple strategies together

**Best practices:**

- Use exponential backoff with jitter
- Set appropriate thresholds and timeouts
- Implement comprehensive monitoring
- Test failure scenarios
- Provide meaningful fallbacks
- Document resilience behavior
- Monitor circuit breaker states
- Use bulkheads for resource isolation

Resilience patterns are essential for production async applications, providing robustness against the inevitable failures in distributed systems.