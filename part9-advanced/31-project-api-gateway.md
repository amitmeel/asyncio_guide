# Chapter 31: API Gateway with Load Balancing

## Project Overview

Build a production-ready API gateway demonstrating:
- Request routing
- Load balancing (round-robin, least-connections, weighted)
- Health checking
- Circuit breakers
- Rate limiting
- Request/response transformation
- Caching
- Authentication

**Complexity**: High
**Patterns Used**: 20+
**Lines of Code**: ~1800

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    API Gateway                          │
│  ┌───────────────────────────────────────────────────┐ │
│  │  Request Handler                                  │ │
│  │  - Parse request                                  │ │
│  │  - Authenticate                                   │ │
│  │  - Rate limit                                     │ │
│  └───────────────────────────────────────────────────┘ │
│  ┌───────────────────────────────────────────────────┐ │
│  │  Router                                           │ │
│  │  - Match routes                                   │ │
│  │  - Select backend                                 │ │
│  │  - Transform request                              │ │
│  └───────────────────────────────────────────────────┘ │
│  ┌───────────────────────────────────────────────────┐ │
│  │  Load Balancer                                    │ │
│  │  - Round-robin                                    │ │
│  │  - Least connections                              │ │
│  │  - Weighted                                       │ │
│  └───────────────────────────────────────────────────┘ │
│  ┌───────────────────────────────────────────────────┐ │
│  │  Health Checker                                   │ │
│  │  - Periodic health checks                         │ │
│  │  - Mark unhealthy backends                        │ │
│  │  - Auto-recovery                                  │ │
│  └───────────────────────────────────────────────────┘ │
│  ┌───────────────────────────────────────────────────┐ │
│  │  Circuit Breaker                                  │ │
│  │  - Track failures                                 │ │
│  │  - Open/close circuit                             │ │
│  │  - Fallback responses                             │ │
│  └───────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────┘
                          │
        ┌─────────────────┼─────────────────┐
        │                 │                 │
┌───────▼───────┐ ┌───────▼───────┐ ┌───────▼───────┐
│  Backend 1    │ │  Backend 2    │ │  Backend 3    │
│  (Service A)  │ │  (Service A)  │ │  (Service B)  │
└───────────────┘ └───────────────┘ └───────────────┘
```

## Design Decisions

### 1. Load Balancing Strategy

**Decision**: Support multiple strategies
**Why**:
- Round-robin: Simple, fair distribution
- Least-connections: Better for long-lived connections
- Weighted: Handle heterogeneous backends

**Trade-off**: More complex than single strategy

### 2. Health Checking

**Decision**: Active health checks with passive monitoring
**Why**:
- Active: Detect failures proactively
- Passive: React to real request failures
- Combined: Best reliability

**Alternative**: Passive only (simpler but slower detection)

### 3. Circuit Breaker Pattern

**Decision**: Per-backend circuit breakers
**Why**:
- Prevent cascade failures
- Fast failure detection
- Automatic recovery

**Trade-off**: More state to manage

## Implementation

### 1. Configuration (config.py)

```python
"""
Configuration for API gateway.

Design Decision: Declarative configuration
Why: Easy to understand, validate, and modify
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional
from enum import Enum
import os

class LoadBalancingStrategy(Enum):
    """Load balancing strategies"""
    ROUND_ROBIN = "round_robin"
    LEAST_CONNECTIONS = "least_connections"
    WEIGHTED = "weighted"

@dataclass
class Backend:
    """Backend server configuration"""
    id: str
    host: str
    port: int
    weight: int = 1
    max_connections: int = 100
    
    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}"

@dataclass
class Route:
    """Route configuration"""
    path: str
    service: str
    methods: List[str] = field(default_factory=lambda: ["GET"])
    strip_prefix: bool = False
    timeout: float = 30.0
    
    # Request transformation
    add_headers: Dict[str, str] = field(default_factory=dict)
    remove_headers: List[str] = field(default_factory=list)

@dataclass
class Service:
    """Service configuration"""
    name: str
    backends: List[Backend]
    strategy: LoadBalancingStrategy = LoadBalancingStrategy.ROUND_ROBIN
    health_check_path: str = "/health"
    health_check_interval: float = 10.0
    health_check_timeout: float = 5.0

@dataclass
class CircuitBreakerConfig:
    """Circuit breaker configuration"""
    failure_threshold: int = 5
    success_threshold: int = 2
    timeout: float = 60.0  # seconds in open state

@dataclass
class RateLimitConfig:
    """Rate limiting configuration"""
    requests_per_second: int = 100
    burst: int = 200

@dataclass
class GatewayConfig:
    """Gateway configuration"""
    host: str = os.getenv("GATEWAY_HOST", "0.0.0.0")
    port: int = int(os.getenv("GATEWAY_PORT", "8080"))
    
    routes: List[Route] = field(default_factory=list)
    services: Dict[str, Service] = field(default_factory=dict)
    
    circuit_breaker: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)
    rate_limit: RateLimitConfig = field(default_factory=RateLimitConfig)
    
    # Caching
    cache_enabled: bool = True
    cache_ttl: int = 300  # seconds

# Example configuration
config = GatewayConfig(
    routes=[
        Route(
            path="/api/users",
            service="user-service",
            methods=["GET", "POST"],
            add_headers={"X-Gateway": "v1"}
        ),
        Route(
            path="/api/orders",
            service="order-service",
            methods=["GET", "POST", "PUT", "DELETE"]
        )
    ],
    services={
        "user-service": Service(
            name="user-service",
            backends=[
                Backend(id="user-1", host="localhost", port=9001),
                Backend(id="user-2", host="localhost", port=9002),
            ],
            strategy=LoadBalancingStrategy.ROUND_ROBIN
        ),
        "order-service": Service(
            name="order-service",
            backends=[
                Backend(id="order-1", host="localhost", port=9003, weight=2),
                Backend(id="order-2", host="localhost", port=9004, weight=1),
            ],
            strategy=LoadBalancingStrategy.WEIGHTED
        )
    }
)
```

### 2. Backend Manager (backend.py)

```python
"""
Backend management with health checking.

Design Decision: Track backend state and connections
Why: Enable intelligent load balancing and failure detection
"""

import asyncio
import logging
from typing import Optional, Dict
from datetime import datetime
from enum import Enum
import aiohttp

class BackendState(Enum):
    """Backend health states"""
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"

class BackendInfo:
    """
    Backend runtime information.
    
    Design Decision: Track connections and health
    Why: Enable least-connections and health-based routing
    """
    
    def __init__(self, backend: Backend):
        self.backend = backend
        self.state = BackendState.UNKNOWN
        self.active_connections = 0
        self.total_requests = 0
        self.failed_requests = 0
        self.last_health_check: Optional[datetime] = None
        self.last_failure: Optional[datetime] = None
    
    def is_healthy(self) -> bool:
        """Check if backend is healthy"""
        return self.state == BackendState.HEALTHY
    
    def can_accept_connection(self) -> bool:
        """Check if backend can accept new connection"""
        return (
            self.is_healthy() and
            self.active_connections < self.backend.max_connections
        )
    
    def acquire_connection(self):
        """Acquire connection slot"""
        self.active_connections += 1
        self.total_requests += 1
    
    def release_connection(self):
        """Release connection slot"""
        self.active_connections = max(0, self.active_connections - 1)
    
    def record_failure(self):
        """Record request failure"""
        self.failed_requests += 1
        self.last_failure = datetime.utcnow()
    
    def get_stats(self) -> dict:
        """Get backend statistics"""
        return {
            'id': self.backend.id,
            'url': self.backend.url,
            'state': self.state.value,
            'active_connections': self.active_connections,
            'total_requests': self.total_requests,
            'failed_requests': self.failed_requests,
            'failure_rate': (
                self.failed_requests / self.total_requests
                if self.total_requests > 0 else 0
            )
        }

class BackendManager:
    """
    Manage backends with health checking.
    
    Design Decision: Active health checks with passive monitoring
    Why: Proactive failure detection + real-time feedback
    
    Patterns:
    - Health check pattern
    - State management
    - Periodic monitoring
    """
    
    def __init__(self, service: Service):
        self.service = service
        self.backends: Dict[str, BackendInfo] = {
            backend.id: BackendInfo(backend)
            for backend in service.backends
        }
        self.logger = logging.getLogger(__name__)
        self._health_check_task: Optional[asyncio.Task] = None
    
    async def start(self):
        """Start health checking"""
        self._health_check_task = asyncio.create_task(self._health_check_loop())
        self.logger.info(f"Backend manager started for {self.service.name}")
    
    async def stop(self):
        """Stop health checking"""
        if self._health_check_task:
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass
        self.logger.info(f"Backend manager stopped for {self.service.name}")
    
    def get_healthy_backends(self) -> List[BackendInfo]:
        """Get list of healthy backends"""
        return [
            info for info in self.backends.values()
            if info.can_accept_connection()
        ]
    
    async def _health_check_loop(self):
        """
        Periodic health check loop.
        
        Design Decision: Concurrent checks with timeout
        Why: Fast detection, don't block on slow backends
        """
        while True:
            try:
                await asyncio.sleep(self.service.health_check_interval)
                
                # Check all backends concurrently
                tasks = [
                    self._check_backend(info)
                    for info in self.backends.values()
                ]
                await asyncio.gather(*tasks, return_exceptions=True)
            
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Health check loop error: {e}")
    
    async def _check_backend(self, info: BackendInfo):
        """
        Check single backend health.
        
        Design Decision: HTTP health check with timeout
        Why: Standard, simple, reliable
        """
        url = f"{info.backend.url}{self.service.health_check_path}"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with asyncio.timeout(self.service.health_check_timeout):
                    async with session.get(url) as response:
                        if response.status == 200:
                            info.state = BackendState.HEALTHY
                            info.last_health_check = datetime.utcnow()
                        else:
                            info.state = BackendState.UNHEALTHY
                            self.logger.warning(
                                f"Backend {info.backend.id} unhealthy: {response.status}"
                            )
        
        except asyncio.TimeoutError:
            info.state = BackendState.UNHEALTHY
            self.logger.warning(f"Backend {info.backend.id} health check timeout")
        
        except Exception as e:
            info.state = BackendState.UNHEALTHY
            self.logger.error(f"Backend {info.backend.id} health check failed: {e}")
```

### 3. Load Balancer (balancer.py)

```python
"""
Load balancing strategies.

Design Decision: Strategy pattern
Why: Pluggable algorithms, easy to test, extensible
"""

import asyncio
import logging
from typing import Optional, Protocol
from abc import ABC, abstractmethod

class LoadBalancer(ABC):
    """
    Base load balancer.
    
    Design Decision: Strategy pattern
    Why: Different algorithms for different use cases
    """
    
    @abstractmethod
    async def select_backend(self, backends: List[BackendInfo]) -> Optional[BackendInfo]:
        """Select backend for request"""
        pass

class RoundRobinBalancer(LoadBalancer):
    """
    Round-robin load balancer.
    
    Design Decision: Simple counter with modulo
    Why: Fair distribution, no state per backend
    
    Pattern: Round-robin scheduling
    """
    
    def __init__(self):
        self.counter = 0
        self.lock = asyncio.Lock()
    
    async def select_backend(self, backends: List[BackendInfo]) -> Optional[BackendInfo]:
        """Select next backend in round-robin order"""
        if not backends:
            return None
        
        async with self.lock:
            backend = backends[self.counter % len(backends)]
            self.counter += 1
            return backend

class LeastConnectionsBalancer(LoadBalancer):
    """
    Least connections load balancer.
    
    Design Decision: Select backend with fewest active connections
    Why: Better for long-lived connections, adaptive to load
    
    Pattern: Least-loaded selection
    """
    
    async def select_backend(self, backends: List[BackendInfo]) -> Optional[BackendInfo]:
        """Select backend with least active connections"""
        if not backends:
            return None
        
        return min(backends, key=lambda b: b.active_connections)

class WeightedBalancer(LoadBalancer):
    """
    Weighted load balancer.
    
    Design Decision: Weighted round-robin
    Why: Handle heterogeneous backends (different capacities)
    
    Pattern: Weighted scheduling
    """
    
    def __init__(self):
        self.current_weight = 0
        self.lock = asyncio.Lock()
    
    async def select_backend(self, backends: List[BackendInfo]) -> Optional[BackendInfo]:
        """Select backend based on weights"""
        if not backends:
            return None
        
        async with self.lock:
            # Calculate total weight
            total_weight = sum(b.backend.weight for b in backends)
            
            # Select based on current weight
            self.current_weight = (self.current_weight + 1) % total_weight
            
            cumulative = 0
            for backend in backends:
                cumulative += backend.backend.weight
                if self.current_weight < cumulative:
                    return backend
            
            return backends[0]

def create_balancer(strategy: LoadBalancingStrategy) -> LoadBalancer:
    """Factory function to create load balancer"""
    if strategy == LoadBalancingStrategy.ROUND_ROBIN:
        return RoundRobinBalancer()
    elif strategy == LoadBalancingStrategy.LEAST_CONNECTIONS:
        return LeastConnectionsBalancer()
    elif strategy == LoadBalancingStrategy.WEIGHTED:
        return WeightedBalancer()
    else:
        raise ValueError(f"Unknown strategy: {strategy}")
```

### 4. Circuit Breaker (circuit_breaker.py)

```python
"""
Circuit breaker implementation.

Design Decision: Per-backend circuit breakers
Why: Isolate failures, prevent cascade, fast failure
"""

import asyncio
import logging
from typing import Optional
from datetime import datetime, timedelta
from enum import Enum

class CircuitState(Enum):
    """Circuit breaker states"""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing recovery

class CircuitBreaker:
    """
    Circuit breaker for backend.
    
    Design Decision: Three-state circuit breaker
    Why: Standard pattern, proven reliability
    
    States:
    - CLOSED: Normal operation, track failures
    - OPEN: Too many failures, reject requests
    - HALF_OPEN: Testing recovery, allow limited requests
    
    Pattern: Circuit breaker (Chapter 26)
    """
    
    def __init__(self, backend_id: str, config: CircuitBreakerConfig):
        self.backend_id = backend_id
        self.config = config
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.logger = logging.getLogger(__name__)
        self.lock = asyncio.Lock()
    
    async def call(self, func, *args, **kwargs):
        """
        Execute function with circuit breaker protection.
        
        Design Decision: Async context manager style
        Why: Clean API, automatic state management
        """
        async with self.lock:
            # Check if circuit is open
            if self.state == CircuitState.OPEN:
                # Check if timeout expired
                if self._should_attempt_reset():
                    self.state = CircuitState.HALF_OPEN
                    self.logger.info(f"Circuit {self.backend_id} half-open")
                else:
                    raise CircuitBreakerOpenError(
                        f"Circuit breaker open for {self.backend_id}"
                    )
        
        # Execute function
        try:
            result = await func(*args, **kwargs)
            await self._on_success()
            return result
        
        except Exception as e:
            await self._on_failure()
            raise
    
    async def _on_success(self):
        """Handle successful request"""
        async with self.lock:
            self.failure_count = 0
            
            if self.state == CircuitState.HALF_OPEN:
                self.success_count += 1
                
                # Close circuit if enough successes
                if self.success_count >= self.config.success_threshold:
                    self.state = CircuitState.CLOSED
                    self.success_count = 0
                    self.logger.info(f"Circuit {self.backend_id} closed")
    
    async def _on_failure(self):
        """Handle failed request"""
        async with self.lock:
            self.failure_count += 1
            self.last_failure_time = datetime.utcnow()
            
            # Open circuit if too many failures
            if self.failure_count >= self.config.failure_threshold:
                if self.state != CircuitState.OPEN:
                    self.state = CircuitState.OPEN
                    self.logger.warning(f"Circuit {self.backend_id} opened")
            
            # If half-open, go back to open on any failure
            if self.state == CircuitState.HALF_OPEN:
                self.state = CircuitState.OPEN
                self.success_count = 0
                self.logger.warning(f"Circuit {self.backend_id} reopened")
    
    def _should_attempt_reset(self) -> bool:
        """Check if should attempt to reset circuit"""
        if not self.last_failure_time:
            return True
        
        elapsed = datetime.utcnow() - self.last_failure_time
        return elapsed.total_seconds() >= self.config.timeout
    
    def get_state(self) -> dict:
        """Get circuit breaker state"""
        return {
            'backend_id': self.backend_id,
            'state': self.state.value,
            'failure_count': self.failure_count,
            'success_count': self.success_count
        }

class CircuitBreakerOpenError(Exception):
    """Raised when circuit breaker is open"""
    pass
```

### 5. Rate Limiter (rate_limiter.py)

```python
"""
Rate limiting implementation.

Design Decision: Token bucket algorithm
Why: Allows bursts, smooth rate limiting, efficient
"""

import asyncio
import logging
from typing import Dict
from datetime import datetime

class TokenBucket:
    """
    Token bucket rate limiter.
    
    Design Decision: Token bucket algorithm
    Why: Allows bursts while maintaining average rate
    
    Pattern: Token bucket rate limiting
    """
    
    def __init__(self, rate: float, burst: int):
        self.rate = rate  # tokens per second
        self.burst = burst  # max tokens
        self.tokens = burst
        self.last_update = datetime.utcnow()
        self.lock = asyncio.Lock()
    
    async def acquire(self, tokens: int = 1) -> bool:
        """
        Try to acquire tokens.
        
        Returns True if acquired, False if rate limited
        """
        async with self.lock:
            now = datetime.utcnow()
            elapsed = (now - self.last_update).total_seconds()
            
            # Add new tokens based on elapsed time
            self.tokens = min(
                self.burst,
                self.tokens + elapsed * self.rate
            )
            self.last_update = now
            
            # Try to consume tokens
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            
            return False

class RateLimiter:
    """
    Per-client rate limiter.
    
    Design Decision: Separate bucket per client
    Why: Fair per-client limits, prevent single client abuse
    """
    
    def __init__(self, config: RateLimitConfig):
        self.config = config
        self.buckets: Dict[str, TokenBucket] = {}
        self.logger = logging.getLogger(__name__)
    
    async def check_rate_limit(self, client_id: str) -> bool:
        """
        Check if client is rate limited.
        
        Returns True if allowed, False if rate limited
        """
        # Get or create bucket for client
        if client_id not in self.buckets:
            self.buckets[client_id] = TokenBucket(
                rate=self.config.requests_per_second,
                burst=self.config.burst
            )
        
        bucket = self.buckets[client_id]
        allowed = await bucket.acquire()
        
        if not allowed:
            self.logger.warning(f"Rate limit exceeded for {client_id}")
        
        return allowed
```

This chapter continues in part 2 with the complete gateway implementation, routing, caching, and deployment.