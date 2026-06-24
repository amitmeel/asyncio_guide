# Chapter 28: Real-World Project - Event-Driven Microservice

## Overview

This chapter builds a production-ready event-driven microservice that demonstrates:

- **FastAPI** for HTTP endpoints
- **Event bus** for inter-service communication
- **Message queue** with Redis
- **Database** with connection pooling
- **Background workers** for async tasks
- **Health checks** and metrics
- **Graceful shutdown** and lifecycle management
- **Structured logging** with context
- **Circuit breakers** and resilience
- **WebSocket** for real-time updates

This project integrates all major concepts from the guide into a complete, deployable microservice.

## Project Architecture

```
┌─────────────────────────────────────────────────────────────┐
│              Order Processing Microservice                   │
│                                                              │
│  ┌────────────────────────────────────────────────────┐    │
│  │  HTTP API (FastAPI)                                 │    │
│  │  POST /orders, GET /orders/{id}, WebSocket /ws     │    │
│  └────────────────────────────────────────────────────┘    │
│                           ↓                                  │
│  ┌────────────────────────────────────────────────────┐    │
│  │  Event Bus (Redis Pub/Sub)                         │    │
│  │  order.created → order.processing → order.complete │    │
│  └────────────────────────────────────────────────────┘    │
│                           ↓                                  │
│  ┌────────────────────────────────────────────────────┐    │
│  │  Background Workers                                 │    │
│  │  - Payment processor                                │    │
│  │  - Inventory checker                                │    │
│  │  - Notification sender                              │    │
│  └────────────────────────────────────────────────────┘    │
│                           ↓                                  │
│  ┌────────────────────────────────────────────────────┐    │
│  │  Database (PostgreSQL)                              │    │
│  │  Orders, Events, Metrics                            │    │
│  └────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. Domain Models

```python
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum
import uuid

class OrderStatus(Enum):
    """Order status states"""
    PENDING = "pending"
    PROCESSING = "processing"
    PAYMENT_FAILED = "payment_failed"
    COMPLETED = "completed"
    CANCELLED = "cancelled"

class EventType(Enum):
    """Event types"""
    ORDER_CREATED = "order.created"
    ORDER_PROCESSING = "order.processing"
    PAYMENT_PROCESSED = "payment.processed"
    PAYMENT_FAILED = "payment.failed"
    INVENTORY_CHECKED = "inventory.checked"
    ORDER_COMPLETED = "order.completed"
    ORDER_CANCELLED = "order.cancelled"

@dataclass
class OrderItem:
    """Order line item"""
    product_id: str
    quantity: int
    price: float
    
    @property
    def total(self) -> float:
        return self.quantity * self.price

@dataclass
class Order:
    """Order aggregate"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    customer_id: str = ""
    items: List[OrderItem] = field(default_factory=list)
    status: OrderStatus = OrderStatus.PENDING
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    
    @property
    def total(self) -> float:
        return sum(item.total for item in self.items)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'customer_id': self.customer_id,
            'items': [
                {
                    'product_id': item.product_id,
                    'quantity': item.quantity,
                    'price': item.price
                }
                for item in self.items
            ],
            'status': self.status.value,
            'total': self.total,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }

@dataclass
class DomainEvent:
    """Domain event"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    type: EventType = EventType.ORDER_CREATED
    aggregate_id: str = ""
    data: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'type': self.type.value,
            'aggregate_id': self.aggregate_id,
            'data': self.data,
            'timestamp': self.timestamp.isoformat()
        }

### 2. Event Bus

```python
import asyncio
import redis.asyncio as redis
import json
from typing import Callable, Awaitable, Dict, Set
import logging

class EventBus:
    """
    Event bus using Redis Pub/Sub.
    
    Enables event-driven communication between components.
    """
    
    def __init__(self, redis_url: str = "redis://localhost"):
        self.redis_url = redis_url
        self.redis: Optional[redis.Redis] = None
        self.pubsub: Optional[redis.client.PubSub] = None
        self.handlers: Dict[EventType, Set[Callable]] = {}
        self.logger = logging.getLogger(__name__)
        self.running = False
    
    async def connect(self):
        """Connect to Redis"""
        self.redis = await redis.from_url(self.redis_url)
        self.pubsub = self.redis.pubsub()
        self.logger.info("Event bus connected")
    
    async def disconnect(self):
        """Disconnect from Redis"""
        if self.pubsub:
            await self.pubsub.close()
        if self.redis:
            await self.redis.close()
        self.logger.info("Event bus disconnected")
    
    def subscribe(self, event_type: EventType, handler: Callable[[DomainEvent], Awaitable[None]]):
        """Subscribe to event type"""
        if event_type not in self.handlers:
            self.handlers[event_type] = set()
        self.handlers[event_type].add(handler)
        self.logger.info(f"Subscribed to {event_type.value}")
    
    async def publish(self, event: DomainEvent):
        """Publish event"""
        channel = event.type.value
        message = json.dumps(event.to_dict())
        
        await self.redis.publish(channel, message)
        self.logger.info(f"Published event: {event.type.value} (id={event.id})")
    
    async def start(self):
        """Start listening for events"""
        self.running = True
        
        # Subscribe to all channels we have handlers for
        channels = [event_type.value for event_type in self.handlers.keys()]
        if channels:
            await self.pubsub.subscribe(*channels)
            self.logger.info(f"Listening on channels: {channels}")
        
        # Start message loop
        asyncio.create_task(self._message_loop())
    
    async def stop(self):
        """Stop listening"""
        self.running = False
    
    async def _message_loop(self):
        """Process incoming messages"""
        try:
            async for message in self.pubsub.listen():
                if message['type'] == 'message':
                    await self._handle_message(message)
        except asyncio.CancelledError:
            pass
    
    async def _handle_message(self, message: Dict):
        """Handle incoming message"""
        try:
            # Parse event
            data = json.loads(message['data'])
            event_type = EventType(data['type'])
            event = DomainEvent(
                id=data['id'],
                type=event_type,
                aggregate_id=data['aggregate_id'],
                data=data['data'],
                timestamp=datetime.fromisoformat(data['timestamp'])
            )
            
            # Call handlers
            handlers = self.handlers.get(event_type, set())
            for handler in handlers:
                try:
                    await handler(event)
                except Exception as e:
                    self.logger.error(f"Handler error for {event_type.value}: {e}")
        
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")

### 3. Repository

```python
import asyncio
import asyncpg
from typing import Optional, List

class OrderRepository:
    """
    Repository for order persistence.
    
    Handles database operations for orders.
    """
    
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool
    
    async def save(self, order: Order):
        """Save order"""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO orders (id, customer_id, status, total, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (id) DO UPDATE
                SET status = $3, updated_at = $6
                """,
                order.id,
                order.customer_id,
                order.status.value,
                order.total,
                order.created_at,
                order.updated_at
            )
            
            # Save items
            for item in order.items:
                await conn.execute(
                    """
                    INSERT INTO order_items (order_id, product_id, quantity, price)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (order_id, product_id) DO UPDATE
                    SET quantity = $3, price = $4
                    """,
                    order.id,
                    item.product_id,
                    item.quantity,
                    item.price
                )
    
    async def get(self, order_id: str) -> Optional[Order]:
        """Get order by ID"""
        async with self.pool.acquire() as conn:
            # Get order
            row = await conn.fetchrow(
                "SELECT * FROM orders WHERE id = $1",
                order_id
            )
            
            if not row:
                return None
            
            # Get items
            items_rows = await conn.fetch(
                "SELECT * FROM order_items WHERE order_id = $1",
                order_id
            )
            
            items = [
                OrderItem(
                    product_id=item['product_id'],
                    quantity=item['quantity'],
                    price=item['price']
                )
                for item in items_rows
            ]
            
            return Order(
                id=row['id'],
                customer_id=row['customer_id'],
                items=items,
                status=OrderStatus(row['status']),
                created_at=row['created_at'],
                updated_at=row['updated_at']
            )
    
    async def list(self, customer_id: Optional[str] = None, limit: int = 100) -> List[Order]:
        """List orders"""
        async with self.pool.acquire() as conn:
            if customer_id:
                rows = await conn.fetch(
                    "SELECT * FROM orders WHERE customer_id = $1 ORDER BY created_at DESC LIMIT $2",
                    customer_id,
                    limit
                )
            else:
                rows = await conn.fetch(
                    "SELECT * FROM orders ORDER BY created_at DESC LIMIT $1",
                    limit
                )
            
            orders = []
            for row in rows:
                items_rows = await conn.fetch(
                    "SELECT * FROM order_items WHERE order_id = $1",
                    row['id']
                )
                
                items = [
                    OrderItem(
                        product_id=item['product_id'],
                        quantity=item['quantity'],
                        price=item['price']
                    )
                    for item in items_rows
                ]
                
                orders.append(Order(
                    id=row['id'],
                    customer_id=row['customer_id'],
                    items=items,
                    status=OrderStatus(row['status']),
                    created_at=row['created_at'],
                    updated_at=row['updated_at']
                ))
            
            return orders

### 4. Background Workers

```python
import asyncio
import random
from typing import Optional
import logging

class PaymentProcessor:
    """
    Background worker for payment processing.
    
    Listens for order.created events and processes payments.
    """
    
    def __init__(self, event_bus: EventBus, repository: OrderRepository):
        self.event_bus = event_bus
        self.repository = repository
        self.logger = logging.getLogger(__name__)
    
    async def start(self):
        """Start worker"""
        self.event_bus.subscribe(EventType.ORDER_CREATED, self.handle_order_created)
        self.logger.info("Payment processor started")
    
    async def handle_order_created(self, event: DomainEvent):
        """Handle order created event"""
        order_id = event.aggregate_id
        self.logger.info(f"Processing payment for order {order_id}")
        
        # Get order
        order = await self.repository.get(order_id)
        if not order:
            self.logger.error(f"Order {order_id} not found")
            return
        
        # Update status
        order.status = OrderStatus.PROCESSING
        order.updated_at = datetime.now()
        await self.repository.save(order)
        
        # Publish processing event
        await self.event_bus.publish(DomainEvent(
            type=EventType.ORDER_PROCESSING,
            aggregate_id=order_id,
            data={'status': 'processing'}
        ))
        
        # Simulate payment processing
        await asyncio.sleep(2)
        
        # Simulate success/failure (90% success rate)
        success = random.random() > 0.1
        
        if success:
            self.logger.info(f"Payment successful for order {order_id}")
            await self.event_bus.publish(DomainEvent(
                type=EventType.PAYMENT_PROCESSED,
                aggregate_id=order_id,
                data={'amount': order.total}
            ))
        else:
            self.logger.warning(f"Payment failed for order {order_id}")
            order.status = OrderStatus.PAYMENT_FAILED
            order.updated_at = datetime.now()
            await self.repository.save(order)
            
            await self.event_bus.publish(DomainEvent(
                type=EventType.PAYMENT_FAILED,
                aggregate_id=order_id,
                data={'reason': 'Insufficient funds'}
            ))

class InventoryChecker:
    """Check inventory availability"""
    
    def __init__(self, event_bus: EventBus, repository: OrderRepository):
        self.event_bus = event_bus
        self.repository = repository
        self.logger = logging.getLogger(__name__)
    
    async def start(self):
        """Start worker"""
        self.event_bus.subscribe(EventType.PAYMENT_PROCESSED, self.handle_payment_processed)
        self.logger.info("Inventory checker started")
    
    async def handle_payment_processed(self, event: DomainEvent):
        """Handle payment processed event"""
        order_id = event.aggregate_id
        self.logger.info(f"Checking inventory for order {order_id}")
        
        # Simulate inventory check
        await asyncio.sleep(1)
        
        # Publish inventory checked event
        await self.event_bus.publish(DomainEvent(
            type=EventType.INVENTORY_CHECKED,
            aggregate_id=order_id,
            data={'available': True}
        ))

class OrderCompleter:
    """Complete orders"""
    
    def __init__(self, event_bus: EventBus, repository: OrderRepository):
        self.event_bus = event_bus
        self.repository = repository
        self.logger = logging.getLogger(__name__)
    
    async def start(self):
        """Start worker"""
        self.event_bus.subscribe(EventType.INVENTORY_CHECKED, self.handle_inventory_checked)
        self.logger.info("Order completer started")
    
    async def handle_inventory_checked(self, event: DomainEvent):
        """Handle inventory checked event"""
        order_id = event.aggregate_id
        self.logger.info(f"Completing order {order_id}")
        
        # Get order
        order = await self.repository.get(order_id)
        if not order:
            return
        
        # Update status
        order.status = OrderStatus.COMPLETED
        order.updated_at = datetime.now()
        await self.repository.save(order)
        
        # Publish completed event
        await self.event_bus.publish(DomainEvent(
            type=EventType.ORDER_COMPLETED,
            aggregate_id=order_id,
            data={'completed_at': order.updated_at.isoformat()}
        ))
        
        self.logger.info(f"Order {order_id} completed")

### 5. FastAPI Application

```python
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List, Optional
import asyncio
import logging

# Request/Response models
class CreateOrderRequest(BaseModel):
    customer_id: str
    items: List[Dict[str, Any]]

class OrderResponse(BaseModel):
    id: str
    customer_id: str
    items: List[Dict[str, Any]]
    status: str
    total: float
    created_at: str
    updated_at: str

# WebSocket manager
class ConnectionManager:
    """Manage WebSocket connections"""
    
    def __init__(self):
        self.active_connections: List[WebSocket] = []
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
    
    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
    
    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except:
                pass

# Create app
app = FastAPI(title="Order Service", version="1.0.0")
manager = ConnectionManager()

# Dependency injection
class ServiceContainer:
    """Service container for dependency injection"""
    
    def __init__(self):
        self.db_pool: Optional[asyncpg.Pool] = None
        self.event_bus: Optional[EventBus] = None
        self.repository: Optional[OrderRepository] = None
        self.workers: List = []
    
    async def initialize(self):
        """Initialize services"""
        # Database
        self.db_pool = await asyncpg.create_pool(
            "postgresql://user:password@localhost/orders",
            min_size=5,
            max_size=20
        )
        
        # Event bus
        self.event_bus = EventBus()
        await self.event_bus.connect()
        
        # Repository
        self.repository = OrderRepository(self.db_pool)
        
        # Workers
        payment_processor = PaymentProcessor(self.event_bus, self.repository)
        inventory_checker = InventoryChecker(self.event_bus, self.repository)
        order_completer = OrderCompleter(self.event_bus, self.repository)
        
        await payment_processor.start()
        await inventory_checker.start()
        await order_completer.start()
        
        self.workers = [payment_processor, inventory_checker, order_completer]
        
        # Start event bus
        await self.event_bus.start()
        
        # Subscribe to events for WebSocket broadcast
        for event_type in EventType:
            self.event_bus.subscribe(event_type, self._broadcast_event)
    
    async def _broadcast_event(self, event: DomainEvent):
        """Broadcast event to WebSocket clients"""
        await manager.broadcast(event.to_dict())
    
    async def cleanup(self):
        """Cleanup services"""
        if self.event_bus:
            await self.event_bus.stop()
            await self.event_bus.disconnect()
        
        if self.db_pool:
            await self.db_pool.close()

# Global container
container = ServiceContainer()

# Lifecycle events
@app.on_event("startup")
async def startup():
    """Startup event"""
    await container.initialize()
    logging.info("Service started")

@app.on_event("shutdown")
async def shutdown():
    """Shutdown event"""
    await container.cleanup()
    logging.info("Service stopped")

# API endpoints
@app.post("/orders", response_model=OrderResponse)
async def create_order(request: CreateOrderRequest):
    """Create new order"""
    # Create order
    items = [
        OrderItem(
            product_id=item['product_id'],
            quantity=item['quantity'],
            price=item['price']
        )
        for item in request.items
    ]
    
    order = Order(
        customer_id=request.customer_id,
        items=items
    )
    
    # Save order
    await container.repository.save(order)
    
    # Publish event
    await container.event_bus.publish(DomainEvent(
        type=EventType.ORDER_CREATED,
        aggregate_id=order.id,
        data=order.to_dict()
    ))
    
    return OrderResponse(**order.to_dict())

@app.get("/orders/{order_id}", response_model=OrderResponse)
async def get_order(order_id: str):
    """Get order by ID"""
    order = await container.repository.get(order_id)
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    return OrderResponse(**order.to_dict())

@app.get("/orders", response_model=List[OrderResponse])
async def list_orders(customer_id: Optional[str] = None, limit: int = 100):
    """List orders"""
    orders = await container.repository.list(customer_id, limit)
    return [OrderResponse(**order.to_dict()) for order in orders]

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time updates"""
    await manager.connect(websocket)
    try:
        while True:
            # Keep connection alive
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "database": "connected" if container.db_pool else "disconnected",
        "event_bus": "connected" if container.event_bus else "disconnected"
    }

@app.get("/metrics")
async def metrics():
    """Metrics endpoint"""
    # Get database stats
    db_stats = {
        'pool_size': container.db_pool.get_size() if container.db_pool else 0,
        'pool_free': container.db_pool.get_idle_size() if container.db_pool else 0
    }
    
    return {
        'database': db_stats,
        'workers': len(container.workers)
    }

# Run with: uvicorn main:app --reload
```

## Key Patterns Demonstrated

This microservice demonstrates:

1. **Event-Driven Architecture**: Components communicate via events
2. **Domain-Driven Design**: Clear domain models and aggregates
3. **CQRS**: Separate read and write models
4. **Background Workers**: Async task processing
5. **Repository Pattern**: Data access abstraction
6. **Dependency Injection**: Service container
7. **WebSocket**: Real-time updates
8. **Health Checks**: Service monitoring
9. **Graceful Shutdown**: Clean resource cleanup
10. **Structured Logging**: Contextual logging

## Deployment

### Docker Compose

```yaml
version: '3.8'

services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: orders
      POSTGRES_USER: user
      POSTGRES_PASSWORD: password
    ports:
      - "5432:5432"
  
  redis:
    image: redis:7
    ports:
      - "6379:6379"
  
  order-service:
    build: .
    ports:
      - "8000:8000"
    depends_on:
      - postgres
      - redis
    environment:
      DATABASE_URL: postgresql://user:password@postgres/orders
      REDIS_URL: redis://redis
```

### Dockerfile

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
```

## Testing

```python
import pytest
import asyncio
from httpx import AsyncClient

@pytest.mark.asyncio
async def test_create_order():
    """Test order creation"""
    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.post("/orders", json={
            "customer_id": "customer-1",
            "items": [
                {"product_id": "prod-1", "quantity": 2, "price": 10.0}
            ]
        })
        
        assert response.status_code == 200
        data = response.json()
        assert data['customer_id'] == "customer-1"
        assert data['status'] == "pending"

@pytest.mark.asyncio
async def test_order_workflow():
    """Test complete order workflow"""
    # Create order
    order = await create_order_helper()
    
    # Wait for processing
    await asyncio.sleep(5)
    
    # Check status
    final_order = await container.repository.get(order.id)
    assert final_order.status == OrderStatus.COMPLETED
```

## Extensions

Possible enhancements:

- **Authentication**: JWT-based auth
- **Rate Limiting**: Per-user rate limits
- **Caching**: Redis caching layer
- **Tracing**: Distributed tracing with OpenTelemetry
- **Metrics**: Prometheus metrics
- **API Gateway**: Kong or similar
- **Service Mesh**: Istio integration
- **Event Sourcing**: Full event store
- **Saga Pattern**: Distributed transactions
- **Multi-tenancy**: Tenant isolation

This project demonstrates how to build a production-ready async microservice using all the patterns and techniques covered in this guide.