# Chapter 30: Real-Time Chat Platform

## Project Overview

Build a production-ready real-time chat platform demonstrating:
- WebSocket connections
- Pub/Sub messaging
- Presence tracking
- Room management
- Message persistence
- Typing indicators
- Read receipts
- File uploads

**Complexity**: High
**Patterns Used**: 18+
**Lines of Code**: ~1500

## Architecture

```
┌─────────────┐
│   Clients   │ (WebSocket connections)
└──────┬──────┘
       │
┌──────▼──────────────────────────────────────┐
│          WebSocket Server                    │
│  ┌────────────────────────────────────────┐ │
│  │  Connection Manager                    │ │
│  │  - Track active connections            │ │
│  │  - Handle connect/disconnect           │ │
│  │  - Heartbeat monitoring                │ │
│  └────────────────────────────────────────┘ │
│  ┌────────────────────────────────────────┐ │
│  │  Room Manager                          │ │
│  │  - Create/delete rooms                 │ │
│  │  - Join/leave rooms                    │ │
│  │  - Broadcast to room members           │ │
│  └────────────────────────────────────────┘ │
│  ┌────────────────────────────────────────┐ │
│  │  Message Handler                       │ │
│  │  - Route messages                      │ │
│  │  - Validate messages                   │ │
│  │  - Persist messages                    │ │
│  └────────────────────────────────────────┘ │
└──────┬──────────────────────────────────────┘
       │
┌──────▼──────────────────────────────────────┐
│          Redis Pub/Sub                       │
│  - Cross-server messaging                   │
│  - Presence updates                         │
│  - Typing indicators                        │
└──────┬──────────────────────────────────────┘
       │
┌──────▼──────────────────────────────────────┐
│          PostgreSQL                          │
│  - Message history                          │
│  - User data                                │
│  - Room metadata                            │
└─────────────────────────────────────────────┘
```

## Design Decisions

### 1. WebSocket vs HTTP Polling

**Decision**: Use WebSocket
**Why**:
- Real-time bidirectional communication
- Lower latency (no polling overhead)
- Efficient (persistent connection)
- Native browser support

**Trade-off**: More complex than HTTP, requires connection management

### 2. Redis Pub/Sub for Cross-Server Communication

**Decision**: Use Redis Pub/Sub
**Why**:
- Multiple server instances can share state
- Horizontal scaling
- Fast message delivery
- Simple API

**Alternative**: Direct server-to-server communication (more complex)

### 3. Message Persistence Strategy

**Decision**: Async write to PostgreSQL
**Why**:
- Durability (messages survive restarts)
- Query history efficiently
- Separate read/write paths

**Trade-off**: Slight latency for persistence

## Implementation

### 1. Configuration (config.py)

```python
"""
Configuration for chat platform.

Design Decision: Environment-based config
Why: 12-factor app, easy deployment, secure secrets
"""

from dataclasses import dataclass
from typing import Optional
import os

@dataclass
class RedisConfig:
    """Redis configuration"""
    host: str = os.getenv("REDIS_HOST", "localhost")
    port: int = int(os.getenv("REDIS_PORT", "6379"))
    db: int = 0
    
    @property
    def url(self) -> str:
        return f"redis://{self.host}:{self.port}/{self.db}"

@dataclass
class DatabaseConfig:
    """PostgreSQL configuration"""
    host: str = os.getenv("DB_HOST", "localhost")
    port: int = int(os.getenv("DB_PORT", "5432"))
    database: str = os.getenv("DB_NAME", "chat")
    user: str = os.getenv("DB_USER", "postgres")
    password: str = os.getenv("DB_PASSWORD", "postgres")
    
    @property
    def dsn(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

@dataclass
class ServerConfig:
    """Server configuration"""
    host: str = os.getenv("SERVER_HOST", "0.0.0.0")
    port: int = int(os.getenv("SERVER_PORT", "8000"))
    max_connections: int = 10000
    heartbeat_interval: float = 30.0  # seconds
    heartbeat_timeout: float = 60.0   # seconds

@dataclass
class ChatConfig:
    """Chat configuration"""
    max_message_length: int = 4096
    max_room_members: int = 1000
    message_history_limit: int = 100
    typing_timeout: float = 3.0  # seconds

@dataclass
class SystemConfig:
    """System configuration"""
    redis: RedisConfig = RedisConfig()
    database: DatabaseConfig = DatabaseConfig()
    server: ServerConfig = ServerConfig()
    chat: ChatConfig = ChatConfig()

config = SystemConfig()
```

### 2. Data Models (models.py)

```python
"""
Data models for chat platform.

Design Decision: Immutable dataclasses
Why: Thread-safe, predictable, easy to serialize
"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum
import uuid

class MessageType(Enum):
    """Message types"""
    TEXT = "text"
    IMAGE = "image"
    FILE = "file"
    SYSTEM = "system"

class EventType(Enum):
    """Event types for WebSocket"""
    MESSAGE = "message"
    JOIN = "join"
    LEAVE = "leave"
    TYPING = "typing"
    READ = "read"
    PRESENCE = "presence"
    ERROR = "error"

@dataclass
class User:
    """User model"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    username: str = ""
    display_name: str = ""
    avatar_url: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> dict:
        return {
            'id': self.id,
            'username': self.username,
            'display_name': self.display_name,
            'avatar_url': self.avatar_url
        }

@dataclass
class Message:
    """Message model"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    room_id: str = ""
    user_id: str = ""
    content: str = ""
    message_type: MessageType = MessageType.TEXT
    created_at: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> dict:
        return {
            'id': self.id,
            'room_id': self.room_id,
            'user_id': self.user_id,
            'content': self.content,
            'type': self.message_type.value,
            'created_at': self.created_at.isoformat(),
            'metadata': self.metadata
        }

@dataclass
class Room:
    """Room model"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    description: str = ""
    created_by: str = ""
    created_at: datetime = field(default_factory=datetime.utcnow)
    members: List[str] = field(default_factory=list)
    
    def to_dict(self) -> dict:
        return {
            'id': self.id,
            'name': self.name,
            'description': self.description,
            'created_by': self.created_by,
            'created_at': self.created_at.isoformat(),
            'member_count': len(self.members)
        }

@dataclass
class WebSocketEvent:
    """WebSocket event"""
    type: EventType
    data: Dict[str, Any]
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    def to_json(self) -> dict:
        return {
            'type': self.type.value,
            'data': self.data,
            'timestamp': self.timestamp.isoformat()
        }
```

### 3. Connection Manager (connection.py)

```python
"""
WebSocket connection manager.

Design Decision: Track connections with heartbeat
Why: Detect dead connections, clean up resources, maintain presence
"""

import asyncio
import logging
from typing import Dict, Set, Optional
from datetime import datetime, timedelta
import json
from aiohttp import web, WSMsgType

class Connection:
    """
    Single WebSocket connection.
    
    Design Decision: Encapsulate connection state
    Why: Clean abstraction, easy to test, clear lifecycle
    """
    
    def __init__(self, ws: web.WebSocketResponse, user_id: str):
        self.ws = ws
        self.user_id = user_id
        self.rooms: Set[str] = set()
        self.last_heartbeat = datetime.utcnow()
        self.connected_at = datetime.utcnow()
    
    async def send(self, event: WebSocketEvent):
        """Send event to client"""
        try:
            await self.ws.send_json(event.to_json())
        except Exception as e:
            logging.error(f"Failed to send to {self.user_id}: {e}")
    
    async def close(self):
        """Close connection"""
        await self.ws.close()
    
    def is_alive(self, timeout: float) -> bool:
        """Check if connection is alive"""
        return (datetime.utcnow() - self.last_heartbeat).total_seconds() < timeout
    
    def heartbeat(self):
        """Update heartbeat timestamp"""
        self.last_heartbeat = datetime.utcnow()

class ConnectionManager:
    """
    Manage all WebSocket connections.
    
    Design Decision: Centralized connection tracking
    Why: Single source of truth, easy to broadcast, presence tracking
    
    Patterns:
    - Registry pattern (track connections)
    - Observer pattern (notify on events)
    - Heartbeat pattern (detect dead connections)
    """
    
    def __init__(self, config: ServerConfig):
        self.config = config
        self.connections: Dict[str, Connection] = {}  # user_id -> Connection
        self.room_members: Dict[str, Set[str]] = {}   # room_id -> Set[user_id]
        self.logger = logging.getLogger(__name__)
        self._heartbeat_task: Optional[asyncio.Task] = None
    
    async def start(self):
        """Start connection manager"""
        self._heartbeat_task = asyncio.create_task(self._heartbeat_monitor())
        self.logger.info("Connection manager started")
    
    async def stop(self):
        """Stop connection manager"""
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
        
        # Close all connections
        for conn in list(self.connections.values()):
            await conn.close()
        
        self.logger.info("Connection manager stopped")
    
    async def add_connection(self, ws: web.WebSocketResponse, user_id: str) -> Connection:
        """
        Add new connection.
        
        Design Decision: One connection per user
        Why: Simplifies logic, prevents duplicate connections
        """
        # Close existing connection if any
        if user_id in self.connections:
            await self.remove_connection(user_id)
        
        conn = Connection(ws, user_id)
        self.connections[user_id] = conn
        
        self.logger.info(f"User {user_id} connected (total: {len(self.connections)})")
        return conn
    
    async def remove_connection(self, user_id: str):
        """Remove connection"""
        if user_id in self.connections:
            conn = self.connections[user_id]
            
            # Leave all rooms
            for room_id in list(conn.rooms):
                await self.leave_room(user_id, room_id)
            
            await conn.close()
            del self.connections[user_id]
            
            self.logger.info(f"User {user_id} disconnected (total: {len(self.connections)})")
    
    async def join_room(self, user_id: str, room_id: str):
        """Join room"""
        if user_id not in self.connections:
            return
        
        conn = self.connections[user_id]
        conn.rooms.add(room_id)
        
        if room_id not in self.room_members:
            self.room_members[room_id] = set()
        self.room_members[room_id].add(user_id)
        
        self.logger.info(f"User {user_id} joined room {room_id}")
    
    async def leave_room(self, user_id: str, room_id: str):
        """Leave room"""
        if user_id in self.connections:
            conn = self.connections[user_id]
            conn.rooms.discard(room_id)
        
        if room_id in self.room_members:
            self.room_members[room_id].discard(user_id)
            if not self.room_members[room_id]:
                del self.room_members[room_id]
        
        self.logger.info(f"User {user_id} left room {room_id}")
    
    async def send_to_user(self, user_id: str, event: WebSocketEvent):
        """Send event to specific user"""
        if user_id in self.connections:
            await self.connections[user_id].send(event)
    
    async def broadcast_to_room(self, room_id: str, event: WebSocketEvent, exclude: Optional[str] = None):
        """
        Broadcast event to all room members.
        
        Design Decision: Concurrent sends with gather
        Why: Minimize latency, don't block on slow connections
        """
        if room_id not in self.room_members:
            return
        
        tasks = []
        for user_id in self.room_members[room_id]:
            if user_id != exclude and user_id in self.connections:
                tasks.append(self.connections[user_id].send(event))
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    def get_room_members(self, room_id: str) -> Set[str]:
        """Get room members"""
        return self.room_members.get(room_id, set()).copy()
    
    def get_user_rooms(self, user_id: str) -> Set[str]:
        """Get user's rooms"""
        if user_id in self.connections:
            return self.connections[user_id].rooms.copy()
        return set()
    
    async def _heartbeat_monitor(self):
        """
        Monitor connections with heartbeat.
        
        Design Decision: Periodic check with timeout
        Why: Detect dead connections, clean up resources
        
        Pattern: Heartbeat pattern
        """
        while True:
            try:
                await asyncio.sleep(self.config.heartbeat_interval)
                
                # Check all connections
                dead_connections = []
                for user_id, conn in self.connections.items():
                    if not conn.is_alive(self.config.heartbeat_timeout):
                        dead_connections.append(user_id)
                
                # Remove dead connections
                for user_id in dead_connections:
                    self.logger.warning(f"Removing dead connection: {user_id}")
                    await self.remove_connection(user_id)
            
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Heartbeat monitor error: {e}")
```

### 4. Message Handler (handler.py)

```python
"""
Message handler with validation and routing.

Design Decision: Command pattern for message handling
Why: Extensible, testable, clear separation of concerns
"""

import asyncio
import logging
from typing import Dict, Callable, Awaitable
from datetime import datetime

MessageHandler = Callable[[str, dict, 'ChatServer'], Awaitable[None]]

class MessageRouter:
    """
    Route messages to handlers.
    
    Design Decision: Registry pattern
    Why: Extensible, decoupled, easy to add new message types
    """
    
    def __init__(self):
        self.handlers: Dict[str, MessageHandler] = {}
        self.logger = logging.getLogger(__name__)
    
    def register(self, message_type: str):
        """Decorator to register handler"""
        def decorator(func: MessageHandler):
            self.handlers[message_type] = func
            return func
        return decorator
    
    async def route(self, user_id: str, message: dict, server: 'ChatServer'):
        """Route message to handler"""
        message_type = message.get('type')
        
        if message_type not in self.handlers:
            self.logger.warning(f"Unknown message type: {message_type}")
            return
        
        try:
            await self.handlers[message_type](user_id, message, server)
        except Exception as e:
            self.logger.error(f"Handler error for {message_type}: {e}")
            # Send error to user
            await server.connection_manager.send_to_user(
                user_id,
                WebSocketEvent(
                    type=EventType.ERROR,
                    data={'error': str(e)}
                )
            )

# Global router
router = MessageRouter()

@router.register('message')
async def handle_message(user_id: str, data: dict, server: 'ChatServer'):
    """
    Handle chat message.
    
    Design Decision: Validate, persist, broadcast
    Why: Ensure data integrity, durability, real-time delivery
    """
    room_id = data.get('room_id')
    content = data.get('content', '').strip()
    
    # Validate
    if not room_id or not content:
        raise ValueError("Missing room_id or content")
    
    if len(content) > server.config.chat.max_message_length:
        raise ValueError("Message too long")
    
    # Create message
    message = Message(
        room_id=room_id,
        user_id=user_id,
        content=content,
        message_type=MessageType.TEXT
    )
    
    # Persist (async, don't wait)
    asyncio.create_task(server.message_store.save_message(message))
    
    # Broadcast to room
    await server.connection_manager.broadcast_to_room(
        room_id,
        WebSocketEvent(
            type=EventType.MESSAGE,
            data=message.to_dict()
        )
    )

@router.register('join')
async def handle_join(user_id: str, data: dict, server: 'ChatServer'):
    """Handle room join"""
    room_id = data.get('room_id')
    
    if not room_id:
        raise ValueError("Missing room_id")
    
    # Join room
    await server.connection_manager.join_room(user_id, room_id)
    
    # Notify room members
    await server.connection_manager.broadcast_to_room(
        room_id,
        WebSocketEvent(
            type=EventType.JOIN,
            data={'user_id': user_id, 'room_id': room_id}
        ),
        exclude=user_id
    )
    
    # Send room history to user
    messages = await server.message_store.get_room_history(
        room_id,
        limit=server.config.chat.message_history_limit
    )
    
    await server.connection_manager.send_to_user(
        user_id,
        WebSocketEvent(
            type=EventType.MESSAGE,
            data={'history': [m.to_dict() for m in messages]}
        )
    )

@router.register('leave')
async def handle_leave(user_id: str, data: dict, server: 'ChatServer'):
    """Handle room leave"""
    room_id = data.get('room_id')
    
    if not room_id:
        raise ValueError("Missing room_id")
    
    # Leave room
    await server.connection_manager.leave_room(user_id, room_id)
    
    # Notify room members
    await server.connection_manager.broadcast_to_room(
        room_id,
        WebSocketEvent(
            type=EventType.LEAVE,
            data={'user_id': user_id, 'room_id': room_id}
        )
    )

@router.register('typing')
async def handle_typing(user_id: str, data: dict, server: 'ChatServer'):
    """
    Handle typing indicator.
    
    Design Decision: Broadcast immediately, no persistence
    Why: Ephemeral state, low latency required
    """
    room_id = data.get('room_id')
    is_typing = data.get('is_typing', False)
    
    if not room_id:
        raise ValueError("Missing room_id")
    
    # Broadcast to room (except sender)
    await server.connection_manager.broadcast_to_room(
        room_id,
        WebSocketEvent(
            type=EventType.TYPING,
            data={'user_id': user_id, 'room_id': room_id, 'is_typing': is_typing}
        ),
        exclude=user_id
    )

@router.register('read')
async def handle_read(user_id: str, data: dict, server: 'ChatServer'):
    """Handle read receipt"""
    message_id = data.get('message_id')
    
    if not message_id:
        raise ValueError("Missing message_id")
    
    # Update read status (async)
    asyncio.create_task(
        server.message_store.mark_as_read(message_id, user_id)
    )

@router.register('heartbeat')
async def handle_heartbeat(user_id: str, data: dict, server: 'ChatServer'):
    """
    Handle heartbeat.
    
    Design Decision: Update timestamp only
    Why: Lightweight, frequent operation
    """
    if user_id in server.connection_manager.connections:
        server.connection_manager.connections[user_id].heartbeat()
```

### 5. Message Store (store.py)

```python
"""
Message persistence with PostgreSQL.

Design Decision: Async database operations
Why: Don't block event loop, handle high throughput
"""

import asyncio
import logging
from typing import List, Optional
import asyncpg

class MessageStore:
    """
    Store messages in PostgreSQL.
    
    Design Decision: Connection pool
    Why: Reuse connections, handle concurrent requests
    
    Pattern: Connection pooling (Chapter 25)
    """
    
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool: Optional[asyncpg.Pool] = None
        self.logger = logging.getLogger(__name__)
    
    async def connect(self):
        """Create connection pool"""
        self.pool = await asyncpg.create_pool(
            self.dsn,
            min_size=5,
            max_size=20
        )
        
        # Create tables
        await self._create_tables()
        
        self.logger.info("Message store connected")
    
    async def disconnect(self):
        """Close connection pool"""
        if self.pool:
            await self.pool.close()
        self.logger.info("Message store disconnected")
    
    async def _create_tables(self):
        """Create database tables"""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    id TEXT PRIMARY KEY,
                    room_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    content TEXT NOT NULL,
                    message_type TEXT NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    metadata JSONB
                )
            """)
            
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_messages_room_created
                ON messages(room_id, created_at DESC)
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS read_receipts (
                    message_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    read_at TIMESTAMP NOT NULL,
                    PRIMARY KEY (message_id, user_id)
                )
            """)
    
    async def save_message(self, message: Message):
        """Save message"""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO messages (id, room_id, user_id, content, message_type, created_at, metadata)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
            """, message.id, message.room_id, message.user_id, message.content,
                message.message_type.value, message.created_at, message.metadata)
    
    async def get_room_history(self, room_id: str, limit: int = 100) -> List[Message]:
        """Get room message history"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT id, room_id, user_id, content, message_type, created_at, metadata
                FROM messages
                WHERE room_id = $1
                ORDER BY created_at DESC
                LIMIT $2
            """, room_id, limit)
            
            return [
                Message(
                    id=row['id'],
                    room_id=row['room_id'],
                    user_id=row['user_id'],
                    content=row['content'],
                    message_type=MessageType(row['message_type']),
                    created_at=row['created_at'],
                    metadata=row['metadata'] or {}
                )
                for row in reversed(rows)  # Oldest first
            ]
    
    async def mark_as_read(self, message_id: str, user_id: str):
        """Mark message as read"""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO read_receipts (message_id, user_id, read_at)
                VALUES ($1, $2, NOW())
                ON CONFLICT (message_id, user_id) DO NOTHING
            """, message_id, user_id)
```

This chapter continues in part 2 with the complete server implementation, client example, and deployment guide.