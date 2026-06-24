# Chapter 25: Database Integration

## Overview

Database operations are inherently I/O-bound, making them perfect candidates for async programming. However, integrating databases with asyncio requires careful attention to:

- **Connection pooling**: Reusing database connections efficiently
- **Transaction management**: Ensuring ACID properties in async context
- **Query batching**: Optimizing multiple queries
- **Error handling**: Dealing with connection failures and timeouts
- **Concurrency control**: Managing concurrent database access
- **Migration from sync**: Adapting existing database code

This chapter covers practical patterns for working with async databases, focusing on PostgreSQL (asyncpg), SQLite (aiosqlite), and general patterns applicable to any database.

## Mental Model

Think of async database access like a **restaurant with multiple servers**:

```
┌─────────────────────────────────────────────────────────┐
│                   Database Server                        │
│  ┌────────────────────────────────────────────────┐    │
│  │  Connection Pool (Tables)                       │    │
│  │  ┌──────┐  ┌──────┐  ┌──────┐  ┌──────┐       │    │
│  │  │Conn 1│  │Conn 2│  │Conn 3│  │Conn 4│       │    │
│  │  └──────┘  └──────┘  └──────┘  └──────┘       │    │
│  └────────────────────────────────────────────────┘    │
│                           ↕                              │
│  ┌────────────────────────────────────────────────┐    │
│  │  Async Tasks (Customers)                        │    │
│  │  [Task1] [Task2] [Task3] [Task4] [Task5] ...   │    │
│  └────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────┘
```

**Key concepts:**

1. **Connection pool**: Pre-created connections ready to use
2. **Acquire/release**: Borrow connection, use it, return it
3. **Transactions**: Group of operations that succeed or fail together
4. **Concurrent queries**: Multiple tasks querying simultaneously
5. **Connection lifecycle**: Creation, use, cleanup

## Connection Pooling

### 1. Basic Connection Pool (asyncpg)

```python
import asyncio
import asyncpg
from typing import Optional, Any, List
from contextlib import asynccontextmanager

class DatabasePool:
    """
    Async database connection pool.
    
    Manages a pool of connections to PostgreSQL database.
    """
    
    def __init__(
        self,
        dsn: str,
        min_size: int = 10,
        max_size: int = 20,
        command_timeout: float = 60.0
    ):
        self.dsn = dsn
        self.min_size = min_size
        self.max_size = max_size
        self.command_timeout = command_timeout
        self.pool: Optional[asyncpg.Pool] = None
    
    async def initialize(self):
        """Initialize connection pool"""
        self.pool = await asyncpg.create_pool(
            self.dsn,
            min_size=self.min_size,
            max_size=self.max_size,
            command_timeout=self.command_timeout
        )
        print(f"Database pool initialized: {self.min_size}-{self.max_size} connections")
    
    async def close(self):
        """Close all connections"""
        if self.pool:
            await self.pool.close()
            print("Database pool closed")
    
    @asynccontextmanager
    async def acquire(self):
        """
        Acquire connection from pool.
        
        Usage:
            async with pool.acquire() as conn:
                result = await conn.fetch("SELECT * FROM users")
        """
        if not self.pool:
            raise RuntimeError("Pool not initialized")
        
        async with self.pool.acquire() as connection:
            yield connection
    
    async def execute(self, query: str, *args) -> str:
        """Execute query without returning results"""
        async with self.acquire() as conn:
            return await conn.execute(query, *args)
    
    async def fetch(self, query: str, *args) -> List[asyncpg.Record]:
        """Fetch multiple rows"""
        async with self.acquire() as conn:
            return await conn.fetch(query, *args)
    
    async def fetchrow(self, query: str, *args) -> Optional[asyncpg.Record]:
        """Fetch single row"""
        async with self.acquire() as conn:
            return await conn.fetchrow(query, *args)
    
    async def fetchval(self, query: str, *args) -> Any:
        """Fetch single value"""
        async with self.acquire() as conn:
            return await conn.fetchval(query, *args)

# Example usage
async def basic_pool_example():
    """Demonstrate basic pool usage"""
    pool = DatabasePool(
        dsn="postgresql://user:password@localhost/dbname",
        min_size=5,
        max_size=10
    )
    
    await pool.initialize()
    
    try:
        # Simple query
        users = await pool.fetch("SELECT * FROM users WHERE active = $1", True)
        print(f"Found {len(users)} active users")
        
        # Single value
        count = await pool.fetchval("SELECT COUNT(*) FROM users")
        print(f"Total users: {count}")
        
        # Insert
        await pool.execute(
            "INSERT INTO users (name, email) VALUES ($1, $2)",
            "Alice",
            "alice@example.com"
        )
    
    finally:
        await pool.close()
```

### 2. Connection Pool with Health Checks

```python
import asyncio
import asyncpg
from typing import Optional
import time

class HealthCheckedPool(DatabasePool):
    """
    Connection pool with health checking.
    
    Periodically checks connection health and recreates pool if needed.
    """
    
    def __init__(self, *args, health_check_interval: float = 30.0, **kwargs):
        super().__init__(*args, **kwargs)
        self.health_check_interval = health_check_interval
        self.health_check_task: Optional[asyncio.Task] = None
        self.last_health_check = 0.0
        self.healthy = True
    
    async def initialize(self):
        """Initialize pool and start health checks"""
        await super().initialize()
        self.health_check_task = asyncio.create_task(self._health_check_loop())
    
    async def close(self):
        """Stop health checks and close pool"""
        if self.health_check_task:
            self.health_check_task.cancel()
            await asyncio.gather(self.health_check_task, return_exceptions=True)
        await super().close()
    
    async def _health_check_loop(self):
        """Periodically check pool health"""
        while True:
            try:
                await asyncio.sleep(self.health_check_interval)
                await self._check_health()
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Health check error: {e}")
    
    async def _check_health(self):
        """Check if pool is healthy"""
        try:
            # Try simple query
            async with self.acquire() as conn:
                await conn.fetchval("SELECT 1")
            
            self.healthy = True
            self.last_health_check = time.time()
            print("Health check: OK")
        
        except Exception as e:
            print(f"Health check failed: {e}")
            self.healthy = False
            
            # Try to recreate pool
            try:
                await self.pool.close()
                await super().initialize()
                print("Pool recreated")
            except Exception as recreate_error:
                print(f"Failed to recreate pool: {recreate_error}")
```

## Transaction Management

### 1. Basic Transactions

```python
import asyncio
import asyncpg
from contextlib import asynccontextmanager
from typing import Optional

class TransactionManager:
    """
    Manage database transactions.
    
    Ensures ACID properties for groups of operations.
    """
    
    def __init__(self, pool: DatabasePool):
        self.pool = pool
    
    @asynccontextmanager
    async def transaction(self, isolation: str = "read_committed"):
        """
        Execute operations in a transaction.
        
        Usage:
            async with manager.transaction():
                await conn.execute("INSERT ...")
                await conn.execute("UPDATE ...")
                # Commits on success, rolls back on exception
        """
        async with self.pool.acquire() as conn:
            async with conn.transaction(isolation=isolation):
                yield conn

# Example: Bank transfer with transaction
async def transfer_money(
    manager: TransactionManager,
    from_account: int,
    to_account: int,
    amount: float
):
    """
    Transfer money between accounts atomically.
    
    Either both operations succeed or both fail.
    """
    async with manager.transaction() as conn:
        # Check balance
        balance = await conn.fetchval(
            "SELECT balance FROM accounts WHERE id = $1 FOR UPDATE",
            from_account
        )
        
        if balance < amount:
            raise ValueError("Insufficient funds")
        
        # Debit from source
        await conn.execute(
            "UPDATE accounts SET balance = balance - $1 WHERE id = $2",
            amount,
            from_account
        )
        
        # Credit to destination
        await conn.execute(
            "UPDATE accounts SET balance = balance + $1 WHERE id = $2",
            amount,
            to_account
        )
        
        # Log transaction
        await conn.execute(
            "INSERT INTO transactions (from_account, to_account, amount) VALUES ($1, $2, $3)",
            from_account,
            to_account,
            amount
        )
    
    print(f"Transferred ${amount} from {from_account} to {to_account}")

async def transaction_example():
    """Demonstrate transaction usage"""
    pool = DatabasePool("postgresql://user:password@localhost/bank")
    await pool.initialize()
    
    manager = TransactionManager(pool)
    
    try:
        # Successful transfer
        await transfer_money(manager, from_account=1, to_account=2, amount=100.0)
        
        # Failed transfer (insufficient funds)
        try:
            await transfer_money(manager, from_account=1, to_account=2, amount=10000.0)
        except ValueError as e:
            print(f"Transfer failed: {e}")
    
    finally:
        await pool.close()
```

### 2. Nested Transactions (Savepoints)

```python
import asyncio
import asyncpg

class SavepointManager:
    """
    Manage nested transactions using savepoints.
    
    Allows partial rollback within a transaction.
    """
    
    def __init__(self, connection: asyncpg.Connection):
        self.connection = connection
        self.savepoint_counter = 0
    
    @asynccontextmanager
    async def savepoint(self):
        """
        Create a savepoint.
        
        Can rollback to this point without affecting outer transaction.
        """
        self.savepoint_counter += 1
        savepoint_name = f"sp_{self.savepoint_counter}"
        
        await self.connection.execute(f"SAVEPOINT {savepoint_name}")
        
        try:
            yield
            # Savepoint succeeded, release it
            await self.connection.execute(f"RELEASE SAVEPOINT {savepoint_name}")
        except Exception:
            # Rollback to savepoint
            await self.connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
            raise

# Example: Batch insert with partial rollback
async def batch_insert_with_savepoints(pool: DatabasePool, users: list):
    """
    Insert users in batches, rolling back failed batches.
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            sp_manager = SavepointManager(conn)
            
            successful = 0
            failed = 0
            
            for user in users:
                try:
                    async with sp_manager.savepoint():
                        await conn.execute(
                            "INSERT INTO users (name, email) VALUES ($1, $2)",
                            user['name'],
                            user['email']
                        )
                        successful += 1
                except Exception as e:
                    print(f"Failed to insert {user['name']}: {e}")
                    failed += 1
            
            print(f"Inserted {successful} users, {failed} failed")
```

## Query Optimization

### 1. Prepared Statements

```python
import asyncio
import asyncpg
from typing import List, Any

class PreparedStatementCache:
    """
    Cache prepared statements for better performance.
    
    Prepared statements are parsed once and reused.
    """
    
    def __init__(self, connection: asyncpg.Connection):
        self.connection = connection
        self.statements = {}
    
    async def prepare(self, name: str, query: str):
        """Prepare a statement"""
        if name not in self.statements:
            self.statements[name] = await self.connection.prepare(query)
        return self.statements[name]
    
    async def execute(self, name: str, *args):
        """Execute prepared statement"""
        stmt = self.statements.get(name)
        if not stmt:
            raise ValueError(f"Statement {name} not prepared")
        return await stmt.fetch(*args)

# Example: Using prepared statements
async def prepared_statement_example(pool: DatabasePool):
    """Demonstrate prepared statement usage"""
    async with pool.acquire() as conn:
        cache = PreparedStatementCache(conn)
        
        # Prepare statement
        await cache.prepare(
            "get_user_by_email",
            "SELECT * FROM users WHERE email = $1"
        )
        
        # Execute multiple times (efficient)
        for email in ["alice@example.com", "bob@example.com", "carol@example.com"]:
            user = await cache.execute("get_user_by_email", email)
            print(f"Found user: {user}")
```

### 2. Batch Operations

```python
import asyncio
import asyncpg
from typing import List, Tuple, Any

async def batch_insert(
    pool: DatabasePool,
    table: str,
    columns: List[str],
    values: List[Tuple[Any, ...]]
):
    """
    Efficiently insert multiple rows.
    
    Uses COPY for maximum performance.
    """
    async with pool.acquire() as conn:
        # Use COPY for bulk insert (fastest method)
        await conn.copy_records_to_table(
            table,
            records=values,
            columns=columns
        )
        print(f"Inserted {len(values)} rows into {table}")

async def batch_update(
    pool: DatabasePool,
    updates: List[Tuple[Any, ...]]
):
    """
    Efficiently update multiple rows.
    
    Uses executemany for batching.
    """
    async with pool.acquire() as conn:
        await conn.executemany(
            "UPDATE users SET name = $1 WHERE id = $2",
            updates
        )
        print(f"Updated {len(updates)} rows")

# Example: Batch operations
async def batch_operations_example(pool: DatabasePool):
    """Demonstrate batch operations"""
    # Batch insert
    users = [
        ("Alice", "alice@example.com"),
        ("Bob", "bob@example.com"),
        ("Carol", "carol@example.com")
    ]
    await batch_insert(pool, "users", ["name", "email"], users)
    
    # Batch update
    updates = [
        ("Alice Smith", 1),
        ("Bob Jones", 2),
        ("Carol Williams", 3)
    ]
    await batch_update(pool, updates)
```

## Concurrent Query Patterns

### 1. Parallel Queries

```python
import asyncio
from typing import List, Any

async def parallel_queries(pool: DatabasePool, user_ids: List[int]):
    """
    Execute multiple queries in parallel.
    
    Each query gets its own connection from the pool.
    """
    async def fetch_user(user_id: int):
        return await pool.fetchrow(
            "SELECT * FROM users WHERE id = $1",
            user_id
        )
    
    # Execute all queries concurrently
    users = await asyncio.gather(*[fetch_user(uid) for uid in user_ids])
    return users

async def parallel_aggregations(pool: DatabasePool):
    """
    Execute multiple aggregation queries in parallel.
    """
    # Run multiple expensive queries concurrently
    results = await asyncio.gather(
        pool.fetchval("SELECT COUNT(*) FROM users"),
        pool.fetchval("SELECT AVG(age) FROM users"),
        pool.fetchval("SELECT MAX(created_at) FROM users"),
        pool.fetchval("SELECT COUNT(DISTINCT country) FROM users")
    )
    
    total_users, avg_age, latest_signup, countries = results
    
    return {
        'total_users': total_users,
        'avg_age': avg_age,
        'latest_signup': latest_signup,
        'countries': countries
    }
```

### 2. Query Streaming

```python
import asyncio
import asyncpg
from typing import AsyncIterator

async def stream_large_result(
    pool: DatabasePool,
    query: str,
    *args,
    chunk_size: int = 1000
) -> AsyncIterator[asyncpg.Record]:
    """
    Stream large result set in chunks.
    
    Avoids loading entire result into memory.
    """
    async with pool.acquire() as conn:
        # Use cursor for streaming
        async with conn.transaction():
            cursor = await conn.cursor(query, *args)
            
            while True:
                chunk = await cursor.fetch(chunk_size)
                if not chunk:
                    break
                
                for record in chunk:
                    yield record

# Example: Process large dataset
async def process_large_dataset(pool: DatabasePool):
    """Process millions of rows efficiently"""
    count = 0
    
    async for user in stream_large_result(
        pool,
        "SELECT * FROM users ORDER BY id",
        chunk_size=1000
    ):
        # Process each user
        count += 1
        if count % 10000 == 0:
            print(f"Processed {count} users...")
    
    print(f"Total processed: {count}")
```

## Error Handling and Retry

### 1. Connection Retry Logic

```python
import asyncio
import asyncpg
from typing import Callable, Any, TypeVar
from functools import wraps

T = TypeVar('T')

class DatabaseRetry:
    """
    Retry database operations on transient failures.
    """
    
    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 0.1,
        max_delay: float = 5.0
    ):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
    
    def retry(self, func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        """Decorator to retry database operations"""
        @wraps(func)
        async def wrapper(*args, **kwargs) -> T:
            last_exception = None
            
            for attempt in range(self.max_retries):
                try:
                    return await func(*args, **kwargs)
                
                except (
                    asyncpg.ConnectionDoesNotExistError,
                    asyncpg.ConnectionFailureError,
                    asyncpg.InterfaceError
                ) as e:
                    last_exception = e
                    
                    if attempt < self.max_retries - 1:
                        # Exponential backoff
                        delay = min(
                            self.base_delay * (2 ** attempt),
                            self.max_delay
                        )
                        print(f"Retry attempt {attempt + 1} after {delay}s: {e}")
                        await asyncio.sleep(delay)
                    else:
                        print(f"Max retries exceeded")
                
                except Exception as e:
                    # Non-retryable error
                    raise
            
            raise last_exception
        
        return wrapper

# Example usage
retry_handler = DatabaseRetry(max_retries=3)

@retry_handler.retry
async def fetch_with_retry(pool: DatabasePool, user_id: int):
    """Fetch user with automatic retry"""
    return await pool.fetchrow(
        "SELECT * FROM users WHERE id = $1",
        user_id
    )
```

### 2. Deadlock Handling

```python
import asyncio
import asyncpg
from typing import Callable, Any

async def handle_deadlock(
    func: Callable[..., Awaitable[Any]],
    *args,
    max_retries: int = 3,
    **kwargs
):
    """
    Handle database deadlocks with retry.
    
    Deadlocks are transient and usually succeed on retry.
    """
    for attempt in range(max_retries):
        try:
            return await func(*args, **kwargs)
        
        except asyncpg.DeadlockDetectedError as e:
            if attempt < max_retries - 1:
                # Random delay to avoid repeated deadlock
                delay = 0.1 * (attempt + 1) + (asyncio.get_event_loop().time() % 0.1)
                print(f"Deadlock detected, retrying in {delay:.2f}s...")
                await asyncio.sleep(delay)
            else:
                raise

# Example: Transfer with deadlock handling
async def safe_transfer(manager: TransactionManager, from_id: int, to_id: int, amount: float):
    """Transfer money with deadlock handling"""
    await handle_deadlock(
        transfer_money,
        manager,
        from_id,
        to_id,
        amount
    )
```

## SQLite with aiosqlite

```python
import asyncio
import aiosqlite
from typing import List, Any, Optional
from contextlib import asynccontextmanager

class SQLitePool:
    """
    Simple connection pool for SQLite.
    
    SQLite doesn't support true connection pooling,
    but we can manage multiple connections.
    """
    
    def __init__(self, database: str, max_connections: int = 5):
        self.database = database
        self.max_connections = max_connections
        self.connections: List[aiosqlite.Connection] = []
        self.semaphore = asyncio.Semaphore(max_connections)
    
    async def initialize(self):
        """Initialize connections"""
        for _ in range(self.max_connections):
            conn = await aiosqlite.connect(self.database)
            await conn.execute("PRAGMA journal_mode=WAL")  # Enable WAL mode
            self.connections.append(conn)
    
    async def close(self):
        """Close all connections"""
        for conn in self.connections:
            await conn.close()
        self.connections.clear()
    
    @asynccontextmanager
    async def acquire(self):
        """Acquire connection"""
        async with self.semaphore:
            if not self.connections:
                raise RuntimeError("No connections available")
            
            conn = self.connections.pop()
            try:
                yield conn
            finally:
                self.connections.append(conn)
    
    async def execute(self, query: str, *args):
        """Execute query"""
        async with self.acquire() as conn:
            await conn.execute(query, args)
            await conn.commit()
    
    async def fetch(self, query: str, *args) -> List[Any]:
        """Fetch multiple rows"""
        async with self.acquire() as conn:
            async with conn.execute(query, args) as cursor:
                return await cursor.fetchall()
    
    async def fetchone(self, query: str, *args) -> Optional[Any]:
        """Fetch single row"""
        async with self.acquire() as conn:
            async with conn.execute(query, args) as cursor:
                return await cursor.fetchone()

# Example: SQLite usage
async def sqlite_example():
    """Demonstrate SQLite usage"""
    pool = SQLitePool("example.db", max_connections=3)
    await pool.initialize()
    
    try:
        # Create table
        await pool.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL
            )
        """)
        
        # Insert data
        await pool.execute(
            "INSERT INTO users (name, email) VALUES (?, ?)",
            "Alice",
            "alice@example.com"
        )
        
        # Query data
        users = await pool.fetch("SELECT * FROM users")
        print(f"Users: {users}")
    
    finally:
        await pool.close()
```

## Real-World Example: User Service

```python
import asyncio
import asyncpg
from typing import Optional, List
from dataclasses import dataclass
from datetime import datetime

@dataclass
class User:
    """User model"""
    id: Optional[int]
    name: str
    email: str
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

class UserService:
    """
    Complete user service with database operations.
    
    Demonstrates real-world patterns for database integration.
    """
    
    def __init__(self, pool: DatabasePool):
        self.pool = pool
        self.retry = DatabaseRetry(max_retries=3)
    
    @retry_handler.retry
    async def create_user(self, name: str, email: str) -> User:
        """Create new user"""
        row = await self.pool.fetchrow(
            """
            INSERT INTO users (name, email, created_at, updated_at)
            VALUES ($1, $2, NOW(), NOW())
            RETURNING id, name, email, created_at, updated_at
            """,
            name,
            email
        )
        return User(**dict(row))
    
    @retry_handler.retry
    async def get_user(self, user_id: int) -> Optional[User]:
        """Get user by ID"""
        row = await self.pool.fetchrow(
            "SELECT * FROM users WHERE id = $1",
            user_id
        )
        return User(**dict(row)) if row else None
    
    @retry_handler.retry
    async def update_user(self, user_id: int, name: str = None, email: str = None) -> User:
        """Update user"""
        updates = []
        args = []
        arg_num = 1
        
        if name:
            updates.append(f"name = ${arg_num}")
            args.append(name)
            arg_num += 1
        
        if email:
            updates.append(f"email = ${arg_num}")
            args.append(email)
            arg_num += 1
        
        updates.append(f"updated_at = NOW()")
        args.append(user_id)
        
        query = f"""
            UPDATE users
            SET {', '.join(updates)}
            WHERE id = ${arg_num}
            RETURNING id, name, email, created_at, updated_at
        """
        
        row = await self.pool.fetchrow(query, *args)
        return User(**dict(row))
    
    @retry_handler.retry
    async def delete_user(self, user_id: int) -> bool:
        """Delete user"""
        result = await self.pool.execute(
            "DELETE FROM users WHERE id = $1",
            user_id
        )
        return "DELETE 1" in result
    
    @retry_handler.retry
    async def list_users(self, limit: int = 100, offset: int = 0) -> List[User]:
        """List users with pagination"""
        rows = await self.pool.fetch(
            "SELECT * FROM users ORDER BY id LIMIT $1 OFFSET $2",
            limit,
            offset
        )
        return [User(**dict(row)) for row in rows]

# Example: Using user service
async def user_service_example():
    """Demonstrate user service"""
    pool = DatabasePool("postgresql://user:password@localhost/myapp")
    await pool.initialize()
    
    service = UserService(pool)
    
    try:
        # Create user
        user = await service.create_user("Alice", "alice@example.com")
        print(f"Created user: {user}")
        
        # Get user
        fetched = await service.get_user(user.id)
        print(f"Fetched user: {fetched}")
        
        # Update user
        updated = await service.update_user(user.id, name="Alice Smith")
        print(f"Updated user: {updated}")
        
        # List users
        users = await service.list_users(limit=10)
        print(f"Total users: {len(users)}")
    
    finally:
        await pool.close()
```

## Mental Model Summary

**Database integration in async requires:**

1. **Connection pooling**: Reuse connections efficiently
2. **Transaction management**: Ensure ACID properties
3. **Error handling**: Retry transient failures
4. **Query optimization**: Use prepared statements and batching
5. **Concurrency control**: Manage concurrent access

**Key patterns:**

- **Pool lifecycle**: Initialize → Use → Close
- **Acquire/release**: Borrow connection, use it, return it
- **Transactions**: Group operations atomically
- **Retry logic**: Handle transient failures
- **Batch operations**: Optimize multiple queries

**Best practices:**

- Always use connection pooling
- Set appropriate pool sizes (min/max)
- Use transactions for related operations
- Implement retry logic for transient errors
- Use prepared statements for repeated queries
- Batch operations when possible
- Stream large result sets
- Monitor pool health
- Handle deadlocks gracefully
- Use appropriate isolation levels

Async database integration provides significant performance benefits for I/O-bound applications, but requires careful attention to connection management, transactions, and error handling.