# Chapter 18: Networking

## Introduction

Asyncio excels at network I/O. This chapter covers asyncio's networking primitives for building TCP servers, clients, and understanding the transport/protocol layers. We'll also explore practical patterns with aiohttp for HTTP communication.

By the end of this chapter, you'll understand:
- Streams API for TCP connections
- Server and client patterns
- Transport and protocol layers
- Connection handling and lifecycle
- HTTP patterns with aiohttp
- WebSocket basics
- Practical networking examples

---

## Streams API

The Streams API provides high-level async networking primitives.

### TCP Client

```python
import asyncio

async def tcp_client_demo():
    """Basic TCP client using streams"""
    
    # Connect to server
    reader, writer = await asyncio.open_connection(
        'httpbin.org', 80
    )
    
    # Send HTTP request
    request = (
        b'GET /get HTTP/1.1\r\n'
        b'Host: httpbin.org\r\n'
        b'Connection: close\r\n'
        b'\r\n'
    )
    
    writer.write(request)
    await writer.drain()
    
    # Read response
    response = await reader.read()
    
    print(f"Response:\n{response.decode()[:500]}...")
    
    # Close connection
    writer.close()
    await writer.wait_closed()

asyncio.run(tcp_client_demo())
```

### TCP Server

```python
import asyncio

async def tcp_server_demo():
    """Basic TCP server using streams"""
    
    async def handle_client(reader, writer):
        """Handle client connection"""
        addr = writer.get_extra_info('peername')
        print(f"Client connected: {addr}")
        
        try:
            # Read data
            data = await reader.read(1024)
            message = data.decode()
            
            print(f"Received: {message}")
            
            # Send response
            response = f"Echo: {message}"
            writer.write(response.encode())
            await writer.drain()
            
        finally:
            print(f"Closing connection: {addr}")
            writer.close()
            await writer.wait_closed()
    
    # Start server
    server = await asyncio.start_server(
        handle_client,
        '127.0.0.1',
        8888
    )
    
    addr = server.sockets[0].getsockname()
    print(f"Server listening on {addr}")
    
    async with server:
        await server.serve_forever()

# asyncio.run(tcp_server_demo())
```

---

## Echo Server and Client

Complete echo server with multiple clients.

### Echo Server

```python
import asyncio
from typing import Set

class EchoServer:
    """
    Echo server that handles multiple clients.
    
    Features:
    - Multiple concurrent clients
    - Graceful shutdown
    - Connection tracking
    """
    
    def __init__(self, host: str = '127.0.0.1', port: int = 8888):
        self.host = host
        self.port = port
        self.clients: Set[asyncio.StreamWriter] = set()
        self.server = None
    
    async def handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter
    ):
        """Handle individual client connection"""
        addr = writer.get_extra_info('peername')
        print(f"[Server] Client connected: {addr}")
        
        self.clients.add(writer)
        
        try:
            while True:
                # Read data
                data = await reader.read(1024)
                
                if not data:
                    break
                
                message = data.decode().strip()
                print(f"[Server] Received from {addr}: {message}")
                
                # Echo back
                response = f"Echo: {message}\n"
                writer.write(response.encode())
                await writer.drain()
        
        except asyncio.CancelledError:
            print(f"[Server] Connection cancelled: {addr}")
            raise
        
        except Exception as e:
            print(f"[Server] Error with {addr}: {e}")
        
        finally:
            print(f"[Server] Closing connection: {addr}")
            self.clients.discard(writer)
            writer.close()
            await writer.wait_closed()
    
    async def start(self):
        """Start server"""
        self.server = await asyncio.start_server(
            self.handle_client,
            self.host,
            self.port
        )
        
        addr = self.server.sockets[0].getsockname()
        print(f"[Server] Listening on {addr}")
        
        async with self.server:
            await self.server.serve_forever()
    
    async def shutdown(self):
        """Shutdown server gracefully"""
        print("[Server] Shutting down...")
        
        # Close all client connections
        for writer in list(self.clients):
            writer.close()
            await writer.wait_closed()
        
        # Close server
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        
        print("[Server] Shutdown complete")

async def echo_server_demo():
    """Run echo server"""
    server = EchoServer()
    await server.start()

# asyncio.run(echo_server_demo())
```

### Echo Client

```python
import asyncio

class EchoClient:
    """Echo client for testing server"""
    
    def __init__(self, host: str = '127.0.0.1', port: int = 8888):
        self.host = host
        self.port = port
        self.reader = None
        self.writer = None
    
    async def connect(self):
        """Connect to server"""
        self.reader, self.writer = await asyncio.open_connection(
            self.host, self.port
        )
        print(f"[Client] Connected to {self.host}:{self.port}")
    
    async def send(self, message: str) -> str:
        """Send message and receive response"""
        # Send
        self.writer.write(f"{message}\n".encode())
        await self.writer.drain()
        
        # Receive
        response = await self.reader.readline()
        return response.decode().strip()
    
    async def close(self):
        """Close connection"""
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()
        print("[Client] Connection closed")

async def echo_client_demo():
    """Test echo client"""
    client = EchoClient()
    
    try:
        await client.connect()
        
        # Send messages
        for i in range(5):
            response = await client.send(f"Message {i}")
            print(f"[Client] Received: {response}")
            await asyncio.sleep(1.0)
    
    finally:
        await client.close()

# asyncio.run(echo_client_demo())
```

---

## Protocol and Transport

Lower-level networking with protocols and transports.

### Basic Protocol

```python
import asyncio

class EchoProtocol(asyncio.Protocol):
    """Echo protocol using transport/protocol API"""
    
    def connection_made(self, transport):
        """Called when connection is established"""
        peername = transport.get_extra_info('peername')
        print(f'Connection from {peername}')
        self.transport = transport
    
    def data_received(self, data):
        """Called when data is received"""
        message = data.decode()
        print(f'Received: {message}')
        
        # Echo back
        self.transport.write(data)
    
    def connection_lost(self, exc):
        """Called when connection is closed"""
        print('Connection closed')

async def protocol_server_demo():
    """Server using protocol/transport"""
    loop = asyncio.get_running_loop()
    
    server = await loop.create_server(
        EchoProtocol,
        '127.0.0.1',
        8889
    )
    
    async with server:
        await server.serve_forever()

# asyncio.run(protocol_server_demo())
```

---

## HTTP with aiohttp

Practical HTTP client and server patterns.

### HTTP Client

```python
import asyncio
import aiohttp

async def http_client_demo():
    """HTTP client with aiohttp"""
    
    async with aiohttp.ClientSession() as session:
        # GET request
        async with session.get('https://httpbin.org/get') as response:
            print(f"Status: {response.status}")
            data = await response.json()
            print(f"Data: {data}")
        
        # POST request
        async with session.post(
            'https://httpbin.org/post',
            json={'key': 'value'}
        ) as response:
            data = await response.json()
            print(f"Posted: {data['json']}")
        
        # Multiple concurrent requests
        urls = [
            'https://httpbin.org/delay/1',
            'https://httpbin.org/delay/1',
            'https://httpbin.org/delay/1'
        ]
        
        async def fetch(url):
            async with session.get(url) as response:
                return await response.json()
        
        results = await asyncio.gather(*[fetch(url) for url in urls])
        print(f"Fetched {len(results)} URLs concurrently")

asyncio.run(http_client_demo())
```

### HTTP Server

```python
import asyncio
from aiohttp import web

async def http_server_demo():
    """HTTP server with aiohttp"""
    
    async def handle_get(request):
        """Handle GET request"""
        name = request.match_info.get('name', 'World')
        return web.json_response({
            'message': f'Hello, {name}!',
            'method': 'GET'
        })
    
    async def handle_post(request):
        """Handle POST request"""
        data = await request.json()
        return web.json_response({
            'received': data,
            'method': 'POST'
        })
    
    # Create application
    app = web.Application()
    
    # Add routes
    app.router.add_get('/', handle_get)
    app.router.add_get('/{name}', handle_get)
    app.router.add_post('/data', handle_post)
    
    # Run server
    runner = web.AppRunner(app)
    await runner.setup()
    
    site = web.TCPSite(runner, '127.0.0.1', 8080)
    await site.start()
    
    print("Server started at http://127.0.0.1:8080")
    
    # Keep running
    await asyncio.Event().wait()

# asyncio.run(http_server_demo())
```

---

## Real-world: Chat Server

Complete chat server with multiple clients.

```python
import asyncio
from typing import Set, Dict
from dataclasses import dataclass
from datetime import datetime

@dataclass
class Client:
    """Chat client"""
    name: str
    writer: asyncio.StreamWriter
    reader: asyncio.StreamReader

class ChatServer:
    """
    Multi-client chat server.
    
    Features:
    - Multiple concurrent clients
    - Broadcast messages
    - Private messages
    - Client list
    """
    
    def __init__(self, host: str = '127.0.0.1', port: int = 9999):
        self.host = host
        self.port = port
        self.clients: Dict[str, Client] = {}
        self.server = None
    
    async def broadcast(self, message: str, exclude: str = None):
        """Broadcast message to all clients"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        formatted = f"[{timestamp}] {message}\n"
        
        for name, client in self.clients.items():
            if name != exclude:
                try:
                    client.writer.write(formatted.encode())
                    await client.writer.drain()
                except Exception as e:
                    print(f"Error broadcasting to {name}: {e}")
    
    async def send_to_client(self, name: str, message: str):
        """Send message to specific client"""
        if name in self.clients:
            client = self.clients[name]
            try:
                client.writer.write(f"{message}\n".encode())
                await client.writer.drain()
            except Exception as e:
                print(f"Error sending to {name}: {e}")
    
    async def handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter
    ):
        """Handle client connection"""
        addr = writer.get_extra_info('peername')
        print(f"[Server] New connection from {addr}")
        
        # Get client name
        writer.write(b"Enter your name: ")
        await writer.drain()
        
        name_data = await reader.readline()
        name = name_data.decode().strip()
        
        if not name or name in self.clients:
            writer.write(b"Invalid or taken name\n")
            await writer.drain()
            writer.close()
            await writer.wait_closed()
            return
        
        # Register client
        client = Client(name=name, writer=writer, reader=reader)
        self.clients[name] = client
        
        # Welcome message
        await self.send_to_client(
            name,
            f"Welcome {name}! Type /help for commands."
        )
        
        # Announce join
        await self.broadcast(f"{name} joined the chat", exclude=name)
        
        try:
            while True:
                # Read message
                data = await reader.readline()
                
                if not data:
                    break
                
                message = data.decode().strip()
                
                if not message:
                    continue
                
                # Handle commands
                if message.startswith('/'):
                    await self.handle_command(name, message)
                else:
                    # Broadcast message
                    await self.broadcast(f"{name}: {message}", exclude=name)
        
        except asyncio.CancelledError:
            print(f"[Server] Connection cancelled: {name}")
        
        except Exception as e:
            print(f"[Server] Error with {name}: {e}")
        
        finally:
            # Remove client
            if name in self.clients:
                del self.clients[name]
            
            # Announce leave
            await self.broadcast(f"{name} left the chat")
            
            writer.close()
            await writer.wait_closed()
            print(f"[Server] {name} disconnected")
    
    async def handle_command(self, sender: str, command: str):
        """Handle client commands"""
        parts = command.split()
        cmd = parts[0].lower()
        
        if cmd == '/help':
            help_text = """
Commands:
  /help - Show this help
  /list - List online users
  /msg <user> <message> - Send private message
  /quit - Disconnect
            """
            await self.send_to_client(sender, help_text)
        
        elif cmd == '/list':
            users = ', '.join(self.clients.keys())
            await self.send_to_client(sender, f"Online users: {users}")
        
        elif cmd == '/msg':
            if len(parts) < 3:
                await self.send_to_client(sender, "Usage: /msg <user> <message>")
                return
            
            recipient = parts[1]
            message = ' '.join(parts[2:])
            
            if recipient in self.clients:
                await self.send_to_client(
                    recipient,
                    f"[Private from {sender}] {message}"
                )
                await self.send_to_client(
                    sender,
                    f"[Private to {recipient}] {message}"
                )
            else:
                await self.send_to_client(sender, f"User {recipient} not found")
        
        elif cmd == '/quit':
            await self.send_to_client(sender, "Goodbye!")
            if sender in self.clients:
                self.clients[sender].writer.close()
        
        else:
            await self.send_to_client(sender, f"Unknown command: {cmd}")
    
    async def start(self):
        """Start chat server"""
        self.server = await asyncio.start_server(
            self.handle_client,
            self.host,
            self.port
        )
        
        addr = self.server.sockets[0].getsockname()
        print(f"[Server] Chat server listening on {addr}")
        
        async with self.server:
            await self.server.serve_forever()

async def chat_server_demo():
    """Run chat server"""
    server = ChatServer()
    await server.start()

# asyncio.run(chat_server_demo())
```

---

## Real-world: HTTP API Client

Complete HTTP API client with retry logic and rate limiting.

```python
import asyncio
import aiohttp
from typing import Optional, Dict, Any
from dataclasses import dataclass
import time

@dataclass
class APIResponse:
    """API response"""
    status: int
    data: Any
    headers: Dict[str, str]
    duration: float

class APIClient:
    """
    HTTP API client with advanced features.
    
    Features:
    - Automatic retries
    - Rate limiting
    - Timeout handling
    - Connection pooling
    """
    
    def __init__(
        self,
        base_url: str,
        timeout: float = 30.0,
        max_retries: int = 3,
        rate_limit: Optional[float] = None
    ):
        self.base_url = base_url.rstrip('/')
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.max_retries = max_retries
        self.rate_limit = rate_limit
        self.last_request_time = 0.0
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession(timeout=self.timeout)
        return self
    
    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()
    
    async def _rate_limit_wait(self):
        """Wait for rate limit"""
        if self.rate_limit:
            elapsed = time.time() - self.last_request_time
            if elapsed < self.rate_limit:
                await asyncio.sleep(self.rate_limit - elapsed)
            self.last_request_time = time.time()
    
    async def request(
        self,
        method: str,
        endpoint: str,
        **kwargs
    ) -> APIResponse:
        """
        Make HTTP request with retries.
        
        Args:
            method: HTTP method
            endpoint: API endpoint
            **kwargs: Additional request parameters
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        for attempt in range(self.max_retries):
            try:
                # Rate limiting
                await self._rate_limit_wait()
                
                # Make request
                start = time.time()
                
                async with self.session.request(method, url, **kwargs) as response:
                    duration = time.time() - start
                    
                    # Parse response
                    if response.content_type == 'application/json':
                        data = await response.json()
                    else:
                        data = await response.text()
                    
                    return APIResponse(
                        status=response.status,
                        data=data,
                        headers=dict(response.headers),
                        duration=duration
                    )
            
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt == self.max_retries - 1:
                    raise
                
                # Exponential backoff
                wait_time = 2 ** attempt
                print(f"Request failed (attempt {attempt + 1}), retrying in {wait_time}s...")
                await asyncio.sleep(wait_time)
    
    async def get(self, endpoint: str, **kwargs) -> APIResponse:
        """GET request"""
        return await self.request('GET', endpoint, **kwargs)
    
    async def post(self, endpoint: str, **kwargs) -> APIResponse:
        """POST request"""
        return await self.request('POST', endpoint, **kwargs)
    
    async def put(self, endpoint: str, **kwargs) -> APIResponse:
        """PUT request"""
        return await self.request('PUT', endpoint, **kwargs)
    
    async def delete(self, endpoint: str, **kwargs) -> APIResponse:
        """DELETE request"""
        return await self.request('DELETE', endpoint, **kwargs)

async def api_client_demo():
    """Demonstrate API client"""
    
    async with APIClient(
        base_url='https://jsonplaceholder.typicode.com',
        timeout=10.0,
        max_retries=3,
        rate_limit=0.5  # 0.5 seconds between requests
    ) as client:
        # GET request
        response = await client.get('/posts/1')
        print(f"GET Status: {response.status}")
        print(f"GET Data: {response.data}")
        print(f"GET Duration: {response.duration:.2f}s")
        
        # POST request
        response = await client.post(
            '/posts',
            json={
                'title': 'Test Post',
                'body': 'This is a test',
                'userId': 1
            }
        )
        print(f"\nPOST Status: {response.status}")
        print(f"POST Data: {response.data}")
        
        # Multiple concurrent requests
        endpoints = [f'/posts/{i}' for i in range(1, 6)]
        
        responses = await asyncio.gather(*[
            client.get(endpoint)
            for endpoint in endpoints
        ])
        
        print(f"\nFetched {len(responses)} posts concurrently")

asyncio.run(api_client_demo())
```

---

## Common Patterns

### Pattern 1: Connection Pool

```python
import asyncio
from typing import List

class ConnectionPool:
    """Simple connection pool"""
    
    def __init__(self, host: str, port: int, pool_size: int = 10):
        self.host = host
        self.port = port
        self.pool_size = pool_size
        self.connections = asyncio.Queue()
    
    async def initialize(self):
        """Initialize connection pool"""
        for _ in range(self.pool_size):
            reader, writer = await asyncio.open_connection(
                self.host, self.port
            )
            await self.connections.put((reader, writer))
    
    async def acquire(self):
        """Acquire connection from pool"""
        return await self.connections.get()
    
    async def release(self, connection):
        """Release connection back to pool"""
        await self.connections.put(connection)
    
    async def close_all(self):
        """Close all connections"""
        while not self.connections.empty():
            reader, writer = await self.connections.get()
            writer.close()
            await writer.wait_closed()
```

### Pattern 2: Reconnection Logic

```python
import asyncio

async def connect_with_retry(host, port, max_retries=5):
    """Connect with automatic retry"""
    for attempt in range(max_retries):
        try:
            reader, writer = await asyncio.open_connection(host, port)
            print(f"Connected on attempt {attempt + 1}")
            return reader, writer
        
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            
            wait_time = 2 ** attempt
            print(f"Connection failed, retrying in {wait_time}s...")
            await asyncio.sleep(wait_time)
```

---

## Best Practices

### ✅ DO: Use connection pooling

```python
# Good: Reuse connections
async with aiohttp.ClientSession() as session:
    for url in urls:
        async with session.get(url) as response:
            data = await response.json()
```

### ✅ DO: Set timeouts

```python
# Good: Always use timeouts
timeout = aiohttp.ClientTimeout(total=30.0)
async with aiohttp.ClientSession(timeout=timeout) as session:
    async with session.get(url) as response:
        data = await response.json()
```

### ✅ DO: Handle connection errors

```python
# Good: Handle errors gracefully
try:
    async with session.get(url) as response:
        data = await response.json()
except aiohttp.ClientError as e:
    logger.error(f"Request failed: {e}")
```

### ❌ DON'T: Create session per request

```python
# Bad: Creates new session each time
async def fetch(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.json()

# Good: Reuse session
async def fetch_all(urls):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_one(session, url) for url in urls]
        return await asyncio.gather(*tasks)
```

---

## Summary: Networking Mental Model

✅ **Use Streams API** - high-level, easy to use

✅ **Connection pooling** - reuse connections

✅ **Always set timeouts** - prevent hanging

✅ **Handle errors gracefully** - network is unreliable

✅ **Rate limiting** - respect API limits

✅ **Retry with backoff** - handle transient failures

✅ **Close connections** - prevent resource leaks

---

## What's Next?

We've completed Part 6 on Integration. Next, we explore advanced architecture patterns.

In [Chapter 19: Worker Pools](../part7-patterns/19-worker-pools.md), we'll cover:
- Fixed worker pools
- Dynamic worker pools
- Bounded worker pools
- Work stealing
- Load balancing
- Practical worker pool patterns

Worker pools are essential for managing concurrent work efficiently.

---

**Previous:** [← Chapter 17: Subprocesses](./17-subprocesses.md)  
**Next:** [Chapter 19: Worker Pools →](../part7-patterns/19-worker-pools.md)