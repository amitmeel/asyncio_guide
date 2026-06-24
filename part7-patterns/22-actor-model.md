# Chapter 22: Actor Model

## Overview

The **actor model** is a mathematical model of concurrent computation where "actors" are the fundamental units of computation. Each actor:

1. Has a **mailbox** (message queue) for receiving messages
2. Processes messages **one at a time** (sequential processing)
3. Can **send messages** to other actors
4. Can **create new actors**
5. Can **change its own state** in response to messages

The actor model provides **strong isolation** - actors don't share state, they only communicate through messages. This eliminates many concurrency bugs like race conditions and deadlocks.

## Mental Model

Think of actors like **independent workers with mailboxes**:

```
┌─────────────┐         ┌─────────────┐         ┌─────────────┐
│  Actor A    │         │  Actor B    │         │  Actor C    │
│             │         │             │         │             │
│  ┌───────┐  │         │  ┌───────┐  │         │  ┌───────┐  │
│  │Mailbox│  │         │  │Mailbox│  │         │  │Mailbox│  │
│  │ [msg] │  │         │  │ [msg] │  │         │  │ [msg] │  │
│  │ [msg] │  │         │  │ [msg] │  │         │  │ [msg] │  │
│  └───────┘  │         │  └───────┘  │         │  └───────┘  │
│             │         │             │         │             │
│  ┌───────┐  │         │  ┌───────┐  │         │  ┌───────┐  │
│  │ State │  │         │  │ State │  │         │  │ State │  │
│  └───────┘  │         │  └───────┘  │         │  └───────┘  │
└─────────────┘         └─────────────┘         └─────────────┘
       │                       │                       │
       └───────── messages ────┴───────────────────────┘
```

**Key principles:**

1. **Isolation**: Each actor has private state
2. **Message passing**: Only way to communicate
3. **Sequential processing**: One message at a time per actor
4. **Location transparency**: Actors can be local or remote
5. **Supervision**: Actors can monitor and restart other actors

## Core Actor Implementation

### 1. Basic Actor

```python
import asyncio
from typing import Any, Dict, Optional, Callable, Awaitable
from dataclasses import dataclass
from enum import Enum, auto
import time

class MessageType(Enum):
    """Types of messages actors can send"""
    NORMAL = auto()
    SYSTEM = auto()
    STOP = auto()

@dataclass
class Message:
    """Message sent between actors"""
    type: MessageType
    payload: Any
    sender: Optional[str] = None
    reply_to: Optional[asyncio.Queue] = None
    timestamp: float = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = time.time()

class Actor:
    """
    Base actor implementation.
    
    Each actor:
    - Has a unique name
    - Has a mailbox (asyncio.Queue)
    - Processes messages sequentially
    - Maintains private state
    """
    
    def __init__(self, name: str, mailbox_size: int = 0):
        self.name = name
        self.mailbox = asyncio.Queue(maxsize=mailbox_size)
        self.state: Dict[str, Any] = {}
        self.running = False
        self._task: Optional[asyncio.Task] = None
    
    async def send(self, message: Message):
        """Send message to this actor's mailbox"""
        message.timestamp = time.time()
        await self.mailbox.put(message)
    
    async def send_and_wait(self, message: Message, timeout: float = None) -> Any:
        """
        Send message and wait for reply.
        Creates a reply queue and waits for response.
        """
        reply_queue = asyncio.Queue(maxsize=1)
        message.reply_to = reply_queue
        await self.send(message)
        
        try:
            reply = await asyncio.wait_for(reply_queue.get(), timeout=timeout)
            return reply
        except asyncio.TimeoutError:
            raise TimeoutError(f"No reply from {self.name} within {timeout}s")
    
    async def reply(self, message: Message, response: Any):
        """Reply to a message"""
        if message.reply_to:
            await message.reply_to.put(response)
    
    async def receive(self) -> Message:
        """Receive next message from mailbox"""
        return await self.mailbox.get()
    
    async def handle_message(self, message: Message):
        """
        Override this to implement actor behavior.
        Called for each message received.
        """
        raise NotImplementedError(f"{self.__class__.__name__} must implement handle_message")
    
    async def on_start(self):
        """Called when actor starts. Override for initialization."""
        pass
    
    async def on_stop(self):
        """Called when actor stops. Override for cleanup."""
        pass
    
    async def run(self):
        """Main actor loop"""
        self.running = True
        await self.on_start()
        print(f"[{self.name}] Started")
        
        try:
            while self.running:
                message = await self.receive()
                
                # Handle system messages
                if message.type == MessageType.STOP:
                    print(f"[{self.name}] Received STOP")
                    break
                
                # Handle user messages
                try:
                    await self.handle_message(message)
                except Exception as e:
                    print(f"[{self.name}] Error handling message: {e}")
                    # In production, would notify supervisor
        
        except asyncio.CancelledError:
            print(f"[{self.name}] Cancelled")
            raise
        finally:
            await self.on_stop()
            self.running = False
            print(f"[{self.name}] Stopped")
    
    def start(self) -> asyncio.Task:
        """Start the actor (returns task)"""
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self.run())
        return self._task
    
    async def stop(self):
        """Stop the actor gracefully"""
        await self.send(Message(type=MessageType.STOP, payload=None))
        if self._task:
            await self._task
```

### 2. Example Actors

```python
class CounterActor(Actor):
    """Actor that maintains a counter"""
    
    async def on_start(self):
        self.state['count'] = 0
    
    async def handle_message(self, message: Message):
        if message.payload == 'increment':
            self.state['count'] += 1
            print(f"[{self.name}] Count: {self.state['count']}")
            await self.reply(message, self.state['count'])
        
        elif message.payload == 'get':
            await self.reply(message, self.state['count'])
        
        elif message.payload == 'reset':
            self.state['count'] = 0
            await self.reply(message, 'reset')

class LoggerActor(Actor):
    """Actor that logs messages"""
    
    async def on_start(self):
        self.state['logs'] = []
    
    async def handle_message(self, message: Message):
        log_entry = f"[{message.timestamp:.2f}] {message.sender}: {message.payload}"
        self.state['logs'].append(log_entry)
        print(f"[{self.name}] {log_entry}")
        
        if message.payload == 'get_logs':
            await self.reply(message, self.state['logs'])

async def basic_actor_example():
    """Demonstrate basic actor usage"""
    # Create actors
    counter = CounterActor("Counter")
    logger = LoggerActor("Logger")
    
    # Start actors
    counter.start()
    logger.start()
    
    # Send messages
    await counter.send(Message(
        type=MessageType.NORMAL,
        payload='increment',
        sender='main'
    ))
    
    await logger.send(Message(
        type=MessageType.NORMAL,
        payload='System started',
        sender='main'
    ))
    
    # Request-reply pattern
    count = await counter.send_and_wait(Message(
        type=MessageType.NORMAL,
        payload='get',
        sender='main'
    ), timeout=1.0)
    print(f"Current count: {count}")
    
    # Cleanup
    await asyncio.sleep(0.5)
    await counter.stop()
    await logger.stop()
```

## Example 1: Bank Account System

A realistic example showing how actors handle concurrent transactions:

```python
from decimal import Decimal
from typing import Tuple

class BankAccountActor(Actor):
    """
    Actor representing a bank account.
    Processes transactions sequentially, preventing race conditions.
    """
    
    async def on_start(self):
        self.state['balance'] = Decimal('0.00')
        self.state['transactions'] = []
    
    async def handle_message(self, message: Message):
        action = message.payload.get('action')
        
        if action == 'deposit':
            amount = Decimal(str(message.payload['amount']))
            if amount <= 0:
                await self.reply(message, {'success': False, 'error': 'Invalid amount'})
                return
            
            self.state['balance'] += amount
            self.state['transactions'].append({
                'type': 'deposit',
                'amount': amount,
                'balance': self.state['balance'],
                'timestamp': message.timestamp
            })
            
            print(f"[{self.name}] Deposited ${amount}, balance: ${self.state['balance']}")
            await self.reply(message, {
                'success': True,
                'balance': self.state['balance']
            })
        
        elif action == 'withdraw':
            amount = Decimal(str(message.payload['amount']))
            if amount <= 0:
                await self.reply(message, {'success': False, 'error': 'Invalid amount'})
                return
            
            if self.state['balance'] < amount:
                await self.reply(message, {'success': False, 'error': 'Insufficient funds'})
                return
            
            self.state['balance'] -= amount
            self.state['transactions'].append({
                'type': 'withdraw',
                'amount': amount,
                'balance': self.state['balance'],
                'timestamp': message.timestamp
            })
            
            print(f"[{self.name}] Withdrew ${amount}, balance: ${self.state['balance']}")
            await self.reply(message, {
                'success': True,
                'balance': self.state['balance']
            })
        
        elif action == 'get_balance':
            await self.reply(message, {
                'balance': self.state['balance']
            })
        
        elif action == 'get_transactions':
            await self.reply(message, {
                'transactions': self.state['transactions']
            })

class TransferCoordinator(Actor):
    """
    Coordinates transfers between accounts.
    Implements two-phase commit pattern.
    """
    
    def __init__(self, name: str, accounts: Dict[str, BankAccountActor]):
        super().__init__(name)
        self.accounts = accounts
    
    async def handle_message(self, message: Message):
        if message.payload.get('action') == 'transfer':
            from_account = message.payload['from']
            to_account = message.payload['to']
            amount = message.payload['amount']
            
            # Phase 1: Withdraw from source
            withdraw_result = await self.accounts[from_account].send_and_wait(
                Message(
                    type=MessageType.NORMAL,
                    payload={'action': 'withdraw', 'amount': amount},
                    sender=self.name
                ),
                timeout=2.0
            )
            
            if not withdraw_result['success']:
                await self.reply(message, {
                    'success': False,
                    'error': f"Withdrawal failed: {withdraw_result.get('error')}"
                })
                return
            
            # Phase 2: Deposit to destination
            deposit_result = await self.accounts[to_account].send_and_wait(
                Message(
                    type=MessageType.NORMAL,
                    payload={'action': 'deposit', 'amount': amount},
                    sender=self.name
                ),
                timeout=2.0
            )
            
            if not deposit_result['success']:
                # Rollback: deposit back to source
                await self.accounts[from_account].send_and_wait(
                    Message(
                        type=MessageType.NORMAL,
                        payload={'action': 'deposit', 'amount': amount},
                        sender=self.name
                    ),
                    timeout=2.0
                )
                await self.reply(message, {
                    'success': False,
                    'error': 'Deposit failed, transaction rolled back'
                })
                return
            
            print(f"[{self.name}] Transfer complete: ${amount} from {from_account} to {to_account}")
            await self.reply(message, {'success': True})

async def bank_system_example():
    """Run bank account system"""
    # Create accounts
    alice_account = BankAccountActor("Alice")
    bob_account = BankAccountActor("Bob")
    
    accounts = {
        'alice': alice_account,
        'bob': bob_account
    }
    
    # Create coordinator
    coordinator = TransferCoordinator("Coordinator", accounts)
    
    # Start all actors
    alice_account.start()
    bob_account.start()
    coordinator.start()
    
    # Initial deposits
    await alice_account.send_and_wait(Message(
        type=MessageType.NORMAL,
        payload={'action': 'deposit', 'amount': '1000.00'},
        sender='main'
    ))
    
    await bob_account.send_and_wait(Message(
        type=MessageType.NORMAL,
        payload={'action': 'deposit', 'amount': '500.00'},
        sender='main'
    ))
    
    # Transfer money
    result = await coordinator.send_and_wait(Message(
        type=MessageType.NORMAL,
        payload={
            'action': 'transfer',
            'from': 'alice',
            'to': 'bob',
            'amount': '250.00'
        },
        sender='main'
    ), timeout=5.0)
    
    print(f"Transfer result: {result}")
    
    # Check balances
    alice_balance = await alice_account.send_and_wait(Message(
        type=MessageType.NORMAL,
        payload={'action': 'get_balance'},
        sender='main'
    ))
    
    bob_balance = await bob_account.send_and_wait(Message(
        type=MessageType.NORMAL,
        payload={'action': 'get_balance'},
        sender='main'
    ))
    
    print(f"Alice balance: ${alice_balance['balance']}")
    print(f"Bob balance: ${bob_balance['balance']}")
    
    # Cleanup
    await asyncio.sleep(0.5)
    await alice_account.stop()
    await bob_account.stop()
    await coordinator.stop()
```

## Actor Supervision

Supervision is a key pattern where parent actors monitor and restart child actors:

```python
from enum import Enum

class SupervisionStrategy(Enum):
    """How to handle child actor failures"""
    RESTART = auto()  # Restart the failed actor
    STOP = auto()     # Stop the failed actor
    ESCALATE = auto() # Escalate to parent supervisor

class Supervisor(Actor):
    """
    Supervisor actor that monitors and manages child actors.
    
    Implements the "let it crash" philosophy - actors can fail,
    and supervisors handle recovery.
    """
    
    def __init__(self, name: str, strategy: SupervisionStrategy = SupervisionStrategy.RESTART):
        super().__init__(name)
        self.strategy = strategy
        self.children: Dict[str, Actor] = {}
        self.child_tasks: Dict[str, asyncio.Task] = {}
    
    async def spawn_child(self, child: Actor) -> str:
        """Spawn and monitor a child actor"""
        self.children[child.name] = child
        task = child.start()
        self.child_tasks[child.name] = task
        
        # Monitor child
        asyncio.create_task(self._monitor_child(child.name, task))
        
        print(f"[{self.name}] Spawned child: {child.name}")
        return child.name
    
    async def _monitor_child(self, child_name: str, task: asyncio.Task):
        """Monitor child actor and handle failures"""
        try:
            await task
            print(f"[{self.name}] Child {child_name} completed normally")
        except Exception as e:
            print(f"[{self.name}] Child {child_name} failed: {e}")
            await self._handle_child_failure(child_name, e)
    
    async def _handle_child_failure(self, child_name: str, error: Exception):
        """Handle child actor failure based on strategy"""
        if self.strategy == SupervisionStrategy.RESTART:
            print(f"[{self.name}] Restarting child: {child_name}")
            child = self.children[child_name]
            task = child.start()
            self.child_tasks[child_name] = task
            asyncio.create_task(self._monitor_child(child_name, task))
        
        elif self.strategy == SupervisionStrategy.STOP:
            print(f"[{self.name}] Stopping child: {child_name}")
            del self.children[child_name]
            del self.child_tasks[child_name]
        
        elif self.strategy == SupervisionStrategy.ESCALATE:
            print(f"[{self.name}] Escalating failure of {child_name}")
            # Would notify parent supervisor
            raise error
    
    async def handle_message(self, message: Message):
        """Handle supervisor messages"""
        action = message.payload.get('action')
        
        if action == 'spawn':
            child = message.payload['child']
            child_name = await self.spawn_child(child)
            await self.reply(message, {'child_name': child_name})
        
        elif action == 'stop_child':
            child_name = message.payload['child_name']
            if child_name in self.children:
                await self.children[child_name].stop()
                await self.reply(message, {'success': True})
            else:
                await self.reply(message, {'success': False, 'error': 'Child not found'})
        
        elif action == 'list_children':
            await self.reply(message, {
                'children': list(self.children.keys())
            })
    
    async def on_stop(self):
        """Stop all children when supervisor stops"""
        print(f"[{self.name}] Stopping all children")
        for child in self.children.values():
            await child.stop()
```

## Example 2: Worker Pool with Supervision

```python
class WorkerActor(Actor):
    """Worker that processes tasks"""
    
    async def handle_message(self, message: Message):
        task_data = message.payload
        
        # Simulate work
        print(f"[{self.name}] Processing task: {task_data['id']}")
        await asyncio.sleep(task_data.get('duration', 0.1))
        
        # Simulate occasional failures
        if task_data.get('should_fail', False):
            raise RuntimeError(f"Task {task_data['id']} failed!")
        
        result = {
            'task_id': task_data['id'],
            'result': f"Processed by {self.name}",
            'timestamp': time.time()
        }
        
        await self.reply(message, result)

class WorkerPoolSupervisor(Supervisor):
    """Supervisor managing a pool of workers"""
    
    def __init__(self, name: str, num_workers: int):
        super().__init__(name, strategy=SupervisionStrategy.RESTART)
        self.num_workers = num_workers
        self.task_queue = asyncio.Queue()
    
    async def on_start(self):
        """Create worker pool"""
        for i in range(self.num_workers):
            worker = WorkerActor(f"Worker-{i}")
            await self.spawn_child(worker)
        
        # Start task distributor
        asyncio.create_task(self._distribute_tasks())
    
    async def _distribute_tasks(self):
        """Distribute tasks to available workers"""
        while self.running:
            try:
                task_data = await asyncio.wait_for(
                    self.task_queue.get(),
                    timeout=1.0
                )
                
                # Find available worker (simple round-robin)
                worker_names = list(self.children.keys())
                if worker_names:
                    worker_name = worker_names[task_data['id'] % len(worker_names)]
                    worker = self.children[worker_name]
                    
                    try:
                        result = await worker.send_and_wait(
                            Message(
                                type=MessageType.NORMAL,
                                payload=task_data,
                                sender=self.name
                            ),
                            timeout=5.0
                        )
                        print(f"[{self.name}] Task {task_data['id']} completed: {result}")
                    except Exception as e:
                        print(f"[{self.name}] Task {task_data['id']} failed: {e}")
            
            except asyncio.TimeoutError:
                continue
    
    async def handle_message(self, message: Message):
        """Handle pool messages"""
        action = message.payload.get('action')
        
        if action == 'submit_task':
            task_data = message.payload['task']
            await self.task_queue.put(task_data)
            await self.reply(message, {'queued': True})
        else:
            await super().handle_message(message)

async def worker_pool_example():
    """Run worker pool with supervision"""
    supervisor = WorkerPoolSupervisor("PoolSupervisor", num_workers=3)
    supervisor.start()
    
    await asyncio.sleep(0.5)  # Let workers start
    
    # Submit tasks
    for i in range(10):
        await supervisor.send_and_wait(Message(
            type=MessageType.NORMAL,
            payload={
                'action': 'submit_task',
                'task': {
                    'id': i,
                    'duration': 0.2,
                    'should_fail': i == 5  # Task 5 will fail
                }
            },
            sender='main'
        ))
    
    # Let tasks complete
    await asyncio.sleep(3)
    
    # Check children
    children = await supervisor.send_and_wait(Message(
        type=MessageType.NORMAL,
        payload={'action': 'list_children'},
        sender='main'
    ))
    print(f"Active workers: {children['children']}")
    
    await supervisor.stop()
```

## Actor Registry

For actor discovery and location transparency:

```python
class ActorRegistry:
    """
    Global registry for actor lookup.
    Enables location transparency - actors can find each other by name.
    """
    
    def __init__(self):
        self._actors: Dict[str, Actor] = {}
        self._lock = asyncio.Lock()
    
    async def register(self, actor: Actor):
        """Register an actor"""
        async with self._lock:
            if actor.name in self._actors:
                raise ValueError(f"Actor {actor.name} already registered")
            self._actors[actor.name] = actor
            print(f"[Registry] Registered: {actor.name}")
    
    async def unregister(self, name: str):
        """Unregister an actor"""
        async with self._lock:
            if name in self._actors:
                del self._actors[name]
                print(f"[Registry] Unregistered: {name}")
    
    async def lookup(self, name: str) -> Optional[Actor]:
        """Find actor by name"""
        async with self._lock:
            return self._actors.get(name)
    
    async def send_to(self, name: str, message: Message) -> bool:
        """Send message to actor by name"""
        actor = await self.lookup(name)
        if actor:
            await actor.send(message)
            return True
        return False
    
    async def list_actors(self) -> list[str]:
        """List all registered actors"""
        async with self._lock:
            return list(self._actors.keys())

# Global registry instance
_registry = ActorRegistry()

async def register_actor(actor: Actor):
    """Register actor in global registry"""
    await _registry.register(actor)

async def lookup_actor(name: str) -> Optional[Actor]:
    """Lookup actor in global registry"""
    return await _registry.lookup(name)
```

## Example 3: Chat Room System

A complete chat system using actors:

```python
class ChatRoomActor(Actor):
    """Chat room that manages participants and messages"""
    
    async def on_start(self):
        self.state['participants'] = set()
        self.state['messages'] = []
    
    async def handle_message(self, message: Message):
        action = message.payload.get('action')
        
        if action == 'join':
            user_name = message.payload['user']
            self.state['participants'].add(user_name)
            print(f"[{self.name}] {user_name} joined")
            
            # Notify all participants
            join_msg = f"{user_name} joined the room"
            self.state['messages'].append(join_msg)
            await self._broadcast(join_msg, exclude=user_name)
            
            await self.reply(message, {'success': True})
        
        elif action == 'leave':
            user_name = message.payload['user']
            self.state['participants'].discard(user_name)
            print(f"[{self.name}] {user_name} left")
            
            leave_msg = f"{user_name} left the room"
            self.state['messages'].append(leave_msg)
            await self._broadcast(leave_msg, exclude=user_name)
            
            await self.reply(message, {'success': True})
        
        elif action == 'send_message':
            user_name = message.payload['user']
            text = message.payload['text']
            
            chat_msg = f"{user_name}: {text}"
            self.state['messages'].append(chat_msg)
            print(f"[{self.name}] {chat_msg}")
            
            await self._broadcast(chat_msg, exclude=user_name)
            await self.reply(message, {'success': True})
        
        elif action == 'get_messages':
            await self.reply(message, {
                'messages': self.state['messages'][-10:]  # Last 10 messages
            })
    
    async def _broadcast(self, text: str, exclude: Optional[str] = None):
        """Broadcast message to all participants"""
        for participant in self.state['participants']:
            if participant != exclude:
                # In real system, would send to participant actor
                participant_actor = await lookup_actor(participant)
                if participant_actor:
                    await participant_actor.send(Message(
                        type=MessageType.NORMAL,
                        payload={'action': 'receive_message', 'text': text},
                        sender=self.name
                    ))

class ChatUserActor(Actor):
    """User in chat system"""
    
    def __init__(self, name: str, room_name: str):
        super().__init__(name)
        self.room_name = room_name
    
    async def on_start(self):
        """Join room on start"""
        await register_actor(self)
        
        room = await lookup_actor(self.room_name)
        if room:
            await room.send_and_wait(Message(
                type=MessageType.NORMAL,
                payload={'action': 'join', 'user': self.name},
                sender=self.name
            ))
    
    async def handle_message(self, message: Message):
        action = message.payload.get('action')
        
        if action == 'receive_message':
            text = message.payload['text']
            print(f"[{self.name}] Received: {text}")
        
        elif action == 'send':
            text = message.payload['text']
            room = await lookup_actor(self.room_name)
            if room:
                await room.send_and_wait(Message(
                    type=MessageType.NORMAL,
                    payload={
                        'action': 'send_message',
                        'user': self.name,
                        'text': text
                    },
                    sender=self.name
                ))
    
    async def on_stop(self):
        """Leave room on stop"""
        room = await lookup_actor(self.room_name)
        if room:
            await room.send_and_wait(Message(
                type=MessageType.NORMAL,
                payload={'action': 'leave', 'user': self.name},
                sender=self.name
            ))

async def chat_system_example():
    """Run chat system"""
    # Create room
    room = ChatRoomActor("GeneralRoom")
    await register_actor(room)
    room.start()
    
    # Create users
    alice = ChatUserActor("Alice", "GeneralRoom")
    bob = ChatUserActor("Bob", "GeneralRoom")
    carol = ChatUserActor("Carol", "GeneralRoom")
    
    alice.start()
    bob.start()
    carol.start()
    
    await asyncio.sleep(0.5)
    
    # Send messages
    await alice.send(Message(
        type=MessageType.NORMAL,
        payload={'action': 'send', 'text': 'Hello everyone!'},
        sender='main'
    ))
    
    await asyncio.sleep(0.2)
    
    await bob.send(Message(
        type=MessageType.NORMAL,
        payload={'action': 'send', 'text': 'Hi Alice!'},
        sender='main'
    ))
    
    await asyncio.sleep(0.2)
    
    await carol.send(Message(
        type=MessageType.NORMAL,
        payload={'action': 'send', 'text': 'Hey folks!'},
        sender='main'
    ))
    
    await asyncio.sleep(0.5)
    
    # Cleanup
    await alice.stop()
    await bob.stop()
    await carol.stop()
    await room.stop()
```

## Advanced Patterns

### 1. Ask Pattern (Request-Reply with Timeout)

```python
async def ask(actor: Actor, payload: Any, timeout: float = 5.0) -> Any:
    """
    Ask pattern: send message and wait for reply with timeout.
    Commonly used in actor systems for synchronous-style communication.
    """
    message = Message(
        type=MessageType.NORMAL,
        payload=payload,
        sender='ask_pattern'
    )
    return await actor.send_and_wait(message, timeout=timeout)
```

### 2. Publish-Subscribe with Actors

```python
class PubSubBroker(Actor):
    """Broker for publish-subscribe messaging"""
    
    async def on_start(self):
        self.state['subscriptions'] = {}  # topic -> set of actor names
    
    async def handle_message(self, message: Message):
        action = message.payload.get('action')
        
        if action == 'subscribe':
            topic = message.payload['topic']
            subscriber = message.payload['subscriber']
            
            if topic not in self.state['subscriptions']:
                self.state['subscriptions'][topic] = set()
            
            self.state['subscriptions'][topic].add(subscriber)
            print(f"[{self.name}] {subscriber} subscribed to {topic}")
            await self.reply(message, {'success': True})
        
        elif action == 'publish':
            topic = message.payload['topic']
            data = message.payload['data']
            
            subscribers = self.state['subscriptions'].get(topic, set())
            print(f"[{self.name}] Publishing to {topic}: {len(subscribers)} subscribers")
            
            for subscriber_name in subscribers:
                subscriber = await lookup_actor(subscriber_name)
                if subscriber:
                    await subscriber.send(Message(
                        type=MessageType.NORMAL,
                        payload={
                            'action': 'notification',
                            'topic': topic,
                            'data': data
                        },
                        sender=self.name
                    ))
            
            await self.reply(message, {'delivered': len(subscribers)})
```

## When to Use Actor Model

### ✅ Use When:

1. **Strong isolation needed**: No shared state between components
2. **Message-based communication**: Natural fit for distributed systems
3. **Fault tolerance**: Supervision trees handle failures gracefully
4. **Location transparency**: Actors can be local or remote
5. **Scalability**: Easy to distribute across machines
6. **Stateful services**: Each actor maintains its own state

**Examples:**
- Chat systems (users, rooms as actors)
- Game servers (players, game sessions as actors)
- Financial systems (accounts, transactions as actors)
- IoT systems (devices as actors)
- Microservices (services as actors)

### ❌ Avoid When:

1. **Shared state required**: Actors isolate state
2. **Low latency critical**: Message passing adds overhead
3. **Simple request-response**: Overkill for simple APIs
4. **Tight coupling needed**: Actors are loosely coupled
5. **Synchronous workflows**: Actors are inherently asynchronous

## Performance Considerations

```python
class PerformantActor(Actor):
    """Actor with performance optimizations"""
    
    def __init__(self, name: str, batch_size: int = 10):
        super().__init__(name, mailbox_size=1000)  # Bounded mailbox
        self.batch_size = batch_size
    
    async def run(self):
        """Process messages in batches"""
        self.running = True
        await self.on_start()
        
        try:
            while self.running:
                # Collect batch
                batch = []
                for _ in range(self.batch_size):
                    try:
                        msg = await asyncio.wait_for(
                            self.mailbox.get(),
                            timeout=0.01
                        )
                        batch.append(msg)
                    except asyncio.TimeoutError:
                        break
                
                if not batch:
                    # No messages, wait for one
                    batch = [await self.mailbox.get()]
                
                # Process batch
                for message in batch:
                    if message.type == MessageType.STOP:
                        return
                    await self.handle_message(message)
        
        finally:
            await self.on_stop()
```

## Mental Model Summary

**Actor model is like a company with employees:**

1. **Actors = Employees**: Each has their own desk (state) and inbox (mailbox)
2. **Messages = Memos**: Communication happens through written messages
3. **Mailbox = Inbox**: Messages queue up, processed one at a time
4. **Supervisor = Manager**: Monitors employees, handles failures
5. **Registry = Directory**: Find employees by name

**Key insights:**

- **Isolation**: No shared state, only message passing
- **Sequential**: Each actor processes one message at a time
- **Asynchronous**: Sending messages doesn't block
- **Fault-tolerant**: Supervisors restart failed actors
- **Scalable**: Easy to distribute across machines

**Common patterns:**
- **Request-reply**: Ask pattern with timeout
- **Fire-and-forget**: Send without waiting for reply
- **Supervision**: Parent monitors children
- **Pub-sub**: Broker distributes messages to subscribers

**Best practices:**
- Keep actors small and focused
- Use supervision for fault tolerance
- Implement timeouts on requests
- Bound mailbox sizes
- Use registry for actor discovery
- Test with message ordering variations

The actor model provides strong isolation and fault tolerance, making it ideal for distributed systems and applications requiring high reliability.