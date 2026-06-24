# Chapter 17: Subprocesses

## Introduction

Running external programs is a common requirement in real-world applications. Asyncio provides tools to run subprocesses without blocking the event loop, allowing you to execute shell commands, run external tools, and process their output asynchronously.

By the end of this chapter, you'll understand:
- `asyncio.create_subprocess_exec()` for running programs
- `asyncio.create_subprocess_shell()` for shell commands
- Streaming stdout/stderr asynchronously
- Process communication and pipes
- Timeout and cancellation of subprocesses
- Practical subprocess patterns

---

## The Problem: Blocking Subprocess Calls

Traditional subprocess calls block the event loop.

### Blocking Subprocess

```python
import asyncio
import subprocess

async def blocking_subprocess_problem():
    """Demonstrate blocking subprocess issue"""
    
    async def fast_task(task_id):
        for i in range(5):
            print(f"Task {task_id}: iteration {i}")
            await asyncio.sleep(0.5)
    
    async def blocking_subprocess():
        print("Subprocess: Starting")
        # ❌ BLOCKS EVENT LOOP!
        result = subprocess.run(
            ["sleep", "3"],
            capture_output=True
        )
        print("Subprocess: Done")
        return result
    
    await asyncio.gather(
        fast_task(1),
        fast_task(2),
        blocking_subprocess()
    )

# asyncio.run(blocking_subprocess_problem())
# Fast tasks freeze during subprocess execution!
```

---

## Solution: Async Subprocesses

### create_subprocess_exec()

Run programs without blocking the event loop.

```python
import asyncio

async def async_subprocess_demo():
    """Basic async subprocess usage"""
    
    # Create subprocess
    process = await asyncio.create_subprocess_exec(
        "echo",
        "Hello from subprocess!",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    
    # Wait for completion
    stdout, stderr = await process.communicate()
    
    print(f"Exit code: {process.returncode}")
    print(f"Output: {stdout.decode()}")
    print(f"Errors: {stderr.decode()}")

asyncio.run(async_subprocess_demo())

# Output:
# Exit code: 0
# Output: Hello from subprocess!
# Errors: 
```

### create_subprocess_shell()

Run shell commands (use with caution - security risk).

```python
import asyncio

async def subprocess_shell_demo():
    """Using shell commands"""
    
    # Run shell command
    process = await asyncio.create_subprocess_shell(
        "echo 'Hello' && sleep 1 && echo 'World'",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    
    stdout, stderr = await process.communicate()
    
    print(f"Output:\n{stdout.decode()}")

asyncio.run(subprocess_shell_demo())

# Output:
# Output:
# Hello
# World
```

**Security warning:** `create_subprocess_shell()` is vulnerable to shell injection. Prefer `create_subprocess_exec()` when possible.

---

## Streaming Output

Stream subprocess output line by line without waiting for completion.

### Streaming stdout

```python
import asyncio

async def stream_output_demo():
    """Stream subprocess output line by line"""
    
    process = await asyncio.create_subprocess_exec(
        "python", "-c",
        "import time; [print(f'Line {i}') or time.sleep(0.5) for i in range(5)]",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    
    # Read output line by line
    while True:
        line = await process.stdout.readline()
        if not line:
            break
        
        print(f"Received: {line.decode().strip()}")
    
    # Wait for process to complete
    await process.wait()
    print(f"Exit code: {process.returncode}")

asyncio.run(stream_output_demo())

# Output:
# Received: Line 0
# Received: Line 1
# Received: Line 2
# Received: Line 3
# Received: Line 4
# Exit code: 0
```

### Streaming Both stdout and stderr

```python
import asyncio

async def stream_both_demo():
    """Stream both stdout and stderr"""
    
    process = await asyncio.create_subprocess_exec(
        "python", "-c",
        """
import sys
import time
for i in range(3):
    print(f'stdout: {i}', flush=True)
    print(f'stderr: {i}', file=sys.stderr, flush=True)
    time.sleep(0.5)
        """,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    
    async def read_stream(stream, name):
        """Read from stream"""
        while True:
            line = await stream.readline()
            if not line:
                break
            print(f"[{name}] {line.decode().strip()}")
    
    # Read both streams concurrently
    await asyncio.gather(
        read_stream(process.stdout, "OUT"),
        read_stream(process.stderr, "ERR")
    )
    
    await process.wait()

asyncio.run(stream_both_demo())

# Output:
# [OUT] stdout: 0
# [ERR] stderr: 0
# [OUT] stdout: 1
# [ERR] stderr: 1
# [OUT] stdout: 2
# [ERR] stderr: 2
```

---

## Process Communication

Send input to subprocess and receive output.

### stdin Communication

```python
import asyncio

async def stdin_communication_demo():
    """Send input to subprocess"""
    
    process = await asyncio.create_subprocess_exec(
        "python", "-c",
        """
import sys
for line in sys.stdin:
    print(f'Echo: {line.strip()}', flush=True)
        """,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    
    # Send input
    input_data = "Hello\nWorld\nTest\n"
    stdout, stderr = await process.communicate(input_data.encode())
    
    print(f"Output:\n{stdout.decode()}")

asyncio.run(stdin_communication_demo())

# Output:
# Output:
# Echo: Hello
# Echo: World
# Echo: Test
```

### Interactive Communication

```python
import asyncio

async def interactive_communication():
    """Interactive communication with subprocess"""
    
    process = await asyncio.create_subprocess_exec(
        "python", "-u", "-c",  # -u for unbuffered
        """
import sys
while True:
    line = input()
    if line == 'quit':
        break
    print(f'Response: {line.upper()}', flush=True)
        """,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    
    async def send_and_receive(message):
        """Send message and receive response"""
        # Send
        process.stdin.write(f"{message}\n".encode())
        await process.stdin.drain()
        
        # Receive
        response = await process.stdout.readline()
        return response.decode().strip()
    
    # Interactive communication
    print(await send_and_receive("hello"))
    print(await send_and_receive("world"))
    print(await send_and_receive("test"))
    
    # Quit
    process.stdin.write(b"quit\n")
    await process.stdin.drain()
    
    await process.wait()

asyncio.run(interactive_communication())

# Output:
# Response: HELLO
# Response: WORLD
# Response: TEST
```

---

## Timeout and Cancellation

Control subprocess execution time and handle cancellation.

### Subprocess with Timeout

```python
import asyncio

async def subprocess_timeout_demo():
    """Subprocess with timeout"""
    
    async def run_with_timeout(command, timeout):
        """Run subprocess with timeout"""
        try:
            process = await asyncio.create_subprocess_exec(
                *command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            async with asyncio.timeout(timeout):
                stdout, stderr = await process.communicate()
                return stdout.decode(), stderr.decode(), process.returncode
        
        except asyncio.TimeoutError:
            print(f"Process timed out, terminating...")
            process.terminate()
            
            try:
                await asyncio.wait_for(process.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                print("Process didn't terminate, killing...")
                process.kill()
                await process.wait()
            
            raise
    
    # Fast command (completes)
    try:
        stdout, stderr, code = await run_with_timeout(
            ["echo", "Hello"],
            timeout=5.0
        )
        print(f"Fast command: {stdout.strip()}")
    except asyncio.TimeoutError:
        print("Fast command timed out")
    
    # Slow command (times out)
    try:
        stdout, stderr, code = await run_with_timeout(
            ["sleep", "10"],
            timeout=2.0
        )
        print("Slow command completed")
    except asyncio.TimeoutError:
        print("Slow command timed out (expected)")

asyncio.run(subprocess_timeout_demo())
```

### Graceful Termination

```python
import asyncio
import signal

async def graceful_termination():
    """Gracefully terminate subprocess"""
    
    process = await asyncio.create_subprocess_exec(
        "python", "-c",
        """
import signal
import time
import sys

def handler(signum, frame):
    print('Received signal, cleaning up...', flush=True)
    sys.exit(0)

signal.signal(signal.SIGTERM, handler)

print('Process started', flush=True)
while True:
    time.sleep(1)
        """,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    
    # Let it run
    await asyncio.sleep(2.0)
    
    # Graceful termination
    print("Sending SIGTERM...")
    process.terminate()
    
    try:
        await asyncio.wait_for(process.wait(), timeout=5.0)
        print(f"Process exited with code: {process.returncode}")
    except asyncio.TimeoutError:
        print("Process didn't respond to SIGTERM, killing...")
        process.kill()
        await process.wait()

asyncio.run(graceful_termination())
```

---

## Real-world: Command Runner

Complete command runner with output streaming and error handling.

```python
import asyncio
from typing import Optional, List, Tuple
from dataclasses import dataclass
import time

@dataclass
class CommandResult:
    """Result of command execution"""
    command: List[str]
    returncode: int
    stdout: str
    stderr: str
    duration: float
    timed_out: bool = False

class CommandRunner:
    """
    Async command runner with streaming output.
    
    Features:
    - Streaming output
    - Timeout support
    - Concurrent command execution
    - Error handling
    """
    
    def __init__(self, timeout: Optional[float] = None):
        self.timeout = timeout
    
    async def run(
        self,
        command: List[str],
        timeout: Optional[float] = None,
        stream_output: bool = False
    ) -> CommandResult:
        """
        Run command asynchronously.
        
        Args:
            command: Command and arguments
            timeout: Timeout in seconds (overrides default)
            stream_output: Print output as it arrives
        """
        timeout = timeout or self.timeout
        start_time = time.time()
        
        try:
            process = await asyncio.create_subprocess_exec(
                *command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            if stream_output:
                stdout, stderr = await self._stream_output(process, timeout)
            else:
                stdout, stderr = await self._capture_output(process, timeout)
            
            duration = time.time() - start_time
            
            return CommandResult(
                command=command,
                returncode=process.returncode,
                stdout=stdout,
                stderr=stderr,
                duration=duration
            )
        
        except asyncio.TimeoutError:
            duration = time.time() - start_time
            
            # Terminate process
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()
            
            return CommandResult(
                command=command,
                returncode=-1,
                stdout="",
                stderr="Command timed out",
                duration=duration,
                timed_out=True
            )
    
    async def _capture_output(
        self,
        process: asyncio.subprocess.Process,
        timeout: Optional[float]
    ) -> Tuple[str, str]:
        """Capture output without streaming"""
        if timeout:
            async with asyncio.timeout(timeout):
                stdout, stderr = await process.communicate()
        else:
            stdout, stderr = await process.communicate()
        
        return stdout.decode(), stderr.decode()
    
    async def _stream_output(
        self,
        process: asyncio.subprocess.Process,
        timeout: Optional[float]
    ) -> Tuple[str, str]:
        """Stream output as it arrives"""
        stdout_lines = []
        stderr_lines = []
        
        async def read_stream(stream, lines, prefix):
            while True:
                line = await stream.readline()
                if not line:
                    break
                
                decoded = line.decode().strip()
                lines.append(decoded)
                print(f"[{prefix}] {decoded}")
        
        async def read_both():
            await asyncio.gather(
                read_stream(process.stdout, stdout_lines, "OUT"),
                read_stream(process.stderr, stderr_lines, "ERR")
            )
            await process.wait()
        
        if timeout:
            async with asyncio.timeout(timeout):
                await read_both()
        else:
            await read_both()
        
        return "\n".join(stdout_lines), "\n".join(stderr_lines)
    
    async def run_many(
        self,
        commands: List[List[str]],
        max_concurrent: int = 5
    ) -> List[CommandResult]:
        """
        Run multiple commands concurrently.
        
        Args:
            commands: List of commands to run
            max_concurrent: Maximum concurrent processes
        """
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def run_with_limit(command):
            async with semaphore:
                return await self.run(command)
        
        return await asyncio.gather(*[
            run_with_limit(cmd) for cmd in commands
        ])

async def command_runner_demo():
    """Demonstrate command runner"""
    
    runner = CommandRunner(timeout=10.0)
    
    # Single command
    result = await runner.run(
        ["echo", "Hello, World!"],
        stream_output=True
    )
    print(f"\nResult: {result.stdout}")
    print(f"Duration: {result.duration:.2f}s")
    
    # Multiple commands
    commands = [
        ["echo", "Command 1"],
        ["echo", "Command 2"],
        ["echo", "Command 3"]
    ]
    
    results = await runner.run_many(commands, max_concurrent=2)
    
    for result in results:
        print(f"Command: {' '.join(result.command)}")
        print(f"Output: {result.stdout}")
        print(f"Duration: {result.duration:.2f}s")
        print()

asyncio.run(command_runner_demo())
```

---

## Real-world: Log Processor

Process log files from external tools in real-time.

```python
import asyncio
import re
from typing import Callable, Optional
from dataclasses import dataclass
from datetime import datetime

@dataclass
class LogEntry:
    """Parsed log entry"""
    timestamp: datetime
    level: str
    message: str
    raw: str

class LogProcessor:
    """
    Process logs from subprocess in real-time.
    
    Parses log entries and calls handlers based on log level.
    """
    
    def __init__(self):
        self.handlers = {
            "ERROR": [],
            "WARNING": [],
            "INFO": [],
            "DEBUG": []
        }
        self.log_pattern = re.compile(
            r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \[(\w+)\] (.+)'
        )
    
    def on_error(self, handler: Callable[[LogEntry], None]):
        """Register error handler"""
        self.handlers["ERROR"].append(handler)
    
    def on_warning(self, handler: Callable[[LogEntry], None]):
        """Register warning handler"""
        self.handlers["WARNING"].append(handler)
    
    def on_info(self, handler: Callable[[LogEntry], None]):
        """Register info handler"""
        self.handlers["INFO"].append(handler)
    
    def parse_log_line(self, line: str) -> Optional[LogEntry]:
        """Parse log line"""
        match = self.log_pattern.match(line)
        if not match:
            return None
        
        timestamp_str, level, message = match.groups()
        timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
        
        return LogEntry(
            timestamp=timestamp,
            level=level,
            message=message,
            raw=line
        )
    
    async def process_command(
        self,
        command: List[str],
        timeout: Optional[float] = None
    ):
        """Process command output as logs"""
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        async def process_stream(stream):
            while True:
                line = await stream.readline()
                if not line:
                    break
                
                decoded = line.decode().strip()
                entry = self.parse_log_line(decoded)
                
                if entry:
                    # Call handlers
                    for handler in self.handlers.get(entry.level, []):
                        handler(entry)
        
        try:
            if timeout:
                async with asyncio.timeout(timeout):
                    await asyncio.gather(
                        process_stream(process.stdout),
                        process_stream(process.stderr)
                    )
                    await process.wait()
            else:
                await asyncio.gather(
                    process_stream(process.stdout),
                    process_stream(process.stderr)
                )
                await process.wait()
        
        except asyncio.TimeoutError:
            process.terminate()
            await process.wait()
            raise

async def log_processor_demo():
    """Demonstrate log processor"""
    
    processor = LogProcessor()
    
    # Register handlers
    error_count = 0
    warning_count = 0
    
    def handle_error(entry: LogEntry):
        nonlocal error_count
        error_count += 1
        print(f"❌ ERROR: {entry.message}")
    
    def handle_warning(entry: LogEntry):
        nonlocal warning_count
        warning_count += 1
        print(f"⚠️  WARNING: {entry.message}")
    
    processor.on_error(handle_error)
    processor.on_warning(handle_warning)
    
    # Simulate log-producing command
    command = [
        "python", "-c",
        """
import time
import sys
from datetime import datetime

logs = [
    ('INFO', 'Application started'),
    ('INFO', 'Loading configuration'),
    ('WARNING', 'Config file not found, using defaults'),
    ('INFO', 'Connecting to database'),
    ('ERROR', 'Database connection failed'),
    ('INFO', 'Retrying connection'),
    ('INFO', 'Connected successfully'),
]

for level, message in logs:
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f'{timestamp} [{level}] {message}', flush=True)
    time.sleep(0.5)
        """
    ]
    
    await processor.process_command(command, timeout=10.0)
    
    print(f"\nSummary:")
    print(f"Errors: {error_count}")
    print(f"Warnings: {warning_count}")

asyncio.run(log_processor_demo())
```

---

## Common Patterns

### Pattern 1: Pipeline of Commands

```python
import asyncio

async def command_pipeline():
    """Chain commands together"""
    
    # Command 1: Generate data
    proc1 = await asyncio.create_subprocess_exec(
        "echo", "hello\nworld\ntest",
        stdout=asyncio.subprocess.PIPE
    )
    
    # Command 2: Process data (uppercase)
    proc2 = await asyncio.create_subprocess_exec(
        "python", "-c", "import sys; [print(line.upper(), end='') for line in sys.stdin]",
        stdin=proc1.stdout,
        stdout=asyncio.subprocess.PIPE
    )
    
    # Get final output
    stdout, _ = await proc2.communicate()
    print(f"Result:\n{stdout.decode()}")
```

### Pattern 2: Parallel Command Execution

```python
import asyncio

async def parallel_commands():
    """Run commands in parallel"""
    
    async def run_command(command):
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE
        )
        stdout, _ = await process.communicate()
        return stdout.decode()
    
    results = await asyncio.gather(
        run_command(["echo", "Command 1"]),
        run_command(["echo", "Command 2"]),
        run_command(["echo", "Command 3"])
    )
    
    for i, result in enumerate(results, 1):
        print(f"Result {i}: {result.strip()}")
```

---

## Best Practices

### ✅ DO: Use create_subprocess_exec over shell

```python
# Good: Safe from injection
await asyncio.create_subprocess_exec("ls", "-la")

# Bad: Vulnerable to injection
await asyncio.create_subprocess_shell("ls -la")
```

### ✅ DO: Always use timeouts

```python
# Good: Timeout prevents hanging
async with asyncio.timeout(30.0):
    await process.communicate()
```

### ✅ DO: Handle termination gracefully

```python
# Good: Graceful then forceful
process.terminate()
try:
    await asyncio.wait_for(process.wait(), timeout=5.0)
except asyncio.TimeoutError:
    process.kill()
```

### ❌ DON'T: Forget to close pipes

```python
# Bad: Pipes may leak
process = await asyncio.create_subprocess_exec(...)
# ... use process ...

# Good: Ensure cleanup
try:
    await process.communicate()
finally:
    if process.returncode is None:
        process.kill()
```

---

## Summary: Subprocess Mental Model

✅ **Use `create_subprocess_exec()`** - safe and non-blocking

✅ **Stream output for long-running processes** - don't wait for completion

✅ **Always use timeouts** - prevent hanging processes

✅ **Terminate gracefully** - SIGTERM then SIGKILL

✅ **Handle both stdout and stderr** - concurrent reading

✅ **Avoid shell=True** - security risk

✅ **Clean up processes** - ensure termination

---

## What's Next?

Subprocesses handle external programs. For network communication, we need async networking primitives.

In [Chapter 18: Networking](./18-networking.md), we'll cover:
- Streams API for TCP connections
- Server and client patterns
- Protocol and transport layers
- WebSocket integration
- HTTP client patterns with aiohttp
- Practical networking examples

Networking is essential for building distributed systems.

---

**Previous:** [← Chapter 16: Threads and Async](./16-threads-and-async.md)  
**Next:** [Chapter 18: Networking →](./18-networking.md)