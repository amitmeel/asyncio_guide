# Chapter 27: Real-World Project - Web Scraper with Rate Limiting

## Overview

This chapter builds a production-ready async web scraper that demonstrates:

- **Concurrent HTTP requests** with aiohttp
- **Rate limiting** to respect server limits
- **Retry logic** for failed requests
- **Circuit breakers** for failing domains
- **Connection pooling** for efficiency
- **Progress tracking** and metrics
- **Data extraction** with BeautifulSoup
- **Storage** with async database
- **Graceful shutdown** and cleanup

This project integrates concepts from throughout the guide into a real, working application.

## Project Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Web Scraper System                       │
│                                                              │
│  ┌────────────────────────────────────────────────────┐    │
│  │  URL Queue (Priority)                               │    │
│  │  [url1, url2, url3, ...]                           │    │
│  └────────────────────────────────────────────────────┘    │
│                           ↓                                  │
│  ┌────────────────────────────────────────────────────┐    │
│  │  Worker Pool (10 workers)                          │    │
│  │  Rate Limiter → Circuit Breaker → HTTP Client      │    │
│  └────────────────────────────────────────────────────┘    │
│                           ↓                                  │
│  ┌────────────────────────────────────────────────────┐    │
│  │  Parser Pipeline                                    │    │
│  │  HTML → Extract → Validate → Transform             │    │
│  └────────────────────────────────────────────────────┘    │
│                           ↓                                  │
│  ┌────────────────────────────────────────────────────┐    │
│  │  Storage (Database + Cache)                        │    │
│  │  Save results, track visited URLs                  │    │
│  └────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. Rate Limiter

```python
import asyncio
import time
from typing import Dict, Optional
from collections import deque
from urllib.parse import urlparse

class RateLimiter:
    """
    Token bucket rate limiter per domain.
    
    Ensures we don't overwhelm servers with requests.
    """
    
    def __init__(
        self,
        requests_per_second: float = 2.0,
        burst_size: int = 5
    ):
        self.requests_per_second = requests_per_second
        self.burst_size = burst_size
        
        # Per-domain buckets
        self.buckets: Dict[str, deque] = {}
        self.locks: Dict[str, asyncio.Lock] = {}
    
    def _get_domain(self, url: str) -> str:
        """Extract domain from URL"""
        return urlparse(url).netloc
    
    async def acquire(self, url: str):
        """
        Acquire permission to make request.
        
        Blocks until rate limit allows the request.
        """
        domain = self._get_domain(url)
        
        # Get or create lock for this domain
        if domain not in self.locks:
            self.locks[domain] = asyncio.Lock()
        
        async with self.locks[domain]:
            # Get or create bucket for this domain
            if domain not in self.buckets:
                self.buckets[domain] = deque(maxlen=self.burst_size)
            
            bucket = self.buckets[domain]
            now = time.time()
            
            # Remove old tokens
            while bucket and now - bucket[0] > 1.0 / self.requests_per_second:
                bucket.popleft()
            
            # Wait if bucket is full
            if len(bucket) >= self.burst_size:
                oldest = bucket[0]
                wait_time = (1.0 / self.requests_per_second) - (now - oldest)
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
                bucket.popleft()
            
            # Add token
            bucket.append(time.time())

### 2. HTTP Client with Resilience

```python
import aiohttp
import asyncio
from typing import Optional, Dict, Any
from dataclasses import dataclass
import logging

@dataclass
class ScraperConfig:
    """Scraper configuration"""
    max_concurrent: int = 10
    requests_per_second: float = 2.0
    timeout: float = 30.0
    max_retries: int = 3
    user_agent: str = "AsyncScraper/1.0"

class HTTPClient:
    """
    HTTP client with rate limiting and resilience.
    
    Combines rate limiting, retries, circuit breakers, and connection pooling.
    """
    
    def __init__(self, config: ScraperConfig):
        self.config = config
        self.rate_limiter = RateLimiter(
            requests_per_second=config.requests_per_second
        )
        self.session: Optional[aiohttp.ClientSession] = None
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        self.logger = logging.getLogger(__name__)
    
    async def start(self):
        """Initialize HTTP session"""
        connector = aiohttp.TCPConnector(
            limit=self.config.max_concurrent,
            limit_per_host=5,
            ttl_dns_cache=300
        )
        
        timeout = aiohttp.ClientTimeout(total=self.config.timeout)
        
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={'User-Agent': self.config.user_agent}
        )
        
        self.logger.info("HTTP client started")
    
    async def close(self):
        """Close HTTP session"""
        if self.session:
            await self.session.close()
            self.logger.info("HTTP client closed")
    
    def _get_circuit_breaker(self, url: str) -> CircuitBreaker:
        """Get or create circuit breaker for domain"""
        domain = urlparse(url).netloc
        
        if domain not in self.circuit_breakers:
            config = CircuitBreakerConfig(
                failure_threshold=5,
                timeout=60.0
            )
            self.circuit_breakers[domain] = CircuitBreaker(domain, config)
        
        return self.circuit_breakers[domain]
    
    async def fetch(self, url: str) -> Optional[str]:
        """
        Fetch URL with rate limiting and resilience.
        
        Returns HTML content or None on failure.
        """
        # Rate limiting
        await self.rate_limiter.acquire(url)
        
        # Circuit breaker
        breaker = self._get_circuit_breaker(url)
        
        # Retry logic
        for attempt in range(self.config.max_retries):
            try:
                async def make_request():
                    async with self.session.get(url) as response:
                        response.raise_for_status()
                        return await response.text()
                
                content = await breaker.call(make_request)
                self.logger.info(f"Fetched: {url}")
                return content
            
            except CircuitBreakerError:
                self.logger.warning(f"Circuit open for {url}")
                return None
            
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                self.logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                
                if attempt < self.config.max_retries - 1:
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
                else:
                    self.logger.error(f"All retries failed for {url}")
                    return None
        
        return None

### 3. HTML Parser

```python
from bs4 import BeautifulSoup
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from urllib.parse import urljoin, urlparse
import re

@dataclass
class ParsedPage:
    """Parsed page data"""
    url: str
    title: Optional[str]
    links: List[str]
    text: str
    metadata: Dict[str, Any]

class HTMLParser:
    """
    Parse HTML and extract data.
    
    Extracts links, text, and metadata from HTML pages.
    """
    
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.base_domain = urlparse(base_url).netloc
    
    def parse(self, url: str, html: str) -> ParsedPage:
        """Parse HTML content"""
        soup = BeautifulSoup(html, 'html.parser')
        
        # Extract title
        title = soup.title.string if soup.title else None
        
        # Extract links
        links = self._extract_links(soup, url)
        
        # Extract text
        text = self._extract_text(soup)
        
        # Extract metadata
        metadata = self._extract_metadata(soup)
        
        return ParsedPage(
            url=url,
            title=title,
            links=links,
            text=text,
            metadata=metadata
        )
    
    def _extract_links(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Extract and normalize links"""
        links = []
        
        for anchor in soup.find_all('a', href=True):
            href = anchor['href']
            
            # Resolve relative URLs
            absolute_url = urljoin(base_url, href)
            
            # Only include links from same domain
            if urlparse(absolute_url).netloc == self.base_domain:
                # Remove fragments
                absolute_url = absolute_url.split('#')[0]
                
                # Skip non-HTTP(S) URLs
                if absolute_url.startswith(('http://', 'https://')):
                    links.append(absolute_url)
        
        return list(set(links))  # Remove duplicates
    
    def _extract_text(self, soup: BeautifulSoup) -> str:
        """Extract visible text"""
        # Remove script and style elements
        for script in soup(['script', 'style']):
            script.decompose()
        
        # Get text
        text = soup.get_text()
        
        # Clean up whitespace
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = ' '.join(chunk for chunk in chunks if chunk)
        
        return text
    
    def _extract_metadata(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract metadata from page"""
        metadata = {}
        
        # Meta tags
        for meta in soup.find_all('meta'):
            name = meta.get('name') or meta.get('property')
            content = meta.get('content')
            if name and content:
                metadata[name] = content
        
        # Headings
        metadata['headings'] = [h.get_text().strip() for h in soup.find_all(['h1', 'h2', 'h3'])]
        
        return metadata

### 4. URL Queue with Priority

```python
import asyncio
from typing import Optional, Set
from dataclasses import dataclass, field
from enum import IntEnum
import heapq

class Priority(IntEnum):
    """URL priority levels"""
    HIGH = 1
    NORMAL = 2
    LOW = 3

@dataclass(order=True)
class QueuedURL:
    """URL with priority"""
    priority: int
    url: str = field(compare=False)
    depth: int = field(compare=False)

class URLQueue:
    """
    Priority queue for URLs to scrape.
    
    Tracks visited URLs and manages scraping frontier.
    """
    
    def __init__(self, max_depth: int = 3):
        self.max_depth = max_depth
        self.queue: List[QueuedURL] = []
        self.visited: Set[str] = set()
        self.in_progress: Set[str] = set()
        self.lock = asyncio.Lock()
    
    async def add(self, url: str, priority: Priority = Priority.NORMAL, depth: int = 0):
        """Add URL to queue"""
        async with self.lock:
            # Skip if already visited or in progress
            if url in self.visited or url in self.in_progress:
                return
            
            # Skip if too deep
            if depth > self.max_depth:
                return
            
            # Add to queue
            heapq.heappush(self.queue, QueuedURL(priority, url, depth))
    
    async def get(self) -> Optional[QueuedURL]:
        """Get next URL to scrape"""
        async with self.lock:
            if not self.queue:
                return None
            
            queued = heapq.heappop(self.queue)
            self.in_progress.add(queued.url)
            return queued
    
    async def mark_done(self, url: str):
        """Mark URL as completed"""
        async with self.lock:
            self.in_progress.discard(url)
            self.visited.add(url)
    
    async def mark_failed(self, url: str):
        """Mark URL as failed (can be retried)"""
        async with self.lock:
            self.in_progress.discard(url)
    
    async def size(self) -> int:
        """Get queue size"""
        async with self.lock:
            return len(self.queue)
    
    async def stats(self) -> Dict[str, int]:
        """Get queue statistics"""
        async with self.lock:
            return {
                'queued': len(self.queue),
                'in_progress': len(self.in_progress),
                'visited': len(self.visited)
            }

### 5. Storage Layer

```python
import asyncio
import aiosqlite
from typing import Optional, List
from datetime import datetime

class ScraperStorage:
    """
    Storage for scraped data.
    
    Stores pages and tracks scraping progress.
    """
    
    def __init__(self, db_path: str = "scraper.db"):
        self.db_path = db_path
        self.conn: Optional[aiosqlite.Connection] = None
    
    async def initialize(self):
        """Initialize database"""
        self.conn = await aiosqlite.connect(self.db_path)
        
        await self.conn.execute("""
            CREATE TABLE IF NOT EXISTS pages (
                url TEXT PRIMARY KEY,
                title TEXT,
                content TEXT,
                scraped_at TIMESTAMP,
                depth INTEGER
            )
        """)
        
        await self.conn.execute("""
            CREATE TABLE IF NOT EXISTS links (
                source_url TEXT,
                target_url TEXT,
                PRIMARY KEY (source_url, target_url)
            )
        """)
        
        await self.conn.commit()
    
    async def close(self):
        """Close database connection"""
        if self.conn:
            await self.conn.close()
    
    async def save_page(self, page: ParsedPage, depth: int):
        """Save scraped page"""
        await self.conn.execute(
            """
            INSERT OR REPLACE INTO pages (url, title, content, scraped_at, depth)
            VALUES (?, ?, ?, ?, ?)
            """,
            (page.url, page.title, page.text, datetime.now(), depth)
        )
        
        # Save links
        for link in page.links:
            await self.conn.execute(
                """
                INSERT OR IGNORE INTO links (source_url, target_url)
                VALUES (?, ?)
                """,
                (page.url, link)
            )
        
        await self.conn.commit()
    
    async def get_page(self, url: str) -> Optional[Dict]:
        """Get scraped page"""
        async with self.conn.execute(
            "SELECT * FROM pages WHERE url = ?",
            (url,)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return {
                    'url': row[0],
                    'title': row[1],
                    'content': row[2],
                    'scraped_at': row[3],
                    'depth': row[4]
                }
        return None
    
    async def get_stats(self) -> Dict[str, int]:
        """Get storage statistics"""
        async with self.conn.execute("SELECT COUNT(*) FROM pages") as cursor:
            pages_count = (await cursor.fetchone())[0]
        
        async with self.conn.execute("SELECT COUNT(*) FROM links") as cursor:
            links_count = (await cursor.fetchone())[0]
        
        return {
            'pages': pages_count,
            'links': links_count
        }

### 6. Worker Pool

```python
import asyncio
from typing import Optional
import logging

class ScraperWorker:
    """
    Worker that scrapes URLs from queue.
    
    Fetches, parses, and stores pages.
    """
    
    def __init__(
        self,
        worker_id: int,
        http_client: HTTPClient,
        parser: HTMLParser,
        url_queue: URLQueue,
        storage: ScraperStorage
    ):
        self.worker_id = worker_id
        self.http_client = http_client
        self.parser = parser
        self.url_queue = url_queue
        self.storage = storage
        self.logger = logging.getLogger(f"Worker-{worker_id}")
        self.pages_scraped = 0
    
    async def run(self):
        """Main worker loop"""
        self.logger.info("Worker started")
        
        while True:
            # Get next URL
            queued = await self.url_queue.get()
            if queued is None:
                await asyncio.sleep(1)
                continue
            
            try:
                await self._scrape_url(queued)
            except Exception as e:
                self.logger.error(f"Error scraping {queued.url}: {e}")
                await self.url_queue.mark_failed(queued.url)
    
    async def _scrape_url(self, queued: QueuedURL):
        """Scrape single URL"""
        self.logger.info(f"Scraping: {queued.url} (depth={queued.depth})")
        
        # Fetch HTML
        html = await self.http_client.fetch(queued.url)
        if not html:
            await self.url_queue.mark_failed(queued.url)
            return
        
        # Parse HTML
        page = self.parser.parse(queued.url, html)
        
        # Save to storage
        await self.storage.save_page(page, queued.depth)
        
        # Add discovered links to queue
        for link in page.links:
            await self.url_queue.add(
                link,
                priority=Priority.NORMAL,
                depth=queued.depth + 1
            )
        
        # Mark as done
        await self.url_queue.mark_done(queued.url)
        self.pages_scraped += 1
        
        self.logger.info(
            f"Completed: {queued.url} "
            f"(found {len(page.links)} links, total scraped: {self.pages_scraped})"
        )

### 7. Progress Monitor

```python
import asyncio
from typing import List
import time

class ProgressMonitor:
    """
    Monitor scraping progress.
    
    Displays statistics and progress updates.
    """
    
    def __init__(
        self,
        url_queue: URLQueue,
        storage: ScraperStorage,
        workers: List[ScraperWorker],
        update_interval: float = 5.0
    ):
        self.url_queue = url_queue
        self.storage = storage
        self.workers = workers
        self.update_interval = update_interval
        self.start_time = time.time()
    
    async def run(self):
        """Monitor loop"""
        while True:
            await asyncio.sleep(self.update_interval)
            await self._print_stats()
    
    async def _print_stats(self):
        """Print current statistics"""
        queue_stats = await self.url_queue.stats()
        storage_stats = await self.storage.get_stats()
        
        elapsed = time.time() - self.start_time
        pages_per_second = storage_stats['pages'] / elapsed if elapsed > 0 else 0
        
        worker_stats = sum(w.pages_scraped for w in self.workers)
        
        print("\n" + "="*60)
        print(f"Scraping Progress (elapsed: {elapsed:.1f}s)")
        print("="*60)
        print(f"Queue:")
        print(f"  Queued:      {queue_stats['queued']}")
        print(f"  In Progress: {queue_stats['in_progress']}")
        print(f"  Visited:     {queue_stats['visited']}")
        print(f"\nStorage:")
        print(f"  Pages:       {storage_stats['pages']}")
        print(f"  Links:       {storage_stats['links']}")
        print(f"\nPerformance:")
        print(f"  Pages/sec:   {pages_per_second:.2f}")
        print(f"  Worker total: {worker_stats}")
        print("="*60)

### 8. Main Scraper

```python
import asyncio
import logging
from typing import List

class WebScraper:
    """
    Main web scraper orchestrator.
    
    Coordinates all components and manages lifecycle.
    """
    
    def __init__(self, start_url: str, config: Optional[ScraperConfig] = None):
        self.start_url = start_url
        self.config = config or ScraperConfig()
        
        # Components
        self.http_client = HTTPClient(self.config)
        self.parser = HTMLParser(start_url)
        self.url_queue = URLQueue(max_depth=3)
        self.storage = ScraperStorage()
        self.workers: List[ScraperWorker] = []
        self.monitor: Optional[ProgressMonitor] = None
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    async def start(self):
        """Start scraper"""
        self.logger.info("Starting web scraper")
        
        # Initialize components
        await self.http_client.start()
        await self.storage.initialize()
        
        # Add start URL
        await self.url_queue.add(self.start_url, priority=Priority.HIGH, depth=0)
        
        # Create workers
        self.workers = [
            ScraperWorker(
                worker_id=i,
                http_client=self.http_client,
                parser=self.parser,
                url_queue=self.url_queue,
                storage=self.storage
            )
            for i in range(self.config.max_concurrent)
        ]
        
        # Create monitor
        self.monitor = ProgressMonitor(
            self.url_queue,
            self.storage,
            self.workers
        )
        
        self.logger.info(f"Started {len(self.workers)} workers")
    
    async def run(self, duration: Optional[float] = None):
        """Run scraper"""
        try:
            # Start all workers
            worker_tasks = [
                asyncio.create_task(worker.run())
                for worker in self.workers
            ]
            
            # Start monitor
            monitor_task = asyncio.create_task(self.monitor.run())
            
            # Run for specified duration or until interrupted
            if duration:
                await asyncio.sleep(duration)
            else:
                # Run until queue is empty
                while True:
                    stats = await self.url_queue.stats()
                    if stats['queued'] == 0 and stats['in_progress'] == 0:
                        break
                    await asyncio.sleep(5)
            
            # Cancel tasks
            for task in worker_tasks + [monitor_task]:
                task.cancel()
            
            await asyncio.gather(*worker_tasks, monitor_task, return_exceptions=True)
        
        except KeyboardInterrupt:
            self.logger.info("Interrupted by user")
        
        finally:
            await self.stop()
    
    async def stop(self):
        """Stop scraper"""
        self.logger.info("Stopping web scraper")
        
        # Print final stats
        if self.monitor:
            await self.monitor._print_stats()
        
        # Cleanup
        await self.http_client.close()
        await self.storage.close()
        
        self.logger.info("Web scraper stopped")

# Main entry point
async def main():
    """Run web scraper"""
    scraper = WebScraper(
        start_url="https://example.com",
        config=ScraperConfig(
            max_concurrent=10,
            requests_per_second=2.0,
            timeout=30.0
        )
    )
    
    await scraper.start()
    await scraper.run(duration=60.0)  # Run for 60 seconds

if __name__ == "__main__":
    asyncio.run(main())
```

## Key Patterns Demonstrated

This scraper demonstrates:

1. **Rate Limiting**: Token bucket per domain
2. **Circuit Breakers**: Stop calling failing domains
3. **Connection Pooling**: Reuse HTTP connections
4. **Worker Pool**: Fixed number of concurrent workers
5. **Priority Queue**: Process important URLs first
6. **Graceful Shutdown**: Clean up resources properly
7. **Progress Monitoring**: Track scraping progress
8. **Error Handling**: Retry failed requests
9. **Data Storage**: Persist scraped data
10. **Structured Concurrency**: Use TaskGroup for cleanup

## Extensions

Possible enhancements:

- **Robots.txt**: Respect robots.txt rules
- **Sitemap**: Parse and use sitemaps
- **JavaScript**: Render JavaScript with Playwright
- **Distributed**: Scale across multiple machines
- **Deduplication**: Content-based deduplication
- **Scheduling**: Periodic re-scraping
- **Export**: Export to various formats
- **Search**: Full-text search on scraped content

This project demonstrates how to build a production-ready async application using patterns from throughout the guide.