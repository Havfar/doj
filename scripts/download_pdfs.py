#!/usr/bin/env python3
import argparse
import asyncio
import json
import os
import random
import re
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urlparse

import aiohttp
import yarl
from http.cookies import SimpleCookie


DEFAULT_INPUT = "pdf-links.txt"
DEFAULT_OUT_DIR = "downloads"
DEFAULT_CONCURRENCY = 5  # Reduced from 32 to avoid detection
DEFAULT_TIMEOUT = 60
DEFAULT_RETRIES = 5  # More retries with longer backoff
CHUNK_SIZE = 1024 * 256
MIN_DELAY = 1.0  # Minimum delay between requests (seconds)
MAX_DELAY = 3.0  # Maximum delay between requests (seconds)
BLOCK_PAUSE = 1200  # 20 minutes pause when IP is blocked (403)
CORRUPT_THRESHOLD = 5  # Trigger cookie refresh after this many consecutive corrupt downloads
PDF_MAGIC = b'%PDF'  # PDF files start with this magic byte sequence

# Rotate through realistic User-Agent strings
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

def get_random_headers() -> Dict[str, str]:
    """Generate realistic browser-like headers."""
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/pdf",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
    }


def parse_cookies(cookie_string: str) -> Dict[str, str]:
    """Parse cookies from string format 'name=value; name2=value2' or 'name=value,name2=value2'."""
    cookies: Dict[str, str] = {}
    if not cookie_string:
        return cookies
    
    # Support both semicolon and comma separators
    for sep in [';', ',']:
        if sep in cookie_string:
            parts = cookie_string.split(sep)
            break
    else:
        parts = [cookie_string]
    
    for part in parts:
        part = part.strip()
        if '=' in part:
            name, value = part.split('=', 1)
            cookies[name.strip()] = value.strip()
    
    return cookies


def load_cookies_from_file(path: Path) -> Dict[str, str]:
    """Load cookies from a file (one 'name=value' per line or Netscape format)."""
    cookies: Dict[str, str] = {}
    if not path.exists():
        return cookies
    
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            # Simple format: name=value
            if '=' in line and '\t' not in line:
                name, value = line.split('=', 1)
                cookies[name.strip()] = value.strip()
            # Netscape format: domain\tflag\tpath\tsecure\texpiry\tname\tvalue
            elif '\t' in line:
                parts = line.split('\t')
                if len(parts) >= 7:
                    cookies[parts[5]] = parts[6]
    
    return cookies


def load_cookies_from_storage_state(path: Path) -> Dict[str, str]:
    """Load cookies from Playwright storage-state.json format."""
    cookies: Dict[str, str] = {}
    if not path.exists():
        return cookies

    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
            for cookie in data.get("cookies", []):
                cookies[cookie["name"]] = cookie["value"]
    except (json.JSONDecodeError, KeyError):
        pass

    return cookies


def find_corrupt_pdfs(out_dir: Path, verbose: bool = False) -> List[Path]:
    """Find downloaded files that are actually HTML, not PDF."""
    corrupt = []
    pdf_files = list(out_dir.glob("*.pdf"))
    total = len(pdf_files)

    for i, pdf_file in enumerate(pdf_files):
        if verbose and i % 10000 == 0:
            print(f"Checking files: {i}/{total}...", file=sys.stderr)
        try:
            with pdf_file.open("rb") as f:
                header = f.read(10)
                if not header.startswith(PDF_MAGIC):
                    corrupt.append(pdf_file)
        except (IOError, OSError):
            continue

    return corrupt


def clean_corrupt_pdfs(out_dir: Path, verbose: bool = False) -> int:
    """Delete corrupted PDF files (HTML masquerading as PDF)."""
    corrupt = find_corrupt_pdfs(out_dir, verbose=verbose)
    count = len(corrupt)

    if count > 0:
        print(f"Found {count} corrupt PDF files (HTML content)", file=sys.stderr)
        for pdf_file in corrupt:
            try:
                pdf_file.unlink()
                if verbose:
                    print(f"Deleted: {pdf_file.name}", file=sys.stderr)
            except (IOError, OSError) as e:
                print(f"Failed to delete {pdf_file}: {e}", file=sys.stderr)

    return count


async def refresh_cookies_with_playwright(
    storage_state_path: str,
    start_url: str = "https://www.justice.gov/epstein/doj-disclosures/data-set-9-files"
) -> Dict[str, str]:
    """Launch browser for user to solve captcha, then save cookies."""
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        print("Error: playwright not installed. Run: pip install playwright && playwright install", file=sys.stderr)
        return {}

    print("\n" + "=" * 60, file=sys.stderr)
    print("COOKIE REFRESH REQUIRED", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    print("A browser window will open. Please:", file=sys.stderr)
    print("  1. Solve any captcha if prompted", file=sys.stderr)
    print("  2. Accept age verification if prompted", file=sys.stderr)
    print("  3. Wait for the page to fully load", file=sys.stderr)
    print("  4. Press Enter in this terminal when done", file=sys.stderr)
    print("=" * 60 + "\n", file=sys.stderr)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        await page.goto(start_url)

        # Wait for user to solve captcha
        await asyncio.get_event_loop().run_in_executor(None, input)

        # Save storage state
        await context.storage_state(path=storage_state_path)

        # Extract cookies
        cookies_list = await context.cookies()
        cookies = {c["name"]: c["value"] for c in cookies_list}

        await browser.close()

    print(f"Saved {len(cookies)} cookies to {storage_state_path}", file=sys.stderr)
    return cookies


def create_cookie_jar(cookies: Dict[str, str], domains: List[str]) -> aiohttp.CookieJar:
    """Create an aiohttp CookieJar with pre-set cookies for given domains."""
    jar = aiohttp.CookieJar(unsafe=True)  # unsafe=True allows cookies for IP addresses
    
    for domain in domains:
        for name, value in cookies.items():
            # Create a morsel for each cookie
            jar.update_cookies({name: value}, response_url=yarl.URL(f"https://{domain}/"))
    
    return jar


def read_urls(path: Path, deduplicate: bool = True) -> List[str]:
    """Read URLs from file, optionally deduplicating while preserving order."""
    urls: List[str] = []
    seen: Set[str] = set()
    duplicates = 0
    with path.open("r", encoding="utf-8") as handle:
        for raw in handle:
            line = raw.strip()
            if not line:
                continue
            if deduplicate:
                if line in seen:
                    duplicates += 1
                    continue
                seen.add(line)
            urls.append(line)
    if deduplicate and duplicates > 0:
        print(f"Removed {duplicates} duplicate URLs from input", file=sys.stderr)
    return urls


def sanitize_filename(name: str) -> str:
    name = name.strip()
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", name)
    return name or "downloaded.pdf"


def filename_from_url(url: str) -> str:
    parsed = urlparse(url)
    base = os.path.basename(parsed.path)
    if not base:
        return "downloaded.pdf"
    base = sanitize_filename(base)
    if "." not in base:
        base += ".pdf"
    return base


def unique_name(name: str, out_dir: Path, reserved: Set[str]) -> str:
    if name not in reserved and not (out_dir / name).exists():
        reserved.add(name)
        return name
    stem, suffix = os.path.splitext(name)
    idx = 2
    while True:
        candidate = f"{stem}-{idx}{suffix}"
        if candidate not in reserved and not (out_dir / candidate).exists():
            reserved.add(candidate)
            return candidate
        idx += 1


async def head_content_length(
    session: aiohttp.ClientSession, url: str, timeout: aiohttp.ClientTimeout
) -> Optional[int]:
    try:
        headers = get_random_headers()
        async with session.head(url, timeout=timeout, allow_redirects=True, headers=headers) as resp:
            if resp.status >= 400:
                return None
            value = resp.headers.get("Content-Length")
            if value is None:
                return None
            return int(value)
    except Exception:
        return None


def parse_retry_after(value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


class GlobalBlockState:
    """Tracks global IP block state across all workers."""
    def __init__(self, block_pause: float):
        self.block_pause = block_pause
        self.blocked_until: float = 0
        self.lock = asyncio.Lock()
    
    async def check_and_wait(self) -> None:
        """If we're in a block period, wait until it expires."""
        while True:
            async with self.lock:
                now = time.monotonic()
                if now >= self.blocked_until:
                    return
                wait_time = self.blocked_until - now
            
            mins = int(wait_time // 60)
            secs = int(wait_time % 60)
            print(f"\r[BLOCKED] IP blocked, waiting {mins}m {secs}s...   ", end="", file=sys.stderr, flush=True)
            await asyncio.sleep(min(10, wait_time))  # Check every 10s
    
    async def trigger_block(self) -> None:
        """Called when a 403 is detected - triggers global pause."""
        async with self.lock:
            new_blocked_until = time.monotonic() + self.block_pause
            if new_blocked_until > self.blocked_until:
                self.blocked_until = new_blocked_until
                mins = int(self.block_pause // 60)
                print(f"\n[403 DETECTED] IP appears blocked. Pausing ALL requests for {mins} minutes...", file=sys.stderr)


async def download_one(
    session: aiohttp.ClientSession,
    url: str,
    dest: Path,
    timeout: aiohttp.ClientTimeout,
    retries: int,
    resume: bool,
    domain_locks: Dict[str, asyncio.Lock],
    domain_last_request: Dict[str, float],
    min_delay: float,
    max_delay: float,
    block_state: GlobalBlockState,
    validate_pdf: bool = True,
) -> Tuple[str, Optional[str]]:
    if resume and dest.exists():
        length = await head_content_length(session, url, timeout)
        if length is not None and dest.stat().st_size == length:
            return ("skipped", None)

    # Extract domain for per-domain rate limiting
    parsed = urlparse(url)
    domain = parsed.netloc

    attempt = 0
    while True:
        try:
            # Check if we're in a global block period (IP blocked)
            await block_state.check_and_wait()

            # Per-domain rate limiting to avoid hammering single servers
            if domain not in domain_locks:
                domain_locks[domain] = asyncio.Lock()

            async with domain_locks[domain]:
                # Enforce minimum delay between requests to same domain
                now = time.monotonic()
                last = domain_last_request.get(domain, 0)
                elapsed = now - last
                min_wait = random.uniform(min_delay, max_delay)
                if elapsed < min_wait:
                    await asyncio.sleep(min_wait - elapsed)
                domain_last_request[domain] = time.monotonic()

            # Use random browser-like headers
            headers = get_random_headers()

            async with session.get(url, timeout=timeout, headers=headers) as resp:
                # Handle 403 - trigger global pause since IP is blocked
                if resp.status == 403:
                    await block_state.trigger_block()
                    raise aiohttp.ClientResponseError(
                        request_info=resp.request_info,
                        history=resp.history,
                        status=resp.status,
                        message="IP blocked (403)",
                        headers=resp.headers,
                    )
                # Other retryable errors
                if resp.status in (429, 500, 502, 503, 504):
                    raise aiohttp.ClientResponseError(
                        request_info=resp.request_info,
                        history=resp.history,
                        status=resp.status,
                        message=f"retryable ({resp.status})",
                        headers=resp.headers,
                    )
                resp.raise_for_status()

                # Read content into memory first for validation
                content = await resp.read()

                # Validate PDF magic bytes if enabled
                if validate_pdf:
                    if not content.startswith(PDF_MAGIC):
                        # Check if it's HTML (common when redirected to login/captcha page)
                        if content.startswith(b'<!DOCTYPE') or content.startswith(b'<html') or content.startswith(b'\n<!DOCTYPE'):
                            return ("corrupt", "Received HTML instead of PDF (likely captcha/auth page)")
                        return ("corrupt", f"Invalid PDF: missing magic bytes (got: {content[:20]!r})")

                # Write validated content to file
                tmp_path = dest.with_suffix(dest.suffix + ".part")
                with tmp_path.open("wb") as handle:
                    handle.write(content)
                tmp_path.replace(dest)
                return ("downloaded", None)
        except Exception as exc:
            if attempt >= retries:
                return ("failed", f"{type(exc).__name__}: {exc}")
            attempt += 1

            # For 403, the global pause handles the wait - just do a short delay before retry
            is_403 = isinstance(exc, aiohttp.ClientResponseError) and getattr(exc, "status", 0) == 403
            if is_403:
                # Global pause already triggered, just wait a moment before retry
                await asyncio.sleep(random.uniform(1, 3))
                continue

            # Calculate backoff for other errors
            retry_after = None
            if isinstance(exc, aiohttp.ClientResponseError):
                retry_after = parse_retry_after(
                    getattr(exc, "headers", {}).get("Retry-After")
                )

            if retry_after is None:
                retry_after = min(60, 5 * (2 ** (attempt - 1))) * random.uniform(0.8, 1.2)

            await asyncio.sleep(retry_after)


async def run_downloads(
    urls: Iterable[str],
    out_dir: Path,
    concurrency: int,
    timeout_seconds: int,
    retries: int,
    resume: bool,
    show_progress: bool,
    verbose: bool,
    min_delay: float = MIN_DELAY,
    max_delay: float = MAX_DELAY,
    block_pause: float = BLOCK_PAUSE,
    cookies: Optional[Dict[str, str]] = None,
    fast_skip: bool = False,
    validate_pdf: bool = True,
    storage_state_path: Optional[str] = None,
) -> Tuple[int, int, int, int, List[str]]:
    timeout = aiohttp.ClientTimeout(total=timeout_seconds)
    connector = aiohttp.TCPConnector(limit=concurrency, enable_cleanup_closed=True)

    # Create cookie jar - unsafe=True allows cookies for IP addresses
    cookie_jar = aiohttp.CookieJar(unsafe=True)
    reserved: Set[str] = set()
    reserved_lock = asyncio.Lock()
    counter_lock = asyncio.Lock()
    done_event = asyncio.Event()

    # Per-domain rate limiting
    domain_locks: Dict[str, asyncio.Lock] = {}
    domain_last_request: Dict[str, float] = defaultdict(float)

    # Global block state for IP-level blocks
    block_state = GlobalBlockState(block_pause)

    # Corrupt download tracking for auto-refresh
    corrupt_streak = [0]  # Use list to allow modification in nested function
    needs_refresh = [False]

    queue: asyncio.Queue[str] = asyncio.Queue()
    url_list = list(urls)
    for url in url_list:
        queue.put_nowait(url)

    total = len(url_list)

    # Pre-populate cookies for all unique domains
    unique_domains: Set[str] = set()
    for url in url_list:
        parsed = urlparse(url)
        if parsed.netloc:
            unique_domains.add(parsed.netloc)

    if cookies:
        for domain in unique_domains:
            for name, value in cookies.items():
                cookie_jar.update_cookies({name: value}, response_url=yarl.URL(f"https://{domain}/"))

        print(f"Loaded {len(cookies)} cookie(s) for {len(unique_domains)} domain(s)", file=sys.stderr)

    counters = {"downloaded": 0, "skipped": 0, "failed": 0, "corrupt": 0}
    failed_urls: List[str] = []

    async def progress_loop() -> None:
        spinner = ["|", "/", "-", "\\"]
        idx = 0
        last_len = 0
        start = time.monotonic()
        while not done_event.is_set():
            async with counter_lock:
                downloaded = counters["downloaded"]
                skipped = counters["skipped"]
                failed = counters["failed"]
                corrupt = counters["corrupt"]
            done = downloaded + skipped + failed + corrupt
            elapsed = max(0.001, time.monotonic() - start)
            rate = done / elapsed
            width = 30
            filled = int(width * (done / total)) if total else width
            bar = "=" * filled + "." * (width - filled)
            line = (
                f"{spinner[idx % len(spinner)]} [{bar}] {done}/{total} "
                f"(ok {downloaded} skip {skipped} fail {failed} corrupt {corrupt}) "
                f"{rate:.1f}/s"
            )
            idx += 1
            padding = " " * max(0, last_len - len(line))
            print(f"\r{line}{padding}", end="", file=sys.stderr, flush=True)
            last_len = len(line)
            await asyncio.sleep(0.2)
        print("\r" + (" " * last_len) + "\r", end="", file=sys.stderr, flush=True)

    async with aiohttp.ClientSession(connector=connector, cookie_jar=cookie_jar) as session:
        async def worker() -> None:
            nonlocal cookies
            while True:
                # Check if cookie refresh was triggered
                if needs_refresh[0] and storage_state_path:
                    async with counter_lock:
                        if needs_refresh[0]:  # Double-check under lock
                            needs_refresh[0] = False
                            print("\n[REFRESH] Too many corrupt downloads - refreshing cookies...", file=sys.stderr)
                            new_cookies = await refresh_cookies_with_playwright(storage_state_path)
                            if new_cookies:
                                cookies = new_cookies
                                # Update cookie jar
                                for domain in unique_domains:
                                    for name, value in new_cookies.items():
                                        cookie_jar.update_cookies({name: value}, response_url=yarl.URL(f"https://{domain}/"))
                                corrupt_streak[0] = 0

                try:
                    url = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return

                # Check for existing file BEFORE generating unique name
                base_filename = filename_from_url(url)
                base_dest = out_dir / base_filename

                if resume and base_dest.exists():
                    # File exists - check if we should skip
                    should_skip = False
                    if fast_skip:
                        # Fast mode: skip without verifying size
                        should_skip = True
                    else:
                        # Also validate that existing file is actually a PDF
                        if validate_pdf:
                            try:
                                with base_dest.open("rb") as f:
                                    if not f.read(4).startswith(PDF_MAGIC):
                                        should_skip = False  # Re-download corrupt file
                                    else:
                                        # Valid PDF, check size
                                        length = await head_content_length(session, url, timeout)
                                        if length is not None and base_dest.stat().st_size == length:
                                            should_skip = True
                            except (IOError, OSError):
                                should_skip = False
                        else:
                            length = await head_content_length(session, url, timeout)
                            if length is not None and base_dest.stat().st_size == length:
                                should_skip = True

                    if should_skip:
                        async with counter_lock:
                            counters["skipped"] += 1
                        if verbose:
                            print(f"SKIP   {url}")
                        queue.task_done()
                        continue

                # File doesn't exist or is incomplete - get unique name and download
                async with reserved_lock:
                    filename = unique_name(base_filename, out_dir, reserved)
                dest = out_dir / filename
                status, error = await download_one(
                    session=session,
                    url=url,
                    dest=dest,
                    timeout=timeout,
                    retries=retries,
                    resume=False,
                    domain_locks=domain_locks,
                    domain_last_request=domain_last_request,
                    min_delay=min_delay,
                    max_delay=max_delay,
                    block_state=block_state,
                    validate_pdf=validate_pdf,
                )
                async with counter_lock:
                    if status == "downloaded":
                        counters["downloaded"] += 1
                        corrupt_streak[0] = 0  # Reset on successful download
                    elif status == "skipped":
                        counters["skipped"] += 1
                    elif status == "corrupt":
                        counters["corrupt"] += 1
                        failed_urls.append(url)
                        corrupt_streak[0] += 1
                        # Trigger refresh if too many consecutive corrupt downloads
                        if corrupt_streak[0] >= CORRUPT_THRESHOLD and storage_state_path:
                            needs_refresh[0] = True
                    else:
                        counters["failed"] += 1
                        failed_urls.append(url)
                if verbose:
                    if status == "downloaded":
                        print(f"OK     {url}")
                    elif status == "skipped":
                        print(f"SKIP   {url}")
                    elif status == "corrupt":
                        print(f"CORRUPT {url} ({error})")
                    else:
                        print(f"FAIL   {url} ({error})")
                queue.task_done()

        progress_task = None
        if show_progress and total:
            progress_task = asyncio.create_task(progress_loop())
        workers = [asyncio.create_task(worker()) for _ in range(concurrency)]
        await queue.join()
        for task in workers:
            task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        done_event.set()
        if progress_task:
            await progress_task

    downloaded = counters["downloaded"]
    skipped = counters["skipped"]
    failed = counters["failed"]
    corrupt = counters["corrupt"]

    return downloaded, skipped, failed, corrupt, failed_urls


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Download PDFs from a URL list with anti-blocking measures.",
        epilog="""
Anti-blocking features:
  - Cookie support for age gates and authentication (--cookies, --cookie-file)
  - Randomized browser User-Agent headers
  - Per-domain rate limiting (--min-delay, --max-delay)
  - GLOBAL PAUSE on 403: When any request gets 403, ALL workers pause
    for --block-pause seconds (default 20 min) since your IP is blocked
  - Exponential backoff on 429/5xx errors
  - Low default concurrency to avoid detection

Getting cookies for age verification:
  1. Open the site in your browser (incognito)
  2. Accept the age gate / click "I'm over 18"
  3. Open DevTools (F12) -> Application -> Cookies
  4. Find the cookie (e.g., "age_verified=1" or "over18=true")
  5. Use: --cookies "cookie_name=cookie_value"

If you're still getting blocked, try:
  - Lowering --concurrency to 1-2
  - Increasing --min-delay and --max-delay
  - Increasing --block-pause if blocks last longer than 20 min
  - Using a proxy or VPN
  - Running at different times of day
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--input", default=DEFAULT_INPUT, help="Path to URL list file.")
    parser.add_argument("--out-dir", default=DEFAULT_OUT_DIR, help="Output folder.")
    parser.add_argument(
        "--concurrency",
        type=int,
        default=DEFAULT_CONCURRENCY,
        help=f"Number of concurrent downloads (default: {DEFAULT_CONCURRENCY}, lower = safer).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help="Per-request timeout in seconds.",
    )
    parser.add_argument(
        "--retries", type=int, default=DEFAULT_RETRIES, help="Retry attempts."
    )
    parser.add_argument(
        "--min-delay",
        type=float,
        default=MIN_DELAY,
        help=f"Minimum delay between requests to same domain (default: {MIN_DELAY}s).",
    )
    parser.add_argument(
        "--max-delay",
        type=float,
        default=MAX_DELAY,
        help=f"Maximum delay between requests to same domain (default: {MAX_DELAY}s).",
    )
    parser.add_argument(
        "--block-pause",
        type=int,
        default=BLOCK_PAUSE,
        help=f"Seconds to pause ALL requests when 403 detected (default: {BLOCK_PAUSE}s = 20min).",
    )
    parser.add_argument(
        "--cookies",
        type=str,
        default="",
        help="Cookies to send with requests: 'name=value; name2=value2' (for age gates, etc.)",
    )
    parser.add_argument(
        "--cookie-file",
        type=str,
        default="",
        help="Path to cookie file (one 'name=value' per line, or Netscape format).",
    )
    parser.add_argument(
        "--storage-state",
        type=str,
        default="",
        help="Path to Playwright storage-state.json file (loads cookies from browser session).",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Disable resume/skip behavior.",
    )
    parser.add_argument(
        "--fast-skip",
        action="store_true",
        help="Skip existing files without verifying size (faster, assumes previous downloads are complete).",
    )
    parser.add_argument(
        "--failed-file",
        default="failed.txt",
        help="Write failed URLs to this file (empty to disable).",
    )
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable progress animation.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print per-file status lines.",
    )
    parser.add_argument(
        "--no-dedupe",
        action="store_true",
        help="Disable URL deduplication (download duplicates as separate files).",
    )
    parser.add_argument(
        "--no-validate",
        action="store_true",
        help="Disable PDF validation (save all responses without checking magic bytes).",
    )
    parser.add_argument(
        "--clean-corrupt",
        action="store_true",
        help="Delete corrupted PDF files (HTML masquerading as PDF) before starting downloads.",
    )
    parser.add_argument(
        "--refresh-cookies",
        action="store_true",
        help="Launch browser to refresh cookies before starting downloads.",
    )
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()

    input_path = Path(args.input)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if not input_path.exists():
        print(f"Input file not found: {input_path}")
        return

    # Clean corrupt PDFs if requested
    if args.clean_corrupt:
        print("Scanning for corrupt PDF files...", file=sys.stderr)
        deleted = clean_corrupt_pdfs(out_dir, verbose=args.verbose)
        if deleted > 0:
            print(f"Deleted {deleted} corrupt files", file=sys.stderr)
        else:
            print("No corrupt files found", file=sys.stderr)

    urls = read_urls(input_path, deduplicate=not args.no_dedupe)
    if not urls:
        print("No URLs found.")
        return

    # Load cookies from command line, file, and/or Playwright storage state
    cookies: Dict[str, str] = {}
    storage_state_path = args.storage_state if args.storage_state else None

    # Refresh cookies via browser if requested
    if args.refresh_cookies:
        if not storage_state_path:
            storage_state_path = "storage-state.json"
        new_cookies = asyncio.run(refresh_cookies_with_playwright(storage_state_path))
        if new_cookies:
            cookies.update(new_cookies)

    if storage_state_path:
        storage_state_file = Path(storage_state_path)
        if storage_state_file.exists():
            cookies.update(load_cookies_from_storage_state(storage_state_file))
            print(f"Loaded cookies from Playwright storage state: {storage_state_path}")
        elif not args.refresh_cookies:
            print(f"Warning: Storage state file not found: {storage_state_path}")

    if args.cookie_file:
        cookie_file_path = Path(args.cookie_file)
        if cookie_file_path.exists():
            cookies.update(load_cookies_from_file(cookie_file_path))
        else:
            print(f"Warning: Cookie file not found: {cookie_file_path}")
    if args.cookies:
        cookies.update(parse_cookies(args.cookies))

    show_progress = not args.no_progress and not args.verbose
    downloaded, skipped, failed, corrupt, failed_urls = asyncio.run(
        run_downloads(
            urls=urls,
            out_dir=out_dir,
            concurrency=max(1, args.concurrency),
            timeout_seconds=max(1, args.timeout),
            retries=max(0, args.retries),
            resume=not args.no_resume,
            show_progress=show_progress,
            verbose=args.verbose,
            min_delay=max(0.0, args.min_delay),
            max_delay=max(args.min_delay, args.max_delay),
            block_pause=max(60, args.block_pause),
            cookies=cookies if cookies else None,
            fast_skip=args.fast_skip,
            validate_pdf=not args.no_validate,
            storage_state_path=storage_state_path,
        )
    )

    if args.failed_file:
        failed_path = Path(args.failed_file)
        if failed_urls:
            failed_path.write_text("\n".join(failed_urls) + "\n", encoding="utf-8")
        elif failed_path.exists():
            failed_path.unlink()

    total = downloaded + skipped + failed + corrupt
    print(
        f"Done. Total: {total}, Downloaded: {downloaded}, "
        f"Skipped: {skipped}, Failed: {failed}, Corrupt: {corrupt}"
    )


if __name__ == "__main__":
    main()
