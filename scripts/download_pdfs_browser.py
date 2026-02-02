#!/usr/bin/env python3
"""
Browser-based PDF downloader using Playwright.
Uses real browser tabs for downloads to avoid bot detection.
"""

import argparse
import asyncio
import os
import random
import re
import signal
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

try:
    from playwright.async_api import async_playwright, Page, BrowserContext
except ImportError:
    print("Playwright not installed. Run: pip install playwright && playwright install chromium")
    sys.exit(1)

# Configuration
DEFAULT_INPUT = "pdf-links.txt"
DEFAULT_OUT_DIR = "downloads"
DEFAULT_PROGRESS_FILE = "download-browser-progress.txt"
DEFAULT_FAILED_FILE = "failed-browser.txt"
DEFAULT_STORAGE_STATE = "storage-state.json"

NUM_TABS = 5  # Number of concurrent browser tabs
MIN_DELAY = 1.0
MAX_DELAY = 3.0
TIMEOUT_MS = 60000
PROGRESS_SAVE_INTERVAL = 50
BLOCK_PAUSE = 600  # 10 minutes

# Global flags
shutdown_requested = False
verbose = False


def read_urls(path: Path) -> list[str]:
    """Read URLs from file, deduplicating."""
    seen = set()
    urls = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            url = line.strip()
            if url and url not in seen:
                seen.add(url)
                urls.append(url)
    return urls


def load_progress(progress_file: Path) -> set[str]:
    """Load completed URLs from progress file."""
    if progress_file.exists():
        with progress_file.open("r", encoding="utf-8") as f:
            return set(line.strip() for line in f if line.strip())
    return set()


def save_progress(progress_file: Path, completed_urls: set[str]):
    """Save completed URLs to progress file."""
    with progress_file.open("w", encoding="utf-8") as f:
        f.write("\n".join(sorted(completed_urls)) + "\n")


def sanitize_filename(name: str) -> str:
    """Sanitize filename for filesystem."""
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", name.strip())
    return name or "downloaded.pdf"


def filename_from_url(url: str) -> str:
    """Extract filename from URL."""
    base = os.path.basename(urlparse(url).path)
    if not base:
        return "downloaded.pdf"
    base = sanitize_filename(base)
    if "." not in base:
        base += ".pdf"
    return base


def unique_name(name: str, out_dir: Path, reserved: set[str]) -> str:
    """Generate unique filename."""
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


class DownloadStats:
    """Thread-safe download statistics."""

    def __init__(self):
        self.downloaded = 0
        self.skipped = 0
        self.failed = 0
        self.blocked = 0
        self.lock = asyncio.Lock()
        self.start_time = time.monotonic()


class BlockState:
    """Global block state for pausing all workers."""

    def __init__(self, pause_duration: int = BLOCK_PAUSE):
        self.pause_duration = pause_duration
        self.blocked_until = 0.0
        self.lock = asyncio.Lock()

    async def check_and_wait(self) -> bool:
        """Wait if blocked. Returns True if shutdown requested during wait."""
        while not shutdown_requested:
            async with self.lock:
                now = time.monotonic()
                if now >= self.blocked_until:
                    return False
                wait_time = self.blocked_until - now
            print(f"\r[WAITING] {int(wait_time)}s remaining...   ", end="", flush=True)
            await asyncio.sleep(min(5, wait_time))
        return True

    async def trigger_block(self, reason: str = ""):
        """Trigger a global block."""
        async with self.lock:
            now = time.monotonic()
            if now >= self.blocked_until:
                self.blocked_until = now + self.pause_duration
                print(f"\n[BLOCKED] {reason} - pausing {self.pause_duration}s...")


async def handle_verification(page: Page) -> bool:
    """Handle CAPTCHA and age verification. Returns True if handled."""
    handled = False

    try:
        # Check for CAPTCHA
        captcha_button = page.get_by_role("button", name="I am not a robot")
        if await captcha_button.count() > 0:
            print("\n[CAPTCHA] Detected - clicking...")
            await captcha_button.click()
            await page.wait_for_load_state("networkidle", timeout=10000)
            handled = True

            # Check if manual intervention needed
            if await captcha_button.count() > 0:
                print("[CAPTCHA] Manual intervention required. Solve and press Enter...")
                await asyncio.get_event_loop().run_in_executor(None, input)
    except Exception:
        pass

    try:
        # Check for age verification
        age_button = page.get_by_role("button", name="I am over 18")
        if await age_button.count() > 0:
            print("\n[AGE] Clicking age verification...")
            await age_button.click()
            await page.wait_for_load_state("networkidle", timeout=10000)
            handled = True
    except Exception:
        pass

    return handled


async def download_pdf_with_tab(
    page: Page,
    url: str,
    dest: Path,
    block_state: BlockState,
) -> tuple[str, str | None]:
    """Download a single PDF using a browser tab."""
    global shutdown_requested

    # Check for global block
    if await block_state.check_and_wait():
        return ("shutdown", None)

    # Random delay to look more human
    delay = random.uniform(MIN_DELAY, MAX_DELAY)
    await asyncio.sleep(delay)

    if shutdown_requested:
        return ("shutdown", None)

    try:
        # Navigate to PDF URL
        response = await page.goto(url, wait_until="load", timeout=TIMEOUT_MS)

        if not response:
            return ("failed", "No response")

        # Check for verification pages
        current_url = page.url
        if "age-verify" in current_url or "queue" in current_url.lower():
            if await handle_verification(page):
                # Try navigating again after verification
                response = await page.goto(url, wait_until="load", timeout=TIMEOUT_MS)

        status = response.status if response else 0

        # Check page content for blocks/errors
        content = await page.content()

        # Check for Access Denied / rate limiting
        if "Access Denied" in content or "SERVE_404" in content:
            await block_state.trigger_block("Access Denied by CDN")
            return ("blocked", "Access Denied")

        if status == 403:
            await block_state.trigger_block("403 Forbidden")
            return ("blocked", "403 Forbidden")

        if status == 404:
            # Check if it's a real 404 or rate limiting
            if "Access Denied" in content or len(content) < 1000:
                await block_state.trigger_block("404 rate limit")
                return ("blocked", "404 rate limit")
            return ("failed", "404 Not Found")

        if status != 200:
            return ("failed", f"HTTP {status}")

        # Try to get PDF content via JavaScript
        # This works because the browser has already loaded the PDF
        pdf_data = await page.evaluate("""
            async () => {
                try {
                    const response = await fetch(window.location.href);
                    const blob = await response.blob();
                    const buffer = await blob.arrayBuffer();
                    const bytes = new Uint8Array(buffer);
                    return Array.from(bytes);
                } catch (e) {
                    return null;
                }
            }
        """)

        if not pdf_data:
            # Fallback: check if we're on an error page
            if "captcha" in content.lower() or "robot" in content.lower():
                await handle_verification(page)
                return ("retry", "CAPTCHA page")
            return ("failed", "Could not fetch PDF data")

        # Convert to bytes and validate
        content_bytes = bytes(pdf_data)

        if not content_bytes.startswith(b"%PDF"):
            if b"Access Denied" in content_bytes:
                await block_state.trigger_block("Access Denied in PDF response")
                return ("blocked", "Access Denied")
            return ("failed", f"Not a PDF (got {len(content_bytes)} bytes)")

        # Write to temp file, then rename
        tmp_path = dest.with_suffix(".pdf.part")
        tmp_path.write_bytes(content_bytes)
        tmp_path.rename(dest)

        return ("downloaded", None)

    except asyncio.TimeoutError:
        return ("failed", "Timeout")
    except Exception as e:
        error_msg = str(e)[:100]
        if "net::ERR" in error_msg:
            await block_state.trigger_block(f"Network error: {error_msg}")
            return ("blocked", error_msg)
        return ("failed", error_msg)


async def worker(
    worker_id: int,
    page: Page,
    queue: asyncio.Queue,
    out_dir: Path,
    stats: DownloadStats,
    block_state: BlockState,
    completed_urls: set[str],
    reserved_names: set[str],
    name_lock: asyncio.Lock,
    progress_file: Path,
    failed_urls: list[str],
):
    """Worker coroutine that processes URLs from the queue."""
    global shutdown_requested, verbose

    while not shutdown_requested:
        try:
            url = queue.get_nowait()
        except asyncio.QueueEmpty:
            return

        base_name = filename_from_url(url)
        base_dest = out_dir / base_name

        # Skip if already downloaded
        if base_dest.exists():
            async with stats.lock:
                stats.skipped += 1
            queue.task_done()
            continue

        # Get unique filename
        async with name_lock:
            filename = unique_name(base_name, out_dir, reserved_names)
        dest = out_dir / filename

        # Download with retries
        max_retries = 3
        attempt = 0
        while not shutdown_requested:
            status, error = await download_pdf_with_tab(
                page, url, dest, block_state
            )

            if verbose and status not in ("downloaded", "shutdown"):
                print(f"\n[{status.upper()}] {filename}: {error}")

            if status == "shutdown":
                queue.task_done()
                return

            if status == "downloaded":
                async with stats.lock:
                    stats.downloaded += 1
                    completed_urls.add(url)

                    # Periodic save
                    if stats.downloaded % PROGRESS_SAVE_INTERVAL == 0:
                        save_progress(progress_file, completed_urls)
                break

            elif status == "blocked":
                # Blocked - wait happened in download function, retry indefinitely
                async with stats.lock:
                    stats.blocked += 1
                continue

            elif status == "retry":
                attempt += 1
                if attempt < max_retries:
                    await asyncio.sleep(2 ** attempt)
                    continue
                async with stats.lock:
                    stats.failed += 1
                    failed_urls.append(url)
                break

            else:  # failed
                async with stats.lock:
                    stats.failed += 1
                    failed_urls.append(url)
                break

        queue.task_done()


async def run_downloads(
    urls: list[str],
    out_dir: Path,
    storage_state: str,
    num_tabs: int,
    progress_file: Path,
    block_pause: int,
    completed_urls: set[str],
) -> tuple[DownloadStats, list[str]]:
    """Main download orchestrator."""
    global shutdown_requested

    stats = DownloadStats()
    block_state = BlockState(block_pause)
    reserved_names: set[str] = set()
    name_lock = asyncio.Lock()
    failed_urls: list[str] = []
    done_event = asyncio.Event()

    # Create URL queue
    queue: asyncio.Queue[str] = asyncio.Queue()
    for url in urls:
        queue.put_nowait(url)

    total = len(urls)

    async with async_playwright() as p:
        # Launch browser with stealth settings
        browser = await p.chromium.launch(
            headless=False,
            channel="chrome",
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-features=IsolateOrigins,site-per-process",
            ],
        )

        # Create context with storage state and realistic settings
        storage_path = Path(storage_state)
        context = await browser.new_context(
            storage_state=storage_state if storage_path.exists() else None,
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            locale="en-US",
            timezone_id="America/New_York",
        )

        # Add age verification cookie
        await context.add_cookies([
            {
                "name": "justiceGovAgeVerified",
                "value": "true",
                "domain": ".justice.gov",
                "path": "/",
            }
        ])

        # Remove webdriver flag
        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });

            // Overwrite the plugins length
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5]
            });

            // Overwrite the languages
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en']
            });
        """)

        # Handle initial CAPTCHA/verification
        print("Checking for initial verification...")
        init_page = await context.new_page()
        try:
            await init_page.goto(
                "https://www.justice.gov/epstein/doj-disclosures/data-set-9-files",
                wait_until="domcontentloaded",
                timeout=30000,
            )
            await handle_verification(init_page)
            await asyncio.sleep(2)
        except Exception as e:
            print(f"Warning: {e}")
        await init_page.close()

        # Create tab pool
        pages: list[Page] = []
        for i in range(num_tabs):
            page = await context.new_page()
            pages.append(page)
            # Stagger tab creation
            await asyncio.sleep(0.5)

        print(f"Created {num_tabs} browser tabs")
        print(f"Starting download of {total} PDFs...")

        # Progress printer
        async def progress_printer():
            while not done_event.is_set():
                elapsed = time.monotonic() - stats.start_time
                rate = stats.downloaded / elapsed if elapsed > 0 else 0
                done = stats.downloaded + stats.skipped + stats.failed

                # Check block state
                async with block_state.lock:
                    block_remaining = block_state.blocked_until - time.monotonic()

                if block_remaining > 0:
                    print(
                        f"\r[PAUSED {int(block_remaining)}s] OK:{stats.downloaded} "
                        f"Skip:{stats.skipped} Fail:{stats.failed} Blocked:{stats.blocked}   ",
                        end="",
                        flush=True,
                    )
                else:
                    print(
                        f"\r[{done}/{total}] OK:{stats.downloaded} Skip:{stats.skipped} "
                        f"Fail:{stats.failed} ({rate:.2f}/s)   ",
                        end="",
                        flush=True,
                    )
                await asyncio.sleep(0.5)

        progress_task = asyncio.create_task(progress_printer())

        # Create workers (one per tab)
        workers = []
        for i, page in enumerate(pages):
            w = asyncio.create_task(
                worker(
                    i,
                    page,
                    queue,
                    out_dir,
                    stats,
                    block_state,
                    completed_urls,
                    reserved_names,
                    name_lock,
                    progress_file,
                    failed_urls,
                )
            )
            workers.append(w)

        # Wait for completion or shutdown
        while not shutdown_requested and not queue.empty():
            await asyncio.sleep(0.5)

        # Cancel workers on shutdown
        for w in workers:
            w.cancel()
        await asyncio.gather(*workers, return_exceptions=True)

        # Drain remaining queue
        while not queue.empty():
            try:
                queue.get_nowait()
                queue.task_done()
            except asyncio.QueueEmpty:
                break

        done_event.set()
        await progress_task

        # Final progress line
        done = stats.downloaded + stats.skipped + stats.failed
        print(
            f"\r[{done}/{total}] OK:{stats.downloaded} Skip:{stats.skipped} "
            f"Fail:{stats.failed}                    "
        )

        # Close all
        for page in pages:
            await page.close()
        await context.close()
        await browser.close()

    return stats, failed_urls


def main():
    global shutdown_requested, verbose

    parser = argparse.ArgumentParser(
        description="Download PDFs using real browser tabs (anti-bot)"
    )
    parser.add_argument("--input", default=DEFAULT_INPUT, help="URL list file")
    parser.add_argument("--out-dir", default=DEFAULT_OUT_DIR, help="Output folder")
    parser.add_argument(
        "--storage-state",
        default=DEFAULT_STORAGE_STATE,
        help="Playwright storage state file",
    )
    parser.add_argument(
        "--progress-file",
        default=DEFAULT_PROGRESS_FILE,
        help="Track completed URLs for resume",
    )
    parser.add_argument(
        "--failed-file", default=DEFAULT_FAILED_FILE, help="Write failed URLs here"
    )
    parser.add_argument(
        "--tabs", type=int, default=NUM_TABS, help="Number of browser tabs"
    )
    parser.add_argument(
        "--block-pause", type=int, default=BLOCK_PAUSE, help="Seconds to pause when blocked"
    )
    parser.add_argument(
        "--no-resume", action="store_true", help="Don't resume from progress file"
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Show detailed failure reasons"
    )
    args = parser.parse_args()

    verbose = args.verbose

    input_path = Path(args.input)
    out_dir = Path(args.out_dir)
    progress_file = Path(args.progress_file)
    out_dir.mkdir(parents=True, exist_ok=True)

    if not input_path.exists():
        print(f"Input file not found: {input_path}")
        sys.exit(1)

    all_urls = read_urls(input_path)
    if not all_urls:
        print("No URLs found.")
        return

    # Filter out already completed URLs
    completed_urls: set[str] = set()
    if not args.no_resume:
        completed_urls = load_progress(progress_file)
        if completed_urls:
            print(f"Resuming: {len(completed_urls)} URLs already completed")

    urls = [u for u in all_urls if u not in completed_urls]
    if not urls:
        print("All URLs already downloaded!")
        return

    print(
        f"Will download {len(urls)} URLs "
        f"({len(all_urls)} total, {len(completed_urls)} done)"
    )
    print(f"Using {args.tabs} browser tabs with {MIN_DELAY}-{MAX_DELAY}s delays")

    # Set up signal handlers for graceful shutdown
    def signal_handler(_sig, _frame):
        global shutdown_requested
        if not shutdown_requested:
            print("\n[SHUTDOWN] Graceful shutdown requested...")
            shutdown_requested = True
        else:
            print("\n[SHUTDOWN] Forced exit")
            sys.exit(1)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Run downloads
    stats, failed_urls = asyncio.run(
        run_downloads(
            urls=urls,
            out_dir=out_dir,
            storage_state=args.storage_state,
            num_tabs=args.tabs,
            progress_file=progress_file,
            block_pause=args.block_pause,
            completed_urls=completed_urls,
        )
    )

    # Save final progress
    save_progress(progress_file, completed_urls)
    print(f"Progress saved to {progress_file}")

    # Save failed URLs
    if args.failed_file and failed_urls:
        Path(args.failed_file).write_text("\n".join(failed_urls) + "\n")
        print(f"Failed URLs saved to {args.failed_file}")

    print(
        f"\nDone. Downloaded: {stats.downloaded}, Skipped: {stats.skipped}, "
        f"Failed: {stats.failed}"
    )


if __name__ == "__main__":
    main()
