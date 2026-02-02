#!/usr/bin/env python3
"""Download PDFs using cookies from Playwright storage state with aiohttp for speed."""

import argparse
import asyncio
import json
import os
import re
import sys
from pathlib import Path
from urllib.parse import urlparse

import aiohttp

DEFAULT_INPUT = "pdf-links.txt"
DEFAULT_OUT_DIR = "downloads"
DEFAULT_CONCURRENCY = 50  # Increased for bulk downloads
DEFAULT_TIMEOUT = 30  # Reduced - fail fast and retry
DEFAULT_RETRIES = 3
BLOCK_PAUSE = 60  # Reduced pause
PROGRESS_SAVE_INTERVAL = 100  # Save progress every N downloads


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
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", name.strip())
    return name or "downloaded.pdf"


def filename_from_url(url: str) -> str:
    base = os.path.basename(urlparse(url).path)
    if not base:
        return "downloaded.pdf"
    base = sanitize_filename(base)
    if "." not in base:
        base += ".pdf"
    return base


def unique_name(name: str, out_dir: Path, reserved: set[str]) -> str:
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


def load_cookies_from_storage_state(storage_state_path: str) -> dict[str, str]:
    """Load cookies from Playwright storage state file."""
    cookies = {}
    if Path(storage_state_path).exists():
        with open(storage_state_path) as f:
            data = json.load(f)
            for cookie in data.get("cookies", []):
                cookies[cookie["name"]] = cookie["value"]
    return cookies


async def run_downloads(
    urls: list[str],
    out_dir: Path,
    concurrency: int,
    timeout: int,
    retries: int,
    skip_existing: bool,
    storage_state: str | None,
    block_pause: int = BLOCK_PAUSE,
    progress_file: Path | None = None,
) -> tuple[int, int, int, list[str]]:
    """Main download loop using aiohttp with cookies from Playwright storage state."""

    reserved: set[str] = set()
    lock = asyncio.Lock()
    counters = {"downloaded": 0, "skipped": 0, "failed": 0, "blocked": 0}
    failed_urls: list[str] = []
    completed_urls: set[str] = set()
    total = len(urls)
    done_event = asyncio.Event()
    last_save_count = [0]

    # Block state
    import time
    block_until = [0.0]
    
    # Load cookies from storage state
    cookies = {}
    if storage_state:
        cookies = load_cookies_from_storage_state(storage_state)
        if cookies:
            print(f"Loaded {len(cookies)} cookies from {storage_state}")
    
    async def progress_printer():
        import time
        start = time.monotonic()
        while not done_event.is_set():
            elapsed = time.monotonic() - start
            done = counters["downloaded"] + counters["skipped"] + counters["failed"]
            dl_rate = counters["downloaded"] / elapsed if elapsed > 0 else 0
            
            block_remaining = block_until[0] - time.monotonic()
            if block_remaining > 0:
                print(f"\r[PAUSED {int(block_remaining)}s] OK:{counters['downloaded']} Skip:{counters['skipped']} Fail:{counters['failed']}   ", end="", flush=True)
            else:
                print(f"\r[{done}/{total}] OK:{counters['downloaded']} Skip:{counters['skipped']} Fail:{counters['failed']} ({dl_rate:.1f} dl/s)   ", end="", flush=True)
            await asyncio.sleep(0.5)
        done = counters["downloaded"] + counters["skipped"] + counters["failed"]
        print(f"\r[{done}/{total}] OK:{counters['downloaded']} Skip:{counters['skipped']} Fail:{counters['failed']}                    ")
    
    # Set up aiohttp session with cookies - optimized for bulk downloads
    connector = aiohttp.TCPConnector(
        limit=concurrency,
        limit_per_host=min(concurrency, 20),  # Don't hammer single host too hard
        ttl_dns_cache=600,  # Cache DNS for 10 mins
        keepalive_timeout=30,
        enable_cleanup_closed=True,
    )
    client_timeout = aiohttp.ClientTimeout(total=timeout, connect=10)
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/pdf,*/*",
    }
    
    async with aiohttp.ClientSession(
        connector=connector,
        timeout=client_timeout,
        headers=headers,
        cookies=cookies,
    ) as session:
        
        async def worker(worker_id: int, queue: asyncio.Queue):
            while True:
                try:
                    url = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                
                base_name = filename_from_url(url)
                base_dest = out_dir / base_name
                
                # Skip if exists
                if skip_existing and base_dest.exists():
                    async with lock:
                        counters["skipped"] += 1
                    queue.task_done()
                    continue
                
                # Get unique filename
                async with lock:
                    filename = unique_name(base_name, out_dir, reserved)
                dest = out_dir / filename
                
                # Try to download
                success = False
                for attempt in range(retries + 1):
                    # Check if we're blocked
                    import time as time_mod
                    while time_mod.monotonic() < block_until[0]:
                        await asyncio.sleep(5)
                    
                    try:
                        async with session.get(url) as response:
                            if response.status == 200:
                                content = await response.read()
                                # Verify it's a PDF
                                if content[:4] == b'%PDF':
                                    dest.write_bytes(content)
                                    success = True
                                    break
                                else:
                                    # Not a PDF, might be HTML error page
                                    if attempt < retries:
                                        await asyncio.sleep(2 ** attempt)
                                    continue
                            elif response.status == 404:
                                break
                            elif response.status == 403:
                                async with lock:
                                    if time_mod.monotonic() >= block_until[0]:
                                        block_until[0] = time_mod.monotonic() + block_pause
                                        counters["blocked"] += 1
                                        print(f"\n[BLOCKED] 403 detected, pausing {block_pause}s...")
                                if attempt < retries:
                                    continue
                                break
                            else:
                                if attempt < retries:
                                    await asyncio.sleep(2 ** attempt)
                    except asyncio.TimeoutError:
                        if attempt < retries:
                            await asyncio.sleep(2 ** attempt)
                        continue
                    except Exception as e:
                        if attempt < retries:
                            await asyncio.sleep(2 ** attempt)
                        continue
                
                async with lock:
                    if success:
                        counters["downloaded"] += 1
                        completed_urls.add(url)
                        # Periodic progress save
                        if progress_file and counters["downloaded"] - last_save_count[0] >= PROGRESS_SAVE_INTERVAL:
                            save_progress(progress_file, completed_urls)
                            last_save_count[0] = counters["downloaded"]
                    else:
                        counters["failed"] += 1
                        failed_urls.append(url)

                queue.task_done()
        
        # Create queue and fill it
        queue: asyncio.Queue[str] = asyncio.Queue()
        for url in urls:
            queue.put_nowait(url)
        
        # Start progress printer
        progress_task = asyncio.create_task(progress_printer())
        
        # Start workers
        workers = [asyncio.create_task(worker(i, queue)) for i in range(concurrency)]
        
        # Wait for queue to be processed
        await queue.join()
        
        # Clean up
        for w in workers:
            w.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        
        done_event.set()
        await progress_task

        # Final progress save
        if progress_file and completed_urls:
            save_progress(progress_file, completed_urls)

    return counters["downloaded"], counters["skipped"], counters["failed"], failed_urls


def main():
    parser = argparse.ArgumentParser(description="Download PDFs using cookies from Playwright storage state")
    parser.add_argument("--input", default=DEFAULT_INPUT, help="URL list file")
    parser.add_argument("--out-dir", default=DEFAULT_OUT_DIR, help="Output folder")
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY, help="Parallel downloads")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, help="Timeout per request (seconds)")
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES, help="Retry attempts")
    parser.add_argument("--no-skip", action="store_true", help="Don't skip existing files")
    parser.add_argument("--failed-file", default="failed.txt", help="Write failed URLs here")
    parser.add_argument("--storage-state", default="storage-state.json", help="Playwright storage state file (cookies)")
    parser.add_argument("--block-pause", type=int, default=BLOCK_PAUSE, help="Seconds to pause when blocked")
    parser.add_argument("--progress-file", default="download-progress.txt", help="Track completed URLs for resume")
    parser.add_argument("--no-resume", action="store_true", help="Don't resume from progress file")
    args = parser.parse_args()

    input_path = Path(args.input)
    out_dir = Path(args.out_dir)
    progress_file = Path(args.progress_file) if not args.no_resume else None
    out_dir.mkdir(parents=True, exist_ok=True)

    if not input_path.exists():
        print(f"Input file not found: {input_path}")
        sys.exit(1)

    all_urls = read_urls(input_path)
    if not all_urls:
        print("No URLs found.")
        return

    # Filter out already completed URLs
    completed = set()
    if progress_file:
        completed = load_progress(progress_file)
        if completed:
            print(f"Resuming: {len(completed)} URLs already completed")

    urls = [u for u in all_urls if u not in completed]
    if not urls:
        print("All URLs already downloaded!")
        return

    print(f"Downloading {len(urls)} URLs ({len(all_urls)} total, {len(completed)} done) with {args.concurrency} concurrent connections")
    
    downloaded, skipped, failed, failed_urls = asyncio.run(
        run_downloads(
            urls=urls,
            out_dir=out_dir,
            concurrency=args.concurrency,
            timeout=args.timeout,
            retries=args.retries,
            skip_existing=not args.no_skip,
            storage_state=args.storage_state,
            block_pause=args.block_pause,
            progress_file=progress_file,
        )
    )
    
    if args.failed_file and failed_urls:
        Path(args.failed_file).write_text("\n".join(failed_urls) + "\n")
    
    print(f"Done. Downloaded: {downloaded}, Skipped: {skipped}, Failed: {failed}")


if __name__ == "__main__":
    main()
