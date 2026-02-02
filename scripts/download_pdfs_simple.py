#!/usr/bin/env python3
"""Simple, fast PDF downloader without anti-blocking overhead."""

import argparse
import asyncio
import json
import os
import re
import sys
from pathlib import Path
from urllib.parse import urlparse

import aiohttp
import yarl

DEFAULT_INPUT = "pdf-links.txt"
DEFAULT_OUT_DIR = "downloads"
DEFAULT_CONCURRENCY = 20
DEFAULT_TIMEOUT = 30
DEFAULT_RETRIES = 3
CHUNK_SIZE = 1024 * 256
DEFAULT_COOKIE_FILE = "cookies.json"


def load_cookies_from_json(path: Path) -> dict[str, str]:
    """Load cookies from Playwright/browser JSON format."""
    cookies = {}
    if not path.exists():
        return cookies
    
    with path.open("r") as f:
        data = json.load(f)
    
    # Handle both list format and {cookies: [...]} format
    cookie_list = data if isinstance(data, list) else data.get("cookies", [])
    
    for cookie in cookie_list:
        name = cookie.get("name")
        value = cookie.get("value")
        domain = cookie.get("domain", "")
        # Only include justice.gov cookies
        if name and value and "justice.gov" in domain:
            cookies[name] = value
    
    return cookies


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


async def download_one(
    session: aiohttp.ClientSession,
    url: str,
    dest: Path,
    timeout: aiohttp.ClientTimeout,
    retries: int,
) -> tuple[str, str | None]:
    """Download a single file. Returns (status, error)."""
    
    for attempt in range(retries + 1):
        try:
            async with session.get(url, timeout=timeout) as resp:
                if resp.status == 404:
                    return ("failed", "404 Not Found")
                resp.raise_for_status()
                
                tmp = dest.with_suffix(dest.suffix + ".part")
                with tmp.open("wb") as f:
                    async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                        f.write(chunk)
                tmp.replace(dest)
                return ("downloaded", None)
                
        except Exception as e:
            if attempt < retries:
                await asyncio.sleep(2 ** attempt)  # 1, 2, 4 seconds
                continue
            return ("failed", f"{type(e).__name__}: {e}")
    
    return ("failed", "max retries")


async def run_downloads(
    urls: list[str],
    out_dir: Path,
    concurrency: int,
    timeout_seconds: int,
    retries: int,
    skip_existing: bool,
    cookies: dict[str, str] | None = None,
) -> tuple[int, int, int, list[str]]:
    """Main download loop."""
    
    timeout = aiohttp.ClientTimeout(total=timeout_seconds)
    connector = aiohttp.TCPConnector(limit=concurrency)
    
    # Set up cookies
    cookie_jar = aiohttp.CookieJar(unsafe=True)
    if cookies:
        # Pre-populate cookies for all unique domains
        domains = {urlparse(u).netloc for u in urls if urlparse(u).netloc}
        for domain in domains:
            for name, value in cookies.items():
                cookie_jar.update_cookies({name: value}, response_url=yarl.URL(f"https://{domain}/"))
        print(f"Using cookies: {cookies}")
    
    reserved: set[str] = set()
    lock = asyncio.Lock()  # Protect reserved set and counters
    counters = {"downloaded": 0, "skipped": 0, "failed": 0}
    failed_urls: list[str] = []
    total = len(urls)
    done_event = asyncio.Event()
    
    async def progress_printer():
        """Print progress every 0.5 seconds."""
        import time
        start = time.monotonic()
        while not done_event.is_set():
            elapsed = time.monotonic() - start
            done = counters["downloaded"] + counters["skipped"] + counters["failed"]
            rate = done / elapsed if elapsed > 0 else 0
            print(f"\r[{done}/{total}] OK:{counters['downloaded']} Skip:{counters['skipped']} Fail:{counters['failed']} ({rate:.1f}/s)   ", end="", flush=True)
            await asyncio.sleep(0.5)
        # Final line (newline to not overwrite)
        done = counters["downloaded"] + counters["skipped"] + counters["failed"]
        print(f"\r[{done}/{total}] OK:{counters['downloaded']} Skip:{counters['skipped']} Fail:{counters['failed']}                    ")
    
    async with aiohttp.ClientSession(connector=connector, cookie_jar=cookie_jar) as session:
        sem = asyncio.Semaphore(concurrency)
        
        async def process(url: str) -> None:
            async with sem:
                base_name = filename_from_url(url)
                base_dest = out_dir / base_name
                
                # Skip if file exists
                if skip_existing and base_dest.exists():
                    async with lock:
                        counters["skipped"] += 1
                    return
                
                # Get unique filename (protected by lock)
                async with lock:
                    filename = unique_name(base_name, out_dir, reserved)
                dest = out_dir / filename
                
                status, error = await download_one(session, url, dest, timeout, retries)
                
                async with lock:
                    if status == "downloaded":
                        counters["downloaded"] += 1
                    else:
                        counters["failed"] += 1
                        failed_urls.append(url)
        
        # Start progress printer
        progress_task = asyncio.create_task(progress_printer())
        
        tasks = [process(url) for url in urls]
        await asyncio.gather(*tasks)
        
        done_event.set()
        await progress_task
    
    return counters["downloaded"], counters["skipped"], counters["failed"], failed_urls


def parse_cookies(cookie_str: str) -> dict[str, str]:
    """Parse 'name=value; name2=value2' into dict."""
    cookies = {}
    for part in cookie_str.replace(",", ";").split(";"):
        part = part.strip()
        if "=" in part:
            name, value = part.split("=", 1)
            cookies[name.strip()] = value.strip()
    return cookies


def main():
    parser = argparse.ArgumentParser(description="Simple, fast PDF downloader")
    parser.add_argument("--input", default=DEFAULT_INPUT, help="URL list file")
    parser.add_argument("--out-dir", default=DEFAULT_OUT_DIR, help="Output folder")
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY, help="Parallel downloads")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, help="Timeout per request (seconds)")
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES, help="Retry attempts")
    parser.add_argument("--no-skip", action="store_true", help="Don't skip existing files")
    parser.add_argument("--failed-file", default="failed.txt", help="Write failed URLs here")
    parser.add_argument("--cookies", type=str, help="Extra cookies: 'name=value; name2=value2'")
    parser.add_argument("--cookie-file", type=str, default=DEFAULT_COOKIE_FILE, help="Load cookies from JSON file (default: cookies.json)")
    parser.add_argument("--no-cookie-file", action="store_true", help="Don't load cookies from file")
    args = parser.parse_args()
    
    input_path = Path(args.input)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    
    if not input_path.exists():
        print(f"Input file not found: {input_path}")
        sys.exit(1)
    
    urls = read_urls(input_path)
    if not urls:
        print("No URLs found.")
        return
    
    # Build cookies - load from cookies.json by default
    cookies = {}
    if not args.no_cookie_file:
        cookie_file = Path(args.cookie_file)
        if cookie_file.exists():
            cookies = load_cookies_from_json(cookie_file)
            print(f"Loaded {len(cookies)} cookies from {cookie_file}")
        else:
            print(f"Warning: {cookie_file} not found, using no cookies")
    
    # Add manual cookies
    if args.cookies:
        cookies.update(parse_cookies(args.cookies))
    
    print(f"Downloading {len(urls)} URLs with concurrency={args.concurrency}")
    
    downloaded, skipped, failed, failed_urls = asyncio.run(
        run_downloads(
            urls=urls,
            out_dir=out_dir,
            concurrency=args.concurrency,
            timeout_seconds=args.timeout,
            retries=args.retries,
            skip_existing=not args.no_skip,
            cookies=cookies if cookies else None,
        )
    )
    
    if args.failed_file and failed_urls:
        Path(args.failed_file).write_text("\n".join(failed_urls) + "\n")
    
    print(f"Done. Downloaded: {downloaded}, Skipped: {skipped}, Failed: {failed}")


if __name__ == "__main__":
    main()
