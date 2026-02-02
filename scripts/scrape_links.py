#!/usr/bin/env python3
"""
Pure HTTP PDF link scraper for justice.gov pages.
Uses aiohttp for async requests and BeautifulSoup for HTML parsing.
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
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

import aiohttp
from bs4 import BeautifulSoup


# Configuration defaults
DEFAULT_OUTPUT = "pdf-links.txt"
DEFAULT_CONCURRENCY = 50
DEFAULT_TIMEOUT = 30
DEFAULT_MAX_EMPTY = 5
DEFAULT_START_PAGE = 1
BASE_URL = "https://www.justice.gov/epstein/doj-disclosures/data-set-9-files"

# User agents for rotation
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
]

# Track state for abort handling
last_completed_page = 0
abort_requested = False


def get_random_headers() -> Dict[str, str]:
    """Generate realistic browser-like headers."""
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }


def parse_cookies(cookie_string: str) -> Dict[str, str]:
    """Parse cookies from string format 'name=value; name2=value2'."""
    cookies: Dict[str, str] = {}
    if not cookie_string:
        return cookies
    
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


def load_existing_links(path: Path) -> Set[str]:
    """Load existing links from file to avoid duplicates."""
    if not path.exists():
        print(f"No existing file found at {path}, starting fresh")
        return set()
    
    with path.open("r", encoding="utf-8") as f:
        links = {line.strip() for line in f if line.strip()}
    
    print(f"Loaded {len(links)} existing links from {path}")
    return links


def extract_pdf_links(html: str, base_url: str) -> List[str]:
    """Extract all PDF links from HTML content."""
    soup = BeautifulSoup(html, "lxml")
    links: List[str] = []
    
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        if href.endswith(".pdf"):
            # Make absolute URL
            full_url = urljoin(base_url, href)
            links.append(full_url)
    
    return links


async def fetch_page(
    session: aiohttp.ClientSession,
    page_num: int,
    timeout: aiohttp.ClientTimeout,
) -> Tuple[int, Optional[str], Optional[str]]:
    """Fetch a single page and return (page_num, html_content, error)."""
    url = BASE_URL if page_num == 1 else f"{BASE_URL}?page={page_num}"
    
    try:
        headers = get_random_headers()
        async with session.get(url, timeout=timeout, headers=headers) as resp:
            if resp.status == 403:
                return (page_num, None, "403 Forbidden - need cookies?")
            if resp.status != 200:
                return (page_num, None, f"HTTP {resp.status}")
            
            html = await resp.text()
            return (page_num, html, None)
    except asyncio.TimeoutError:
        return (page_num, None, "timeout")
    except Exception as e:
        return (page_num, None, str(e))


async def scrape_batch(
    session: aiohttp.ClientSession,
    page_numbers: List[int],
    timeout: aiohttp.ClientTimeout,
) -> List[Tuple[int, List[str], Optional[str]]]:
    """Scrape a batch of pages concurrently."""
    tasks = [fetch_page(session, pn, timeout) for pn in page_numbers]
    results = await asyncio.gather(*tasks)
    
    parsed_results: List[Tuple[int, List[str], Optional[str]]] = []
    for page_num, html, error in results:
        if error:
            parsed_results.append((page_num, [], error))
        elif html:
            links = extract_pdf_links(html, BASE_URL)
            parsed_results.append((page_num, links, None))
        else:
            parsed_results.append((page_num, [], "empty response"))
    
    return parsed_results


async def run_scraper(
    output_path: Path,
    start_page: int,
    concurrency: int,
    timeout_seconds: int,
    max_empty: int,
    cookies: Optional[Dict[str, str]],
) -> Tuple[int, int, int]:
    """
    Run the scraper.
    Returns (total_pages_scraped, new_links_found, total_links).
    """
    global last_completed_page, abort_requested
    
    timeout = aiohttp.ClientTimeout(total=timeout_seconds)
    connector = aiohttp.TCPConnector(limit=concurrency)
    
    # Set up cookie jar
    cookie_jar = aiohttp.CookieJar(unsafe=True)
    
    # Load existing links
    existing_links = load_existing_links(output_path)
    initial_count = len(existing_links)
    
    # Pre-populate cookies
    if cookies:
        # Add cookies for the target domain
        domain = urlparse(BASE_URL).netloc
        for name, value in cookies.items():
            cookie_jar.update_cookies({name: value}, response_url=aiohttp.client.URL(f"https://{domain}/"))
        print(f"Loaded {len(cookies)} cookie(s)")
    
    current_page = start_page
    last_completed_page = start_page - 1
    consecutive_empty = 0
    total_new_links = 0
    pages_scraped = 0
    start_time = time.monotonic()
    
    async with aiohttp.ClientSession(connector=connector, cookie_jar=cookie_jar) as session:
        while consecutive_empty < max_empty and not abort_requested:
            # Create batch of page numbers
            batch_pages = list(range(current_page, current_page + concurrency))
            
            # Scrape batch
            results = await scrape_batch(session, batch_pages, timeout)
            
            # Process results
            batch_empty = True
            batch_new_links: List[str] = []
            errors = 0
            
            for page_num, links, error in sorted(results, key=lambda x: x[0]):
                pages_scraped += 1
                
                if error:
                    errors += 1
                    if "403" in error:
                        print(f"\nPage {page_num}: {error}")
                        print("Try adding cookies: --cookies 'justiceGovAgeVerified=true; QueueITAccepted-...'")
                        abort_requested = True
                        break
                elif links:
                    batch_empty = False
                    # Filter out existing links
                    new_links = [link for link in links if link not in existing_links]
                    if new_links:
                        batch_new_links.extend(new_links)
                        for link in new_links:
                            existing_links.add(link)
                        total_new_links += len(new_links)
            
            if abort_requested:
                break
            
            # Write new links to file
            if batch_new_links:
                with output_path.open("a", encoding="utf-8") as f:
                    f.write("\n".join(batch_new_links) + "\n")
            
            # Update progress
            elapsed = time.monotonic() - start_time
            rate = pages_scraped / max(0.001, elapsed)
            max_page = max(batch_pages)
            last_completed_page = max_page
            
            # Check for empty batch
            if batch_empty and errors < concurrency:
                consecutive_empty += 1
                print(f"\rPage {current_page}-{max_page}: empty ({consecutive_empty}/{max_empty}) | "
                      f"{pages_scraped} pages, {total_new_links} new links, {rate:.1f} pages/s   ", end="")
            else:
                consecutive_empty = 0
                links_in_batch = sum(len(r[1]) for r in results)
                new_in_batch = len(batch_new_links)
                print(f"\rPage {current_page}-{max_page}: {links_in_batch} links ({new_in_batch} new) | "
                      f"{pages_scraped} pages, {total_new_links} new links, {rate:.1f} pages/s   ", end="")
            
            current_page += concurrency
    
    print()  # New line after progress
    return pages_scraped, total_new_links, len(existing_links)


def signal_handler(signum, frame):
    """Handle Ctrl+C gracefully."""
    global abort_requested
    print(f"\n\n========================================")
    print(f"ABORTED! Last completed page: {last_completed_page}")
    print(f"Resume with: --start-page {last_completed_page + 1}")
    print(f"========================================\n")
    abort_requested = True


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Scrape PDF links from justice.gov pages using pure HTTP.",
        epilog="""
Example usage:
  python scripts/scrape_links.py --cookies "justiceGovAgeVerified=true"
  python scripts/scrape_links.py --start-page 1000 --concurrency 100
  python scripts/scrape_links.py --cookies "justiceGovAgeVerified=true; QueueITAccepted-SDFrts345E-V3_usdojfiles=TOKEN"
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--start-page",
        type=int,
        default=DEFAULT_START_PAGE,
        help=f"Page number to start from (default: {DEFAULT_START_PAGE}).",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=DEFAULT_CONCURRENCY,
        help=f"Number of concurrent requests (default: {DEFAULT_CONCURRENCY}).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help=f"Request timeout in seconds (default: {DEFAULT_TIMEOUT}).",
    )
    parser.add_argument(
        "--cookies",
        type=str,
        default="",
        help="Cookies to send: 'name=value; name2=value2'",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=DEFAULT_OUTPUT,
        help=f"Output file for PDF links (default: {DEFAULT_OUTPUT}).",
    )
    parser.add_argument(
        "--max-empty",
        type=int,
        default=DEFAULT_MAX_EMPTY,
        help=f"Stop after N consecutive empty pages (default: {DEFAULT_MAX_EMPTY}).",
    )
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    
    output_path = Path(args.output)
    cookies = parse_cookies(args.cookies) if args.cookies else None
    
    # Set up signal handler for graceful abort
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    print(f"Starting scrape from page {args.start_page}")
    print(f"Concurrency: {args.concurrency}, Output: {output_path}")
    if cookies:
        print(f"Cookies: {', '.join(cookies.keys())}")
    print()
    
    pages_scraped, new_links, total_links = asyncio.run(
        run_scraper(
            output_path=output_path,
            start_page=args.start_page,
            concurrency=max(1, args.concurrency),
            timeout_seconds=max(5, args.timeout),
            max_empty=max(1, args.max_empty),
            cookies=cookies,
        )
    )
    
    print(f"\nDone!")
    print(f"Pages scraped: {pages_scraped}")
    print(f"New links found: {new_links}")
    print(f"Total links in file: {total_links}")


if __name__ == "__main__":
    main()
