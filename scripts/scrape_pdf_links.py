#!/usr/bin/env python3
import argparse
import asyncio
import json
from html.parser import HTMLParser
from pathlib import Path
from typing import Iterable, List, Optional, Set, Tuple
from urllib.parse import urljoin

import aiohttp
from yarl import URL


DEFAULT_BASE_URL = "https://www.justice.gov/epstein/doj-disclosures/data-set-9-files"
DEFAULT_OUT_FILE = "pdf-links.txt"
DEFAULT_CONCURRENCY = 8
DEFAULT_TIMEOUT = 30
DEFAULT_RETRIES = 3
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


class PdfLinkParser(HTMLParser):
    def __init__(self, base_url: str) -> None:
        super().__init__()
        self.base_url = base_url
        self.links: Set[str] = set()

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        if tag != "a":
            return
        href = None
        for key, value in attrs:
            if key == "href":
                href = value
                break
        if not href or ".pdf" not in href.lower():
            return
        self.links.add(urljoin(self.base_url, href))


def parse_pdf_links(html: str, base_url: str) -> List[str]:
    parser = PdfLinkParser(base_url)
    parser.feed(html)
    return sorted(parser.links)


def load_cookies(storage_state: Optional[Path]) -> List[dict]:
    if not storage_state:
        return []
    data = json.loads(storage_state.read_text(encoding="utf-8"))
    return data.get("cookies", [])


def build_cookie_jar(cookies: Iterable[dict]) -> aiohttp.CookieJar:
    jar = aiohttp.CookieJar(unsafe=True)
    for cookie in cookies:
        name = cookie.get("name")
        value = cookie.get("value")
        domain = cookie.get("domain")
        if not name or value is None or not domain:
            continue
        url = URL(f"https://{str(domain).lstrip('.')}/")
        jar.update_cookies({name: value}, response_url=url)
    return jar


def build_headers(base_url: str, user_agent: str) -> dict:
    return {
        "User-Agent": user_agent,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": base_url,
        "Connection": "keep-alive",
    }


async def fetch_page(
    session: aiohttp.ClientSession, url: str, timeout: int, retries: int
) -> Tuple[str, Optional[str]]:
    attempt = 0
    while True:
        try:
            async with session.get(url, timeout=timeout) as resp:
                if resp.status in (403, 429, 500, 502, 503, 504):
                    raise aiohttp.ClientResponseError(
                        request_info=resp.request_info,
                        history=resp.history,
                        status=resp.status,
                        message="retryable",
                        headers=resp.headers,
                    )
                if resp.status >= 400:
                    return "", f"HTTP {resp.status}"
                return await resp.text(), None
        except Exception as exc:
            if attempt >= retries:
                return "", f"{type(exc).__name__}: {exc}"
            attempt += 1
            await asyncio.sleep(min(10, 0.5 * (2 ** (attempt - 1))))


async def run(
    base_url: str,
    start_page: int,
    end_page: int,
    out_file: Path,
    storage_state: Optional[Path],
    concurrency: int,
    timeout: int,
    retries: int,
    user_agent: str,
    append: bool,
) -> None:
    cookies = load_cookies(storage_state)
    cookie_jar = build_cookie_jar(cookies)
    connector = aiohttp.TCPConnector(limit=concurrency, enable_cleanup_closed=True)
    headers = build_headers(base_url, user_agent)

    total_links = 0
    failed_pages: List[int] = []
    sem = asyncio.Semaphore(concurrency)

    async with aiohttp.ClientSession(
        cookie_jar=cookie_jar, connector=connector, headers=headers
    ) as session:
        await fetch_page(session, base_url, timeout, retries)

        async def fetch_and_parse(page_num: int) -> Tuple[int, List[str], Optional[str]]:
            async with sem:
                url = f"{base_url}?page={page_num}"
                html, error = await fetch_page(session, url, timeout, retries)
                if error:
                    return page_num, [], error
                links = parse_pdf_links(html, base_url)
                return page_num, links, None

        tasks = [fetch_and_parse(page_num) for page_num in range(start_page, end_page + 1)]
        for coro in asyncio.as_completed(tasks):
            page_num, links, error = await coro
            if error:
                failed_pages.append(page_num)
                print(f"Page {page_num}: failed ({error})")
                continue
            if links:
                total_links += len(links)
                with out_file.open("a", encoding="utf-8") as handle:
                    handle.write("\n".join(links) + "\n")
            print(f"Page {page_num}: {len(links)} PDF links (total {total_links})")

    if failed_pages:
        print(f"Failed pages: {', '.join(str(p) for p in sorted(failed_pages))}")
    print(f"Done. Total links: {total_links}")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Scrape PDF links from HTML pages.")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Base listing URL.")
    parser.add_argument("--start-page", type=int, default=1, help="Start page index.")
    parser.add_argument("--end-page", type=int, help="End page index (inclusive).")
    parser.add_argument("--out-file", default=DEFAULT_OUT_FILE, help="Output file.")
    parser.add_argument(
        "--storage-state",
        help="Playwright storage state JSON for captcha session.",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=DEFAULT_CONCURRENCY,
        help="Number of concurrent page fetches.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help="Per-request timeout in seconds.",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=DEFAULT_RETRIES,
        help="Retry attempts for 403/429/5xx responses.",
    )
    parser.add_argument(
        "--user-agent",
        default=DEFAULT_USER_AGENT,
        help="User-Agent header to send.",
    )
    parser.add_argument(
        "--append",
        action="store_true",
        help="Always append to output file.",
    )
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()

    start_page = max(1, args.start_page)
    end_page = args.end_page if args.end_page is not None else start_page
    if end_page < start_page:
        parser.error("--end-page must be >= --start-page")

    out_file = Path(args.out_file)
    storage_state = Path(args.storage_state) if args.storage_state else None

    if args.append or start_page > 1:
        out_file.parent.mkdir(parents=True, exist_ok=True)
        out_file.open("a", encoding="utf-8").close()
    else:
        out_file.parent.mkdir(parents=True, exist_ok=True)
        out_file.write_text("", encoding="utf-8")

    asyncio.run(
        run(
            base_url=args.base_url,
            start_page=start_page,
            end_page=end_page,
            out_file=out_file,
            storage_state=storage_state,
            concurrency=max(1, args.concurrency),
            timeout=max(1, args.timeout),
            retries=max(0, args.retries),
            user_agent=args.user_agent,
            append=args.append,
        )
    )


if __name__ == "__main__":
    main()
