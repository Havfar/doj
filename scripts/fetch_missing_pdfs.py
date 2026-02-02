#!/usr/bin/env python3
import asyncio
import os
from pathlib import Path
from urllib.parse import urlparse, unquote

import aiohttp

LINKS_FILE = "pdf-links.txt"
DOWNLOAD_DIR = "downloads"
COOKIE = {"justiceGovAgeVerified": "true"}
CONCURRENCY = 10


async def download_one(session, url, filename, download_dir, semaphore, counter, total):
    async with semaphore:
        dest = download_dir / filename
        try:
            async with session.get(url) as resp:
                resp.raise_for_status()
                content = await resp.read()
                dest.write_bytes(content)
                counter[0] += 1
                print(f"[{counter[0]}/{total}] Downloaded: {filename}")
        except Exception as e:
            counter[0] += 1
            print(f"[{counter[0]}/{total}] Failed: {filename} - {e}")


async def main():
    download_dir = Path(DOWNLOAD_DIR)
    download_dir.mkdir(exist_ok=True)

    # Get existing files
    existing = set(f.name for f in download_dir.glob("*.pdf"))

    # Read URLs and filter missing
    with open(LINKS_FILE) as f:
        urls = [line.strip() for line in f if line.strip()]

    missing = []
    for url in urls:
        filename = unquote(os.path.basename(urlparse(url).path))
        if filename not in existing:
            missing.append((url, filename))

    total = len(missing)
    print(f"Total URLs: {len(urls)}, Already downloaded: {len(existing)}, To download: {total}")

    if not missing:
        return

    semaphore = asyncio.Semaphore(CONCURRENCY)
    counter = [0]  # Use list for mutable counter

    async with aiohttp.ClientSession(cookies=COOKIE) as session:
        tasks = [
            download_one(session, url, filename, download_dir, semaphore, counter, total)
            for url, filename in missing
        ]
        await asyncio.gather(*tasks)

    print("Done!")


if __name__ == "__main__":
    asyncio.run(main())
