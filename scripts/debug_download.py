#!/usr/bin/env python3
"""Debug script to test PDF download with Playwright."""

import asyncio
from pathlib import Path
from playwright.async_api import async_playwright, Route, Request

TEST_URL = "https://www.justice.gov/epstein/files/DataSet%209/EFTA00039025.pdf"
STORAGE_STATE = "storage-state.json"


async def test_download():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False, channel="chrome")

        # Load with storage state
        storage_path = Path(STORAGE_STATE)
        context = await browser.new_context(
            storage_state=STORAGE_STATE if storage_path.exists() else None,
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
        print("Added age verification cookie")

        page = await context.new_page()

        # First, check landing page for CAPTCHA
        print("Checking landing page...")
        await page.goto("https://www.justice.gov/epstein/doj-disclosures/data-set-9-files")
        await asyncio.sleep(2)

        captcha = page.get_by_role("button", name="I am not a robot")
        if await captcha.count() > 0:
            print("CAPTCHA found - clicking...")
            await captcha.click()
            await page.wait_for_load_state("networkidle")

        print(f"\nTrying to download: {TEST_URL}")

        # Method: Use request context to fetch directly with cookies
        print("\n--- Using API request context ---")

        # Get cookies from context
        cookies = await context.cookies()
        cookie_header = "; ".join([f"{c['name']}={c['value']}" for c in cookies])
        print(f"Cookies: {cookie_header[:100]}...")

        # Create API request context
        api_context = await p.request.new_context()

        response = await api_context.get(
            TEST_URL,
            headers={
                "Cookie": cookie_header,
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            },
        )

        print(f"Status: {response.status}")
        print(f"Headers: {dict(response.headers)}")

        body = await response.body()
        print(f"Body size: {len(body)} bytes")
        print(f"First 20 bytes: {body[:20]}")
        print(f"Is PDF: {body[:4] == b'%PDF'}")

        if body[:4] == b'%PDF':
            Path("test_download.pdf").write_bytes(body)
            print("SUCCESS! Saved to test_download.pdf")

        await api_context.dispose()
        await browser.close()


if __name__ == "__main__":
    asyncio.run(test_download())
