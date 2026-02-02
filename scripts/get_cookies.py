#!/usr/bin/env python3
"""
Use Playwright to get valid session cookies for justice.gov.
Handles bot protection and age verification automatically.
Exports cookies for use with scrape_links.py.
"""
import asyncio
import json
import sys
from pathlib import Path

try:
    from playwright.async_api import async_playwright
except ImportError:
    print("Playwright not installed. Run: pip install playwright && playwright install chromium")
    sys.exit(1)


BASE_URL = "https://www.justice.gov/epstein/doj-disclosures/data-set-9-files"
COOKIE_FILE = "cookies.json"


async def get_cookies(headless: bool = False) -> list:
    """
    Launch browser, navigate to the page, handle any gates, and return cookies.
    Set headless=False to see the browser and manually solve CAPTCHAs if needed.
    """
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context()
        page = await context.new_page()
        
        print(f"Navigating to {BASE_URL}...")
        await page.goto(BASE_URL)
        
        # Check for "I am not a robot" button (Queue-IT)
        try:
            captcha_button = page.get_by_role("button", name="I am not a robot")
            if await captcha_button.count() > 0:
                print("Found 'I am not a robot' button, clicking...")
                await captcha_button.click()
                await page.wait_for_load_state("networkidle")
                print("Passed bot check!")
        except Exception as e:
            print(f"No captcha button found or error: {e}")
        
        # Check for age verification
        try:
            age_button = page.get_by_role("button", name="I am over 18")
            if await age_button.count() > 0:
                print("Found age verification, clicking...")
                await age_button.click()
                await page.wait_for_load_state("networkidle")
                print("Passed age verification!")
        except Exception as e:
            print(f"No age button found or error: {e}")
        
        # Wait a moment for any cookies to be set
        await asyncio.sleep(2)
        
        # If not headless, give user time to solve any manual CAPTCHAs
        if not headless:
            print("\n" + "="*50)
            print("Browser is open. If there's a CAPTCHA, solve it now.")
            print("Press Enter when ready to continue...")
            print("="*50)
            await asyncio.get_event_loop().run_in_executor(None, input)
        
        # Get all cookies
        cookies = await context.cookies()
        
        await browser.close()
        
        return cookies


def format_cookies_for_cli(cookies: list) -> str:
    """Format cookies as a CLI argument string."""
    # Filter to relevant cookies for justice.gov
    relevant = [c for c in cookies if "justice.gov" in c.get("domain", "")]
    
    if not relevant:
        return ""
    
    parts = [f"{c['name']}={c['value']}" for c in relevant]
    return "; ".join(parts)


async def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Get cookies from justice.gov using Playwright")
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run in headless mode (may fail if manual CAPTCHA needed)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=COOKIE_FILE,
        help=f"Output file for cookies JSON (default: {COOKIE_FILE})",
    )
    args = parser.parse_args()
    
    print("Starting browser to get cookies...")
    print("(Use --headless to run without visible browser)\n")
    
    cookies = await get_cookies(headless=args.headless)
    
    if not cookies:
        print("No cookies retrieved!")
        return
    
    # Save full cookies to JSON
    output_path = Path(args.output)
    output_path.write_text(json.dumps(cookies, indent=2))
    print(f"\nSaved {len(cookies)} cookies to {output_path}")
    
    # Print CLI format
    cli_cookies = format_cookies_for_cli(cookies)
    if cli_cookies:
        print("\n" + "="*50)
        print("Use this with scrape_links.py:")
        print("="*50)
        print(f'\npython scripts/scrape_links.py --cookies "{cli_cookies}"\n')
    
    # Also print individual cookie names for reference
    print("Cookies retrieved:")
    for c in cookies:
        domain = c.get("domain", "unknown")
        name = c.get("name", "unknown")
        print(f"  {domain}: {name}")


if __name__ == "__main__":
    asyncio.run(main())
