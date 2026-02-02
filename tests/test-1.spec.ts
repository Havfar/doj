import { test, expect, Page, BrowserContext } from '@playwright/test';
import { promises as fs } from 'fs';

// Configuration
const START_PAGE: number = 8090; // Resume from this page (safe to set lower - duplicates are filtered)
const TABS_PER_BROWSER = 20; // Number of tabs per browser context
const NUM_BROWSERS = 4; // Number of browser contexts
const TOTAL_CONCURRENCY = TABS_PER_BROWSER * NUM_BROWSERS; // Total: 40 pages per batch
const DELAY_MS = 50; // Delay between batches
const OUTPUT_FILE = 'pdf-links.txt';
const BASE_URL = 'https://www.justice.gov/epstein/doj-disclosures/data-set-9-files';

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

// Track last completed page for abort handling
let lastCompletedPage = START_PAGE - 1;

async function loadExistingLinks(): Promise<Set<string>> {
  try {
    const content = await fs.readFile(OUTPUT_FILE, 'utf-8');
    const links = content.split('\n').filter(line => line.trim().length > 0);
    console.log(`Loaded ${links.length} existing links from ${OUTPUT_FILE}`);
    return new Set(links);
  } catch {
    console.log(`No existing file found, starting fresh`);
    return new Set();
  }
}

async function scrapePage(page: Page, pageNum: number): Promise<{ pageNum: number; hrefs: string[]; error: boolean }> {
  try {
    const url = pageNum === 1 
      ? BASE_URL 
      : `${BASE_URL}?page=${pageNum}`;
    
    const response = await page.goto(url, { 
      waitUntil: 'commit',
      timeout: 15000 
    });

    if (!response || response.status() !== 200) {
      console.log(`Page ${pageNum}: HTTP ${response?.status() || 'no response'}`);
      return { pageNum, hrefs: [], error: true };
    }

    // Wait briefly for content
    await page.waitForSelector('a[href$=".pdf"]', { timeout: 5000 }).catch(() => {});

    const hrefs = await page.$$eval('a[href$=".pdf"]', links => 
      links.map(link => (link as HTMLAnchorElement).href)
    );

    console.log(`Page ${pageNum}: found ${hrefs.length} PDF links`);
    return { pageNum, hrefs, error: false };
  } catch (error) {
    console.log(`Page ${pageNum}: error`);
    return { pageNum, hrefs: [], error: true };
  }
}

test('scrape pdf links', async ({ browser }) => {
  test.setTimeout(0); // No timeout for long-running scrape

  // Register abort handler to show last completed page
  const abortHandler = () => {
    console.log(`\n\n========================================`);
    console.log(`ABORTED! Last completed page: ${lastCompletedPage}`);
    console.log(`Resume with: START_PAGE = ${lastCompletedPage + 1}`);
    console.log(`========================================\n`);
    process.exit(0);
  };
  process.on('SIGINT', abortHandler);
  process.on('SIGTERM', abortHandler);

  // Load existing links to avoid duplicates
  const existingLinks = await loadExistingLinks();

  // Helper to set up resource blocking on a context
  async function setupResourceBlocking(ctx: BrowserContext) {
    await ctx.route('**/*', (route) => {
      const type = route.request().resourceType();
      if (['image', 'stylesheet', 'font', 'media', 'other'].includes(type)) {
        route.abort();
      } else {
        route.continue();
      }
    });
  }

  // Create first context and handle CAPTCHA
  const context1 = await browser.newContext();
  await setupResourceBlocking(context1);
  
  const captchaPage = await context1.newPage();
  await captchaPage.goto(BASE_URL);
  const captchaButton = captchaPage.getByRole('button', { name: 'I am not a robot' });
  if (await captchaButton.count() > 0) {
    console.log('CAPTCHA detected - please solve it...');
    await captchaButton.click();
    await captchaPage.waitForLoadState('networkidle');
  }
  
  // Get cookies after CAPTCHA
  const cookies = await context1.cookies();
  await captchaPage.close();

  // Create all browser contexts and share cookies
  const contexts: BrowserContext[] = [context1];
  for (let i = 1; i < NUM_BROWSERS; i++) {
    const ctx = await browser.newContext();
    await setupResourceBlocking(ctx);
    await ctx.addCookies(cookies);
    contexts.push(ctx);
  }
  console.log(`Created ${NUM_BROWSERS} browser contexts`);

  // Create page pools for each context
  const allPagePools: Page[][] = [];
  for (let i = 0; i < NUM_BROWSERS; i++) {
    const pool: Page[] = [];
    for (let j = 0; j < TABS_PER_BROWSER; j++) {
      pool.push(await contexts[i].newPage());
    }
    allPagePools.push(pool);
  }
  console.log(`Created ${TABS_PER_BROWSER} tabs x ${NUM_BROWSERS} browsers = ${TOTAL_CONCURRENCY} total tabs`);

  let currentPage = START_PAGE;
  let consecutiveEmptyPages = 0;
  const maxEmptyPages = 5;

  console.log(`Starting from page ${currentPage}`);

  while (consecutiveEmptyPages < maxEmptyPages) {
    // Distribute pages across all browsers
    // Browser 1 gets pages: currentPage, currentPage+1, ... currentPage+19
    // Browser 2 gets pages: currentPage+20, currentPage+21, ... currentPage+39
    const allPromises: Promise<{ pageNum: number; hrefs: string[]; error: boolean }>[] = [];
    
    for (let browserIdx = 0; browserIdx < NUM_BROWSERS; browserIdx++) {
      const startOffset = browserIdx * TABS_PER_BROWSER;
      for (let tabIdx = 0; tabIdx < TABS_PER_BROWSER; tabIdx++) {
        const pageNum = currentPage + startOffset + tabIdx;
        allPromises.push(scrapePage(allPagePools[browserIdx][tabIdx], pageNum));
      }
    }

    // Scrape all pages in parallel across all browsers
    const batchResults = await Promise.all(allPromises);

    // Write results in order
    let allEmpty = true;
    for (const result of batchResults.sort((a, b) => a.pageNum - b.pageNum)) {
      if (result.hrefs.length > 0) {
        const newLinks = result.hrefs.filter(href => !existingLinks.has(href));
        
        if (newLinks.length > 0) {
          await fs.appendFile(OUTPUT_FILE, `${newLinks.join('\n')}\n`);
          newLinks.forEach(link => existingLinks.add(link));
          console.log(`  -> Added ${newLinks.length} new (${result.hrefs.length - newLinks.length} dupes)`);
        }
        allEmpty = false;
      }
    }

    // Update last completed page (highest page number in batch)
    const maxPageInBatch = Math.max(...batchResults.map(r => r.pageNum));
    lastCompletedPage = maxPageInBatch;

    if (allEmpty && !batchResults.some(r => r.error)) {
      consecutiveEmptyPages++;
      console.log(`Empty batch ${consecutiveEmptyPages}/${maxEmptyPages}`);
    } else {
      consecutiveEmptyPages = 0;
    }

    currentPage += TOTAL_CONCURRENCY;
    await sleep(DELAY_MS);
  }

  console.log(`Finished at page ${lastCompletedPage}`);
  console.log(`Total unique links: ${existingLinks.size}`);
  
  // Cleanup
  for (const pool of allPagePools) {
    await Promise.all(pool.map(p => p.close()));
  }
  for (const ctx of contexts) {
    await ctx.close();
  }
  
  // Remove abort handlers
  process.off('SIGINT', abortHandler);
  process.off('SIGTERM', abortHandler);
});