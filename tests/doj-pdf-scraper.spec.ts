import { test, expect, Page, BrowserContext } from '@playwright/test';
import { promises as fs } from 'fs';

// Configuration
const START_PAGE: number = 954;// 11730; // Resume from this page (safe to set lower - duplicates are filtered)
const TABS_PER_BROWSER =15; // Number of tabs per browser context
const NUM_BROWSERS = 2; // Number of browser contexts
const TOTAL_CONCURRENCY = TABS_PER_BROWSER * NUM_BROWSERS; // Total: 40 pages per batch
const DELAY_MS = 50; // Delay between batches
const OUTPUT_FILE = 'pdf-links.txt';
const FAILED_PAGES_FILE = 'failed-pages.txt';
const BASE_URL = 'https://www.justice.gov/epstein/doj-disclosures/data-set-9-files';

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

// Track last completed page for abort handling
let lastCompletedPage = START_PAGE - 1;
// Track failed pages
const failedPages: Set<number> = new Set();

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

async function loadExistingFailedPages(): Promise<void> {
  try {
    const content = await fs.readFile(FAILED_PAGES_FILE, 'utf-8');
    const pages = content.split('\n').filter(line => line.trim().length > 0).map(Number);
    pages.forEach(p => failedPages.add(p));
    console.log(`Loaded ${pages.length} existing failed pages from ${FAILED_PAGES_FILE}`);
  } catch {
    console.log(`No existing failed pages file found`);
  }
}

async function saveFailedPages(): Promise<void> {
  if (failedPages.size > 0) {
    const sortedPages = Array.from(failedPages).sort((a, b) => a - b);
    await fs.writeFile(FAILED_PAGES_FILE, sortedPages.join('\n') + '\n');
    console.log(`Saved ${failedPages.size} failed pages to ${FAILED_PAGES_FILE}`);
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

    return { pageNum, hrefs, error: false };
  } catch (error) {
    console.log(`Page ${pageNum}: error`);
    return { pageNum, hrefs: [], error: true };
  }
}

test('scrape pdf links', async ({ browser }) => {
  test.setTimeout(0); // No timeout for long-running scrape

  // Counters for this run (declared here so abort handler can access them)
  let pagesScrapedThisRun = 0;
  let linksAddedThisRun = 0;
  let existingLinks: Set<string> = new Set();
  const startTime = Date.now();
  let completed = false;

  // Helper to get throughput stats
  const getThroughputStats = () => {
    const elapsedMs = Date.now() - startTime;
    const elapsedMinutes = elapsedMs / 60000;
    const elapsedSeconds = Math.floor(elapsedMs / 1000);
    const pagesPerMinute = elapsedMinutes > 0 ? (pagesScrapedThisRun / elapsedMinutes).toFixed(1) : '0';
    const linksPerMinute = elapsedMinutes > 0 ? (linksAddedThisRun / elapsedMinutes).toFixed(1) : '0';
    const mins = Math.floor(elapsedSeconds / 60);
    const secs = elapsedSeconds % 60;
    const timeStr = `${mins}m ${secs}s`;
    return { timeStr, pagesPerMinute, linksPerMinute };
  };

  // Helper to save summary (used by both abort and finally)
  const saveSummary = async (status: string) => {
    const { timeStr, pagesPerMinute, linksPerMinute } = getThroughputStats();
    const summary = `
========================================
${status}
========================================
Duration: ${timeStr}
Pages successfully scraped this run: ${pagesScrapedThisRun} (${pagesPerMinute}/min)
Links added this run: ${linksAddedThisRun} (${linksPerMinute}/min)
Last completed page: ${lastCompletedPage}
Resume with: START_PAGE = ${lastCompletedPage + 1}
Total unique links: ${existingLinks.size}
Failed pages: ${failedPages.size}
========================================
`;
    console.log(summary);
    await fs.writeFile('run-summary.txt', summary);
    await saveFailedPages();
  };

  // Register abort handler for Ctrl+C
  const abortHandler = async () => {
    await saveSummary('ABORTED (Ctrl+C)');
    process.exit(0);
  };
  process.on('SIGINT', abortHandler);
  process.on('SIGTERM', abortHandler);

  // Load existing links to avoid duplicates
  existingLinks = await loadExistingLinks();
  
  // Load existing failed pages
  await loadExistingFailedPages();

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

  const contexts: BrowserContext[] = [];
  const allPagePools: Page[][] = [];

  try {
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
    contexts.push(context1);
    for (let i = 1; i < NUM_BROWSERS; i++) {
      const ctx = await browser.newContext();
      await setupResourceBlocking(ctx);
      await ctx.addCookies(cookies);
      contexts.push(ctx);
    }
    console.log(`Created ${NUM_BROWSERS} browser contexts`);

    // Create page pools for each context
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

      // Write results in order and track failures
      let allEmpty = true;
      for (const result of batchResults.sort((a, b) => a.pageNum - b.pageNum)) {
        if (result.error) {
          failedPages.add(result.pageNum);
        } else {
          pagesScrapedThisRun++;
          // Successfully scraped - remove from failed pages if it was there
          failedPages.delete(result.pageNum);
          
          if (result.hrefs.length > 0) {
            const newLinks = result.hrefs.filter(href => !existingLinks.has(href));
            
            if (newLinks.length > 0) {
              await fs.appendFile(OUTPUT_FILE, `${newLinks.join('\n')}\n`);
              newLinks.forEach(link => existingLinks.add(link));
              linksAddedThisRun += newLinks.length;
            }
            const dupeCount = result.hrefs.length - newLinks.length;
            console.log(`Page ${result.pageNum}: found ${result.hrefs.length} PDF links, added ${newLinks.length} new (${dupeCount} dupes)`);
            allEmpty = false;
          } else {
            console.log(`Page ${result.pageNum}: found 0 PDF links`);
          }
        }
      }

      // Update last completed page (highest page number in batch)
      const maxPageInBatch = Math.max(...batchResults.map(r => r.pageNum));
      lastCompletedPage = maxPageInBatch;

      // Log running totals
      console.log(`  [Total: ${linksAddedThisRun} new links this run, ${existingLinks.size} in ${OUTPUT_FILE}]`);

      if (allEmpty && !batchResults.some(r => r.error)) {
        consecutiveEmptyPages++;
        console.log(`Empty batch ${consecutiveEmptyPages}/${maxEmptyPages}`);
      } else {
        consecutiveEmptyPages = 0;
      }

      currentPage += TOTAL_CONCURRENCY;
      await sleep(DELAY_MS);
    }

    // Mark as completed normally
    completed = true;
    await saveSummary('COMPLETED');

  } finally {
    // This runs whether the test completes, errors, or is terminated
    if (!completed) {
      // Save summary if we didn't complete normally (e.g., debug session closed, error thrown)
      try {
        await saveSummary('INTERRUPTED');
      } catch (e) {
        // Best effort - process might be dying
        console.log(`Resume with: START_PAGE = ${lastCompletedPage + 1}`);
      }
    }

    // Cleanup
    for (const pool of allPagePools) {
      await Promise.all(pool.map(p => p.close().catch(() => {})));
    }
    for (const ctx of contexts) {
      await ctx.close().catch(() => {});
    }
    
    // Remove abort handlers
    process.off('SIGINT', abortHandler);
    process.off('SIGTERM', abortHandler);
  }
});