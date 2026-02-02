import { test } from '@playwright/test';
import type { APIRequestContext } from '@playwright/test';
import fs from 'fs/promises';
import path from 'path';

const BASE_URL = 'https://www.justice.gov/epstein/doj-disclosures/data-set-9-files';
const MAX_PAGE = 1000000003;
const IS_DEBUG = Boolean(process.env.PWDEBUG);
const CONCURRENCY = 1
const EMPTY_PAGES_BEFORE_STOP = 30;
const CHECKPOINT_INTERVAL = 50; // Save progress every N batches
const MAX_RETRIES = 5;
const RETRY_BASE_DELAY_MS = 2000;
const BATCH_DELAY_MS = parseInt(process.env.BATCH_DELAY || '500', 10); // Delay between batches
const BLOCKED_BACKOFF_MS = 30000; // Wait 30s when blocked

// HTTP headers to mimic browser behavior
const REQUEST_HEADERS = {
  'User-Agent':
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
  'Accept-Language': 'en-US,en;q=0.5',
};

test.use({ headless: !IS_DEBUG });

interface ProgressState {
  lastProcessedPage: number;
  totalLinksFound: number;
  timestamp: string;
}

const formatPageUrl = (pageNumber: number) => `${BASE_URL}?page=${pageNumber}`;

const readExistingLinks = async (filePath: string): Promise<string[]> => {
  try {
    const contents = await fs.readFile(filePath, 'utf8');
    const lines = contents
      .split('\n')
      .map((line) => line.trim())
      .filter(Boolean);
    const deduped: string[] = [];
    const seen = new Set<string>();
    for (const line of lines) {
      if (!seen.has(line)) {
        seen.add(line);
        deduped.push(line);
      }
    }
    return deduped;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
      return [];
    }
    throw error;
  }
};

const writeLinks = async (filePath: string, links: string[]) => {
  const output = links.length > 0 ? `${links.join('\n')}\n` : '';
  await fs.writeFile(filePath, output, 'utf8');
};

const readProgress = async (filePath: string): Promise<ProgressState | null> => {
  try {
    const contents = await fs.readFile(filePath, 'utf8');
    return JSON.parse(contents) as ProgressState;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
      return null;
    }
    // If file exists but is corrupted, start fresh
    console.warn('Progress file corrupted, starting fresh');
    return null;
  }
};

const writeProgress = async (filePath: string, state: ProgressState) => {
  await fs.writeFile(filePath, JSON.stringify(state, null, 2), 'utf8');
};

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

type FetchResult =
  | { status: 'ok'; html: string }
  | { status: 'blocked' }
  | { status: 'error' }
  | { status: 'empty' };

const isBlockedResponse = (html: string): boolean => {
  return (
    html.includes('Access Denied') ||
    html.includes("don't have permission") ||
    html.includes('Reference #') ||
    html.includes('errors.edgesuite.net')
  );
};

const fetchWithRetry = async (
  request: APIRequestContext,
  url: string,
  retries = MAX_RETRIES
): Promise<FetchResult> => {
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const response = await request.get(url, { headers: REQUEST_HEADERS });
      const html = await response.text();

      // Check for blocked response even if status is 200
      if (isBlockedResponse(html)) {
        if (attempt < retries) {
          const delay = BLOCKED_BACKOFF_MS * (attempt + 1);
          console.warn(`\nBlocked by server, waiting ${delay / 1000}s before retry ${attempt + 1}/${retries}...`);
          await sleep(delay);
          continue;
        }
        return { status: 'blocked' };
      }

      if (response.ok()) {
        return { status: 'ok', html };
      }

      // Rate limited (403/429) or server error - retry with backoff
      if (response.status() === 403 || response.status() === 429 || response.status() >= 500) {
        if (attempt < retries) {
          const delay = RETRY_BASE_DELAY_MS * Math.pow(2, attempt);
          console.warn(`\nRequest failed (${response.status()}), retrying in ${delay}ms...`);
          await sleep(delay);
          continue;
        }
      }

      return { status: 'error' };
    } catch (error) {
      if (attempt < retries) {
        const delay = RETRY_BASE_DELAY_MS * Math.pow(2, attempt);
        console.warn(`\nRequest error, retrying in ${delay}ms...`, error);
        await sleep(delay);
        continue;
      }
      return { status: 'error' };
    }
  }
  return { status: 'error' };
};

test('scrape DOJ pdf links', async ({ page }) => {
  test.setTimeout(IS_DEBUG ? 0 : 60 * 60 * 1000); // 1 hour timeout for long scrapes

  const outputPath = path.resolve(__dirname, '..', 'pdf-links.txt');
  const progressPath = path.resolve(__dirname, '..', 'pdf-links.progress.json');

  // Load existing progress and links
  const existingProgress = await readProgress(progressPath);
  const existingLinks = await readExistingLinks(outputPath);
  const uniqueLinks = new Set(existingLinks);
  const allLinks = [...existingLinks];

  // Determine start page from progress or environment
  const envStartPage = process.env.START_PAGE ? parseInt(process.env.START_PAGE, 10) : null;
  const resumePage = existingProgress?.lastProcessedPage ?? -1;
  const startPage = envStartPage ?? resumePage + 1;

  console.log(`Output: ${outputPath}`);
  console.log(`Progress: ${progressPath}`);
  console.log(`Concurrency: ${CONCURRENCY} (batch delay: ${BATCH_DELAY_MS}ms)`);
  console.log(`Starting from page: ${startPage} (resumed: ${resumePage >= 0})`);
  console.log(`Existing links: ${existingLinks.length}`);
  console.log(`Tip: If blocked, try: CONCURRENCY=5 BATCH_DELAY=2000 npx playwright test`);

  // Navigate to first page and handle verification
  await page.goto(formatPageUrl(startPage), { waitUntil: 'domcontentloaded' });
  try {
    await page.getByRole('button', { name: 'I am not a robot' }).click({ timeout: 3000 });
  } catch {
    // Ignore if the button is not present.
  }
  try {
    await page.getByRole('button', { name: 'Yes' }).click({ timeout: 3000 });
  } catch {
    // Ignore if the button is not present.
  }

  let pagesProcessed = 0;
  let linksFoundThisRun = 0;
  let newLinksThisRun = 0;
  let lastLogLength = 0;
  const logEnabled = process.stdout.isTTY ?? true;
  let emptyPagesInARow = 0;
  let batchCount = 0;

  const logProgress = (message: string) => {
    if (!logEnabled) {
      return;
    }
    const padded = message.padEnd(lastLogLength, ' ');
    process.stdout.write(`\r${padded}`);
    lastLogLength = Math.max(lastLogLength, message.length);
  };

  const saveCheckpoint = async (lastPage: number) => {
    const state: ProgressState = {
      lastProcessedPage: lastPage,
      totalLinksFound: uniqueLinks.size,
      timestamp: new Date().toISOString(),
    };
    await writeProgress(progressPath, state);
    await writeLinks(outputPath, allLinks);
  };

  let shouldStop = false;
  let lastProcessedPage = startPage - 1;
  let blockedCount = 0;
  const MAX_BLOCKED_BEFORE_STOP = 3; // Stop after 3 blocked batches

  for (let start = startPage; start <= MAX_PAGE && !shouldStop; start += CONCURRENCY) {
    const batchPages = Array.from({ length: CONCURRENCY }, (_, index) => start + index).filter(
      (pageNumber) => pageNumber <= MAX_PAGE
    );

    const batchResults = await Promise.all(
      batchPages.map(async (pageNumber) => {
        const result = await fetchWithRetry(page.request, formatPageUrl(pageNumber));
        if (result.status === 'ok') {
          const hrefs = Array.from(result.html.matchAll(/href="([^"]+\.pdf)"/gi)).map((match) => match[1]);
          return { pageNumber, hrefs, blocked: false };
        }
        return { pageNumber, hrefs: [] as string[], blocked: result.status === 'blocked' };
      })
    );

    batchResults.sort((a, b) => a.pageNumber - b.pageNumber);

    // Check if entire batch was blocked
    const blockedInBatch = batchResults.filter((r) => r.blocked).length;
    if (blockedInBatch === batchResults.length) {
      blockedCount += 1;
      process.stdout.write('\n');
      console.log(`Entire batch blocked (${blockedCount}/${MAX_BLOCKED_BEFORE_STOP}). Waiting ${BLOCKED_BACKOFF_MS / 1000}s...`);

      if (blockedCount >= MAX_BLOCKED_BEFORE_STOP) {
        console.log('Too many blocked requests. Server is rate limiting. Try again later with lower concurrency.');
        console.log('Tip: CONCURRENCY=5 BATCH_DELAY=2000 npx playwright test test-doj.spec.ts');
        shouldStop = true;
        break;
      }

      await sleep(BLOCKED_BACKOFF_MS);
      start -= CONCURRENCY; // Retry this batch
      continue;
    }

    // Reset blocked count if we got through
    if (blockedInBatch === 0) {
      blockedCount = 0;
    }

    for (const result of batchResults) {
      if (result.blocked) {
        continue; // Skip blocked pages, will retry on next run
      }

      lastProcessedPage = result.pageNumber;

      if (result.pageNumber === startPage) {
        console.log(`Page ${startPage} PDF links found: ${result.hrefs.length}`);
      }

      if (result.hrefs.length === 0) {
        emptyPagesInARow += 1;
        if (emptyPagesInARow >= EMPTY_PAGES_BEFORE_STOP) {
          process.stdout.write('\n');
          console.log(
            `No PDF links found on ${EMPTY_PAGES_BEFORE_STOP} consecutive pages (last page=${result.pageNumber}). Stopping.`
          );
          shouldStop = true;
          break;
        }
        continue;
      }

      emptyPagesInARow = 0;
      pagesProcessed += 1;
      linksFoundThisRun += result.hrefs.length;

      for (const href of result.hrefs) {
        const absoluteUrl = new URL(href, BASE_URL).toString();
        if (!uniqueLinks.has(absoluteUrl)) {
          uniqueLinks.add(absoluteUrl);
          allLinks.push(absoluteUrl);
          newLinksThisRun += 1;
        }
      }

      logProgress(
        `Page: ${result.pageNumber} | Found: ${linksFoundThisRun} | New: ${newLinksThisRun} | Total: ${uniqueLinks.size}`
      );
    }

    batchCount += 1;

    // Periodic checkpoint
    if (batchCount % CHECKPOINT_INTERVAL === 0) {
      process.stdout.write('\n');
      console.log(`Checkpoint at page ${lastProcessedPage}. Saving ${allLinks.length} links...`);
      await saveCheckpoint(lastProcessedPage);
    }

    // Delay between batches to avoid rate limiting
    if (BATCH_DELAY_MS > 0 && !shouldStop) {
      await sleep(BATCH_DELAY_MS);
    }
  }

  // Final save
  await saveCheckpoint(lastProcessedPage);

  process.stdout.write('\n');
  console.log(
    `Done. Processed ${pagesProcessed} pages. Added ${newLinksThisRun} new links. Total unique: ${uniqueLinks.size}.`
  );
});