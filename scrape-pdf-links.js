const { chromium } = require('playwright');
const fs = require('fs/promises');
const path = require('path');

const START_URL =
  'https://www.justice.gov/epstein/doj-disclosures/data-set-9-files';
const OUTPUT_PATH = path.resolve(__dirname, '..', 'pdf-links.txt');
const MAX_PAGES = 50;

const normalizeUrl = (href, baseUrl) => {
  try {
    return new URL(href, baseUrl).toString();
  } catch (error) {
    return null;
  }
};

const getPdfLinksOnPage = async (page) => {
  const hrefs = await page
    .locator('a[href$=".pdf"]')
    .evaluateAll((anchors) =>
      anchors.map((anchor) => anchor.getAttribute('href')).filter(Boolean)
    );

  return hrefs
    .map((href) => normalizeUrl(href, page.url()))
    .filter((href) => Boolean(href));
};

const getNextPageHref = async (page) => {
  const pagerNext = page.locator('li.pager__item--next a');
  if ((await pagerNext.count()) > 0) {
    return pagerNext.first().getAttribute('href');
  }

  const exactNext = page.locator('a').filter({ hasText: /^Next$/ });
  if ((await exactNext.count()) > 0) {
    return exactNext.first().getAttribute('href');
  }

  return null;
};

const main = async () => {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();

  const collected = new Set();
  const visitedPages = new Set();
  let nextUrl = START_URL;

  for (let pageIndex = 0; pageIndex < MAX_PAGES; pageIndex += 1) {
    if (!nextUrl || visitedPages.has(nextUrl)) {
      break;
    }

    visitedPages.add(nextUrl);

    const response = await page.request.get(nextUrl);
    if (!response.ok()) {
      throw new Error(`Failed to load page: ${nextUrl} (${response.status()})`);
    }

    const html = await response.text();
    await page.setContent(html, { waitUntil: 'domcontentloaded' });

    const links = await getPdfLinksOnPage(page);
    links.forEach((link) => collected.add(link));

    const nextHref = await getNextPageHref(page);
    if (!nextHref) {
      break;
    }

    nextUrl = normalizeUrl(nextHref, nextUrl);
  }

  const sortedLinks = Array.from(collected).sort();
  await fs.writeFile(OUTPUT_PATH, `${sortedLinks.join('\n')}\n`);

  console.log(`Saved ${sortedLinks.length} links to ${OUTPUT_PATH}`);

  await browser.close();
};

main().catch((error) => {
  console.error('Failed to collect PDF links:', error);
  process.exitCode = 1;
});
