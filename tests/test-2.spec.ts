import { test, expect } from '@playwright/test';

test('test', async ({ page }) => {
  await page.goto('https://www.justice.gov/epstein/doj-disclosures/data-set-9-files?page=1');
  await page.getByRole('button', { name: 'I am not a robot' }).click();
  await page.waitForTimeout(1000);
  await page.locator('.layout__region.layout__region--second > .block.block-general.block-layout-builder.block-inline-blockgeneral.bg-full-width.bg-none').click();
  await page.waitForTimeout(1000);
  await page.getByRole('link', { name: 'Download all files (.zip)' }).click();
  await page.waitForTimeout(1000);
  const downloadPromise = page.waitForEvent('download');
  await page.waitForTimeout(1000);
  await page.getByRole('button', { name: 'Yes' }).click();
  await page.waitForTimeout(10000);
  const download = await downloadPromise;
  await download.saveAs('downloads/data-set-9-files.zip');
  await page.waitForTimeout(10000);
});