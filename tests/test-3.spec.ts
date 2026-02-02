import { test, expect } from '@playwright/test';

test('test', async ({ page }) => {
  await page.goto('https://www.justice.gov/epstein/doj-disclosures/data-set-9-files?page=1');
  await page.getByRole('button', { name: 'I am not a robot' }).click();
  await page.getByRole('button', { name: 'Yes' }).click();
  await page.getByRole('link', { name: 'Next Data Set' }).click();
  await page.getByRole('link', { name: 'Previous Data Set' }).click();
  await page.getByRole('link', { name: 'Previous Data Set' }).click();
});