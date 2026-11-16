import { expect, test, type Page } from "@playwright/test";

// The single stream page and its four tabs, the activity and streams lists, and the overview's
// trends. Runs against mock mode (npm run dev:mock); see playwright.config.ts.

async function signedIn(page: Page) {
  await page.addInitScript(() => window.localStorage.setItem("rillkit.admin_token", "dev-admin-token"));
}

const tab = (page: Page, name: string) => page.getByRole("tab", { name });
const messageRows = (page: Page) => page.getByTestId("messages-table").locator("tbody tr[data-message-row]");
const dlqRows = (page: Page) => page.getByTestId("dlq-table").locator("tbody tr[data-dlq-row]");
const dlqTotal = async (page: Page) =>
  Number((await page.getByTestId("dlq-page").innerText()).match(/· ([\d,]+)/)![1].replace(/,/g, ""));

test("the four tabs, with the tab in the url", async ({ page }) => {
  await signedIn(page);
  await page.goto("/t/acme/streams/orders/billing");
  await expect(tab(page, "Statistics & state")).toHaveAttribute("data-state", "active");
  await expect(page.getByText("Lag", { exact: true }).first()).toBeVisible();
  await expect(page.getByText("Time per message")).toBeVisible();
  await expect(page.getByText("Queue wait", { exact: true })).toBeVisible();
  await expect(page.getByText("Work", { exact: true })).toBeVisible();
  await tab(page, "Messages").click();
  await expect(page).toHaveURL(/\?tab=messages$/);
  await tab(page, "Configs").click();
  await expect(page).toHaveURL(/\?tab=configs$/);
  await expect(page.getByRole("button", { name: /^Apply \d+ per node$/ })).toBeVisible();
  await page.reload();
  await expect(tab(page, "Configs")).toHaveAttribute("data-state", "active");
  await tab(page, "Statistics & state").click();
  await expect(page).toHaveURL(/billing$/);
});

test("messages: a snapshot as of a time, then live appending", async ({ page }) => {
  await signedIn(page);
  await page.goto("/t/acme/streams/orders/billing?tab=messages");
  await expect(messageRows(page)).toHaveCount(50);
  const asOf = await page.getByTestId("as-of").innerText();
  expect(asOf).toMatch(/^As of \d\d:\d\d:\d\d$/);
  const firstId = await messageRows(page).first().locator("td").nth(1).innerText();

  // the snapshot does not change by itself
  await page.waitForTimeout(3000);
  await expect(page.getByTestId("as-of")).toHaveText(asOf);
  await expect(messageRows(page).first().locator("td").nth(1)).toHaveText(firstId);

  // live: new messages are prepended, the earlier ones stay
  await page.getByRole("switch", { name: "Live" }).click();
  await expect(page.getByTestId("live-status")).toBeVisible();
  await expect(page.getByTestId("live-status")).not.toHaveText("Live · 0 new", { timeout: 8000 });
  await expect(messageRows(page).first().locator("td").nth(1)).not.toHaveText(firstId);
  await expect(page.getByTestId("messages-table").getByText(firstId)).toBeVisible();
  expect(await messageRows(page).count()).toBeGreaterThan(50);

  // off: frozen again, as of the last poll
  await page.getByRole("switch", { name: "Live" }).click();
  await expect(page.getByTestId("as-of")).toBeVisible();
  const frozen = await messageRows(page).count();
  await page.waitForTimeout(2500);
  expect(await messageRows(page).count()).toBe(frozen);
  await expect(page.getByRole("button", { name: "Load older" })).toBeVisible();
});

test("messages: search by id expands it in place, with payload, trace and logs", async ({ page }) => {
  await signedIn(page);
  await page.goto("/t/acme/streams/orders/billing?tab=messages");
  await expect(messageRows(page)).toHaveCount(50);
  const id = await messageRows(page).nth(10).locator("td").nth(1).innerText();
  await page.getByLabel("Search messages").fill(id);
  await page.getByRole("button", { name: "Open message" }).click();

  // the detail opens under the row that was clicked, not in a sheet
  const detail = page.getByTestId("message-detail");
  await expect(detail).toBeVisible();
  await expect(detail).toContainText(id);
  await expect(detail).toContainText("order_id");
  await expect(detail.getByLabel("Log records")).toBeVisible();
  await expect(messageRows(page).filter({ hasText: id })).toHaveAttribute("aria-expanded", "true");

  // the trace waterfall expands right there too
  await detail.getByRole("button", { name: "View trace" }).click();
  const trace = page.getByTestId("trace-view");
  await expect(trace).toBeVisible();
  await expect(trace).toContainText("Trace waterfall");
  await expect(trace).toContainText("orders");
  await expect(trace).toContainText("no errors");
  await expect(trace.getByText("Handled").first()).toBeVisible();
  await detail.locator('[data-action="view-trace"]').click();
  await expect(trace).toHaveCount(0);

  await detail.getByRole("button", { name: "Close message" }).click();
  await expect(page.getByTestId("message-detail")).toHaveCount(0);

  // other text filters the loaded rows
  await page.getByLabel("Search messages").fill("no-such-text-anywhere");
  await expect(page.getByText("Nothing matches")).toBeVisible();
});

test("payloads stay hidden where the workers do not allow them", async ({ page }) => {
  await signedIn(page);
  await page.goto("/t/acme/streams/payments.settled/ledger?tab=messages");
  await expect(page.getByText("Payloads are hidden")).toBeVisible();
  await expect(page.getByText("RILLKIT_INSPECT_MESSAGES=true").first()).toBeVisible();
  await expect(page.getByRole("columnheader", { name: "Payload" })).toHaveCount(0);
  await tab(page, "DLQ").click();
  await expect(dlqRows(page).first()).toContainText("hidden");
});

test("dlq: paging, replaying, editing and deleting", async ({ page }) => {
  // New failures keep arriving in the mock's DLQ, so check the entries acted on, not totals.
  const idOf = async (row: number) =>
    ((await dlqRows(page).nth(row).getByRole("checkbox").getAttribute("aria-label")) ?? "").replace("Select ", "");
  const gone = (id: string) => expect(page.getByRole("checkbox", { name: `Select ${id}` })).toHaveCount(0, { timeout: 10_000 });

  await signedIn(page);
  await page.goto("/t/acme/streams/orders/billing?tab=dlq");
  await expect(dlqRows(page)).toHaveCount(25);
  expect(await dlqTotal(page)).toBeGreaterThan(25);

  await page.getByRole("button", { name: "Next" }).click();
  await expect(page.getByTestId("dlq-page")).toContainText("Page 2");
  await page.getByRole("button", { name: "Previous" }).click();
  await expect(page.getByTestId("dlq-page")).toContainText("Page 1");

  // replay two selected entries: they leave the DLQ
  const [a, b] = [await idOf(0), await idOf(1)];
  await dlqRows(page).nth(0).getByRole("checkbox").click();
  await dlqRows(page).nth(1).getByRole("checkbox").click();
  await expect(page.getByText("2 selected")).toBeVisible();
  await page.getByRole("button", { name: "Replay selected" }).click();
  await gone(a);
  await gone(b);
  await expect(page.getByText("replayed 2").first()).toBeVisible();

  // edit one entry's payload, then replay it
  const edited = await idOf(0);
  await dlqRows(page).first().click();
  const detail = page.getByTestId("message-detail");
  await expect(detail).toContainText("Why it failed");
  await detail.getByRole("button", { name: "Edit and replay" }).click();
  const editor = detail.getByLabel("Payload to replay");
  await editor.fill("{ not json");
  await expect(detail.getByRole("alert")).toContainText("Not valid JSON");
  await expect(detail.getByRole("button", { name: "Replay edited" })).toBeDisabled();
  await editor.fill('{"order_id": 1, "amount": 10}');
  await detail.getByRole("button", { name: "Replay edited" }).click();
  await gone(edited);

  // delete one
  const deleted = await idOf(0);
  await dlqRows(page).first().click();
  await page.getByTestId("message-detail").getByRole("button", { name: "Delete" }).click();
  await page.getByRole("alertdialog").getByRole("button", { name: "Delete" }).click();
  await gone(deleted);

  // purging asks first; cancelling keeps everything. New failures arrive and directives refresh
  // asynchronously, so check the DLQ is still not empty rather than one particular row or count.
  await page.getByRole("button", { name: "Purge" }).click();
  await expect(page.getByRole("alertdialog")).toContainText("deleted for good");
  await page.getByRole("alertdialog").getByRole("button", { name: "Cancel" }).click();
  await expect(page.getByRole("alertdialog")).toHaveCount(0);
  expect(await dlqTotal(page)).toBeGreaterThan(0);
});

test("activity is paged on the server and filtered by source", async ({ page }) => {
  await signedIn(page);
  await page.goto("/t/acme/activity");
  await expect(page.getByTestId("activity-page")).toContainText("Page 1 of 3");
  await expect(page.locator("[data-directive]")).toHaveCount(25);
  await page.getByRole("button", { name: "Next" }).click();
  await expect(page.getByTestId("activity-page")).toContainText("Page 2 of 3");
  await page.getByRole("radio", { name: "Autoscaler" }).click();
  await expect(page.getByTestId("activity-page")).toContainText("Page 1 of 2");
  for (const row of await page.locator("[data-directive]").all()) await expect(row).toContainText("Autoscaler");
});

test("streams can be searched and created", async ({ page }) => {
  await signedIn(page);
  await page.goto("/t/acme/streams");
  await expect(page.locator("[data-stream]")).toHaveCount(3);
  await page.getByLabel("Search streams").fill("pay");
  await expect(page.locator("[data-stream]")).toHaveCount(1);
  await page.getByLabel("Search streams").fill("zzz");
  await expect(page.getByText("No stream matches")).toBeVisible();
  await page.getByLabel("Search streams").fill("");

  await page.getByRole("button", { name: "New stream" }).click();
  await page.getByLabel("Stream", { exact: true }).fill("invoices");
  await page.getByLabel("Group", { exact: true }).fill("mailer");
  await page.getByRole("radio", { name: "Everything already there" }).click();
  await page.getByRole("button", { name: "Create stream" }).click();
  await expect(page).toHaveURL(/\/streams\/invoices\/mailer$/);
  await expect(page.getByText("No consumers").first()).toBeVisible();
  // (in-app navigation: a reload would restart the in-memory mock)
  await page.getByRole("link", { name: "All streams" }).click();
  await expect(page.locator('[data-stream="invoices"]')).toContainText("No consumers");
});

test("the overview shows throughput and its trends", async ({ page }) => {
  await signedIn(page);
  await page.goto("/t/acme");
  await expect(page.getByRole("heading", { name: "Throughput" })).toBeVisible();
  for (const label of ["Messages handled", "Success rate", "Throughput", "Data rate", "In the DLQ", "Dead-lettered"])
    await expect(page.locator(`[data-trend-kpi="${label}"]`)).toHaveText(/\d/);
  await expect(page.locator('[data-trend="better"], [data-trend="worse"]').first()).toBeVisible();
  await expect(page.locator(".recharts-surface")).toHaveCount(4);
  await page.getByRole("radio", { name: "24 hours" }).click();
  await expect(page.getByText("Every stream of this tenant, compared with the previous 24 hours.")).toBeVisible();
});
