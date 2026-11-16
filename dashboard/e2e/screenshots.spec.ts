import { expect, test, type Page } from "@playwright/test";

// Captures each screen into e2e/screenshots/ (gitignored) for a visual review.
// Run with: npx playwright test screenshots

const DIR = "e2e/screenshots";

async function signedIn(page: Page, theme: "light" | "dark" = "light") {
  await page.addInitScript((t) => {
    window.localStorage.setItem("rillkit.admin_token", "dev-admin-token");
    window.localStorage.setItem("rillkit.theme", t);
  }, theme);
}

async function settle(page: Page) {
  await page.evaluate(() => document.fonts.ready);
  await page.waitForTimeout(600);
}

test("login", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByRole("heading", { name: "Sign in" })).toBeVisible();
  await settle(page);
  await page.screenshot({ path: `${DIR}/01-login.png`, fullPage: true });
});

test("overview", async ({ page }) => {
  await signedIn(page);
  await page.goto("/t/acme");
  await expect(page.locator("[data-node]").first()).toBeVisible();
  await settle(page);
  await page.screenshot({ path: `${DIR}/02-overview.png`, fullPage: true });
  await page.getByRole("button", { name: "Pause all" }).click();
  await settle(page);
  await page.screenshot({ path: `${DIR}/03-confirm-pause.png` });
});

test("streams and detail", async ({ page }) => {
  await signedIn(page);
  await page.goto("/t/acme/streams");
  await expect(page.locator("[data-stream]").first()).toBeVisible();
  await settle(page);
  await page.screenshot({ path: `${DIR}/04-streams.png`, fullPage: true });

  await page.goto("/t/acme/streams/orders/billing");
  await expect(page.locator(".recharts-surface").first()).toBeVisible();
  await settle(page);
  await page.screenshot({ path: `${DIR}/05-stream-detail.png`, fullPage: true });

  await page.getByRole("tab", { name: "Messages" }).click();
  await expect(page.getByTestId("messages-table")).toBeVisible();
  await settle(page);
  await page.screenshot({ path: `${DIR}/06a-messages.png`, fullPage: true });
  await page.getByTestId("messages-table").locator("tbody tr[data-message-row]").nth(3).click();
  await expect(page.getByTestId("message-detail")).toBeVisible();
  await settle(page);
  await page.screenshot({ path: `${DIR}/06b-message-detail.png`, fullPage: true });

  await page.getByRole("tab", { name: "DLQ" }).click();
  await expect(page.getByTestId("dlq-table")).toBeVisible();
  await page.getByTestId("dlq-table").locator("tbody tr[data-dlq-row]").nth(0).getByRole("checkbox").click();
  await page.getByTestId("dlq-table").locator("tbody tr[data-dlq-row]").nth(2).getByRole("checkbox").click();
  await settle(page);
  await page.screenshot({ path: `${DIR}/06c-dlq.png`, fullPage: true });
  await page.getByTestId("dlq-table").locator("tbody tr[data-dlq-row]").nth(1).click();
  await page.getByTestId("message-detail").getByRole("button", { name: "Edit and replay" }).click();
  await settle(page);
  await page.screenshot({ path: `${DIR}/06d-dlq-edit.png`, fullPage: true });

  await page.getByRole("tab", { name: "Configs" }).click();
  await settle(page);
  await page.screenshot({ path: `${DIR}/06e-configs.png`, fullPage: true });

  await page.goto("/t/acme/streams/payments.settled/ledger?tab=messages");
  await expect(page.getByText("Payloads are hidden")).toBeVisible();
  await settle(page);
  await page.screenshot({ path: `${DIR}/06f-hidden-payloads.png` });
});

test("activity", async ({ page }) => {
  await signedIn(page);
  await page.goto("/t/acme/activity");
  await expect(page.locator("[data-directive]").first()).toBeVisible();
  await page.locator("[data-directive]").nth(1).click();
  await settle(page);
  await page.screenshot({ path: `${DIR}/07-activity.png`, fullPage: true });
});

test("settings", async ({ page }) => {
  await signedIn(page);
  await page.goto("/t/acme/settings");
  await expect(page.getByTestId("env-block")).toBeVisible();
  await settle(page);
  await page.screenshot({ path: `${DIR}/08-settings.png`, fullPage: true });
  await page.getByRole("button", { name: "New key" }).click();
  await page.getByRole("button", { name: "Create key" }).click();
  await expect(page.getByTestId("secret-dialog")).toBeVisible();
  await settle(page);
  await page.screenshot({ path: `${DIR}/09-secret-dialog.png` });
});

test("new tenant dialog", async ({ page }) => {
  await signedIn(page);
  await page.goto("/t/acme");
  await page.getByRole("button", { name: /Switch tenant/ }).click();
  await page.getByRole("menuitem", { name: "New tenant" }).click();
  await expect(page.getByRole("dialog")).toBeVisible();
  await settle(page);
  await page.screenshot({ path: `${DIR}/10-new-tenant.png` });
});

test("dark theme", async ({ page }) => {
  await signedIn(page, "dark");
  await page.goto("/t/acme");
  await expect(page.locator("[data-node]").first()).toBeVisible();
  await settle(page);
  await page.screenshot({ path: `${DIR}/11-overview-dark.png`, fullPage: true });
  await page.goto("/t/acme/streams/orders/billing");
  await expect(page.locator(".recharts-surface").first()).toBeVisible();
  await settle(page);
  await page.screenshot({ path: `${DIR}/12-stream-dark.png`, fullPage: true });
});

test("tablet", async ({ page }) => {
  await page.setViewportSize({ width: 820, height: 1100 });
  await signedIn(page);
  await page.goto("/t/acme");
  await expect(page.locator("[data-node]").first()).toBeVisible();
  await settle(page);
  await page.screenshot({ path: `${DIR}/13-overview-tablet.png`, fullPage: true });
});
