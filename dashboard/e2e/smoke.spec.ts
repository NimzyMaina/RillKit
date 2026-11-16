import { expect, test, type Page } from "@playwright/test";

// Runs against mock mode (npm run dev:mock); see playwright.config.ts.

async function signIn(page: Page) {
  await page.goto("/");
  await page.getByLabel("Admin token").fill("dev-admin-token");
  await page.getByRole("button", { name: "Continue" }).click();
  await expect(page).toHaveURL(/\/t\/acme$/);
}

test("login shows the overview with live KPIs", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByRole("heading", { name: "Sign in" })).toBeVisible();
  // An empty token is refused client-side.
  await page.getByRole("button", { name: "Continue" }).click();
  await expect(page.getByText("Paste the server's admin token.")).toBeVisible();

  await signIn(page);
  for (const label of ["Nodes online", "Active workers", "In flight", "Total lag", "Ack rate"]) {
    await expect(page.locator(`[data-kpi="${label}"]`)).toHaveText(/\d/);
  }
  await expect(page.locator(`[data-kpi="Nodes online"]`)).toHaveText("3");
  await expect(page.locator("[data-node]")).toHaveCount(4);
  await expect(page.getByRole("status").filter({ hasText: "Live" })).toBeVisible();
});

test("pausing a stream changes its state", async ({ page }) => {
  await signIn(page);
  await page.getByRole("navigation", { name: "Main" }).first().getByRole("link", { name: "Streams" }).click();
  await page.getByRole("link", { name: "orders" }).click();
  await expect(page.getByRole("heading", { level: 1 })).toContainText("orders");
  const header = page.locator("header").filter({ has: page.getByRole("heading", { level: 1 }) });
  await expect(header.locator("[data-state-badge]")).toHaveAttribute("data-state-badge", "RUNNING");

  await page.locator('[data-action="pause-stream"]').click();
  await page.getByRole("alertdialog").getByRole("button", { name: "Pause stream" }).click();

  await expect(header.locator("[data-state-badge]")).toHaveAttribute("data-state-badge", "PAUSED", { timeout: 8000 });
  await expect(page.getByText(/Applied on/)).toBeVisible();

  // The directive shows up in Activity, applied.
  await page.getByRole("navigation", { name: "Main" }).first().getByRole("link", { name: "Activity" }).click();
  await page.getByRole("radio", { name: "User" }).click();
  const row = page.locator("[data-directive]").first();
  await expect(row).toContainText("SET_STATE");
  await expect(row).toContainText("APPLIED");
});

test("creating an API key shows the secret once", async ({ page }) => {
  await signIn(page);
  await page.getByRole("navigation", { name: "Main" }).first().getByRole("link", { name: "Settings" }).click();
  await page.getByRole("button", { name: "New key" }).click();
  await page.getByLabel("Label (optional)").fill("smoke test");
  await page.getByRole("button", { name: "Create key" }).click();

  const dialog = page.getByTestId("secret-dialog");
  await expect(dialog).toBeVisible();
  await expect(dialog.getByTestId("secret-value")).toHaveText(/^rk_[\w-]{20,}$/);
  await expect(dialog).toContainText("only time you will see it");
  await dialog.getByRole("button", { name: "I have stored it" }).click();
  await expect(dialog).toBeHidden();

  // The table only shows the prefix, never the secret.
  await expect(page.locator("[data-key]").filter({ hasText: "smoke test" })).toContainText("…");
  await expect(page.getByTestId("env-block")).toContainText("RILLKIT_API_KEY=<paste your key>");
});

test("global, node and stream controls all apply", async ({ page }) => {
  await signIn(page);
  const h1 = page.getByRole("heading", { level: 1 });

  // Maintenance on, then off.
  await page.getByRole("switch", { name: "Maintenance" }).click();
  await expect(h1).toContainText("maintenance", { timeout: 8000 });
  await page.getByRole("switch", { name: "Maintenance" }).click();
  await expect(h1).toContainText("flowing", { timeout: 8000 });

  // Drain all (confirmed), then resume.
  await page.getByRole("button", { name: "Drain" }).click();
  await page.getByRole("alertdialog").getByRole("button", { name: "Drain all" }).click();
  await expect(h1).toContainText("draining", { timeout: 8000 });
  await page.getByRole("button", { name: "Resume" }).click();
  await expect(h1).toContainText("flowing", { timeout: 8000 });

  // Node concurrency: worker-eu-2 has two subscriptions, so 6 lanes each is 12.
  await page.getByRole("button", { name: "Actions for worker-eu-2" }).click();
  await page.getByRole("menuitem", { name: "Set concurrency…" }).click();
  await page.getByRole("dialog").getByRole("spinbutton").fill("6");
  await page.getByRole("dialog").getByRole("button", { name: "Apply 6" }).click();
  await expect(page.locator('[data-node="worker-eu-2"]')).toContainText("12 / 12", { timeout: 8000 });

  // Stream configs: poll settings and the autoscaling policy.
  await page.goto("/t/acme/streams/emails%3Aoutbound/mailer?tab=configs");
  await page.getByLabel("Block (ms)").fill("3000");
  await page.getByRole("button", { name: "Apply poll settings" }).click();
  await expect(page.getByText("Applied on", { exact: false }).first()).toBeVisible({ timeout: 8000 });

  await page.getByLabel("Max workers").fill("12");
  await page.getByRole("button", { name: "Create policy" }).click();
  await expect(page.getByText("Autoscaling policy saved")).toBeVisible();
  await expect(page.getByRole("button", { name: "Save policy" })).toBeVisible();
});

test("a new tenant starts empty, and sign out returns to login", async ({ page }) => {
  await signIn(page);
  await page.getByRole("button", { name: /Switch tenant/ }).click();
  await page.getByRole("menuitem", { name: "New tenant" }).click();
  await page.getByLabel("Id").fill("acme");
  await page.getByLabel("Name").fill("Duplicate");
  await page.getByRole("button", { name: "Create tenant" }).click();
  await expect(page.getByText("already exists")).toBeVisible();

  await page.getByLabel("Id").fill("t-new");
  await page.getByRole("switch", { name: "Enforce keys" }).click();
  await page.getByRole("button", { name: "Create tenant" }).click();
  await expect(page).toHaveURL(/\/t\/t-new$/);
  await expect(page.getByRole("heading", { level: 1 })).toContainText("No workers connected");
  await expect(page.getByRole("button", { name: "Pause all" })).toBeDisabled();

  await page.getByRole("button", { name: "Sign out" }).click();
  await expect(page.getByRole("heading", { name: "Sign in" })).toBeVisible();
  expect(await page.evaluate(() => localStorage.getItem("rillkit.admin_token"))).toBeNull();
});
