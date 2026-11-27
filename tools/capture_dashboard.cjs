const { chromium } = require("playwright");
const fs = require("node:fs/promises");
const path = require("node:path");

async function main() {
  const [directory, browserPath] = process.argv.slice(2);
  const trace = JSON.parse(await fs.readFile(path.join(directory, "trace.json"), "utf8"));
  const base = "http://127.0.0.1:18082";
  const externalBrowser = browserPath?.startsWith("http://127.0.0.1:");
  const browser = externalBrowser ? await chromium.connectOverCDP(browserPath) : await chromium.launch({
    executablePath: browserPath,
    headless: true,
    args: ["--no-sandbox", "--disable-gpu"],
  });
  try {
    const page = externalBrowser
      ? browser.contexts()[0].pages()[0] || await browser.contexts()[0].newPage()
      : await browser.newPage();
    await page.setViewportSize({ width: 1560, height: 970 });
    const charts = [];
    for (const item of trace.message_trace) {
      await page.goto(base, { waitUntil: "domcontentloaded" });
      const traceNavigation = page.getByText(/^(MessageTrace|Message Trace|消息轨迹)$/).first();
      if (await traceNavigation.count()) {
        await traceNavigation.click();
      } else {
        await page.goto(base + "/#/messageTrace", { waitUntil: "domcontentloaded" });
      }
      await page.getByRole("tab", { name: "Message Key", exact: true }).waitFor({ timeout: 20000 });
      const panel = page.locator(".ant-tabs-tabpane-active");
      const topic = panel.locator(".ant-select");
      await topic.locator(".ant-select-selector").click();
      await topic.getByRole("combobox").fill(item.topic);
      await page.getByRole("option", { name: item.topic, exact: true }).click();
      await panel.locator("input.ant-input").fill(item.key);
      const query = page.waitForResponse((r) => r.url().includes("/message/queryMessageByTopicAndKey.query"));
      await page.getByRole("button", { name: /Search|搜索|查s*询/ }).first().click();
      const data = await (await query).json();
      if (data.status !== 0 || !data.data.some((r) => r.msgId === item.message_id)) {
        throw new Error("Dashboard query did not match " + item.key);
      }
      const graphResponse = page.waitForResponse((r) => r.url().includes("viewMessageTraceGraph.query"));
      await page.getByRole("row").filter({ hasText: item.message_id })
        .getByRole("button", { name: /Trace Detail|轨迹详情/ }).click();
      const graph = await (await graphResponse).json();
      if (graph.status !== 0 || !JSON.stringify(graph.data).includes(item.group)) {
        throw new Error("Dashboard graph did not contain the expected consumer " + item.group);
      }
      await page.waitForFunction(() => Array.from(document.querySelectorAll("canvas"))
        .some((canvas) => canvas.getBoundingClientRect().height > 100));
      await page.waitForTimeout(300);
      const file = "dashboard-" + item.role.toLowerCase() + "-" + item.type.toLowerCase() + ".png";
      await page.screenshot({ path: path.join(directory, file) });
      charts.push({ screenshot: file, messageId: item.message_id, group: item.group, graph: graph.data });
    }
    await fs.writeFile(path.join(directory, "dashboard-verified.json"),
      JSON.stringify({ session: trace.session, verified: charts.length, traces: charts }, null, 2));
    await fs.writeFile(path.join(directory, "capture.done"), "OK\n");
    console.log("DASHBOARD_PASS screenshots=" + charts.length);
  } finally {
    await browser.close();
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
