import { test, expect, type Reference, type Snapshot } from "./fixtures";

function terminal(snapshot: Snapshot) {
  return snapshot.events.filter(event => /^(board|repair)_(ready|failed|cancelled)$/.test(event.event));
}
async function one(reference: Reference, name: string) {
  await expect.poll(async () => terminal(await reference.snapshot()).map(event => event.event)).toEqual([name]);
  return terminal(await reference.snapshot())[0].properties;
}

test("current committed data emits one terminal event linked to real OTLP", async ({ page, reference }) => {
  await reference.reset();
  await page.goto(reference.frontendOrigin);
  const responsePromise = page.waitForResponse(response => response.url().startsWith(`${reference.apiOrigin}/api/data`) && response.request().method() === "GET");
  await page.getByRole("button", { name: "Board", exact: true }).click();
  const response = await responsePromise;
  expect(response.status()).toBe(200);
  await expect(page.getByRole("status")).toHaveText("Current items are ready.");
  await expect(page.getByRole("listitem")).toHaveText(["First item"]);
  await expect.poll(async () => (await reference.snapshot()).events.filter(event => event.event === "board_ready").length).toBe(1);
  const traceId = response.headers()["x-altimeter-trace-id"];
  expect(traceId).toMatch(/^[0-9a-f]{32}$/);
  await expect.poll(async () => (await reference.snapshot()).spans.filter(span => span.traceId === traceId && span.kind === 2).length).toBe(1);
  const snapshot = await reference.snapshot();
  expect(snapshot.events.filter(event => event.event === "board_ready")).toHaveLength(1);
  expect(JSON.stringify(snapshot.events)).toContain(traceId);
  expect(JSON.stringify(snapshot.events)).not.toContain("CANARY");
  expect(snapshot.wire.join("\n")).not.toContain("CANARY");
});

test('full navigation, empty commit and repeated completion produce one ready event', async ({ page, reference }) => {
  await reference.reset();
  await page.goto(reference.frontendOrigin + '/?start=board&scenario=empty');
  await expect(page.getByRole('status')).toHaveText('No items yet.');
  await expect(page.getByRole('listitem')).toHaveCount(0);
  await page.evaluate(() => { window.reference.finish(0); window.reference.finish(0); });
  const properties = await one(reference, 'board_ready');
  expect(properties).toMatchObject({ navigation_type: 'full', item_count: 0, outcome: 'success' });
  expect(properties.requests).toHaveLength(1);
});

test('SPA placeholder items cannot finish the next navigation before its required commit', async ({ page, reference }) => {
  await page.goto(reference.frontendOrigin + '/?start=board');
  await expect(page.getByRole('status')).toHaveText('Current items are ready.');
  await expect.poll(async () => terminal(await reference.snapshot()).some(event => event.event === 'board_ready')).toBe(true);
  await reference.reset();
  const started = page.waitForRequest(request => request.url().startsWith(reference.apiOrigin + '/api/data') && request.method() === 'GET');
  await page.getByRole('button', { name: 'Repair', exact: true }).click();
  await started;
  await expect(page.getByRole('listitem')).toHaveText(['First item']);
  await expect(page.getByRole('status')).toHaveText('Loading current items…');
  expect(terminal(await reference.snapshot())).toEqual([]);
  await expect(page.getByRole('status')).toHaveText('Current items are ready.');
  expect((await one(reference, 'repair_ready')).requests).toHaveLength(2);
});

test('real HTTP failure remains usable and Refresh starts a new successful navigation', async ({ page, reference }) => {
  await reference.reset();
  await page.goto(reference.frontendOrigin + '/?scenario=failure');
  await page.getByRole('button', { name: 'Board', exact: true }).click();
  await expect(page.getByRole('status')).toHaveText('Could not load items. Try Refresh.');
  const failed = await one(reference, 'board_failed');
  expect(failed.requests).toHaveLength(1);
  await page.evaluate(() => window.reference.setScenario('success'));
  await page.getByRole('button', { name: 'Refresh', exact: true }).click();
  await expect(page.getByRole('status')).toHaveText('Current items are ready.');
  await expect.poll(async () => terminal(await reference.snapshot()).map(event => event.event)).toEqual(['board_failed', 'board_ready']);
});

test('Cancel emits once and an aborted response never becomes ready', async ({ page, reference }) => {
  await reference.reset();
  await page.goto(reference.frontendOrigin);
  await page.getByRole('button', { name: 'Board', exact: true }).click();
  await page.getByRole('button', { name: 'Cancel', exact: true }).click();
  await expect(page.getByRole('status')).toHaveText('Loading cancelled.');
  await page.evaluate(() => { window.reference.cancel(); window.reference.finish(); });
  const properties = await one(reference, 'board_cancelled');
  expect(properties.cancel_reason).toBe('aborted');
  await page.waitForTimeout(400); // Past the controlled response's header + body delays.
  expect(terminal(await reference.snapshot())).toHaveLength(1);
});

test('consent off loads real data with no recorded selection, hints or analytics request', async ({ page, reference }) => {
  await reference.reset();
  const sent: string[] = [];
  page.on('request', request => { if (/\/(e|batch)\//.test(new URL(request.url()).pathname)) sent.push(request.url()); });
  await page.goto(reference.frontendOrigin + '/?consent=off');
  await page.getByRole('button', { name: 'Repair', exact: true }).click();
  await expect(page.getByRole('status')).toHaveText('Current items are ready.');
  expect(await page.evaluate(() => window.reference.attempt.isActive())).toBe(false);
  expect(await page.evaluate(() => window.reference.attempt.isSelected('detail', 'execution-1'))).toBe(false);
  const requests = (await reference.snapshot()).requests.filter(request => request.method === 'GET');
  expect(requests.length).toBeGreaterThanOrEqual(2);
  const latest = requests.slice(-2);
  for (const request of latest) expect(Object.keys(request.headers).join('\n')).not.toMatch(/x-posthog/i);
  await page.goto(reference.frontendOrigin + '/away');
  await page.waitForTimeout(150);
  expect(sent).toEqual([]);
  expect((await reference.snapshot()).events).toEqual([]);
});

test('revocation permanently discards an in-flight navigation even after regrant', async ({ page, reference }) => {
  await reference.reset();
  await page.goto(reference.frontendOrigin);
  const discardedTrace = await page.evaluate(() => {
    window.reference.begin();
    const pending = window.reference.request({ role: 'board', requestKey: 'CANARY_OLD_KEY', path: '/api/data?header_ms=150&body_ms=180' });
    window.reference.setConsent(false);
    window.reference.setConsent(true);
    return pending.then(response => { window.reference.finish(); return response.traceId; });
  });
  expect(discardedTrace).toBeNull();
  await page.waitForTimeout(150);
  expect((await reference.snapshot()).events).toEqual([]);
  await page.getByRole('button', { name: 'Board', exact: true }).click();
  await expect(page.getByRole('status')).toHaveText('Current items are ready.');
  await one(reference, 'board_ready');
});

test('WSGI shape preflights approved hints and commits both required roles', async ({ page, reference }) => {
  await reference.reset();
  const before = (await reference.snapshot()).requests.length;
  await page.goto(reference.frontendOrigin);
  await page.getByRole('button', { name: 'Repair', exact: true }).click();
  await expect(page.getByRole('status')).toHaveText('Current items are ready.');
  const properties = await one(reference, 'repair_ready');
  const selected = properties.requests as { role: string; trace_id: string; contributor_coverage: string }[];
  expect(selected.map(request => request.role)).toEqual(['detail', 'checklist']);
  expect(selected.every(request => request.contributor_coverage === 'complete')).toBe(true);
  const requests = (await reference.snapshot()).requests.slice(before);
  expect(requests.filter(request => request.method === 'OPTIONS').length).toBeGreaterThanOrEqual(1);
  for (const request of requests.filter(request => request.method === 'GET')) {
    const headers = Object.fromEntries(Object.entries(request.headers).map(([key, value]) => [key.toLowerCase(), value]));
    expect(headers['x-posthog-session-id']).toMatch(/^[0-9a-f-]+$/);
    expect(headers['x-posthog-distinct-id']).toBeTruthy();
  }
  for (const request of selected) {
    await expect.poll(async () => (await reference.snapshot()).spans.filter(span => span.traceId === request.trace_id && span.kind === 2).length).toBe(1);
  }
});

test('ASGI shape, denied app policy and different-port origin send no hints', async ({ page, reference }) => {
  await reference.reset();
  const before = (await reference.snapshot()).requests.length;
  await page.goto(reference.frontendOrigin);
  await page.getByRole('button', { name: 'Board', exact: true }).click();
  await expect(page.getByRole('status')).toHaveText('Current items are ready.');
  await page.evaluate(() => window.reference.setHintPolicy(false));
  await page.getByRole('button', { name: 'Repair', exact: true }).click();
  await expect(page.getByRole('status')).toHaveText('Current items are ready.');
  await page.evaluate(async foreign => {
    window.reference.setHintPolicy(true);
    window.reference.begin({ screen: 'repair', roles: ['detail'] });
    await window.reference.request({ role: 'detail', requestKey: 'foreign', path: foreign + '/foreign' });
    window.reference.finish(0);
  }, reference.restrictedOrigin);
  const snapshot = await reference.snapshot();
  const requests = snapshot.requests.slice(before);
  expect(requests.some(request => request.method === 'GET')).toBe(true);
  for (const request of [...requests, ...snapshot.foreignRequests]) {
    expect(Object.keys(request.headers).join('\n')).not.toMatch(/x-posthog/i);
  }
  expect(snapshot.foreignRequests.filter(request => request.method === 'GET')).toHaveLength(1);
});

test('latest retry remains the representative and partial coverage stays separate from sampling', async ({ page, reference }) => {
  await reference.reset();
  await page.goto(reference.frontendOrigin);
  const selectedTrace = await page.evaluate(async () => {
    window.reference.begin({ screen: 'repair', roles: ['media'] });
    for (let index = 0; index < 5; index++) {
      await window.reference.request({ role: 'media', requestKey: `CANARY_RETRY_${index}`,
        path: `/api/data?execution=${index}`, coverage: 'complete' });
    }
    const final = await window.reference.request({ role: 'media', requestKey: 'CANARY_FINAL',
      path: '/api/data?execution=final', coverage: 'partial' });
    window.reference.finish();
    return final.traceId;
  });
  const properties = await one(reference, 'repair_ready');
  expect(properties.requests).toEqual([expect.objectContaining({ role: 'media', trace_id: selectedTrace,
    sampled: true, contributor_coverage: 'partial', provenance: 'network' })]);
  expect(JSON.stringify(properties)).not.toContain('CANARY');
  expect(JSON.stringify(properties)).not.toContain('/api/data');
});

test('cache uncertainty retains no trace or timing from the real response', async ({ page, reference }) => {
  await reference.reset();
  await page.goto(reference.frontendOrigin);
  await page.evaluate(async () => {
    window.reference.begin();
    await window.reference.request({ role: 'board', requestKey: 'uncertain', path: '/api/data', provenance: 'cache_unverified' });
    window.reference.finish();
  });
  const properties = await one(reference, 'board_ready');
  expect(properties.requests).toEqual([expect.objectContaining({ trace_id: null, sampled: null,
    trace_missing_reason: 'cache_unverified', provenance: 'cache_unverified',
    timing: { before_request: null, first_byte_wait: null, receive_after_first_byte: null, post_transfer_until_response_complete: null } })]);
});

test('SDK finalizes INP and CLS on real page departure and emits only intended vitals', async ({ page, reference }) => {
  await reference.reset();
  await page.goto(reference.frontendOrigin + '/?vitals=on&CANARY_QUERY=private');
  await page.waitForFunction(() => !!(window as unknown as { __PosthogExtensions__?: { postHogWebVitalsCallbacks?: unknown } }).__PosthogExtensions__?.postHogWebVitalsCallbacks);
  // A real interaction and a later layout shift, not fabricated SDK callbacks.
  await page.evaluate(() => document.querySelector('#board')!.addEventListener('click', () => {
    const start = performance.now(); while (performance.now() - start < 90) { /* Controlled application work. */ }
  }));
  await page.getByRole('button', { name: 'Board', exact: true }).click();
  await expect(page.getByRole('status')).toHaveText('Current items are ready.');
  await page.waitForTimeout(600); // Layout shift is outside the recent-input exclusion window.
  await page.evaluate(() => { document.querySelector<HTMLElement>('main')!.style.marginTop = '80px'; });
  await page.waitForTimeout(150);
  expect((await reference.snapshot()).events.filter(event => event.event === '$web_vitals')).toEqual([]);
  await page.goto(reference.frontendOrigin + '/away');
  await expect.poll(async () => {
    const events = (await reference.snapshot()).events.filter(event => event.event === '$web_vitals');
    return { inp: events.some(event => typeof event.properties.$web_vitals_INP_value === 'number'),
      cls: events.some(event => typeof event.properties.$web_vitals_CLS_value === 'number') };
  }).toEqual({ inp: true, cls: true });
  const vitals = (await reference.snapshot()).events.filter(event => event.event === '$web_vitals');
  for (const event of vitals) {
    expect(Object.keys(event.properties).every(key => ['token', 'distinct_id', 'page_category', '$web_vitals_INP_value', '$web_vitals_CLS_value'].includes(key))).toBe(true);
    expect(event.properties.token).toBe('phc_reference_public_project');
    expect(event.properties.page_category).toBe('board');
  }
  expect(JSON.stringify(vitals)).not.toContain('CANARY');
  expect(JSON.stringify(vitals)).not.toContain(reference.frontendOrigin);
  expect(vitals.some(event => Number(event.properties.$web_vitals_INP_value) >= 80)).toBe(true);
  expect(vitals.some(event => Number(event.properties.$web_vitals_CLS_value) > 0)).toBe(true);
});

for (const width of [360, 768, 1440]) {
  test(`reference controls remain usable at ${width}px and 200% zoom`, async ({ page, reference }) => {
    await page.setViewportSize({ width, height: 900 });
    await page.emulateMedia({ reducedMotion: 'reduce' });
    await page.goto(reference.frontendOrigin);
    await page.keyboard.press('Tab');
    await expect(page.getByRole('button', { name: 'Board', exact: true })).toBeFocused();
    const focus = await page.locator('#board').evaluate(node => getComputedStyle(node).outlineWidth);
    expect(parseFloat(focus)).toBeGreaterThanOrEqual(3);
    expect(await page.evaluate(() => {
      function luminance(color: string) {
        const channels = color.match(/\d+/g)!.slice(0, 3).map(Number).map(value => {
          const channel = value / 255;
          return channel <= 0.04045 ? channel / 12.92 : ((channel + 0.055) / 1.055) ** 2.4;
        });
        return channels[0] * 0.2126 + channels[1] * 0.7152 + channels[2] * 0.0722;
      }
      const style = getComputedStyle(document.body);
      const light = luminance(style.backgroundColor);
      const dark = luminance(style.color);
      return (Math.max(light, dark) + 0.05) / (Math.min(light, dark) + 0.05);
    })).toBeGreaterThanOrEqual(4.5);
    const button = await page.locator('#board').boundingBox();
    expect(button!.height).toBeGreaterThanOrEqual(44);
    await page.keyboard.press('Enter');
    await expect(page.getByRole('status')).toHaveText('Current items are ready.');
    await expect(page.getByRole('heading', { name: 'Board', exact: true })).toBeVisible();
    expect(await page.locator('#board').evaluate(node => getComputedStyle(node).transitionDuration)).toBe('0s');
    for (const zoom of ['1', '2']) {
      await page.evaluate(value => { document.documentElement.style.zoom = value; }, zoom);
      expect(await page.evaluate(() => document.documentElement.scrollWidth <= document.documentElement.clientWidth)).toBe(true);
      await expect(page.getByRole('button', { name: 'Refresh', exact: true })).toBeVisible();
      await page.screenshot({ path: `/tmp/altimeter-browser-${width}-${zoom}.png`, fullPage: true });
    }
  });
}
