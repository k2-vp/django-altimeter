import { test, expect, type Snapshot } from './fixtures';

function selectedTiming(snapshot: Snapshot): Record<string, unknown> | null {
  const ready = snapshot.events.find(item => item.event === 'board_ready');
  const requests = ready?.properties.requests;
  if (!Array.isArray(requests)) return null;
  const request = requests.find(item => item.role === 'board');
  return request?.timing ?? null;
}

test('cross-origin axios supplies bounded header and transfer phases linked to the actual SERVER span', async ({ page, reference }) => {
  await reference.reset();
  await page.goto(reference.frontendOrigin);
  const responsePending = page.waitForResponse(response =>
    response.url().startsWith(reference.apiOrigin + '/api/data') && response.request().method() === 'GET');
  await page.getByRole('button', { name: 'Board', exact: true }).click();
  const response = await responsePending;
  expect(response.status()).toBe(200);
  await expect(page.getByRole('status')).toHaveText('Current items are ready.');
  const headers = response.headers();
  expect(headers['timing-allow-origin']).toBe(reference.frontendOrigin);
  expect(headers['vary'].toLowerCase().split(',').map(value => value.trim())).toEqual(expect.arrayContaining(['cookie', 'origin']));
  expect(headers['cache-control']).toContain('private');
  expect(headers['cache-control']).toContain('no-store');
  const traceId = headers['x-altimeter-trace-id'];
  expect(traceId).toMatch(/^[0-9a-f]{32}$/);
  expect(headers['x-altimeter-trace-sampled']).toBe('1');
  await expect.poll(async () => (await reference.snapshot()).spans.filter(span => span.kind === 2 && span.traceId === traceId).length).toBe(1);
  // Independent positive: the real browser exposes the configured delays even
  // before the request owner's helper projects them into a journey event.
  const native = await page.evaluate(url => {
    const entries = performance.getEntriesByName(url, 'resource') as PerformanceResourceTiming[];
    return entries.map(entry => ({
      initiator: entry.initiatorType,
      firstByte: entry.responseStart - entry.requestStart,
      transfer: entry.responseEnd - entry.responseStart,
    }));
  }, response.url());
  expect(native).toHaveLength(1);
  expect(native[0].initiator).toBe('xmlhttprequest');
  expect(native[0].firstByte).toBeGreaterThanOrEqual(100);
  expect(native[0].transfer).toBeGreaterThanOrEqual(120);
  await expect.poll(async () => selectedTiming(await reference.snapshot())).toEqual(expect.objectContaining({
    before_request: expect.any(Number), first_byte_wait: expect.any(Number),
    receive_after_first_byte: expect.any(Number), post_transfer_until_response_complete: expect.any(Number),
  }));
  const snapshot = await reference.snapshot();
  const timing = selectedTiming(snapshot)!;
  expect(timing.first_byte_wait).toBeGreaterThanOrEqual(100);
  expect(timing.receive_after_first_byte).toBeGreaterThanOrEqual(120);
  for (const value of Object.values(timing)) {
    expect(value).toBeGreaterThanOrEqual(0);
    expect(value).toBeLessThanOrEqual(86_400_000);
  }
  expect(JSON.stringify(snapshot.events)).toContain(traceId);
  expect(JSON.stringify(snapshot.events)).not.toContain('CANARY');
  expect(JSON.stringify(snapshot.events)).not.toContain('/api/data');
  expect(snapshot.wire.join('\n')).not.toContain('CANARY');
});

const NULL_PHASES = {
  before_request: null, first_byte_wait: null, receive_after_first_byte: null,
  post_transfer_until_response_complete: null,
};
const NUMERIC_PHASES = {
  before_request: expect.any(Number), first_byte_wait: expect.any(Number),
  receive_after_first_byte: expect.any(Number), post_transfer_until_response_complete: expect.any(Number),
};
interface SelectedRequest {
  trace_id: string | null;
  timing: Record<string, number | null>;
  timing_unavailable_reason: string | null;
}
async function selected(reference: import('./fixtures').Reference): Promise<SelectedRequest> {
  await expect.poll(async () => (await reference.snapshot()).events.filter(item => item.event === 'board_ready').length).toBe(1);
  const ready = (await reference.snapshot()).events.find(item => item.event === 'board_ready')!;
  expect(ready.properties.requests).toHaveLength(1);
  return (ready.properties.requests as SelectedRequest[])[0];
}

// Delegate to native APIs and return their real entries, without changing delivery.
declare global {
  interface Window {
    timingAudit: { drained: number; disconnected: number; cleared: number; resized: number };
  }
}
async function auditNativeObserver(page: import('@playwright/test').Page) {
  await page.addInitScript(() => {
    window.timingAudit = { drained: 0, disconnected: 0, cleared: 0, resized: 0 };
    const take = PerformanceObserver.prototype.takeRecords;
    const disconnect = PerformanceObserver.prototype.disconnect;
    PerformanceObserver.prototype.takeRecords = function () {
      const entries = take.call(this);
      window.timingAudit.drained += entries.length;
      return entries;
    };
    PerformanceObserver.prototype.disconnect = function () {
      window.timingAudit.disconnected += 1;
      disconnect.call(this);
    };
    const clear = performance.clearResourceTimings.bind(performance);
    const resize = performance.setResourceTimingBufferSize.bind(performance);
    performance.clearResourceTimings = () => { window.timingAudit.cleared += 1; clear(); };
    performance.setResourceTimingBufferSize = size => { window.timingAudit.resized += 1; resize(size); };
  });
}

test('CORS-readable response without TAO retains its trace but no restricted phases', async ({ page, reference }) => {
  await reference.reset();
  await page.goto(reference.restrictedOrigin);
  const pending = page.waitForResponse(response => response.url().startsWith(reference.apiOrigin + '/api/data'));
  await page.getByRole('button', { name: 'Board', exact: true }).click();
  const response = await pending;
  await expect(page.getByRole('status')).toHaveText('Current items are ready.');
  expect(response.headers()['timing-allow-origin']).toBeUndefined();
  expect(response.headers()['access-control-allow-origin']).toBe(reference.restrictedOrigin);
  const trace = response.headers()['x-altimeter-trace-id'];
  expect(trace).toMatch(/^[0-9a-f]{32}$/);
  const native = await page.evaluate(url => {
    const entry = performance.getEntriesByName(url, 'resource')[0] as PerformanceResourceTiming;
    return { request: entry.requestStart, response: entry.responseStart };
  }, response.url());
  expect(native).toEqual({ request: 0, response: 0 });
  expect(await selected(reference)).toMatchObject({ trace_id: trace, timing: NULL_PHASES, timing_unavailable_reason: 'restricted' });
  await expect.poll(async () => (await reference.snapshot()).spans.filter(span => span.traceId === trace && span.kind === 2).length).toBe(1);
});

test('fresh observer entries work after the shared resource buffer fills without clearing or resizing it', async ({ page, reference }) => {
  await reference.reset();
  await auditNativeObserver(page);
  await page.goto(reference.frontendOrigin);
  const full = await page.evaluate(async () => {
    let overflow = 0;
    performance.addEventListener('resourcetimingbufferfull', () => { overflow += 1; });
    for (let index = 0; index < 360; index++) await (await fetch('/pad?n=' + index)).text();
    const before = performance.getEntriesByType('resource').length;
    for (let index = 360; index < 380; index++) await (await fetch('/pad?n=' + index)).text();
    return { before, after: performance.getEntriesByType('resource').length, overflow };
  });
  expect(full.before).toBeGreaterThanOrEqual(250);
  expect(full.after).toBe(full.before);
  expect(full.overflow).toBeGreaterThan(0);
  const responsePending = page.waitForResponse(response => response.url().startsWith(reference.apiOrigin + '/api/data'));
  await page.getByRole('button', { name: 'Board', exact: true }).click();
  const response = await responsePending;
  await expect(page.getByRole('status')).toHaveText('Current items are ready.');
  expect(await page.evaluate(url => performance.getEntriesByName(url, 'resource').length, response.url())).toBe(0);
  expect((await selected(reference)).timing).toEqual(NUMERIC_PHASES);
  expect(await page.evaluate(() => ({ cleared: window.timingAudit.cleared, resized: window.timingAudit.resized }))).toEqual({ cleared: 0, resized: 0 });
});

test('completion drains real queued observer entries and terminal cleanup disconnects once', async ({ page, reference }) => {
  await reference.reset();
  await auditNativeObserver(page);
  await page.goto(reference.frontendOrigin);
  await page.evaluate(async () => {
    window.reference.begin();
    await window.reference.request({ role: 'board', requestKey: 'queued', path: '/api/data?header_ms=20&body_ms=20&case=queued' });
    window.reference.finish();
    window.reference.finish();
  });
  expect((await selected(reference)).timing).toEqual(NUMERIC_PHASES);
  const audit = await page.evaluate(() => window.timingAudit);
  expect(audit.drained).toBeGreaterThan(0);
  expect(audit.disconnected).toBe(1);
  expect(audit.cleared).toBe(0);
  expect(audit.resized).toBe(0);
});

test('overlapping executions stay ambiguous while a completed-request retry gets fresh timing', async ({ page, reference }) => {
  await reference.reset();
  await page.goto(reference.frontendOrigin);
  const traces = await page.evaluate(async () => {
    window.reference.begin();
    const path = '/api/data?header_ms=80&body_ms=80&case=overlap';
    const first = window.reference.request({ role: 'board', requestKey: 'CANARY_OLD', path });
    const second = window.reference.request({ role: 'board', requestKey: 'CANARY_CURRENT', path });
    const responses = await Promise.all([first, second]);
    window.reference.finish();
    return responses.map(response => response.traceId);
  });
  expect(traces[0]).toMatch(/^[0-9a-f]{32}$/);
  expect(traces[1]).not.toBe(traces[0]);
  expect(await selected(reference)).toMatchObject({ trace_id: traces[1], timing: NULL_PHASES, timing_unavailable_reason: 'ambiguous' });
  await reference.reset();
  const retry = await page.evaluate(async () => {
    window.reference.begin();
    const path = '/api/data?header_ms=20&body_ms=20&case=completed-retry';
    const first = await window.reference.request({ role: 'board', requestKey: 'first', path });
    const second = await window.reference.request({ role: 'board', requestKey: 'second', path });
    window.reference.finish();
    return { first: first.traceId, second: second.traceId };
  });
  expect(retry.second).not.toBe(retry.first);
  expect(await selected(reference)).toMatchObject({ trace_id: retry.second, timing: NUMERIC_PHASES, timing_unavailable_reason: null });
  expect(JSON.stringify((await reference.snapshot()).events)).not.toContain('CANARY');
});

test('an HTTP response arriving after terminal capture cannot amend or emit another event', async ({ page, reference }) => {
  await reference.reset();
  await auditNativeObserver(page);
  await page.goto(reference.frontendOrigin);
  const responsePending = page.waitForResponse(response => response.url().includes('case=late'));
  const result = await page.evaluate(async () => {
    window.reference.begin();
    const pending = window.reference.request({ role: 'board', requestKey: 'late', path: '/api/data?header_ms=200&body_ms=200&case=late' });
    window.reference.finish();
    const before = await window.reference.getEvents();
    const response = await pending;
    window.reference.finish();
    return { before, trace: response.traceId, events: await window.reference.getEvents(), disconnected: window.timingAudit.disconnected };
  });
  expect(result.before).toHaveLength(1);
  expect(result.events).toEqual(result.before);
  expect(result.events[0].properties.requests).toEqual([]);
  expect(result.disconnected).toBe(1);
  expect(result.trace).toBeNull(); // A retired owner cannot read the response link.
  const trace = (await responsePending).headers()['x-altimeter-trace-id'];
  expect(trace).toMatch(/^[0-9a-f]{32}$/);
  await expect.poll(async () => (await reference.snapshot()).spans.filter(span => span.traceId === trace && span.kind === 2).length).toBe(1);
  expect((await reference.snapshot()).events).toEqual(result.before);
});

test('an actual browser cache hit cannot claim network phases beside a fresh positive', async ({ page, reference }) => {
  await reference.reset();
  await page.goto(reference.frontendOrigin);
  const results = await page.evaluate(async () => {
    const path = location.origin + '/cached';
    window.reference.begin();
    const first = await window.reference.request({ role: 'board', requestKey: 'network', path });
    window.reference.finish(0);
    window.reference.begin();
    const cached = await window.reference.request({ role: 'board', requestKey: 'cached', path });
    window.reference.finish(0);
    const entries = performance.getEntriesByName(path, 'resource') as PerformanceResourceTiming[];
    return { first, cached, sizes: entries.map(entry => entry.transferSize) };
  });
  expect(results.sizes).toHaveLength(2);
  expect(results.sizes[0]).toBeGreaterThan(0);
  expect(results.sizes[1]).toBe(0);
  expect(results.first.timing).toEqual(NUMERIC_PHASES);
  expect(results.cached).toMatchObject({ timing: NULL_PHASES, timingUnavailableReason: 'cache_unverified' });
  await expect.poll(async () => (await reference.snapshot()).events.length).toBe(2);
  const events = (await reference.snapshot()).events;
  expect((events[1].properties.requests as SelectedRequest[])[0]).toMatchObject({ timing: NULL_PHASES, timing_unavailable_reason: 'cache_unverified' });
});

test('a real service worker response remains unavailable instead of inventing network time', async ({ page, reference }) => {
  await reference.reset();
  await page.goto(reference.frontendOrigin);
  await page.evaluate(async () => { await navigator.serviceWorker.register('/service-worker.js'); await navigator.serviceWorker.ready; });
  await page.waitForFunction(() => navigator.serviceWorker.controller !== null);
  const result = await page.evaluate(async () => {
    window.reference.begin();
    const path = location.origin + '/sw-data';
    const response = await window.reference.request({ role: 'board', requestKey: 'sw', path });
    window.reference.finish(0);
    const entry = performance.getEntriesByName(path, 'resource')[0] as PerformanceResourceTiming;
    return { response, workerStart: entry.workerStart };
  });
  expect(result.response.status).toBe(200);
  expect(result.workerStart).toBeGreaterThan(0);
  expect(await selected(reference)).toMatchObject({ timing: NULL_PHASES, timing_unavailable_reason: 'service_worker' });
});

test('candidate overflow disconnects timing but keeps the selected actual response trace', async ({ page, reference }) => {
  await reference.reset();
  await auditNativeObserver(page);
  await page.goto(reference.frontendOrigin);
  const result = await page.evaluate(async api => {
    window.reference.begin();
    const pending: Promise<string>[] = [];
    for (let index = 0; index < 32; index++) {
      const url = api + '/api/data?header_ms=100&body_ms=100&case=capacity&n=' + index;
      window.reference.timing.start({ role: 'board', requestKey: 'pending-' + index, url, initiator: 'fetch' });
      pending.push(fetch(url).then(response => response.text()));
    }
    const selected = await window.reference.request({ role: 'board', requestKey: 'selected', path: '/api/data?case=capacity-selected' });
    window.reference.finish();
    await Promise.all(pending);
    return selected;
  }, reference.apiOrigin);
  expect(result.status).toBe(200);
  expect(result.traceId).toMatch(/^[0-9a-f]{32}$/);
  expect(await selected(reference)).toMatchObject({ trace_id: result.traceId, timing: NULL_PHASES, timing_unavailable_reason: 'overflow' });
  expect(await page.evaluate(() => window.timingAudit.disconnected)).toBe(1);
  await expect.poll(async () => (await reference.snapshot()).spans.filter(span => span.traceId === result.traceId && span.kind === 2).length).toBe(1);
});

test('unsupported observation keeps real data and trace selection working', async ({ page, reference }) => {
  await reference.reset();
  await page.addInitScript(() => { Object.defineProperty(window, 'PerformanceObserver', { value: undefined }); });
  await page.goto(reference.frontendOrigin);
  await page.getByRole('button', { name: 'Board', exact: true }).click();
  await expect(page.getByRole('status')).toHaveText('Current items are ready.');
  const result = await selected(reference);
  expect(result.trace_id).toMatch(/^[0-9a-f]{32}$/);
  expect(result).toMatchObject({ timing: NULL_PHASES, timing_unavailable_reason: 'unsupported' });
});

test('a missing current-window entry cannot borrow an older real request with the same URL', async ({ page, reference }) => {
  await reference.reset();
  await page.goto(reference.frontendOrigin);
  const trace = await page.evaluate(async api => {
    const url = api + '/api/data?case=before-observation';
    const response = await fetch(url);
    await response.text();
    const traceId = response.headers.get('X-Altimeter-Trace-Id');
    window.reference.begin();
    window.reference.attempt.selectRequest({ role: 'board', requestKey: 'current' });
    const ticket = window.reference.timing.start({ role: 'board', requestKey: 'current', url, initiator: 'fetch' });
    window.reference.attempt.recordRequest({ role: 'board', requestKey: 'current',
      traceId, sampled: true, provenance: 'network', contributorCoverage: 'complete',
      ...ticket.complete({ provenance: 'network' }) });
    window.reference.finish();
    return traceId;
  }, reference.apiOrigin);
  expect(trace).toMatch(/^[0-9a-f]{32}$/);
  expect(await selected(reference)).toMatchObject({ trace_id: trace, timing: NULL_PHASES, timing_unavailable_reason: 'not_found' });
});
