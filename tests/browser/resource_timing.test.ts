/** Unit boundaries only: resource timing/CORS evidence lives in the real-browser spec. */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { beginJourney } from '../../src/django_altimeter/browser/journey';
import { createResourceTiming } from '../../src/django_altimeter/browser/resource_timing';

const URL = 'https://api.example.test/data';
const EMPTY = {
  before_request: null,
  first_byte_wait: null,
  receive_after_first_byte: null,
  post_transfer_until_response_complete: null,
};
const PHASES = {
  before_request: 10,
  first_byte_wait: 50,
  receive_after_first_byte: 30,
  post_transfer_until_response_complete: 20,
};
let now = 100;

class ObserverBoundary {
  static instances: ObserverBoundary[] = [];
  static supportedEntryTypes = ['resource'];
  queued: PerformanceEntry[] = [];
  observe = vi.fn();
  disconnect = vi.fn(() => { this.queued = []; });
  takeRecords = vi.fn(() => this.queued.splice(0));
  constructor(readonly callback: PerformanceObserverCallback) {
    ObserverBoundary.instances.push(this);
  }
  deliver(entries: PerformanceEntry[]) {
    this.callback({ getEntries: () => entries } as PerformanceObserverEntryList,
      this as unknown as PerformanceObserver);
  }
}

function entry(extra: Record<string, unknown> = {}): PerformanceEntry {
  return {
    entryType: 'resource', name: URL, initiatorType: 'xmlhttprequest',
    startTime: 105, fetchStart: 110, requestStart: 120,
    responseStart: 170, responseEnd: 200,
    workerStart: 0, transferSize: 500, deliveryType: '',
    toJSON() { throw new Error('CANARY_NO_ENTRY_SERIALIZATION'); },
    ...extra,
  } as unknown as PerformanceEntry;
}

function setup(canCapture = () => true) {
  const capture = vi.fn();
  const attempt = beginJourney({
    key: 'CANARY_NAVIGATION_KEY', screen: 'board', navigationType: 'spa',
    release: 'v1', roles: ['board'], capture, canCapture,
  });
  const owner = createResourceTiming({
    attempt, canCapture, baseUrl: 'https://api.example.test/',
  });
  function start(requestKey = 'one', url = URL, startedAt = 100) {
    attempt.selectRequest({ role: 'board', requestKey });
    return owner.start({ role: 'board', requestKey, url,
      initiator: 'xmlhttprequest', startedAt });
  }
  function record(requestKey: string, result: ReturnType<ReturnType<typeof start>['complete']>) {
    attempt.recordRequest({
      role: 'board', requestKey, traceId: '1'.repeat(32), sampled: true,
      provenance: 'network', contributorCoverage: 'complete', ...result,
    });
  }
  return { attempt, owner, capture, start, record };
}

beforeEach(() => {
  now = 100;
  ObserverBoundary.instances = [];
  vi.stubGlobal('PerformanceObserver', ObserverBoundary);
  vi.spyOn(performance, 'now').mockImplementation(() => now);
});
afterEach(() => { vi.restoreAllMocks(); vi.unstubAllGlobals(); });

describe('bounded numeric resource phases', () => {
  it('drains queued resource entries at completion and exports only safe numbers', () => {
    const { start, record, attempt, capture } = setup();
    const ticket = start('CANARY_REQUEST_KEY', URL + '?CANARY_QUERY=secret');
    const observer = ObserverBoundary.instances[0];
    observer.queued.push(entry({ name: URL + '?CANARY_QUERY=secret' }));
    now = 220;
    const result = ticket.complete({ provenance: 'network', responseComplete: now });
    expect(result).toEqual({ timing: PHASES, timingUnavailableReason: null });
    expect(observer.takeRecords).toHaveBeenCalled();
    expect(observer.observe).toHaveBeenCalledExactlyOnceWith({ type: 'resource', buffered: false });
    record('CANARY_REQUEST_KEY', result);
    attempt.finish({ outcome: 'success', itemCount: 1 });
    expect(capture).toHaveBeenCalledOnce();
    expect(JSON.stringify(capture.mock.calls)).not.toContain('CANARY');
    expect(JSON.stringify(capture.mock.calls)).toContain('"first_byte_wait":50');
    expect(observer.disconnect).toHaveBeenCalledOnce();
  });

  it('keeps valid zero-length phases rather than treating them as missing', () => {
    const { start } = setup();
    const ticket = start();
    ObserverBoundary.instances[0].queued.push(entry({
      fetchStart: 150, requestStart: 150, responseStart: 150, responseEnd: 150,
    }));
    now = 150;
    expect(ticket.complete({ provenance: 'network', responseComplete: now })).toEqual({
      timing: { before_request: 0, first_byte_wait: 0, receive_after_first_byte: 0,
        post_transfer_until_response_complete: 0 }, timingUnavailableReason: null,
    });
  });

  it.each([
    ['restricted', { requestStart: 0, responseStart: 0 }],
    ['invalid', { fetchStart: 130 }],
    ['invalid', { fetchStart: 104 }],
    ['invalid', { transferSize: 1.5 }],
    ['invalid', { responseStart: 119 }],
    ['invalid', { responseEnd: 169 }],
    ['invalid', { requestStart: Number.NaN }],
    ['invalid', { responseStart: Number.POSITIVE_INFINITY }],
    ['invalid', { fetchStart: -1 }],
    ['invalid', { transferSize: Number.NaN }],
    ['cache_unverified', { transferSize: 0 }],
    ['cache_unverified', { deliveryType: 'cache' }],
    ['service_worker', { workerStart: 106 }],
  ] as const)('keeps %s phases null: %j', (reason, extra) => {
    const { start } = setup();
    const ticket = start();
    ObserverBoundary.instances[0].queued.push(entry(extra));
    now = 220;
    expect(ticket.complete({ provenance: 'network', responseComplete: now })).toEqual({
      timing: EMPTY, timingUnavailableReason: reason,
    });
  });

  it.each(['cache_only', 'cache_unverified'] as const)('never promotes %s provenance', provenance => {
    const { start } = setup();
    const ticket = start();
    ObserverBoundary.instances[0].queued.push(entry());
    now = 220;
    expect(ticket.complete({ provenance, responseComplete: now })).toEqual({
      timing: EMPTY, timingUnavailableReason: 'cache_unverified',
    });
    expect(ticket.complete({ provenance: 'network', responseComplete: now })).toEqual({
      timing: EMPTY, timingUnavailableReason: 'cache_unverified',
    });
  });

  it.each([Number.NaN, Number.POSITIVE_INFINITY, -1, 86_400_101])(
    'refuses an invalid or over-budget request window ending at %s', responseComplete => {
      const { start } = setup();
      const ticket = start();
      ObserverBoundary.instances[0].queued.push(entry());
      now = 86_400_102;
      expect(ticket.complete({ provenance: 'network', responseComplete })).toEqual({
        timing: EMPTY, timingUnavailableReason: 'invalid',
      });
    },
  );
});

describe('current-window uniqueness and bounded retention', () => {
  it('ignores unrelated URLs, initiators, earlier starts and later completion windows', () => {
    const { start } = setup();
    const ticket = start();
    ObserverBoundary.instances[0].queued.push(
      entry({ name: URL + '/other' }), entry({ initiatorType: 'fetch' }),
      entry({ startTime: 99 }), entry({ responseEnd: 221 }), entry(),
    );
    now = 220;
    expect(ticket.complete({ provenance: 'network', responseComplete: now }).timing).toEqual(PHASES);
  });

  it('does not choose between two entries in the same request window', () => {
    const { start } = setup();
    const ticket = start();
    ObserverBoundary.instances[0].queued.push(entry(), entry({ startTime: 106 }));
    now = 220;
    expect(ticket.complete({ provenance: 'network', responseComplete: now })).toEqual({
      timing: EMPTY, timingUnavailableReason: 'ambiguous',
    });
  });

  it('marks overlapping same-URL executions ambiguous even before entries arrive', () => {
    const { owner } = setup();
    const first = owner.start({ role: 'board', requestKey: 'one', url: URL,
      initiator: 'xmlhttprequest', startedAt: 100 });
    now = 110;
    const second = owner.start({ role: 'board', requestKey: 'two', url: URL,
      initiator: 'xmlhttprequest', startedAt: 110 });
    ObserverBoundary.instances[0].queued.push(entry());
    now = 220;
    for (const ticket of [first, second]) {
      expect(ticket.complete({ provenance: 'network', responseComplete: now })).toEqual({
        timing: EMPTY, timingUnavailableReason: 'ambiguous',
      });
    }
  });

  it('admits 32 distinct active candidates, then makes overflow explicitly unavailable', () => {
    const { owner, attempt, record, capture } = setup();
    const tickets = Array.from({ length: 32 }, (_, index) => owner.start({
      role: 'board', requestKey: String(index), url: URL + '?n=' + index,
      initiator: 'xmlhttprequest', startedAt: 100,
    }));
    expect(ObserverBoundary.instances).toHaveLength(1);
    const overflow = owner.start({ role: 'board', requestKey: 'overflow', url: URL,
      initiator: 'xmlhttprequest', startedAt: 100 });
    now = 220;
    for (const ticket of [...tickets, overflow]) {
      expect(ticket.complete({ provenance: 'network', responseComplete: now })).toEqual({
        timing: EMPTY, timingUnavailableReason: 'overflow',
      });
    }
    expect(ObserverBoundary.instances[0].disconnect).toHaveBeenCalledOnce();
    attempt.selectRequest({ role: 'board', requestKey: 'overflow' });
    record('overflow', overflow.complete({ provenance: 'network', responseComplete: now }));
    attempt.finish({ outcome: 'success', itemCount: 1 });
    expect(capture).toHaveBeenCalledOnce();
    expect(JSON.stringify(capture.mock.calls)).toContain('1'.repeat(32));
  });

  it('keeps all 32 candidates useful when capacity is not exceeded', () => {
    const { owner } = setup();
    const tickets = Array.from({ length: 32 }, (_, index) => owner.start({
      role: 'board', requestKey: String(index), url: URL + '?n=' + index,
      initiator: 'xmlhttprequest', startedAt: 100,
    }));
    ObserverBoundary.instances[0].queued.push(...tickets.map((_, index) => entry({ name: URL + '?n=' + index })));
    now = 220;
    expect(tickets.map(ticket => ticket.complete({ provenance: 'network', responseComplete: now }).timing))
      .toEqual(tickets.map(() => PHASES));
  });

  it('resolves relative URLs and removes fragments for the actual network URL', () => {
    const { start } = setup();
    const ticket = start('one', '/data#not-sent');
    ObserverBoundary.instances[0].queued.push(entry());
    now = 220;
    expect(ticket.complete({ provenance: 'network', responseComplete: now }).timing).toEqual(PHASES);
  });

  it.each(['x'.repeat(2049), 'data:CANARY', 'https://user:CANARY@api.example.test/data'])
    ('rejects a non-network, oversized or credential-bearing URL without recording it', url => {
      const { start } = setup();
      const ticket = start('one', url);
      now = 220;
      expect(ticket.complete({ provenance: 'network', responseComplete: now })).toEqual({
        timing: EMPTY, timingUnavailableReason: 'invalid',
      });
      expect(ObserverBoundary.instances).toHaveLength(0);
    });
});

describe('journey lifecycle and fail-open behavior', () => {
  it('fills a still-selected request from the terminal takeRecords drain', () => {
    const { start, record, attempt, capture } = setup();
    const ticket = start();
    now = 220;
    record('one', ticket.complete({ provenance: 'network', responseComplete: now }));
    ObserverBoundary.instances[0].queued.push(entry());
    attempt.finish({ outcome: 'success', itemCount: 1 });
    expect(capture).toHaveBeenCalledOnce();
    expect(JSON.stringify(capture.mock.calls)).toContain('"first_byte_wait":50');
    expect(ObserverBoundary.instances[0].disconnect).toHaveBeenCalledOnce();
    const emitted = JSON.stringify(capture.mock.calls);
    ObserverBoundary.instances[0].deliver([entry({ responseStart: 130 })]);
    expect(JSON.stringify(capture.mock.calls)).toBe(emitted);
  });

  it('lets a later callback fill null phases without changing the selected trace', () => {
    const { start, record, attempt, capture } = setup();
    const ticket = start();
    now = 220;
    record('one', ticket.complete({ provenance: 'network', responseComplete: now }));
    ObserverBoundary.instances[0].deliver([entry()]);
    attempt.finish({ outcome: 'success', itemCount: 1 });
    expect(JSON.stringify(capture.mock.calls)).toContain('"first_byte_wait":50');
    expect(JSON.stringify(capture.mock.calls)).toContain('1'.repeat(32));
  });

  it('cannot revive a replaced request with a late callback or completion', () => {
    const { start, record, attempt, capture } = setup();
    const old = start('old');
    now = 220;
    record('old', old.complete({ provenance: 'network', responseComplete: now }));
    now = 230;
    const fresh = start('fresh', URL, 230);
    ObserverBoundary.instances[0].deliver([entry()]);
    now = 400;
    record('fresh', fresh.complete({ provenance: 'network', responseComplete: now }));
    expect(old.complete({ provenance: 'network', responseComplete: now }).timing).toEqual(EMPTY);
    attempt.finish({ outcome: 'success', itemCount: 1 });
    expect(JSON.stringify(capture.mock.calls)).not.toContain('"first_byte_wait":50');
  });

  it('records nothing while consent is off and reads no request URL', () => {
    const { owner, attempt, capture } = setup(() => false);
    const ticket = owner.start({ role: 'board', requestKey: 'one',
      get url(): string { throw new Error('CANARY_URL_READ'); }, initiator: 'xmlhttprequest' });
    expect(() => ticket.complete({ provenance: 'network' })).not.toThrow();
    attempt.finish({ outcome: 'success', itemCount: 1 });
    expect(ObserverBoundary.instances).toHaveLength(0);
    expect(capture).not.toHaveBeenCalled();
  });

  it('revocation clears matching state permanently across regrant', () => {
    let allowed = true;
    const { start, attempt, capture, owner } = setup(() => allowed);
    const ticket = start();
    const observer = ObserverBoundary.instances[0];
    observer.queued.push(entry());
    allowed = false;
    owner.flush();
    allowed = true;
    now = 220;
    expect(ticket.complete({ provenance: 'network', responseComplete: now }).timing).toEqual(EMPTY);
    observer.deliver([entry()]);
    attempt.finish({ outcome: 'success', itemCount: 1 });
    expect(capture).not.toHaveBeenCalled();
    expect(observer.disconnect).toHaveBeenCalledOnce();
  });

  it('does not retain a candidate after explicit discard', () => {
    const { start, attempt, capture } = setup();
    const ticket = start();
    attempt.discard();
    now = 220;
    ObserverBoundary.instances[0].deliver([entry()]);
    expect(ticket.complete({ provenance: 'network', responseComplete: now }).timing).toEqual(EMPTY);
    attempt.finish({ outcome: 'success', itemCount: 1 });
    expect(capture).not.toHaveBeenCalled();
    expect(ObserverBoundary.instances[0].disconnect).toHaveBeenCalledOnce();
  });

  it('keeps the trace positive when resource observation is unsupported', () => {
    vi.stubGlobal('PerformanceObserver', undefined);
    const { start, record, attempt, capture } = setup();
    const ticket = start();
    now = 220;
    const result = ticket.complete({ provenance: 'network', responseComplete: now });
    expect(result).toEqual({ timing: EMPTY, timingUnavailableReason: 'unsupported' });
    record('one', result);
    attempt.finish({ outcome: 'success', itemCount: 1 });
    expect(capture).toHaveBeenCalledOnce();
    expect(JSON.stringify(capture.mock.calls)).toContain('1'.repeat(32));
  });

  it('contains an observer failure and a malformed native entry getter', () => {
    const { start } = setup();
    const ticket = start();
    const observer = ObserverBoundary.instances[0];
    const malformed = entry();
    Object.defineProperty(malformed, 'requestStart', { get() { throw new Error('CANARY_ENTRY'); } });
    observer.queued.push(malformed);
    now = 220;
    expect(ticket.complete({ provenance: 'network', responseComplete: now })).toEqual({
      timing: EMPTY, timingUnavailableReason: 'invalid',
    });
    observer.takeRecords.mockImplementation(() => { throw new Error('CANARY_OBSERVER'); });
    expect(() => observer.deliver([entry()])).not.toThrow();
  });
});

it('an in-flight replacement cannot donate a delayed network entry to its retry', () => {
  const { start, record, attempt, capture } = setup();
  const old = start('old');
  now = 110;
  const current = start('current', URL, now);
  now = 220;
  ObserverBoundary.instances[0].deliver([entry({ startTime: 115 })]);
  expect(old.complete({ provenance: 'network', responseComplete: now }).timing).toEqual(EMPTY);
  const result = current.complete({ provenance: 'network', responseComplete: now });
  expect(result).toEqual({ timing: EMPTY, timingUnavailableReason: 'ambiguous' });
  record('current', result);
  attempt.finish({ outcome: 'success', itemCount: 1 });
  expect(capture).toHaveBeenCalledOnce();
  expect(JSON.stringify(capture.mock.calls)).toContain('1'.repeat(32));
});

it('duplicate start and completion notifications keep one native observer and one timing result', () => {
  const { owner, start } = setup();
  const first = start();
  const duplicate = owner.start({ role: 'board', requestKey: 'one', url: URL,
    initiator: 'xmlhttprequest', startedAt: 100 });
  ObserverBoundary.instances[0].queued.push(entry());
  now = 220;
  expect(first.complete({ provenance: 'network', responseComplete: now }).timing).toEqual(PHASES);
  expect(duplicate.complete({ provenance: 'network', responseComplete: now }).timing).toEqual(PHASES);
  expect(ObserverBoundary.instances).toHaveLength(1);
});

it('contains takeRecords failure while retaining the current trace', () => {
  const { start, record, attempt, capture } = setup();
  const ticket = start();
  ObserverBoundary.instances[0].takeRecords.mockImplementation(() => {
    throw new Error('CANARY_OBSERVER');
  });
  now = 220;
  const result = ticket.complete({ provenance: 'network', responseComplete: now });
  expect(result).toEqual({ timing: EMPTY, timingUnavailableReason: 'unsupported' });
  record('one', result);
  attempt.finish({ outcome: 'success', itemCount: 1 });
  expect(capture).toHaveBeenCalledOnce();
  expect(JSON.stringify(capture.mock.calls)).toContain('1'.repeat(32));
  expect(JSON.stringify(capture.mock.calls)).not.toContain('CANARY');
});

it('bounds callback work when the browser queues an oversized batch', () => {
  const { start } = setup();
  const ticket = start();
  ObserverBoundary.instances[0].queued.push(...Array.from({ length: 257 }, () => entry()));
  now = 220;
  expect(ticket.complete({ provenance: 'network', responseComplete: now })).toEqual({
    timing: EMPTY, timingUnavailableReason: 'overflow',
  });
  expect(ObserverBoundary.instances[0].disconnect).toHaveBeenCalledOnce();
});
