import { runInNewContext } from 'node:vm';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  beginJourney,
  MAX_DURATION_MS,
  type JourneyAttempt,
  type JourneyEventName,
  type JourneyEventProperties,
  type JourneyOptions,
  type RequestObservation,
} from '../../src/django_altimeter/browser/journey';

const TRACE = '1234567890abcdef1234567890abcdef';
const NEXT_TRACE = 'abcdef1234567890abcdef1234567890';
const CANARY = 'private-canary-never-serialize';
const REPAIR_ROLES = ['detail', 'checklist', 'hidden_documents', 'media'] as const;
let now = 100;

beforeEach(() => {
  now = 100;
  vi.spyOn(performance, 'now').mockImplementation(() => now);
});
afterEach(() => vi.restoreAllMocks());

function navigation(overrides: Partial<JourneyOptions> = {}) {
  let allowed = true;
  const events: { event: JourneyEventName; properties: JourneyEventProperties }[] = [];
  const attempt = beginJourney({
    key: 'navigation-' + CANARY,
    screen: 'board', navigationType: 'spa', release: 'test-1', roles: ['board'],
    capture: (event, properties) => { events.push({ event, properties }); },
    canCapture: () => allowed,
    ...overrides,
  });
  return { attempt, events, consent(value: boolean) { allowed = value; } };
}

function observation(overrides: Partial<RequestObservation> = {}): RequestObservation {
  return {
    role: 'board', requestKey: 'request-' + CANARY,
    traceId: TRACE, sampled: true, provenance: 'network',
    contributorCoverage: 'complete', ...overrides,
  };
}

function selected(attempt: JourneyAttempt, overrides: Partial<RequestObservation> = {}) {
  const record = observation(overrides);
  expect(attempt.selectRequest(record)).toBe(true);
  attempt.recordRequest(record);
  return record;
}

function terminal(attempt: JourneyAttempt, outcome: 'success' | 'failure' | 'cancelled') {
  if (outcome === 'cancelled') attempt.cancel('navigation');
  else attempt.finish({ outcome, itemCount: outcome === 'success' ? 3 : undefined });
}

describe('one terminal event owned by the navigation', () => {
  for (const screen of ['board', 'repair'] as const) {
    for (const navigationType of ['full', 'spa'] as const) {
      it.each(['success', 'failure', 'cancelled'] as const)(
        `${screen}/${navigationType}/%s terminates exactly once`, (outcome) => {
          const role = screen === 'board' ? 'board' : 'detail';
          const { attempt, events } = navigation({ screen, navigationType, roles: [role] });
          selected(attempt, { role });
          now = 170;
          terminal(attempt, outcome);
          terminal(attempt, 'success');
          attempt.cancel('superseded');
          attempt.recordRequest(observation({ traceId: NEXT_TRACE }));
          expect(events).toHaveLength(1);
          expect(events[0]?.event).toBe(`${screen}_${outcome === 'success' ? 'ready' : outcome === 'failure' ? 'failed' : 'cancelled'}`);
          expect(events[0]?.properties).toMatchObject({
            screen, navigation_type: navigationType, release: 'test-1',
            duration_ms: 70, outcome,
          });
          expect(events[0]?.properties.requests).toHaveLength(1);
          expect(attempt.isActive()).toBe(false);
        },
      );
    }
  }

  it('keeps empty readiness explicit and does not infer readiness from a request', () => {
    const { attempt, events } = navigation();
    selected(attempt);
    expect(events).toEqual([]);
    attempt.finish({ outcome: 'success', itemCount: 0 });
    expect(events).toHaveLength(1);
    expect(events[0]?.properties.item_count).toBe(0);
  });

  it('keeps a retained attempt across effect replay and placeholder commits', () => {
    const { attempt, events } = navigation();
    const effect = () => { attempt.selectRequest({ role: 'board', requestKey: 'current' }); };
    effect();
    // Development cleanup is not navigation departure; the existing owner retains it.
    effect();
    expect(events).toEqual([]);
    attempt.recordRequest(observation({ requestKey: 'obsolete-placeholder' }));
    expect(events).toEqual([]);
    attempt.recordRequest(observation({ requestKey: 'current' }));
    attempt.finish({ outcome: 'success', itemCount: 2 });
    effect();
    attempt.finish({ outcome: 'success', itemCount: 2 });
    expect(events).toHaveLength(1);
    expect(events[0]?.properties.requests[0]?.trace_id).toBe(TRACE);
  });

  it.each(['cache_only', 'cache_unverified'] as const)('%s never replays cached correlation or timing', (provenance) => {
    const { attempt, events } = navigation();
    selected(attempt, { provenance, timing: { first_byte_wait: 25 } });
    attempt.recordTiming({ role: 'board', requestKey: 'request-' + CANARY, timing: { first_byte_wait: 8 } });
    attempt.finish({ outcome: 'success', traceMissingReason: provenance });
    expect(events[0]?.properties.requests[0]).toMatchObject({
      trace_id: null, sampled: null, trace_missing_reason: provenance,
      contributor_coverage: 'complete', timing: { first_byte_wait: null },
    });
    expect(events[0]?.properties.trace_missing_reason).toBe(provenance);
  });
});

describe('permission is current and discard is permanent', () => {
  it('records nothing while consent is off, including no clock or metadata reads', () => {
    let permitted = false;
    const capture = vi.fn();
    const attempt = beginJourney({
      key: 'private', screen: 'board', navigationType: 'full', release: 'v1',
      roles: ['board'], capture, canCapture: () => permitted,
    });
    const reads = vi.fn(() => { throw new Error(CANARY); });
    const input = Object.defineProperty({}, 'role', { get: reads });
    attempt.selectRequest(input as never);
    attempt.recordRequest(input as never);
    attempt.recordTiming(input as never);
    permitted = true;
    attempt.finish({ outcome: 'success' });
    expect(capture).not.toHaveBeenCalled();
    expect(reads).not.toHaveBeenCalled();
    expect(performance.now).not.toHaveBeenCalled();
    expect(attempt.isActive()).toBe(false);
  });

  it.each(['consent revoked', 'account switched', 'tenant switched'])(
    '%s aborts permanently, including revoke then regrant', () => {
      const lifecycle = new AbortController();
      const { attempt, events, consent } = navigation({ signal: lifecycle.signal });
      selected(attempt);
      consent(false);
      lifecycle.abort();
      consent(true);
      selectedAfterDiscard(attempt);
      attempt.finish({ outcome: 'success' });
      expect(events).toEqual([]);
      expect(attempt.isActive()).toBe(false);
    },
  );

  it('an already aborted lifecycle records nothing', () => {
    const lifecycle = new AbortController();
    lifecycle.abort();
    const { attempt, events } = navigation({ signal: lifecycle.signal });
    expect(attempt.selectRequest(observation())).toBe(false);
    attempt.finish({ outcome: 'success' });
    expect(events).toEqual([]);
    expect(performance.now).not.toHaveBeenCalled();
  });

  it('a denied record permanently discards even when permission later returns', () => {
    const { attempt, events, consent } = navigation();
    selected(attempt);
    consent(false);
    attempt.recordTiming({ role: 'board', requestKey: 'request-' + CANARY, timing: { first_byte_wait: 1 } });
    consent(true);
    attempt.finish({ outcome: 'success' });
    expect(events).toEqual([]);
  });

  it('rechecks permission before a terminal capture', () => {
    const { attempt, events, consent } = navigation();
    selected(attempt);
    consent(false);
    attempt.finish({ outcome: 'success' });
    consent(true);
    attempt.finish({ outcome: 'success' });
    expect(events).toEqual([]);
  });

  it.each(['finish', 'cancel', 'discard'] as const)('%s removes its lifecycle listener', (method) => {
    const lifecycle = new AbortController();
    const remove = vi.spyOn(lifecycle.signal, 'removeEventListener');
    const { attempt } = navigation({ signal: lifecycle.signal });
    if (method === 'finish') attempt.finish({ outcome: 'success' });
    else if (method === 'cancel') attempt.cancel('navigation');
    else attempt.discard();
    expect(remove).toHaveBeenCalledOnce();
    expect(remove).toHaveBeenCalledWith('abort', expect.any(Function));
  });
});

function selectedAfterDiscard(attempt: JourneyAttempt) {
  attempt.selectRequest({ role: 'board', requestKey: 'new' });
  attempt.recordRequest(observation({ requestKey: 'new', traceId: NEXT_TRACE }));
}

describe('bounded current role selections', () => {
  it('rejects a record until its execution is selected', () => {
    const { attempt, events } = navigation();
    attempt.recordRequest(observation());
    attempt.finish({ outcome: 'failure' });
    expect(events[0]?.properties.requests).toEqual([]);
  });

  it('same role/key is idempotent and cannot replace identity or coverage', () => {
    const { attempt, events } = navigation();
    selected(attempt);
    selected(attempt, { traceId: NEXT_TRACE, sampled: false, contributorCoverage: 'partial' });
    attempt.finish({ outcome: 'success' });
    expect(events[0]?.properties.requests).toHaveLength(1);
    expect(events[0]?.properties.requests[0]).toMatchObject({
      trace_id: TRACE, sampled: true, contributor_coverage: 'complete',
    });
  });

  it('retry replaces coverage and timing; an obsolete completion cannot replace it', () => {
    const { attempt, events } = navigation();
    const old = selected(attempt, { requestKey: 'old', timing: { first_byte_wait: 100 }, contributorCoverage: 'partial' });
    const next = selected(attempt, { requestKey: 'new', traceId: NEXT_TRACE, contributorCoverage: 'complete' });
    attempt.recordRequest(old);
    attempt.recordTiming({ ...old, timing: { first_byte_wait: 300 } });
    expect(attempt.isSelected('board', next.requestKey)).toBe(true);
    expect(attempt.isSelected('board', old.requestKey)).toBe(false);
    attempt.finish({ outcome: 'success' });
    expect(events[0]?.properties.requests).toHaveLength(1);
    expect(events[0]?.properties.requests[0]).toMatchObject({
      trace_id: NEXT_TRACE, contributor_coverage: 'complete', timing: { first_byte_wait: null },
    });
  });

  it('twenty executions cannot crowd out the four final role selections', () => {
    const { attempt, events } = navigation({ screen: 'repair', roles: REPAIR_ROLES });
    for (let index = 0; index < 20; index += 1) {
      selected(attempt, { role: 'detail', requestKey: `retry-${index}`, contributorCoverage: 'partial' });
    }
    for (const role of REPAIR_ROLES) selected(attempt, { role, requestKey: `final-${role}`, traceId: NEXT_TRACE });
    expect(attempt.selectRequest({ role: 'board', requestKey: 'extra' })).toBe(false);
    attempt.finish({ outcome: 'success' });
    expect(events[0]?.properties.requests.map((request) => request.role)).toEqual(REPAIR_ROLES);
    expect(events[0]?.properties.requests.every((request) => request.trace_id === NEXT_TRACE && request.contributor_coverage === 'complete')).toBe(true);
  });

  it.each(['complete', 'partial', 'unknown'] as const)('coverage %s is independent of missing, unsampled and cache linkage', (contributorCoverage) => {
    const { attempt, events } = navigation({ screen: 'repair', roles: REPAIR_ROLES });
    const inputs = [
      { role: 'detail', traceId: null, sampled: null },
      { role: 'checklist', sampled: false },
      { role: 'hidden_documents', provenance: 'cache_only' },
      { role: 'media', provenance: 'cache_unverified' },
    ] as const;
    for (const input of inputs) selected(attempt, { ...input, contributorCoverage });
    attempt.finish({ outcome: 'success' });
    expect(events[0]?.properties.requests.map((request) => request.trace_missing_reason)).toEqual(['missing', 'unsampled', 'cache_only', 'cache_unverified']);
    expect(events[0]?.properties.requests.every((request) => request.contributor_coverage === contributorCoverage)).toBe(true);
    expect(JSON.stringify(events)).not.toContain('ingest');
  });

  it('keeps only the representative physical timing, without inventing fan-out totals', () => {
    const { attempt, events } = navigation({ screen: 'repair', roles: ['media'] });
    selected(attempt, { role: 'media', contributorCoverage: 'partial', timing: { first_byte_wait: 12 } });
    now = 1400;
    attempt.finish({ outcome: 'success' });
    expect(events[0]?.properties.duration_ms).toBe(1300);
    expect(events[0]?.properties.requests[0]?.timing.first_byte_wait).toBe(12);
    expect(events[0]?.properties.requests[0]?.contributor_coverage).toBe('partial');
  });
});

describe('timing fills and one observer lifecycle', () => {
  it('fills null phases only, without changing identity/coverage or emitted events', () => {
    const { attempt, events } = navigation();
    const input = selected(attempt, { timing: { before_request: 0 }, contributorCoverage: 'partial' });
    attempt.recordTiming({ ...input, timing: { before_request: 99, first_byte_wait: 20 }, timingUnavailableReason: 'not_found' });
    attempt.finish({ outcome: 'success' });
    const serialized = JSON.stringify(events);
    attempt.recordTiming({ ...input, timing: { receive_after_first_byte: 10 } });
    expect(JSON.stringify(events)).toBe(serialized);
    expect(events[0]?.properties.requests[0]).toMatchObject({
      trace_id: TRACE, contributor_coverage: 'partial',
      timing: { before_request: 0, first_byte_wait: 20, receive_after_first_byte: null },
    });
  });

  it.each(['restricted', 'ambiguous', 'cache_unverified', 'service_worker', 'overflow', 'invalid', 'unsupported', 'late'] as const)(
    '%s timing provenance cannot be upgraded by a later notification', (reason) => {
      const { attempt, events } = navigation();
      const input = selected(attempt, { timingUnavailableReason: reason });
      attempt.recordTiming({ ...input, timing: { first_byte_wait: 12 }, timingUnavailableReason: null });
      attempt.finish({ outcome: 'success' });
      expect(events[0]?.properties.requests[0]?.timing.first_byte_wait).toBeNull();
      expect(events[0]?.properties.requests[0]?.timing_unavailable_reason).toBe(reason);
    },
  );

  it('drains queued timing before capture, retires superseded keys and disconnects once', () => {
    const { attempt, events } = navigation();
    const retire = vi.fn();
    const disconnect = vi.fn();
    const flush = vi.fn(() => {
      attempt.recordTiming({ role: 'board', requestKey: 'new', timing: { first_byte_wait: 7 } });
      attempt.finish({ outcome: 'failure' }); // Reentrant finish cannot emit a second event.
    });
    expect(attempt.attachTiming({ flush, retire, disconnect })).toBe(true);
    selected(attempt, { requestKey: 'old' });
    selected(attempt, { requestKey: 'new' });
    expect(retire).toHaveBeenCalledExactlyOnceWith('board', 'old');
    attempt.finish({ outcome: 'success' });
    expect(events).toHaveLength(1);
    expect(events[0]?.event).toBe('board_ready');
    expect(events[0]?.properties.requests[0]?.timing.first_byte_wait).toBe(7);
    expect(flush).toHaveBeenCalledOnce();
    expect(disconnect).toHaveBeenCalledOnce();
    attempt.discard();
    expect(disconnect).toHaveBeenCalledOnce();
  });

  it('discard cleans timing without draining or capture, and a second owner is refused', () => {
    const { attempt, events } = navigation();
    const owner = { flush: vi.fn(), retire: vi.fn(), disconnect: vi.fn() };
    const other = { flush: vi.fn(), retire: vi.fn(), disconnect: vi.fn() };
    expect(attempt.attachTiming(owner)).toBe(true);
    expect(attempt.attachTiming(other)).toBe(false);
    expect(other.disconnect).toHaveBeenCalledOnce();
    attempt.discard();
    expect(owner.disconnect).toHaveBeenCalledOnce();
    expect(owner.flush).not.toHaveBeenCalled();
    expect(events).toEqual([]);
  });

  it.each([false, true])('final permission discard=%s is respected after terminal cleanup', (invalidate) => {
    let terminalCleanup = false;
    let discardedInsidePermission = false;
    const { attempt, events } = navigation({
      canCapture() {
        if (terminalCleanup && invalidate) {
          attempt.discard();
          discardedInsidePermission = true;
        }
        return true;
      },
    });
    selected(attempt);
    attempt.attachTiming({
      flush() {}, retire() {}, disconnect() { terminalCleanup = true; },
    });
    attempt.finish({ outcome: 'success' });
    expect(terminalCleanup).toBe(true);
    expect(discardedInsidePermission).toBe(invalidate);
    expect(events).toHaveLength(invalidate ? 0 : 1);
  });

  it('revocation during a queued timing drain wins over terminal capture', () => {
    const controller = new AbortController();
    const { attempt, events } = navigation({ signal: controller.signal });
    selected(attempt);
    attempt.attachTiming({ flush: () => controller.abort(), retire() {}, disconnect() {} });
    attempt.finish({ outcome: 'success' });
    expect(events).toEqual([]);
  });
});

describe('closed output and fail-open telemetry boundaries', () => {
  it('serialized event contains only allow-listed scalars and never private keys or extras', () => {
    const { attempt, events } = navigation();
    const record = {
      ...observation(), url: `https://example.invalid/${CANARY}`, recordId: CANARY,
      title: CANARY, tenantId: CANARY, secret: { toJSON() { throw new Error(CANARY); } },
      timing: { first_byte_wait: 2, url: CANARY },
    };
    selected(attempt, record);
    attempt.finish({ outcome: 'success', itemCount: 4, title: CANARY } as never);
    const properties = events[0]!.properties;
    expect(Object.keys(properties).sort()).toEqual([
      'cancel_reason', 'duration_ms', 'item_count', 'navigation_type', 'outcome',
      'release', 'requests', 'screen', 'trace_missing_reason',
    ]);
    expect(Object.keys(properties.requests[0]!).sort()).toEqual([
      'contributor_coverage', 'provenance', 'role', 'sampled', 'timing',
      'timing_unavailable_reason', 'trace_id', 'trace_missing_reason',
    ]);
    expect(JSON.stringify(events)).not.toContain(CANARY);
    expect(Object.isFrozen(properties)).toBe(true);
    expect(Object.isFrozen(properties.requests)).toBe(true);
    expect(Object.isFrozen(properties.requests[0]?.timing)).toBe(true);
  });

  it('unknown metadata accessors and oversized numbers are never exported', () => {
    const { attempt, events } = navigation();
    const getter = vi.fn(() => { throw new Error(CANARY); });
    const input = observation({ timing: { before_request: -1, first_byte_wait: Infinity, receive_after_first_byte: MAX_DURATION_MS + 1, post_transfer_until_response_complete: 0 } });
    Object.defineProperty(input, 'traceId', { get: getter });
    Object.defineProperty(input, 'unknown', { get: getter });
    expect(attempt.selectRequest(input)).toBe(true);
    expect(() => attempt.recordRequest(input)).not.toThrow();
    attempt.finish({ outcome: 'success', itemCount: Number.MAX_SAFE_INTEGER });
    expect(getter).not.toHaveBeenCalled();
    expect(events[0]?.properties.requests[0]).toMatchObject({
      trace_id: null, timing: { before_request: null, first_byte_wait: null, receive_after_first_byte: null, post_transfer_until_response_complete: 0 },
    });
    expect(events[0]?.properties.item_count).toBeNull();
  });

  it.each([NaN, Infinity, -5, MAX_DURATION_MS + 101])('invalid or oversized clock interval %s stays unknown, not clamped', (clock) => {
    const { attempt, events } = navigation();
    now = clock;
    attempt.finish({ outcome: 'success' });
    expect(events[0]?.properties.duration_ms).toBeNull();
  });

  it('capture and observer failures never reach the application or retry capture', () => {
    const capture = vi.fn(() => { throw new Error(CANARY); });
    const { attempt } = navigation({ capture });
    attempt.attachTiming({ flush() { throw new Error(CANARY); }, retire() { throw new Error(CANARY); }, disconnect() { throw new Error(CANARY); } });
    selected(attempt, { requestKey: 'first' });
    expect(() => selected(attempt, { requestKey: 'second' })).not.toThrow();
    expect(() => attempt.finish({ outcome: 'success' })).not.toThrow();
    expect(() => attempt.finish({ outcome: 'failure' })).not.toThrow();
    expect(capture).toHaveBeenCalledOnce();
  });

  it.each(['native', 'foreign'] as const)('%s rejected capture produces no unhandled rejection', async (realm) => {
    const unhandled: unknown[] = [];
    const observe = (error: unknown) => { unhandled.push(error); };
    let result: Promise<unknown> | undefined;
    const capture = vi.fn(() => {
      result = realm === 'native' ? Promise.reject(new Error('capture_failure'))
        : runInNewContext("Promise.reject(new Error('capture_failure'))") as Promise<unknown>;
      return result;
    });
    process.on('unhandledRejection', observe);
    try {
      const { attempt } = navigation({ capture });
      attempt.finish({ outcome: 'success' });
      attempt.finish({ outcome: 'failure' });
      expect(result instanceof Promise).toBe(realm === 'native');
      await new Promise<void>(resolve => setImmediate(resolve));
      expect(capture).toHaveBeenCalledOnce();
      expect(unhandled).toEqual([]);
    } finally {
      void result?.catch(() => {});
      process.off('unhandledRejection', observe);
    }
  });

  it('assimilates a callable thenable and contains its rejection', async () => {
    const then = vi.fn((_resolve: unknown, reject: (error: Error) => void) => {
      reject(new Error('capture_failure'));
    });
    const capture = vi.fn(() => Object.assign(() => {}, { then }));
    const { attempt } = navigation({ capture });
    expect(() => attempt.finish({ outcome: 'success' })).not.toThrow();
    await new Promise<void>(resolve => setImmediate(resolve));
    expect(then).toHaveBeenCalledOnce();
    expect(capture).toHaveBeenCalledOnce();
  });

  it('contains a hostile capture-result then getter without retrying capture', () => {
    const getter = vi.fn(() => { throw new Error(CANARY); });
    const capture = vi.fn(() => Object.defineProperty({}, 'then', { get: getter }));
    const { attempt } = navigation({ capture });
    expect(() => attempt.finish({ outcome: 'success' })).not.toThrow();
    attempt.finish({ outcome: 'failure' });
    expect(getter).toHaveBeenCalledOnce();
    expect(capture).toHaveBeenCalledOnce();
  });

  it('throwing permission fails closed without changing application control flow', () => {
    const capture = vi.fn();
    expect(() => {
      const { attempt } = navigation({ capture, canCapture() { throw new Error(CANARY); } });
      attempt.finish({ outcome: 'success' });
    }).not.toThrow();
    expect(capture).not.toHaveBeenCalled();
  });

  it.each([
    { roles: [] }, { roles: ['detail', 'checklist', 'hidden_documents', 'media', 'board'] },
    { roles: ['board', 'board'] }, { roles: ['unknown'] },
    { screen: 'unknown' }, { navigationType: 'unknown' },
    { release: CANARY.repeat(200) },
  ])('invalid configuration emits nothing: %j', (invalid) => {
    const { attempt, events } = navigation(invalid as Partial<JourneyOptions>);
    expect(attempt.isActive()).toBe(false);
    attempt.finish({ outcome: 'success' });
    expect(events).toEqual([]);
  });

  it.each(['', 'x'.repeat(201)])('invalid request identity cannot consume a role selection', (requestKey) => {
    const { attempt, events } = navigation();
    expect(attempt.selectRequest({ role: 'board', requestKey })).toBe(false);
    selected(attempt);
    attempt.finish({ outcome: 'success' });
    expect(events[0]?.properties.requests).toHaveLength(1);
  });
});
