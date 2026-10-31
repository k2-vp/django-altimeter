/** Passive request-owner timing. Never intercept HTTP or retain page history. */
import {
  MAX_DURATION_MS, type JourneyAttempt, type JourneyTimingOwner, type RequestKey,
  type RequestRole, type RequestTiming, type TimingUnavailableReason,
} from './journey';
import type { RequestProvenance } from './headers';

export interface ResourceTimingResult {
  readonly timing: RequestTiming;
  readonly timingUnavailableReason: TimingUnavailableReason | null;
}
export interface ResourceRequest {
  readonly role: RequestRole;
  readonly requestKey: RequestKey;
  readonly url: string;
  readonly initiator: 'xmlhttprequest' | 'fetch';
  readonly startedAt?: number;
}
export interface ResourceCompletion {
  readonly provenance: RequestProvenance;
  readonly responseComplete?: number;
}
export interface ResourceTimingTicket {
  complete(input: ResourceCompletion): ResourceTimingResult;
}
export interface ResourceTimingOwner extends JourneyTimingOwner {
  start(input: ResourceRequest): ResourceTimingTicket;
}
export interface ResourceTimingOptions {
  readonly attempt: JourneyAttempt;
  readonly canCapture: () => boolean;
  readonly baseUrl: string;
}

const MAX_PENDING = 32;
const MAX_ENTRY_BATCH = 256;
const URL_LIMIT = 2048;
const EMPTY: RequestTiming = Object.freeze({
  before_request: null, first_byte_wait: null, receive_after_first_byte: null,
  post_transfer_until_response_complete: null,
});
function unavailable(reason: TimingUnavailableReason): ResourceTimingResult {
  return Object.freeze({ timing: EMPTY, timingUnavailableReason: reason });
}
function timestamp(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0 && value <= Number.MAX_SAFE_INTEGER;
}
function duration(value: number): boolean {
  return timestamp(value) && value <= MAX_DURATION_MS;
}
function clock(): number | null {
  try { const value = performance.now(); return timestamp(value) ? value : null; }
  catch { return null; }
}
function resolvedUrl(value: unknown, base: unknown): string | null {
  if (typeof value !== 'string' || value.length < 1 || value.length > URL_LIMIT ||
      typeof base !== 'string' || base.length < 1 || base.length > URL_LIMIT ||
      /[\u0000-\u0020\u007f\\]/.test(value) || /[\u0000-\u0020\u007f\\]/.test(base)) return null;
  const url = new URL(value, base);
  if ((url.protocol !== 'http:' && url.protocol !== 'https:') || url.username || url.password) return null;
  url.hash = '';
  return url.href.length <= URL_LIMIT ? url.href : null;
}

interface Match {
  readonly timing: RequestTiming;
  readonly responseEnd: number | null;
  readonly reason: TimingUnavailableReason | null;
}
/**
 * Only fixed numeric fields leave the native entry. These intervals are browser
 * boundaries, not pure queue/parse time. Receive after the first byte can include
 * header and interim-response work; the final interval includes scheduling/transforms.
 */
function project(entry: PerformanceResourceTiming): Match {
  let responseEnd: number | null = null;
  function refused(reason: TimingUnavailableReason): Match {
    return { timing: EMPTY, responseEnd, reason };
  }
  try {
    const end = entry.responseEnd;
    responseEnd = timestamp(end) ? end : null;
    const startTime = entry.startTime;
    const fetchStart = entry.fetchStart;
    const requestStart = entry.requestStart;
    const responseStart = entry.responseStart;
    const workerStart = entry.workerStart;
    const transferSize = entry.transferSize;
    const deliveryType = (entry as PerformanceResourceTiming & { readonly deliveryType?: unknown }).deliveryType;
    if (responseEnd === null || !timestamp(startTime) || !timestamp(fetchStart) ||
        !timestamp(requestStart) || !timestamp(responseStart) || !timestamp(workerStart) ||
        !timestamp(transferSize) || !Number.isSafeInteger(transferSize) || fetchStart < startTime) return refused('invalid');
    // Cross-origin restricted timestamps are absent, not zero-length phases.
    if (requestStart === 0 || responseStart === 0) return refused('restricted');
    if (workerStart > 0) return refused('service_worker');
    if (deliveryType !== undefined && typeof deliveryType !== 'string') return refused('invalid');
    if (transferSize === 0 || (deliveryType !== undefined && deliveryType !== '')) return refused('cache_unverified');
    const before = requestStart - fetchStart;
    const first = responseStart - requestStart;
    const receive = responseEnd - responseStart;
    if (!duration(before) || !duration(first) || !duration(receive)) return refused('invalid');
    return {
      timing: Object.freeze({ before_request: before, first_byte_wait: first,
        receive_after_first_byte: receive, post_transfer_until_response_complete: null }),
      responseEnd, reason: null,
    };
  } catch { return refused('invalid'); }
}

interface Candidate {
  readonly role: RequestRole;
  readonly key: RequestKey;
  url: string;
  readonly initiator: ResourceRequest['initiator'];
  readonly start: number;
  end: number | null;
  completed: boolean;
  retired: boolean;
  failure: TimingUnavailableReason | null;
  match: Match | null;
  result: ResourceTimingResult;
}

const INACTIVE: ResourceTimingOwner = Object.freeze({
  start() { return Object.freeze({ complete: () => unavailable('late') }); },
  flush() {}, retire() {}, disconnect() {},
});

/**
 * Create once in an existing permitted journey/request owner, before HTTP work.
 * Keep each returned ticket with that physical request, outside query-cache data.
 * Spread complete()'s result into the selected request's recordRequest call.
 *
 * At most 32 candidates retain bounded resolved URLs. Matches retain numbers
 * only; unrelated entries are discarded. Overflow (including observer batches
 * over 256) or retiring in-flight work disables timing for this owner, preserving
 * trace selection. A new journey can observe again. No shared buffer is changed.
 */
export function createResourceTiming(options: ResourceTimingOptions): ResourceTimingOwner {
  try { return createOwner(options.attempt, options.canCapture, options.baseUrl); }
  catch { return INACTIVE; }
}

function createOwner(attempt: JourneyAttempt, canCapture: () => boolean, baseUrl: string): ResourceTimingOwner {
  let observer: PerformanceObserver | null = null;
  let closed = false;
  let disabled: TimingUnavailableReason | null = null;
  const pending: Candidate[] = [];

  function forget(candidate: Candidate): void {
    candidate.url = '';
    candidate.match = null;
  }
  function release(candidate: Candidate): void {
    const index = pending.indexOf(candidate);
    if (index >= 0) pending.splice(index, 1);
    forget(candidate);
  }
  function disconnectObserver(): void {
    const previous = observer;
    observer = null;
    try { previous?.disconnect(); }
    catch { /* Timing teardown cannot change the request or navigation. */ }
  }
  function notify(candidate: Candidate): void {
    try {
      if (permitted() && attempt.isSelected(candidate.role, candidate.key)) {
        attempt.recordTiming({ role: candidate.role, requestKey: candidate.key, ...candidate.result });
      }
    } catch { /* A failed optional timing update cannot change readiness. */ }
  }
  function disable(reason: TimingUnavailableReason): void {
    disabled = reason;
    disconnectObserver();
    while (pending.length) {
      const candidate = pending.pop()!;
      forget(candidate);
      candidate.result = unavailable(reason);
      if (candidate.completed) notify(candidate);
    }
  }
  function disconnect(): void {
    if (closed) return;
    closed = true;
    disconnectObserver(); // disconnect also empties the browser-owned queue.
    while (pending.length) {
      const candidate = pending.pop()!;
      forget(candidate);
      candidate.retired = true;
      candidate.result = unavailable('late');
    }
  }
  function permitted(): boolean {
    if (closed) return false;
    try { if (canCapture() === true && attempt.isActive()) return !closed; }
    catch { /* Broken permission is denial, never a telemetry exception. */ }
    disconnect();
    try { attempt.discard(); } catch { /* Discard remains best effort at this boundary. */ }
    return false;
  }
  function settle(candidate: Candidate): void {
    if (!candidate.completed || candidate.retired || !pending.includes(candidate)) return;
    const match = candidate.match;
    if (candidate.failure !== null) {
      candidate.result = unavailable(candidate.failure);
    } else if (match === null || (match.responseEnd !== null && match.responseEnd > candidate.end!)) {
      candidate.match = null;
      candidate.result = unavailable('not_found');
      return; // A later callback can still fill this current request before terminal.
    } else if (match.reason !== null) {
      candidate.result = unavailable(match.reason);
    } else {
      const post = candidate.end! - match.responseEnd!;
      candidate.result = duration(post) ? Object.freeze({
        timing: Object.freeze({ ...match.timing, post_transfer_until_response_complete: post }),
        timingUnavailableReason: null,
      }) : unavailable('invalid');
    }
    release(candidate);
    notify(candidate);
  }
  function accept(entries: PerformanceEntry[]): void {
    if (!permitted() || disabled !== null || pending.length === 0) return;
    if (!Array.isArray(entries) || entries.length > MAX_ENTRY_BATCH) { disable('overflow'); return; }
    try {
      for (const item of entries) {
        if (item.entryType !== 'resource') continue;
        const entry = item as PerformanceResourceTiming;
        const name = entry.name;
        const initiator = entry.initiatorType;
        const start = entry.startTime;
        if (typeof name !== 'string' || name.length > URL_LIMIT || !timestamp(start)) continue;
        const matches = pending.filter(candidate => candidate.url === name && candidate.initiator === initiator &&
          start >= candidate.start && (candidate.end === null || start <= candidate.end));
        if (!matches.length) continue;
        const match = project(entry);
        for (const candidate of matches) {
          if (candidate.end !== null && match.responseEnd !== null && match.responseEnd > candidate.end) continue;
          if (matches.length > 1 || candidate.match !== null) candidate.failure = 'ambiguous';
          else candidate.match = match;
        }
      }
      // Process the whole batch first: a second entry must invalidate a match.
      for (let index = pending.length - 1; index >= 0; index -= 1) settle(pending[index]!);
    } catch { disable('invalid'); }
  }
  function flush(): void {
    if (!permitted() || disabled !== null || observer === null) return;
    try { accept(observer.takeRecords()); }
    catch { disable('unsupported'); }
  }
  function ensureObserver(): boolean {
    if (observer !== null) return true;
    try {
      if (typeof PerformanceObserver !== 'function') { disable('unsupported'); return false; }
      observer = new PerformanceObserver(list => {
        if (!permitted() || disabled !== null) return;
        try { accept(list.getEntries()); }
        catch { disable('unsupported'); }
      });
      observer.observe({ type: 'resource', buffered: false });
      return true;
    } catch { disable('unsupported'); return false; }
  }
  function ticket(candidate: Candidate): ResourceTimingTicket {
    return Object.freeze({
      complete(input: ResourceCompletion): ResourceTimingResult {
        if (!permitted() || candidate.retired) return unavailable('late');
        if (disabled !== null) return unavailable(disabled);
        if (candidate.completed) {
          try { return attempt.isSelected(candidate.role, candidate.key) ? candidate.result : unavailable('late'); }
          catch { return unavailable('invalid'); }
        }
        candidate.completed = true;
        try {
          const end = input.responseComplete ?? clock();
          const current = clock();
          if (!timestamp(end) || current === null || end > current || !duration(end - candidate.start)) {
            candidate.result = unavailable('invalid');
            release(candidate);
            return candidate.result;
          }
          candidate.end = end;
          const provenance = input.provenance;
          if (provenance !== 'network') {
            candidate.result = unavailable(provenance === 'cache_only' || provenance === 'cache_unverified' ? 'cache_unverified' : 'invalid');
            release(candidate);
            return candidate.result;
          }
          flush();
          if (disabled !== null) return unavailable(disabled);
          settle(candidate);
          return candidate.result;
        } catch {
          candidate.result = unavailable('invalid');
          release(candidate);
          return candidate.result;
        }
      },
    });
  }
  function refused(reason: TimingUnavailableReason): ResourceTimingTicket {
    return Object.freeze({ complete: () => unavailable(permitted() ? reason : 'late') });
  }
  const owner: ResourceTimingOwner = Object.freeze({
    start(input: ResourceRequest): ResourceTimingTicket {
      if (!permitted()) return refused('late');
      if (disabled !== null) return refused(disabled);
      try {
        const role = input.role;
        const key = input.requestKey;
        if (typeof role !== 'string' || role.length < 1 || role.length > 16 ||
            typeof key !== 'string' || key.length < 1 || key.length > 200) return refused('invalid');
        const existing = pending.find(candidate => candidate.role === role && candidate.key === key);
        if (existing) return ticket(existing);
        const url = resolvedUrl(input.url, baseUrl);
        const initiator = input.initiator;
        const start = input.startedAt ?? clock();
        const current = clock();
        if (url === null || !timestamp(start) || current === null || start > current ||
            (initiator !== 'xmlhttprequest' && initiator !== 'fetch')) return refused('invalid');
        if (pending.length >= MAX_PENDING) { disable('overflow'); return refused('overflow'); }
        if (!ensureObserver()) return refused('unsupported');
        const candidate: Candidate = {
          role, key, url, initiator, start, end: null, completed: false, retired: false,
          failure: null, match: null, result: unavailable('pending'),
        };
        for (const other of pending) {
          if (other.url === url && other.initiator === initiator && (other.end === null || start <= other.end)) {
            other.failure = candidate.failure = 'ambiguous';
          }
        }
        pending.push(candidate);
        return ticket(candidate);
      } catch { return refused('invalid'); }
    },
    flush,
    retire(role: RequestRole, key: RequestKey): void {
      const candidate = pending.find(item => item.role === role && item.key === key);
      if (!candidate) return;
      candidate.retired = true;
      if (!candidate.completed) disable('ambiguous');
      else release(candidate);
      candidate.result = unavailable('late');
    },
    disconnect,
  });
  if (attempt.attachTiming(owner) !== true) disconnect();
  return owner;
}
