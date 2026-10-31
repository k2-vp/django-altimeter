/** One consent-bound navigation; callers own readiness and current request selection. */
import { normalizeTraceLink, type RequestProvenance, type TraceMissingReason } from './headers';

export type { RequestProvenance, TraceMissingReason } from './headers';
export const MAX_DURATION_MS = 86_400_000;
export type RequestRole = 'board' | 'detail' | 'checklist' | 'hidden_documents' | 'media';
export type RequestKey = string;
export type ContributorCoverage = 'complete' | 'partial' | 'unknown';
export type TimingUnavailableReason = 'pending' | 'unsupported' | 'not_found' | 'ambiguous' |
  'restricted' | 'cache_unverified' | 'service_worker' | 'overflow' | 'invalid' | 'late';
export interface RequestTiming {
  readonly before_request: number | null;
  readonly first_byte_wait: number | null;
  readonly receive_after_first_byte: number | null;
  readonly post_transfer_until_response_complete: number | null;
}
export interface RequestSelection {
  readonly role: RequestRole;
  readonly requestKey: RequestKey;
}
export interface TimingUpdate extends RequestSelection {
  readonly timing: Partial<RequestTiming>;
  readonly timingUnavailableReason?: TimingUnavailableReason | null;
}
export interface RequestObservation extends RequestSelection {
  readonly traceId: string | null;
  readonly sampled: boolean | null;
  readonly provenance: RequestProvenance;
  readonly contributorCoverage: ContributorCoverage;
  readonly timing?: Partial<RequestTiming>;
  readonly timingUnavailableReason?: TimingUnavailableReason | null;
}
export type JourneyRequest = Readonly<{
  role: RequestRole;
  trace_id: string | null;
  sampled: boolean | null;
  trace_missing_reason: TraceMissingReason | null;
  provenance: RequestProvenance;
  contributor_coverage: ContributorCoverage;
  timing: RequestTiming;
  timing_unavailable_reason: TimingUnavailableReason | null;
}>;
export type CancellationReason = 'navigation' | 'superseded' | 'aborted';
export type JourneyEventName = 'board_ready' | 'board_failed' | 'board_cancelled' |
  'repair_ready' | 'repair_failed' | 'repair_cancelled';
export type JourneyEventProperties = Readonly<{
  screen: 'board' | 'repair';
  navigation_type: 'full' | 'spa';
  release: string;
  outcome: 'success' | 'failure' | 'cancelled';
  duration_ms: number | null;
  item_count: number | null;
  trace_missing_reason: TraceMissingReason | null;
  cancel_reason: CancellationReason | null;
  requests: readonly JourneyRequest[];
}>;
export interface JourneyOptions {
  readonly key: string;
  readonly screen: 'board' | 'repair';
  readonly navigationType: 'full' | 'spa';
  readonly release: string;
  readonly roles: readonly RequestRole[];
  readonly capture: (event: JourneyEventName, properties: JourneyEventProperties) => unknown;
  readonly canCapture: () => boolean;
  readonly signal?: AbortSignal;
}
export interface JourneyFinish {
  readonly outcome: 'success' | 'failure';
  readonly itemCount?: number;
  readonly traceMissingReason?: TraceMissingReason | null;
}
export interface JourneyTimingOwner {
  flush(): void;
  retire(role: RequestRole, requestKey: RequestKey): void;
  disconnect(): void;
}
export interface JourneyAttempt {
  selectRequest(input: RequestSelection): boolean;
  recordRequest(input: RequestObservation): void;
  recordTiming(input: TimingUpdate): void;
  attachTiming(owner: JourneyTimingOwner): boolean;
  isActive(): boolean;
  isSelected(role: RequestRole, requestKey: RequestKey): boolean;
  finish(input: JourneyFinish): void;
  cancel(reason: CancellationReason): void;
  discard(): void;
}

const ROLES: readonly RequestRole[] = ['board', 'detail', 'checklist', 'hidden_documents', 'media'];
const REASONS: readonly TimingUnavailableReason[] = [
  'pending', 'unsupported', 'not_found', 'ambiguous', 'restricted', 'cache_unverified',
  'service_worker', 'overflow', 'invalid', 'late',
];
const MISSING_REASONS: readonly TraceMissingReason[] = ['missing', 'cache_only', 'cache_unverified', 'unsampled'];
const PHASES = ['before_request', 'first_byte_wait', 'receive_after_first_byte', 'post_transfer_until_response_complete'] as const;
const EMPTY_TIMING: RequestTiming = Object.freeze({
  before_request: null, first_byte_wait: null,
  receive_after_first_byte: null, post_transfer_until_response_complete: null,
});

/** Read fixed own data fields only; never enumerate extras or invoke input getters. */
function field(value: unknown, key: string): unknown {
  if (value === null || typeof value !== 'object') return undefined;
  try { return Object.getOwnPropertyDescriptor(value, key)?.value; }
  catch { return undefined; }
}
function member<T extends string>(value: unknown, values: readonly T[]): value is T {
  return typeof value === 'string' && values.includes(value as T);
}
function validKey(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0 && value.length <= 200;
}
function milliseconds(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0 && value <= MAX_DURATION_MS ? value : null;
}
function clock(): number | null {
  try {
    const value = performance.now();
    return typeof value === 'number' && Number.isFinite(value) && value >= 0 ? value : null;
  } catch { return null; }
}
function timing(input: unknown): RequestTiming {
  return Object.freeze({
    before_request: milliseconds(field(input, 'before_request')),
    first_byte_wait: milliseconds(field(input, 'first_byte_wait')),
    receive_after_first_byte: milliseconds(field(input, 'receive_after_first_byte')),
    post_transfer_until_response_complete: milliseconds(field(input, 'post_transfer_until_response_complete')),
  });
}
function restricted(reason: TimingUnavailableReason | null): boolean {
  return reason !== null && reason !== 'pending' && reason !== 'not_found';
}
function reason(input: unknown, phases: RequestTiming): TimingUnavailableReason | null {
  if (member(input, REASONS)) return input;
  if (input !== undefined && input !== null) return 'invalid';
  return PHASES.every((phase) => phases[phase] !== null) ? null : 'pending';
}

interface Configuration extends Omit<JourneyOptions, 'key' | 'signal'> {
  readonly signal: AbortSignal | undefined;
}
function configuration(input: unknown): Configuration | null {
  try {
    const canCapture = field(input, 'canCapture');
    if (typeof canCapture !== 'function' || canCapture() !== true) return null;
    const capture = field(input, 'capture');
    const screen = field(input, 'screen');
    const navigationType = field(input, 'navigationType');
    const release = field(input, 'release');
    const roles = field(input, 'roles');
    const count = Array.isArray(roles) ? roles.length : 0;
    if (
      typeof capture !== 'function' || !validKey(field(input, 'key')) ||
      !member(screen, ['board', 'repair'] as const) ||
      !member(navigationType, ['full', 'spa'] as const) ||
      typeof release !== 'string' || release.length < 1 || release.length > 128 ||
      !/^[A-Za-z0-9][A-Za-z0-9._+\-]*$/.test(release) ||
      !Number.isInteger(count) || count < 1 || count > 4
    ) return null;
    const checked: RequestRole[] = [];
    for (let index = 0; index < count; index += 1) {
      const role = field(roles, String(index));
      if (!member(role, ROLES) || checked.includes(role) ||
          (screen === 'board' ? role !== 'board' : role === 'board')) return null;
      checked.push(role);
    }
    return Object.freeze({
      screen, navigationType, release, roles: Object.freeze(checked),
      canCapture: canCapture as () => boolean,
      capture: capture as JourneyOptions['capture'],
      signal: field(input, 'signal') as AbortSignal | undefined,
    });
  } catch { return null; }
}

const INACTIVE: JourneyAttempt = Object.freeze({
  selectRequest: () => false, recordRequest() {}, recordTiming() {},
  attachTiming(owner: JourneyTimingOwner) {
    try { owner.disconnect(); } catch { /* Failed telemetry cleanup stays local. */ }
    return false;
  },
  isActive: () => false, isSelected: () => false,
  finish() {}, cancel() {}, discard() {},
});

function lifecycleActive(signal: AbortSignal | undefined): boolean {
  try { return signal === undefined || signal.aborted === false; }
  catch { return false; }
}

interface Selection {
  readonly key: RequestKey;
  observation: JourneyRequest | null;
}

/**
 * Retain this attempt in the existing navigation owner, not in a render/effect.
 * Select a current execution before recording it; obsolete completions cannot
 * choose themselves. Readiness remains the application's committed-data decision.
 * No raw keys, URLs, responses, arbitrary fields or request history are exported.
 */
export function beginJourney(input: JourneyOptions): JourneyAttempt {
  const config = configuration(input);
  return config === null || !lifecycleActive(config.signal) ? INACTIVE : createAttempt(config);
}

function createAttempt(config: Configuration): JourneyAttempt {
  let active = true;
  let finishing = false;
  let discarded = false;
  let owner: JourneyTimingOwner | null = null;
  let listening = false;
  const selections = new Map<RequestRole, Selection>();

  function stop(): void {
    active = false;
    selections.clear();
    if (listening) {
      listening = false;
      try { config.signal?.removeEventListener('abort', discard); }
      catch { /* A failed listener cleanup cannot revive this attempt. */ }
    }
    const previous = owner;
    owner = null;
    try { previous?.disconnect(); } catch { /* Never change application control flow. */ }
  }
  function discard(): void {
    discarded = true;
    stop();
  }
  function permitted(): boolean {
    if (!active || discarded) return false;
    try {
      if (config.canCapture() === true && lifecycleActive(config.signal)) return active && !discarded;
    }
    catch { /* A broken permission source is denial. */ }
    discard();
    return false;
  }
  function selection(input: unknown): { role: RequestRole; current: Selection } | null {
    const role = field(input, 'role');
    if (!member(role, ROLES)) return null;
    const current = selections.get(role);
    return current && current.key === field(input, 'requestKey') ? { role, current } : null;
  }
  function finish(input: JourneyFinish, cancellation: CancellationReason | null = null): void {
    if (finishing || !permitted()) return;
    const outcome = cancellation === null ? field(input, 'outcome') : 'cancelled';
    if (!member(outcome, ['success', 'failure', 'cancelled'] as const)) { discard(); return; }
    finishing = true;
    try { owner?.flush(); } catch { /* Missing timing never delays readiness. */ }
    if (!permitted()) return;
    const ended = clock();
    const count = field(input, 'itemCount');
    const missing = field(input, 'traceMissingReason');
    const requests: JourneyRequest[] = [];
    for (const role of config.roles) {
      const observation = selections.get(role)?.observation;
      if (observation) requests.push(observation);
    }
    const properties: JourneyEventProperties = Object.freeze({
      screen: config.screen, navigation_type: config.navigationType, release: config.release,
      outcome, duration_ms: started === null || ended === null ? null : milliseconds(ended - started),
      item_count: typeof count === 'number' && Number.isSafeInteger(count) && count >= 0 && count <= 1_000_000 ? count : null,
      trace_missing_reason: member(missing, MISSING_REASONS) ? missing : null,
      cancel_reason: cancellation, requests: Object.freeze(requests),
    });
    const suffix = outcome === 'success' ? 'ready' : outcome === 'failure' ? 'failed' : 'cancelled';
    const event = `${config.screen}_${suffix}` as JourneyEventName;
    stop();
    try {
      if (config.canCapture() !== true || !lifecycleActive(config.signal)) return;
      // Permission callbacks can synchronously discard this attempt.
      if (discarded) return;
      const result = config.capture(event, properties);
      if (result !== null && (typeof result === 'object' || typeof result === 'function') &&
          typeof (result as { then?: unknown }).then === 'function') {
        void Promise.resolve(result).catch(() => {});
      }
    } catch { /* Best-effort capture is terminal even when the existing client fails. */ }
  }

  if (config.signal !== undefined) {
    listening = true;
    try { config.signal.addEventListener('abort', discard, { once: true }); }
    catch { discard(); return INACTIVE; }
    if (!lifecycleActive(config.signal) || discarded) { discard(); return INACTIVE; }
  }
  const started = clock();

  return Object.freeze({
    selectRequest(input: RequestSelection): boolean {
      if (finishing || !permitted()) return false;
      const role = field(input, 'role');
      const key = field(input, 'requestKey');
      if (!member(role, config.roles) || !validKey(key)) return false;
      const previous = selections.get(role);
      if (previous?.key === key) return true;
      selections.set(role, { key, observation: null });
      if (previous) {
        try { owner?.retire(role, previous.key); } catch { /* Selection still advances. */ }
      }
      return true;
    },
    recordRequest(input: RequestObservation): void {
      if (finishing || !permitted()) return;
      const selected = selection(input);
      if (selected === null || selected.current.observation !== null) return;
      const proposed = field(input, 'provenance');
      const provenance = member(proposed, ['network', 'cache_only', 'cache_unverified'] as const) ? proposed : 'cache_unverified';
      const link = normalizeTraceLink({
        provenance, traceId: field(input, 'traceId'), sampled: field(input, 'sampled'),
      });
      const coverage = field(input, 'contributorCoverage');
      let phases = timing(field(input, 'timing'));
      const unavailable = provenance === 'network' ? reason(field(input, 'timingUnavailableReason'), phases) : 'cache_unverified';
      if (provenance !== 'network' || restricted(unavailable)) phases = EMPTY_TIMING;
      selected.current.observation = Object.freeze({
        role: selected.role, trace_id: link.traceId, sampled: link.sampled,
        trace_missing_reason: link.traceMissingReason, provenance,
        contributor_coverage: member(coverage, ['complete', 'partial', 'unknown'] as const) ? coverage : 'unknown',
        timing: phases, timing_unavailable_reason: unavailable,
      });
    },
    recordTiming(input: TimingUpdate): void {
      if (!permitted()) return;
      const selected = selection(input);
      const previous = selected?.current.observation;
      if (!selected || !previous || previous.provenance !== 'network' || restricted(previous.timing_unavailable_reason)) return;
      const update = timing(field(input, 'timing'));
      const updateReason = reason(field(input, 'timingUnavailableReason'), update);
      const filled = { ...previous.timing };
      if (!restricted(updateReason)) {
        for (const phase of PHASES) if (filled[phase] === null) filled[phase] = update[phase];
      }
      const nextReason = restricted(updateReason) ? updateReason :
        PHASES.every((phase) => filled[phase] !== null) ? null : updateReason;
      selected.current.observation = Object.freeze({
        ...previous, timing: Object.freeze(filled), timing_unavailable_reason: nextReason,
      });
    },
    attachTiming(candidate: JourneyTimingOwner): boolean {
      if (owner === candidate && permitted()) return true;
      if (!finishing && permitted() && owner === null) {
        try {
          if (typeof candidate.flush === 'function' && typeof candidate.retire === 'function' && typeof candidate.disconnect === 'function') {
            owner = candidate;
            return true;
          }
        } catch { /* Malformed observer input is rejected. */ }
      }
      try { candidate.disconnect(); } catch { /* Retain no refused observer. */ }
      return false;
    },
    isActive: permitted,
    isSelected(role: RequestRole, requestKey: RequestKey): boolean {
      return permitted() && selections.get(role)?.key === requestKey;
    },
    finish,
    cancel(reason: CancellationReason): void {
      if (!member(reason, ['navigation', 'superseded', 'aborted'] as const)) { discard(); return; }
      finish({ outcome: 'failure' }, reason);
    },
    discard,
  });
}
