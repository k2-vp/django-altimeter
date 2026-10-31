import axios from 'axios';
import posthog, { type CaptureResult } from 'posthog-js';
import { beginBoardNavigation, boardCommitted } from '../../src/django_altimeter/browser/examples/asgi';
import { beginRepairNavigation, repairCommitted, sessionHintHeaders } from '../../src/django_altimeter/browser/examples/wsgi';
import { readTraceHeaders } from '../../src/django_altimeter/browser/headers';
import type { ContributorCoverage, JourneyAttempt, JourneyOptions, RequestProvenance, RequestRole } from '../../src/django_altimeter/browser/journey';
import * as timingModule from '../../src/django_altimeter/browser/resource_timing';

// The reference application's existing SDK and axios client are configured once.
// Vendored helpers neither initialize clients nor install interceptors.
const apiOrigin = document.querySelector<HTMLMetaElement>('meta[name="api-origin"]')!.content;
const status = document.querySelector<HTMLParagraphElement>('#status')!;
const list = document.querySelector<HTMLUListElement>('#items')!;
const cancelButton = document.querySelector<HTMLButtonElement>('#cancel')!;
const client = axios.create({ baseURL: apiOrigin, timeout: 5_000, adapter: 'xhr' });
const parameters = new URLSearchParams(location.search);
let consent = parameters.get('consent') !== 'off';
let appAllowsHints = parameters.get('hints') !== 'off';
let scenario = parameters.get('scenario') ?? 'success';
let currentScreen: 'board' | 'repair' = 'board';
let attempt: JourneyAttempt;
let timing: ReturnType<typeof timingModule.createResourceTiming>;
let controller: AbortController | undefined;
let navigationNumber = 0;
let executionNumber = 0;
const canCapture = () => consent;

// Keep the SDK ingestion envelope with only fixed journey fields or finalized
// numeric vitals. Raw URL/DOM attribution and campaign properties never leave.
function sanitize(event: CaptureResult | null): CaptureResult | null {
  if (!event || !consent) return null;
  const fields = /^(board|repair)_(ready|failed|cancelled)$/.test(event.event)
    ? ['screen', 'navigation_type', 'release', 'outcome', 'duration_ms', 'item_count',
      'trace_missing_reason', 'cancel_reason', 'requests']
    : event.event === '$web_vitals' ? ['$web_vitals_CLS_value', '$web_vitals_INP_value'] : null;
  if (!fields) return null;
  const properties: Record<string, unknown> = {
    token: event.properties.token, distinct_id: event.properties.distinct_id, page_category: currentScreen,
  };
  for (const field of fields) {
    const value = event.properties[field];
    if (event.event === '$web_vitals') {
      if (typeof value === 'number' && Number.isFinite(value) && value >= 0 && value <= 86_400_000) properties[field] = value;
    } else if (value !== undefined) properties[field] = value;
  }
  return { uuid: event.uuid, event: event.event, properties: properties as CaptureResult['properties'], timestamp: event.timestamp };
}

posthog.init('phc_reference_public_project', {
  debug: parameters.get('debug') === 'on',
  api_host: location.origin, persistence: 'memory', autocapture: false,
  capture_pageview: false, capture_pageleave: false, capture_exceptions: false,
  disable_session_recording: true, disable_surveys: true, advanced_disable_flags: true,
  request_batching: false,
  opt_out_useragent_filter: true, // This reference runs under real browser automation.
  disable_compression: true, // Inspect the real serialized SDK request in the test receiver.
  opt_out_capturing_by_default: !consent,
  capture_performance: parameters.get('vitals') === 'on' ? {
    web_vitals: true, web_vitals_attribution: false,
    web_vitals_allowed_metrics: ['CLS', 'INP'], web_vitals_delayed_flush_ms: 60_000,
  } : false,
  before_send: sanitize,
});

interface Begin {
  screen?: 'board' | 'repair';
  navigationType?: 'full' | 'spa';
  roles?: readonly RequestRole[];
}
function begin(input: Begin = {}): JourneyAttempt {
  attempt?.cancel('navigation');
  controller?.abort();
  controller = new AbortController();
  currentScreen = input.screen ?? 'board';
  const options: Omit<JourneyOptions, 'screen' | 'roles'> = {
    key: `navigation-${++navigationNumber}`, navigationType: input.navigationType ?? 'spa',
    release: 'reference-v1', canCapture,
    capture: (event, properties) => posthog.capture(event, properties, { send_instantly: true }),
  };
  attempt = currentScreen === 'board' ? beginBoardNavigation(options)
    : beginRepairNavigation({ ...options, roles: input.roles ?? ['detail', 'checklist'] });
  timing = timingModule.createResourceTiming({ attempt, canCapture, baseUrl: apiOrigin });
  return attempt;
}

interface RequestInput {
  role: RequestRole;
  requestKey: string;
  path: string;
  provenance?: RequestProvenance;
  coverage?: ContributorCoverage;
}
async function request(input: RequestInput) {
  const owner = attempt;
  const signal = controller?.signal;
  owner.selectRequest({ role: input.role, requestKey: input.requestKey });
  const ticket = timing.start({ role: input.role, requestKey: input.requestKey,
    url: input.path, initiator: 'xmlhttprequest', startedAt: performance.now() });
  const headers = currentScreen === 'repair' ? sessionHintHeaders({
    url: input.path, baseUrl: apiOrigin, allowedOrigins: [apiOrigin], canCapture,
    appAllowsSessionHints: appAllowsHints,
  }, () => ({ sessionId: posthog.get_session_id(), distinctId: posthog.get_distinct_id() })) : {};
  const response = await client.get(input.path, { signal, headers, validateStatus: () => true });
  const provenance = input.provenance ?? 'network';
  const projected = ticket.complete({ provenance, responseComplete: performance.now() });
  const link = readTraceHeaders({ headers: { get: name => response.headers[name.toLowerCase()] },
    provenance, canCapture: () => owner.isActive() && canCapture() });
  owner.recordRequest({ role: input.role, requestKey: input.requestKey, ...link, provenance,
    contributorCoverage: input.coverage ?? 'complete', ...projected });
  return { status: response.status, items: response.data.items as { name: string }[], traceId: link.traceId, ...projected };
}

function finish(itemCount = 1) {
  if (currentScreen === 'board') boardCommitted(attempt, { current: true, placeholder: false, itemCount });
  else repairCommitted(attempt, { current: true, requiredCommitted: true, itemCount });
}

async function load(screen: 'board' | 'repair', navigationType: 'full' | 'spa' = 'spa') {
  const owner = begin({ screen, navigationType });
  document.querySelector('#view-title')!.textContent = screen === 'board' ? 'Board' : 'Repair';
  status.textContent = 'Loading current items…';
  cancelButton.disabled = false;
  try {
    const roles: RequestRole[] = screen === 'board' ? ['board'] : ['detail', 'checklist'];
    const responses = await Promise.all(roles.map(role => request({ role, requestKey: `execution-${++executionNumber}`,
      path: `/api/data?header_ms=150&body_ms=180&scenario=${encodeURIComponent(scenario)}&role=${role}` })));
    if (owner !== attempt) return;
    if (responses.some(response => response.status >= 400)) throw new Error('request_failed');
    const items = responses[0].items;
    list.replaceChildren(...items.map(item => {
      const element = document.createElement('li'); element.textContent = item.name; return element;
    }));
    status.textContent = items.length ? 'Current items are ready.' : 'No items yet.';
    finish(items.length); // Current DOM/data commit precedes readiness.
  } catch (error) {
    if (owner !== attempt) return;
    status.textContent = axios.isCancel(error) ? 'Loading cancelled.' : 'Could not load items. Try Refresh.';
    if (!axios.isCancel(error)) owner.finish({ outcome: 'failure' });
  } finally { if (owner === attempt) cancelButton.disabled = true; }
}

const reference = {
  timingModule,
  get attempt() { return attempt; },
  get timing() { return timing; },
  begin, request, finish,
  cancel() { attempt.cancel('aborted'); controller?.abort(); },
  discard() { attempt.discard(); },
  setConsent(value: boolean) {
    if (!value) { attempt?.discard(); posthog.opt_out_capturing(); }
    consent = value;
    if (value) posthog.opt_in_capturing({ captureEventName: false });
  },
  setHintPolicy(value: boolean) { appAllowsHints = value; },
  setScenario(value: string) { scenario = value; },
  async getEvents() { return (await (await fetch('/snapshot')).json()).events; },
};
declare global { interface Window { reference: typeof reference } }
window.reference = reference;
document.querySelector('#board')!.addEventListener('click', () => void load('board'));
document.querySelector('#repair')!.addEventListener('click', () => void load('repair'));
document.querySelector('#reload')!.addEventListener('click', () => void load(currentScreen));
cancelButton.addEventListener('click', () => reference.cancel());
if (parameters.get('start') === 'board') void load('board', 'full');
if (parameters.get('start') === 'repair') void load('repair', 'full');
