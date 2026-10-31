/** Exact-origin permission and bounded response correlation; no client or interceptor. */

export type RequestProvenance = 'network' | 'cache_only' | 'cache_unverified';
export type TraceMissingReason = 'missing' | 'cache_only' | 'cache_unverified' | 'unsampled';

export interface TraceLink {
  readonly traceId: string | null;
  readonly sampled: boolean | null;
  readonly traceMissingReason: TraceMissingReason | null;
}

export interface TraceLinkInput {
  readonly traceId?: unknown;
  readonly sampled?: unknown;
  readonly provenance: RequestProvenance;
}

export interface TraceHeaderInput {
  readonly headers: { get(name: string): unknown };
  readonly provenance: RequestProvenance;
  readonly canCapture: () => boolean;
}

export interface SessionHintPermission {
  readonly url: string;
  readonly baseUrl: string;
  readonly allowedOrigins: readonly string[];
  readonly canCapture: () => boolean;
  readonly appAllowsSessionHints: boolean;
}

const MISSING: TraceLink = Object.freeze({
  traceId: null, sampled: null, traceMissingReason: 'missing',
});
const URL_LIMIT = 2048;
const ORIGIN_LIMIT = 128;

/** Validate extracted scalar metadata; cache observations never reuse a trace. */
export function normalizeTraceLink(input: TraceLinkInput): TraceLink {
  try {
    const provenance = input.provenance;
    if (provenance === 'cache_only' || provenance === 'cache_unverified') {
      return Object.freeze({ traceId: null, sampled: null, traceMissingReason: provenance });
    }
    if (provenance !== 'network') return MISSING;
    const traceId = input.traceId;
    const sampled = input.sampled;
    if (
      typeof traceId !== 'string' || traceId.length !== 32 ||
      typeof sampled !== 'boolean' || !/^[0-9a-f]{32}$/.test(traceId) ||
      traceId === '00000000000000000000000000000000'
    ) return MISSING;
    return Object.freeze({ traceId, sampled, traceMissingReason: sampled ? null : 'unsampled' });
  } catch {
    return MISSING;
  }
}

/**
 * Read only fresh, permitted response headers. The request owner establishes
 * provenance: an ID or sample flag is never an ingestion receipt. Pass native
 * Headers or the existing client's get(name) reader; do not retain response data.
 */
export function readTraceHeaders(input: TraceHeaderInput): TraceLink {
  try {
    if (input.canCapture() !== true) return MISSING;
    const provenance = input.provenance;
    if (provenance !== 'network') return normalizeTraceLink({ provenance });
    const headers = input.headers;
    const traceId = headers.get('X-Altimeter-Trace-Id');
    const sampled = headers.get('X-Altimeter-Trace-Sampled');
    if (sampled !== '0' && sampled !== '1') return MISSING;
    return normalizeTraceLink({ traceId, sampled: sampled === '1', provenance });
  } catch {
    return MISSING;
  }
}

function boundedUrl(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0 && value.length <= URL_LIMIT &&
    !/[\u0000-\u0020\u007f\\]/.test(value);
}

function httpOrigin(url: URL): boolean {
  return (url.protocol === 'http:' || url.protocol === 'https:') &&
    url.hostname !== '' && !url.hostname.includes('*') && url.port !== '0' &&
    url.username === '' && url.password === '';
}

/** Resolve the effective HTTP(S) URL; compare scheme, host AND port, never suffixes. */
export function isExactOrigin(
  url: string, baseUrl: string, allowedOrigins: readonly string[],
): boolean {
  try {
    if (
      !boundedUrl(url) || !boundedUrl(baseUrl) || !Array.isArray(allowedOrigins)
    ) return false;
    const count = allowedOrigins.length;
    if (!Number.isInteger(count) || count < 1 || count > ORIGIN_LIMIT) return false;
    const base = new URL(baseUrl);
    if (!httpOrigin(base)) return false;
    const destination = new URL(url, base);
    if (!httpOrigin(destination)) return false;
    for (let index = 0; index < count; index += 1) {
      const configured = allowedOrigins[index];
      if (!boundedUrl(configured)) continue;
      const origin = new URL(configured);
      if (
        httpOrigin(origin) && origin.pathname === '/' && origin.search === '' &&
        origin.hash === '' && destination.origin === origin.origin
      ) return true;
    }
    return false;
  } catch {
    return false;
  }
}

/**
 * Permission only: the existing client owns headers and redirects. Use approved
 * routes without cross-origin redirects; recheck consent for every request. A
 * browser hint never establishes server identity. This function sends nothing.
 */
export function maySendSessionHints(input: SessionHintPermission): boolean {
  try {
    if (input.appAllowsSessionHints !== true || input.canCapture() !== true) return false;
    return isExactOrigin(input.url, input.baseUrl, input.allowedOrigins);
  } catch {
    return false;
  }
}
