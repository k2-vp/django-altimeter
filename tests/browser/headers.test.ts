import { describe, expect, it } from 'vitest';

import {
  isExactOrigin,
  maySendSessionHints,
  readTraceHeaders,
  normalizeTraceLink,
} from '../../src/django_altimeter/browser/headers';

const validateTraceCorrelation = (traceId: unknown, sampled: unknown) =>
  normalizeTraceLink({ traceId, sampled, provenance: 'network' });

const TRACE = '1234567890abcdef1234567890abcdef';
const MISSING = { traceId: null, sampled: null, traceMissingReason: 'missing' };
const API = 'https://api.example.test:8443';
const BASE = 'https://app.example.test/current/page';
const allowedOrigins = [API];
const canCapture = () => true;
const headers = (traceId: string = TRACE, sampled: string = '1') =>
  new Headers({ 'X-Altimeter-Trace-Id': traceId, 'X-Altimeter-Trace-Sampled': sampled });

// These are unit checks of native Headers and URL handling, not CORS proof.
describe('trace header validation', () => {
  it.each([true, false])('keeps a safe trace with sampled=%s', (sampled) => {
    const expected = {
      traceId: TRACE,
      sampled,
      traceMissingReason: sampled ? null : 'unsampled',
    };
    expect(validateTraceCorrelation(TRACE, sampled)).toEqual(expected);
    expect(readTraceHeaders({
      headers: headers(TRACE, sampled ? '1' : '0'), provenance: 'network', canCapture,
    })).toEqual(expected);
  });

  it.each([
    '', '0'.repeat(32), 'a'.repeat(31), 'a'.repeat(33), 'A'.repeat(32),
    'g'.repeat(32), `${TRACE}, ${TRACE}`, `${TRACE}\n`, ` ${TRACE}`,
    'é'.repeat(32), 'a'.repeat(100_000), null, undefined, 123, {},
  ])('rejects malformed or oversized trace input %#', (value) => {
    expect(validateTraceCorrelation(value, true)).toEqual(MISSING);
    expect(readTraceHeaders({
      headers: { get: (name) => name === 'X-Altimeter-Trace-Id' ? value : '1' },
      provenance: 'network', canCapture,
    })).toEqual(MISSING);
  });

  it.each(['', 'true', 'false', '01', '1, 1', '1\n', ' 1', '1'.repeat(100_000), null, 1, true])(
    'rejects malformed or missing sampled wire input %#', (value) => {
      expect(readTraceHeaders({
        headers: { get: (name) => name === 'X-Altimeter-Trace-Id' ? TRACE : value },
        provenance: 'network', canCapture,
      })).toEqual(MISSING);
    },
  );

  it.each([null, undefined, 0, 1, '0', '1', [], {}].map((sampled) => ({ sampled })))(
    'does not coerce the sampled scalar %#', ({ sampled }) => {
      expect(validateTraceCorrelation(TRACE, sampled)).toEqual(MISSING);
    },
  );

  it('rejects duplicate native headers rather than selecting one', () => {
    const duplicate = headers();
    duplicate.append('x-altimeter-trace-id', TRACE);
    expect(readTraceHeaders({ headers: duplicate, provenance: 'network', canCapture })).toEqual(MISSING);
    const sampleDuplicate = headers();
    sampleDuplicate.append('x-altimeter-trace-sampled', '1');
    expect(readTraceHeaders({ headers: sampleDuplicate, provenance: 'network', canCapture })).toEqual(MISSING);
  });

  it('reads only the two allowed headers and returns no vendor prose', () => {
    const reads: string[] = [];
    const result = readTraceHeaders({
      headers: { get(name) {
        reads.push(name);
        if (name === 'X-Altimeter-Trace-Id') return TRACE;
        if (name === 'X-Altimeter-Trace-Sampled') return '1';
        throw new Error('CANARY_BODY_AND_AUTH');
      } },
      provenance: 'network', canCapture,
    });
    expect(reads).toEqual(['X-Altimeter-Trace-Id', 'X-Altimeter-Trace-Sampled']);
    expect(result).toEqual({ traceId: TRACE, sampled: true, traceMissingReason: null });
    expect(JSON.stringify(result)).not.toContain('CANARY');
  });

  it.each(['cache_only', 'cache_unverified'] as const)(
    'never reuses or reads cached headers for %s', (provenance) => {
      let reads = 0;
      const result = readTraceHeaders({
        get headers() { reads += 1; return headers(); }, provenance, canCapture,
      });
      expect(result).toEqual({ traceId: null, sampled: null, traceMissingReason: provenance });
      expect(reads).toBe(0);
    },
  );

  it.each(['cache_only', 'cache_unverified'] as const)(
    'normalizes %s without consulting stale scalar accessors', (provenance) => {
      let reads = 0;
      const result = normalizeTraceLink({
        provenance,
        get traceId() { reads += 1; return TRACE; },
        get sampled() { reads += 1; return true; },
      });
      expect(result).toEqual({ traceId: null, sampled: null, traceMissingReason: provenance });
      expect(reads).toBe(0);
    },
  );

  it('never reads browser hints without consent, including a throwing consent check', () => {
    let reads = 0;
    for (const permission of [() => false, () => { throw new Error('CANARY_CONSENT'); }]) {
      expect(readTraceHeaders({
        get headers() { reads += 1; return headers(); }, provenance: 'network', canCapture: permission,
      })).toEqual(MISSING);
    }
    expect(reads).toBe(0);
  });

  it('contains throwing header readers and does not coerce user objects', () => {
    let coercions = 0;
    const hostile = { toString() { coercions += 1; throw new Error('CANARY'); } };
    expect(validateTraceCorrelation(hostile, true)).toEqual(MISSING);
    expect(readTraceHeaders({
      headers: { get() { throw new Error('CANARY_VENDOR_BODY'); } },
      provenance: 'network', canCapture,
    })).toEqual(MISSING);
    expect(coercions).toBe(0);
  });
});

describe('exact-origin policy', () => {
  it.each([
    [`${API}/safe?canary=not_retained`, BASE, [API]],
    ['/safe', `${API}/base`, [API]],
    ['../safe', `${API}/base/child`, [API]],
    [`//api.example.test:8443/safe`, BASE, [API]],
    ['https://API.EXAMPLE.TEST:443/safe', BASE, ['https://api.example.test']],
    ['https://[::1]:8443/safe', BASE, ['https://[::1]:8443']],
  ] as const)('accepts the actual resolved origin %#', (url, base, allowed) => {
    expect(isExactOrigin(url, base, allowed)).toBe(true);
  });

  it.each([
    'http://api.example.test:8443/safe', 'https://api.example.test/safe',
    'https://api.example.test:9443/safe', 'https://api.example.test.evil.test:8443/safe',
    'https://api.example.test:8443@evil.test/safe', `${API}@evil.test/safe`,
    'https://user:password@api.example.test:8443/safe', 'https://other.test/safe',
    'data:text/html,secret', `blob:${API}/id`, 'file:///private/canary',
    'javascript:alert(1)', 'not a URL', '', '\n' + API, 'https:\\api.example.test:8443/safe',
    `${API}/${'x'.repeat(2048)}`,
  ])('declines unapproved, opaque or malformed destinations %#', (url) => {
    expect(isExactOrigin(url, BASE, allowedOrigins)).toBe(false);
  });

  it.each([
    ['*'], ['null'], [`${API}/private`], [`${API}?query=1`], [`${API}#fragment`],
    ['https://user@api.example.test:8443'], Array.from({ length: 129 }, () => API), [],
  ].map((allowed) => ({ allowed })))('declines invalid or oversized origin configuration %#', ({ allowed }) => {
    expect(isExactOrigin(`${API}/safe`, BASE, allowed)).toBe(false);
  });

  it('accepts the maximum bounded URL and final allowed origin', () => {
    const prefix = `${API}/`;
    const url = prefix + 'a'.repeat(2048 - prefix.length);
    const allowed = Array.from({ length: 128 }, () => 'https://other.test');
    allowed[127] = API;
    expect(isExactOrigin(url, BASE, allowed)).toBe(true);
    expect(isExactOrigin(url + 'a', BASE, allowed)).toBe(false);
  });

  it('bounds URL and allow-list work before parsing or walking values', () => {
    let reads = 0;
    const excessive = new Array<string>(129);
    Object.defineProperty(excessive, 0, { get() { reads += 1; return API; } });
    expect(isExactOrigin(`${API}/safe`, BASE, excessive)).toBe(false);
    expect(reads).toBe(0);
    expect(isExactOrigin(`${API}/${'x'.repeat(100_000)}`, BASE, [API])).toBe(false);
  });

  it('does not extend the traversal when a getter changes the configuration', () => {
    const allowed = ['https://other.test'];
    Object.defineProperty(allowed, 0, { get() {
      allowed.push(API);
      return 'https://other.test';
    } });
    expect(isExactOrigin(`${API}/safe`, BASE, allowed)).toBe(false);
  });

  it('contains hostile configuration accessors', () => {
    const allowed: string[] = [];
    Object.defineProperty(allowed, 0, { get() { throw new Error('CANARY'); } });
    expect(isExactOrigin(`${API}/safe`, BASE, allowed)).toBe(false);
  });
});

describe('session-hint permission', () => {
  const options = { url: `${API}/safe`, baseUrl: BASE, allowedOrigins, canCapture, appAllowsSessionHints: true };

  it('requires exact origin, current consent and explicit app policy together', () => {
    expect(maySendSessionHints(options)).toBe(true);
    expect(maySendSessionHints({ ...options, url: 'https://other.test/safe' })).toBe(false);
    expect(maySendSessionHints({ ...options, canCapture: () => false })).toBe(false);
    expect(maySendSessionHints({ ...options, appAllowsSessionHints: false })).toBe(false);
    expect(maySendSessionHints({ ...options, canCapture: () => { throw new Error('CANARY'); } })).toBe(false);
  });

  it('checks permission on each call rather than caching a prior grant', () => {
    let consent = true;
    const current = { ...options, canCapture: () => consent };
    expect(maySendSessionHints(current)).toBe(true);
    consent = false;
    expect(maySendSessionHints(current)).toBe(false);
  });

  it('does not read a destination when policy or consent is off', () => {
    let reads = 0;
    const current = { ...options, get url() { reads += 1; return options.url; } };
    expect(maySendSessionHints({ ...options, appAllowsSessionHints: false, get url() { reads += 1; return options.url; } })).toBe(false);
    current.canCapture = () => false;
    expect(maySendSessionHints(current)).toBe(false);
    expect(reads).toBe(0);
  });
});
