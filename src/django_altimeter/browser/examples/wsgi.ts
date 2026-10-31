/** Adapt these calls in the existing repair navigation and API request owners. */
import { maySendSessionHints, type SessionHintPermission } from '../headers';
import { beginJourney, type JourneyAttempt, type JourneyOptions } from '../journey';

export function beginRepairNavigation(options: Omit<JourneyOptions, 'screen'>): JourneyAttempt {
  return beginJourney({ ...options, screen: 'repair' });
}

/** The existing screen decides its required dataset and preserves its error policy. */
export function repairCommitted(
  attempt: JourneyAttempt,
  result: { readonly current: boolean; readonly requiredCommitted: boolean; readonly itemCount: number },
): void {
  if (result.current && result.requiredCommitted) {
    attempt.finish({ outcome: 'success', itemCount: result.itemCount });
  }
}

/** Add to the existing client's headers only on approved routes without redirects. */
export function sessionHintHeaders(
  permission: SessionHintPermission,
  existingIdentity: () => { readonly sessionId: string; readonly distinctId: string },
): Readonly<Record<string, string>> {
  try {
    if (!maySendSessionHints(permission)) return {};
    const { sessionId, distinctId } = existingIdentity();
    if (![sessionId, distinctId].every(value => typeof value === 'string' &&
      value.length > 0 && value.length <= 128 && /^[\x21-\x7e]+$/.test(value))) return {};
    return { 'X-POSTHOG-SESSION-ID': sessionId, 'X-POSTHOG-DISTINCT-ID': distinctId };
  } catch { return {}; }
}

// Disable overlapping automatic tracing_headers in the existing SDK. Recheck
// consent and exact scheme/host/port per request; browser hints are never server
// identity. Keep existing authentication, tenant and private-route policies.
// Select one current representative per required role. Multi-page media retains
// partial coverage; its timing describes only that physical request, not the
// whole Promise.allSettled group. Late optional work never reopens a terminal.
