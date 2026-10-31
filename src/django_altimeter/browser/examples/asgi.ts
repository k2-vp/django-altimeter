/** Adapt these calls in the existing board navigation and query owners. */
import { beginJourney, type JourneyAttempt, type JourneyOptions } from '../journey';

/** Call once at navigation intent/full-load initialization; retain across effect replay. */
export function beginBoardNavigation(options: Omit<JourneyOptions, 'screen' | 'roles'>): JourneyAttempt {
  return beginJourney({ ...options, screen: 'board', roles: ['board'] });
}

/** Call after the current first-card/empty state is committed, not on placeholder data. */
export function boardCommitted(
  attempt: JourneyAttempt,
  result: { readonly current: boolean; readonly placeholder: boolean; readonly itemCount: number },
): void {
  if (result.current && !result.placeholder) {
    attempt.finish({ outcome: 'success', itemCount: result.itemCount });
  }
}

// The existing axios request owner selects a fresh requestKey before execution,
// reads response correlation before discarding headers, and records only the
// selected current execution. Keep those observations outside query-cache data.
// On terminal failure call attempt.finish({ outcome: 'failure' }); on genuine
// departure call attempt.cancel('navigation'). Identity/consent changes discard
// the attempt (or abort its lifecycle signal); a later grant starts a new one.
