import { describe, expect, it, vi } from 'vitest';
import { beginBoardNavigation, boardCommitted } from '../../src/django_altimeter/browser/examples/asgi';
import { beginRepairNavigation, repairCommitted, sessionHintHeaders } from '../../src/django_altimeter/browser/examples/wsgi';

const options = () => ({ key: 'navigation', navigationType: 'spa' as const,
  release: 'v1', canCapture: () => true, capture: vi.fn() });

describe('existing navigation owner integration', () => {
  it('does not finish placeholder data or obsolete commits; an empty current commit is ready once', () => {
    const input = options();
    const attempt = beginBoardNavigation(input);
    boardCommitted(attempt, { current: true, placeholder: true, itemCount: 3 });
    boardCommitted(attempt, { current: false, placeholder: false, itemCount: 3 });
    expect(input.capture).not.toHaveBeenCalled();
    boardCommitted(attempt, { current: true, placeholder: false, itemCount: 0 });
    boardCommitted(attempt, { current: true, placeholder: false, itemCount: 0 });
    expect(input.capture).toHaveBeenCalledExactlyOnceWith('board_ready', expect.objectContaining({ item_count: 0 }));
  });
  it('waits for the declared repair dataset without changing its readiness policy', () => {
    const input = options();
    const attempt = beginRepairNavigation({ ...input, roles: ['detail', 'checklist'] });
    repairCommitted(attempt, { current: true, requiredCommitted: false, itemCount: 1 });
    expect(input.capture).not.toHaveBeenCalled();
    repairCommitted(attempt, { current: true, requiredCommitted: true, itemCount: 1 });
    expect(input.capture).toHaveBeenCalledExactlyOnceWith('repair_ready', expect.objectContaining({ item_count: 1 }));
  });
});

describe('existing identity header policy', () => {
  const permission = { url: '/data', baseUrl: 'https://api.example.test',
    allowedOrigins: ['https://api.example.test'], canCapture: () => true, appAllowsSessionHints: true };
  it('reads identity only after every gate and preserves approved values', () => {
    const identity = vi.fn(() => ({ sessionId: 'session', distinctId: 'person' }));
    expect(sessionHintHeaders({ ...permission, canCapture: () => false }, identity)).toEqual({});
    expect(sessionHintHeaders({ ...permission, appAllowsSessionHints: false }, identity)).toEqual({});
    expect(sessionHintHeaders({ ...permission, url: 'https://api.example.test:444/data' }, identity)).toEqual({});
    expect(identity).not.toHaveBeenCalled();
    expect(sessionHintHeaders(permission, identity)).toEqual({ 'X-POSTHOG-SESSION-ID': 'session', 'X-POSTHOG-DISTINCT-ID': 'person' });
    expect(identity).toHaveBeenCalledOnce();
  });
  it('does not pass malformed or throwing identity into the HTTP client', () => {
    expect(sessionHintHeaders(permission, () => ({ sessionId: 'bad\r\nheader', distinctId: 'person' }))).toEqual({});
    expect(sessionHintHeaders(permission, () => { throw new Error('CANARY'); })).toEqual({});
  });
});
