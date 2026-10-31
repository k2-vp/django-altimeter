import { spawnSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, readFileSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
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

describe('README axios integration', () => {
  it.each([
    ['5.9.3', 'typescript', 'tsconfig.wsgi.json'],
    ['6.0.3', 'typescript-asgi', 'tsconfig.asgi.json'],
  ])('compiles the verbatim snippet with TypeScript %s', (_version, compiler, profile) => {
    const root = resolve(import.meta.dirname, '../..');
    const readme = readFileSync(join(root, 'README.md'), 'utf8');
    const snippets = [...readme.matchAll(/```typescript\n([\s\S]*?)\n```/g)]
      .map(match => match[1]).filter(source => source.includes('const attempt = beginJourney('));
    expect(snippets).toHaveLength(1);
    const workspace = mkdtempSync(join(tmpdir(), 'altimeter-readme-'));
    try {
      // Resolve the documented vendor imports to shipped source without editing
      // the snippet. Axios comes from the real pinned package, not a stand-in.
      mkdirSync(join(workspace, 'vendor'));
      symlinkSync(join(root, 'src/django_altimeter/browser'), join(workspace, 'vendor/altimeter'), 'dir');
      symlinkSync(resolve(import.meta.dirname, 'node_modules'), join(workspace, 'node_modules'), 'dir');
      const applicationTypes = `
import type { AxiosInstance } from 'axios';
import type { JourneyOptions } from './vendor/altimeter/journey';
declare const api: AxiosInstance;
declare const navigationKey: string, executionKey: string, release: string;
declare const apiBaseUrl: string, requestUrl: string;
declare const identityLifecycle: AbortController;
declare const capture: JourneyOptions['capture'];
declare const canCapture: JourneyOptions['canCapture'];
declare function commitCurrentBoard(data: unknown): void;
`;
      writeFileSync(join(workspace, 'readme.ts'), applicationTypes + snippets[0]);
      writeFileSync(join(workspace, 'tsconfig.json'), JSON.stringify({
        extends: resolve(import.meta.dirname, profile), include: ['./readme.ts'],
      }));
      const result = spawnSync(process.execPath, [
        resolve(import.meta.dirname, 'node_modules', compiler, 'bin/tsc'),
        '-p', join(workspace, 'tsconfig.json'),
      ], { cwd: workspace, encoding: 'utf8', timeout: 60_000 });
      expect(result.error).toBeUndefined();
      expect(result.status, result.stdout + result.stderr).toBe(0);
    } finally {
      rmSync(workspace, { recursive: true, force: true });
    }
  }, 65_000);
});
