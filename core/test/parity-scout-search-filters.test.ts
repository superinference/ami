import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  scoutTool,
  scoutHooks,
  scoutGrepArgs,
  scoutSearchWindow,
} from '../src/tools/scout';
import type { ToolContext } from '../src/types';

function ctx(): ToolContext {
  return { cwd: '/tmp/scout-filters', abortSignal: new AbortController().signal };
}

const orig = { ...scoutHooks };
afterEach(() => { Object.assign(scoutHooks, orig); });

// A cached repo (cache + .git present, origin matches) so execute() skips the
// clone and reaches the search path deterministically.
function cachedRepo(over: Partial<typeof scoutHooks>) {
  scoutHooks.existsSync = (() => true) as typeof scoutHooks.existsSync;
  scoutHooks.rmSync = (() => undefined) as typeof scoutHooks.rmSync;
  scoutHooks.mkdirSync = (() => undefined) as unknown as typeof scoutHooks.mkdirSync;
  scoutHooks.execFileSync = ((cmd: string, args: string[]) => {
    if (args.includes('get-url')) return 'https://github.com/owner/repo\n';
    return '';
  }) as typeof scoutHooks.execFileSync;
  scoutHooks.execSync = (() => '' as unknown as Buffer) as typeof scoutHooks.execSync;
  scoutHooks.readFileSync = (() => '' as unknown as Buffer) as typeof scoutHooks.readFileSync;
  Object.assign(scoutHooks, over);
}

describe('scout – scoutGrepArgs (query-search filters)', () => {
  it('falls back to the default extension set with no filters', () => {
    const args = scoutGrepArgs('needle', {});
    assert.deepEqual(args, [
      '-rn',
      '--include=*.ts', '--include=*.js', '--include=*.py', '--include=*.go', '--include=*.rs',
      '--', 'needle', '.',
    ]);
  });

  it('honours an explicit glob, overriding the default includes', () => {
    const args = scoutGrepArgs('x', { glob: '*.java' });
    assert.ok(args.includes('--include=*.java'));
    assert.ok(!args.includes('--include=*.ts'), 'glob must replace the default set');
  });

  it('maps a known file_type to its extensions', () => {
    const args = scoutGrepArgs('x', { file_type: 'Ruby' });
    assert.ok(args.includes('--include=*.rb'), 'ruby → *.rb (case-insensitive type)');
  });

  it('falls back to *.<type> for an unknown file_type', () => {
    const args = scoutGrepArgs('x', { file_type: 'zig' });
    assert.ok(args.includes('--include=*.zig'));
  });

  it('adds -i only when case_insensitive is true', () => {
    assert.ok(scoutGrepArgs('x', { case_insensitive: true }).includes('-i'));
    assert.ok(!scoutGrepArgs('x', { case_insensitive: false }).includes('-i'));
    assert.ok(!scoutGrepArgs('x', {}).includes('-i'));
  });

  it('places the query after -- so it can never be read as a flag', () => {
    const args = scoutGrepArgs('-rf', {});
    assert.equal(args[args.length - 2], '-rf');
    assert.equal(args.indexOf('--') + 1, args.length - 2);
  });
});

describe('scout – scoutSearchWindow (limit/offset pagination)', () => {
  it('defaults to the 20-match cap and offset 0', () => {
    assert.deepEqual(scoutSearchWindow({}), { limit: 20, offset: 0 });
  });

  it('honours a caller limit and offset', () => {
    assert.deepEqual(scoutSearchWindow({ limit: 5, offset: 3 }), { limit: 5, offset: 3 });
  });

  it('treats limit 0 as the unlimited escape hatch', () => {
    assert.equal(scoutSearchWindow({ limit: 0 }).limit, 0);
  });

  it('ignores negative / non-numeric limit and offset', () => {
    assert.deepEqual(scoutSearchWindow({ limit: -4, offset: -2 }), { limit: 20, offset: 0 });
    assert.deepEqual(scoutSearchWindow({ limit: 'x', offset: 'y' }), { limit: 20, offset: 0 });
  });

  it('floors fractional values', () => {
    assert.deepEqual(scoutSearchWindow({ limit: 7.9, offset: 2.9 }), { limit: 7, offset: 2 });
  });
});

describe('scout – filters threaded through execute()', () => {
  it('passes glob + case-insensitive + file_type into the grep argv', async () => {
    let grepArgs: string[] | undefined;
    cachedRepo({
      execFileSync: ((cmd: string, args: string[]) => {
        if (args.includes('get-url')) return 'https://github.com/owner/repo\n';
        if (cmd === 'grep') { grepArgs = args; return ''; }
        return '';
      }) as typeof scoutHooks.execFileSync,
    });
    await scoutTool.execute(
      { repo: 'owner/repo', query: 'foo', glob: '*.rb', case_insensitive: true },
      ctx(),
    );
    assert.ok(grepArgs, 'grep ran');
    assert.ok(grepArgs!.includes('--include=*.rb'), 'glob threaded to grep');
    assert.ok(grepArgs!.includes('-i'), 'case-insensitive threaded to grep');
  });

  it('pages past the default cap with limit/offset and reports the window', async () => {
    const grepLines = Array.from({ length: 30 }, (_, i) => `./a.ts:${i}:hit`).join('\n');
    cachedRepo({
      execFileSync: ((cmd: string, args: string[]) => {
        if (args.includes('get-url')) return 'https://github.com/owner/repo\n';
        if (cmd === 'grep') return grepLines;
        return '';
      }) as typeof scoutHooks.execFileSync,
    });
    const r = await scoutTool.execute(
      { repo: 'owner/repo', query: 'hit', limit: 5, offset: 10 },
      ctx(),
    );
    // Window = matches[10..15): lines 10..14 shown, lines 0-9 and 15-29 excluded.
    assert.match(r.output, /:10:hit/);
    assert.match(r.output, /:14:hit/);
    assert.doesNotMatch(r.output, /:9:hit\b/);
    assert.doesNotMatch(r.output, /:15:hit/);
    assert.match(r.output, /showing matches 11-15 of 30/i, 'offset window marker');
  });

  it('limit:0 returns every match with no truncation marker', async () => {
    const grepLines = Array.from({ length: 40 }, (_, i) => `./a.ts:${i}:hit`).join('\n');
    cachedRepo({
      execFileSync: ((cmd: string, args: string[]) => {
        if (args.includes('get-url')) return 'https://github.com/owner/repo\n';
        if (cmd === 'grep') return grepLines;
        return '';
      }) as typeof scoutHooks.execFileSync,
    });
    const r = await scoutTool.execute({ repo: 'owner/repo', query: 'hit', limit: 0 }, ctx());
    assert.match(r.output, /:39:hit/, 'all matches present');
    assert.doesNotMatch(r.output, /showing (first|matches)/i, 'no truncation marker when unlimited');
  });
});
