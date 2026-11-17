import { describe, it, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  bashTool,
  _bashHooks,
  encodeUtf16LeBase64,
  rewritePwshInvocation,
  HARDENED_PWSH_FLAGS,
  isSearchShadowEnabled,
  buildSearchShadowPreamble,
  isPluginHintsEnabled,
  resolveOfficialMarketplaces,
  processPluginHints,
} from '../src/tools/bash';
import {
  parsePluginHintTags,
  stripPluginHintTags,
  parsePluginIdentifier,
  evaluatePluginHint,
  formatPluginHintNote,
} from '../src/plugins';
import type { PluginRecommendationHint } from '../src/plugins';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

// Capture the command string handed to the shell layer (no sandbox wrapping).
function captureExec(stderr = '', exitCode = 0) {
  let seen = '';
  mock.method(_bashHooks, 'shouldUseSandbox', () => false);
  mock.method(_bashHooks, 'execCommand', async (cmd: string) => {
    seen = cmd;
    return { stdout: 'ok', stderr, exitCode };
  });
  return () => seen;
}

// ---------------------------------------------------------------------------
// #6 — pwsh invocation hardening (parity: powershell/parser.ts L1167-1180)
// ---------------------------------------------------------------------------

describe('U030/w79 encodeUtf16LeBase64', () => {
  it('encodes a script as UTF-16LE base64 (decodes back to the original)', () => {
    const enc = encodeUtf16LeBase64('Get-Process');
    assert.equal(Buffer.from(enc, 'base64').toString('utf16le'), 'Get-Process');
  });
  it('produces base64 chars only (survives any shell quoting layer)', () => {
    assert.match(encodeUtf16LeBase64('Write-Host "hi!$x"; rm -rf \\'), /^[A-Za-z0-9+/]+=*$/);
  });
});

describe('U030/w79 rewritePwshInvocation', () => {
  it('rewrites `powershell -Command "<script>"` to the hardened encoded form', () => {
    const out = rewritePwshInvocation('powershell -Command "Get-Process"');
    assert.ok(out.startsWith(`powershell ${HARDENED_PWSH_FLAGS.join(' ')} -EncodedCommand `), out);
    const b64 = out.split('-EncodedCommand ')[1];
    assert.equal(Buffer.from(b64, 'base64').toString('utf16le'), 'Get-Process');
  });
  it('supports pwsh, powershell.exe, the -c alias, and leading whitespace', () => {
    assert.ok(rewritePwshInvocation('pwsh -c "echo hi"').includes('-EncodedCommand'));
    assert.ok(rewritePwshInvocation('powershell.exe -Command "echo hi"').includes('-EncodedCommand'));
    assert.ok(rewritePwshInvocation('   pwsh -Command "ls"').startsWith('   pwsh -NoProfile'));
  });
  it('leaves an already -EncodedCommand invocation unchanged (no double-encode)', () => {
    const already = 'pwsh -NoProfile -EncodedCommand ZwBlAHQA';
    assert.equal(rewritePwshInvocation(already), already);
  });
  it('is conservative: a compound/piped pwsh command is left untouched', () => {
    const compound = 'powershell -Command "Get-Process" | grep node';
    assert.equal(rewritePwshInvocation(compound), compound);
  });
  it('leaves a non-powershell command unchanged', () => {
    assert.equal(rewritePwshInvocation('echo hi'), 'echo hi');
  });
});

describe('U030/w79 bashTool.execute — pwsh hardening reaches the executed command only', () => {
  afterEach(() => mock.restoreAll());
  it('executes the hardened -EncodedCommand form while keeping output clean', async () => {
    const getSeen = captureExec();
    await bashTool.execute({ command: 'powershell -Command "Get-Process"' }, ctx());
    const seen = getSeen();
    assert.ok(seen.includes('-EncodedCommand'), seen);
    assert.ok(seen.includes('-NoProfile -NonInteractive -NoLogo'), seen);
    // DISCRIMINATOR: the raw unhardened invocation must NOT be what runs.
    assert.ok(!seen.includes('-Command "Get-Process"'), seen);
  });
});

// ---------------------------------------------------------------------------
// #4 — grep/find shell shadowing (parity: ShellSnapshot.createFindGrepShellIntegration)
// ---------------------------------------------------------------------------

describe('U030/w79 isSearchShadowEnabled', () => {
  it('is off by default and for falsy values', () => {
    assert.equal(isSearchShadowEnabled({}), false);
    assert.equal(isSearchShadowEnabled({ AMI_SEARCH_SHADOW: '0' }), false);
    assert.equal(isSearchShadowEnabled({ AMI_SEARCH_SHADOW: 'nope' }), false);
  });
  it('is on for 1/true/yes/on (case-insensitive)', () => {
    for (const v of ['1', 'true', 'YES', 'On']) {
      assert.equal(isSearchShadowEnabled({ AMI_SEARCH_SHADOW: v }), true, v);
    }
  });
});

describe('U030/w79 buildSearchShadowPreamble', () => {
  it('clears aliases and shadows grep with -I + VCS --exclude-dir defaults', () => {
    const p = buildSearchShadowPreamble();
    assert.ok(p.includes('unalias grep'), p);
    assert.ok(p.includes('unalias find'), p);
    assert.ok(p.includes('grep() { command grep -I '), p);
    for (const d of ['.git', '.svn', '.hg', '.bzr', '.jj', '.sl']) {
      assert.ok(p.includes(`--exclude-dir=${d}`), `${d} missing: ${p}`);
    }
    // User argv ("$@") appears last so a later user flag overrides the defaults.
    assert.ok(p.trimEnd().endsWith('"$@"; }'), p);
  });
});

describe('U030/w79 bashTool.execute — search shadow preamble', () => {
  afterEach(() => { mock.restoreAll(); delete process.env.AMI_SEARCH_SHADOW; });
  it('prepends the shadow preamble to the executed command only when enabled', async () => {
    process.env.AMI_SEARCH_SHADOW = '1';
    const getSeen = captureExec();
    await bashTool.execute({ command: 'grep foo bar.txt' }, ctx());
    const seen = getSeen();
    assert.ok(seen.includes('grep() { command grep -I '), seen);
    assert.ok(seen.endsWith('grep foo bar.txt'), seen);
  });
  it('does NOT alter the executed command when the knob is off (default)', async () => {
    // Distinct command text so a prior read-only grep cache entry can't mask the run.
    const getSeen = captureExec();
    await bashTool.execute({ command: 'grep w79offcase bar.txt' }, ctx());
    assert.equal(getSeen(), 'grep w79offcase bar.txt');
  });
});

// ---------------------------------------------------------------------------
// #5 / #1 — plugin-hint stderr tag: schema, parse, sync-gate, surface
// ---------------------------------------------------------------------------

describe('U030/w79 parsePluginHintTags', () => {
  it('parses a self-closing tag with type/value/sourceCommand (any attr order)', () => {
    const hints = parsePluginHintTags(
      'line1\n<ami-hint value="deploy@official" type="plugin" sourceCommand="kubectl" />\n',
    );
    assert.deepEqual(hints, [{ value: 'deploy@official', type: 'plugin', sourceCommand: 'kubectl' }]);
  });
  it('skips a tag missing type or value, parses multiple tags', () => {
    const hints = parsePluginHintTags(
      '<ami-hint type="plugin" />' +
      '<ami-hint type="plugin" value="a@m" /><ami-hint type="plugin" value="b@m" />',
    );
    assert.deepEqual(hints.map(h => h.value), ['a@m', 'b@m']);
  });
  it('returns [] when there is no tag', () => {
    assert.deepEqual(parsePluginHintTags('ordinary stderr'), []);
  });
});

describe('U030/w79 stripPluginHintTags', () => {
  it('removes the internal tag so it never leaks to the model', () => {
    const cleaned = stripPluginHintTags('real error\n<ami-hint type="plugin" value="a@m" />\nmore');
    assert.ok(!cleaned.includes('<ami-hint'), cleaned);
    assert.ok(cleaned.includes('real error'));
    assert.ok(cleaned.includes('more'));
  });
  it('is a no-op when no tag present', () => {
    assert.equal(stripPluginHintTags('plain'), 'plain');
  });
});

describe('U030/w79 parsePluginIdentifier', () => {
  it('splits name@marketplace', () => {
    assert.deepEqual(parsePluginIdentifier('deploy@official'), { pluginName: 'deploy', marketplaceName: 'official' });
  });
  it('splits on the LAST @ (scoped names keep their @)', () => {
    assert.deepEqual(parsePluginIdentifier('@org/pkg@market'), { pluginName: '@org/pkg', marketplaceName: 'market' });
  });
  it('rejects malformed identifiers', () => {
    assert.equal(parsePluginIdentifier('noatsign'), null);
    assert.equal(parsePluginIdentifier('@only'), null);
    assert.equal(parsePluginIdentifier('name@'), null);
  });
});

describe('U030/w79 evaluatePluginHint (sync gate)', () => {
  it('resolves a valid plugin hint to a recommendation', () => {
    const rec = evaluatePluginHint({ value: 'deploy@official', type: 'plugin', sourceCommand: 'kubectl' });
    assert.deepEqual(rec, { pluginName: 'deploy', marketplaceName: 'official', sourceCommand: 'kubectl' });
  });
  it('rejects a non-plugin type and a malformed identifier', () => {
    assert.equal(evaluatePluginHint({ value: 'a@m', type: 'other' }), null);
    assert.equal(evaluatePluginHint({ value: 'bad', type: 'plugin' }), null);
  });
  it('applies the official-marketplace filter when provided', () => {
    const opts = { officialMarketplaces: new Set(['official']) };
    assert.ok(evaluatePluginHint({ value: 'a@official', type: 'plugin' }, opts));
    assert.equal(evaluatePluginHint({ value: 'a@rogue', type: 'plugin' }, opts), null);
  });
  it('skips an already-installed plugin', () => {
    assert.equal(
      evaluatePluginHint({ value: 'a@m', type: 'plugin' }, { installedPlugins: new Set(['a@m']) }),
      null,
    );
  });
  it('dedups within a session via the mutable seen set (show-once)', () => {
    const seen = new Set<string>();
    assert.ok(evaluatePluginHint({ value: 'a@m', type: 'plugin' }, { seen }));
    assert.equal(evaluatePluginHint({ value: 'a@m', type: 'plugin' }, { seen }), null);
  });
});

describe('U030/w79 formatPluginHintNote', () => {
  it('renders the source-command wording', () => {
    const rec: PluginRecommendationHint = { pluginName: 'deploy', marketplaceName: 'official', sourceCommand: 'kubectl' };
    assert.equal(formatPluginHintNote(rec), "the 'kubectl' command suggests installing deploy from official");
  });
  it('drops the source clause when unknown and appends a description', () => {
    const rec: PluginRecommendationHint = { pluginName: 'deploy', marketplaceName: 'official', sourceCommand: '', pluginDescription: 'CI deploys' };
    assert.equal(formatPluginHintNote(rec), 'suggested plugin: deploy from official — CI deploys');
  });
});

describe('U030/w79 processPluginHints', () => {
  afterEach(() => { delete process.env.AMI_PLUGIN_HINTS; delete process.env.AMI_PLUGIN_HINT_MARKETPLACES; });
  it('always strips the tag; surfaces nothing when the knob is off', () => {
    const r = processPluginHints('err\n<ami-hint type="plugin" value="a@m" sourceCommand="x" />');
    assert.ok(!r.stderr.includes('<ami-hint'));
    assert.deepEqual(r.recommendations, []);
    assert.equal(r.note, '');
  });
  it('surfaces a suggestion + recommendation when the knob is on', () => {
    process.env.AMI_PLUGIN_HINTS = '1';
    const r = processPluginHints('<ami-hint type="plugin" value="deploy@official" sourceCommand="kubectl" />');
    assert.equal(r.recommendations.length, 1);
    assert.ok(r.note.includes("the 'kubectl' command suggests installing deploy from official"), r.note);
  });
  it('honors the official-marketplace env filter', () => {
    process.env.AMI_PLUGIN_HINTS = '1';
    process.env.AMI_PLUGIN_HINT_MARKETPLACES = 'official';
    const r = processPluginHints('<ami-hint type="plugin" value="a@rogue" />');
    assert.equal(r.recommendations.length, 0);
  });
});

describe('U030/w79 resolveOfficialMarketplaces', () => {
  it('returns undefined when unset (no restriction)', () => {
    assert.equal(resolveOfficialMarketplaces({}), undefined);
    assert.equal(resolveOfficialMarketplaces({ AMI_PLUGIN_HINT_MARKETPLACES: '  ' }), undefined);
  });
  it('parses a comma list into a set', () => {
    const set = resolveOfficialMarketplaces({ AMI_PLUGIN_HINT_MARKETPLACES: 'a, b ,c' });
    assert.deepEqual([...(set ?? [])].sort(), ['a', 'b', 'c']);
  });
});

describe('U030/w79 isPluginHintsEnabled', () => {
  it('off by default, on for truthy values', () => {
    assert.equal(isPluginHintsEnabled({}), false);
    assert.equal(isPluginHintsEnabled({ AMI_PLUGIN_HINTS: 'true' }), true);
  });
});

describe('U030/w79 bashTool.execute — plugin hint end-to-end', () => {
  afterEach(() => { mock.restoreAll(); delete process.env.AMI_PLUGIN_HINTS; });
  it('strips the tag from stderr output even when surfacing is disabled', async () => {
    captureExec('boom\n<ami-hint type="plugin" value="deploy@official" sourceCommand="kubectl" />', 1);
    const res = await bashTool.execute({ command: 'kubectl apply' }, ctx());
    assert.ok(!res.output.includes('<ami-hint'), res.output);
    assert.ok(!res.output.includes('[Plugin suggestion'), res.output);
  });
  it('surfaces the suggestion + metadata when AMI_PLUGIN_HINTS is on', async () => {
    process.env.AMI_PLUGIN_HINTS = '1';
    captureExec('done\n<ami-hint type="plugin" value="deploy@official" sourceCommand="kubectl" />', 0);
    const res = await bashTool.execute({ command: 'kubectl apply' }, ctx());
    assert.ok(res.output.includes('[Plugin suggestion:'), res.output);
    assert.ok(!res.output.includes('<ami-hint'), res.output);
    const hints = res.metadata?.pluginHints as PluginRecommendationHint[] | undefined;
    assert.equal(hints?.length, 1);
    assert.equal(hints?.[0].pluginName, 'deploy');
  });
});
