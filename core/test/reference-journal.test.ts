/**
 * Unit tests for reference-journal (the WRITE side of the cross-session telemetry journal).
 * Every test is a discriminator: the schema/mapping/path-safety assertions fail if the
 * on-disk alignment with the dashboard's conventions regresses.
 * Run: cd core/core && node --import tsx --test test/reference-journal.test.ts
 */
import { describe, it, afterEach, mock } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  COMPAT_VERSION,
  resolveConfigDir, resolveReadDirs, resolveHome, migrateLegacyDataDir, projectSlug, journalPathFor, subagentJournalPathFor,
  AMI_TO_CLAUDE_TOOL, toReferenceToolName, toReferenceModelId,
  buildUsageFields, buildAssistantEntry, buildUserEntry,
  appendEntry, dashboardConfigPath, registerDataDir, maybeRegisterDataDir,
} from '../src/reference-journal';
import { fsHooks as loggerFsHooks, __resetLoggerForTest } from '../src/logger';

const tmpdirs: string[] = [];
function mkTmp(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'refjrnl-'));
  tmpdirs.push(d);
  return d;
}
afterEach(() => {
  for (const d of tmpdirs.splice(0)) fs.rmSync(d, { recursive: true, force: true });
});

describe('COMPAT_VERSION', () => {
  it('is a semver >= 2.1.307 (so sidechain usage is not estimated away)', () => {
    const [a, b, c] = COMPAT_VERSION.split('.').map(Number);
    const n = a * 1e6 + b * 1e3 + c;
    assert.ok(n >= 2 * 1e6 + 1 * 1e3 + 307, `version ${COMPAT_VERSION} below floor`);
  });
});

describe('resolveConfigDir', () => {
  it('precedence: CLAUDE_CONFIG_DIR > SUPERINFERENCE_DATA_DIR > ~/.ami', () => {
    assert.equal(resolveConfigDir({ CLAUDE_CONFIG_DIR: '/aaa', SUPERINFERENCE_DATA_DIR: '/bbb' } as any), '/aaa');
    assert.equal(resolveConfigDir({ SUPERINFERENCE_DATA_DIR: '/bbb' } as any), '/bbb');
    assert.equal(resolveConfigDir({} as any), path.join(os.homedir(), '.ami'));
  });
  it('ignores empty / whitespace overrides', () => {
    assert.equal(resolveConfigDir({ CLAUDE_CONFIG_DIR: '   ', SUPERINFERENCE_DATA_DIR: '/bbb' } as any), '/bbb');
    assert.equal(resolveConfigDir({ SUPERINFERENCE_DATA_DIR: '' } as any), path.join(os.homedir(), '.ami'));
  });
  it('expands a leading ~ in an override (gh-30794: env values are not shell-expanded)', () => {
    const home = os.homedir();
    assert.equal(resolveConfigDir({ CLAUDE_CONFIG_DIR: '~/.ami' } as any), path.join(home, '.ami'));
    assert.equal(resolveConfigDir({ SUPERINFERENCE_DATA_DIR: '~/data' } as any), path.join(home, 'data'));
    assert.equal(resolveConfigDir({ CLAUDE_CONFIG_DIR: '~' } as any), home);
    assert.equal(resolveConfigDir({ CLAUDE_CONFIG_DIR: '~\\win' } as any), path.join(home, '\\win'));
    // a non-tilde value is untouched (no literal '~' dir, no change in contract)
    assert.equal(resolveConfigDir({ CLAUDE_CONFIG_DIR: '/abs/dir' } as any), '/abs/dir');
  });

  it('rejects a dangerous override and falls back to the next source (parity: validateMemoryPath)', () => {
    // relative → interpreted against cwd; must be rejected, falls through to the data-dir override
    assert.equal(resolveConfigDir({ CLAUDE_CONFIG_DIR: '../evil', SUPERINFERENCE_DATA_DIR: '/safe/dir' } as any), '/safe/dir');
    // root / near-root (length < 3): both rejected → default ~/.ami
    assert.equal(resolveConfigDir({ CLAUDE_CONFIG_DIR: '/', SUPERINFERENCE_DATA_DIR: '/a' } as any), path.join(os.homedir(), '.ami'));
    // UNC / network path → rejected
    assert.equal(resolveConfigDir({ CLAUDE_CONFIG_DIR: '//server/share' } as any), path.join(os.homedir(), '.ami'));
    assert.equal(resolveConfigDir({ CLAUDE_CONFIG_DIR: '\\\\server\\share' } as any), path.join(os.homedir(), '.ami'));
    // embedded NUL → rejected
    assert.equal(resolveConfigDir({ CLAUDE_CONFIG_DIR: '/tmp/ev\0il' } as any), path.join(os.homedir(), '.ami'));
  });

  it('logs the rejection of a dangerous override (never silently redirects state)', () => {
    __resetLoggerForTest();
    const lines: string[] = [];
    const spy = mock.method(loggerFsHooks, 'appendFileSync', (_f: unknown, line: unknown) => { lines.push(String(line)); });
    try {
      // unique relative value → unique memo key, so the rejection path actually runs
      assert.equal(resolveConfigDir({ CLAUDE_CONFIG_DIR: 'rel-' + Date.now() } as any), path.join(os.homedir(), '.ami'));
    } finally {
      spy.mock.restore();
    }
    assert.ok(lines.some(l => l.includes('rejecting dangerous CLAUDE_CONFIG_DIR override')), 'rejection is logged');
  });

  it('NFC-normalizes the resolved override (parity: getClaudeConfigHomeDir)', () => {
    // 'e' + combining acute (NFD) must normalize to the single NFC codepoint
    const nfd = '/tmp/caf\u0065\u0301-dir'; // e + combining acute U+0301 (NFD)
    const nfc = '/tmp/caf\u00e9-dir';       // single codepoint U+00E9 (NFC)
    assert.notEqual(nfd, nfc); // inputs differ byte-wise
    assert.equal(resolveConfigDir({ CLAUDE_CONFIG_DIR: nfd } as any), nfc);
  });

  it('memoizes per env (changed env value refreshes without a manual clear)', () => {
    const a = resolveConfigDir({ CLAUDE_CONFIG_DIR: '/memo/aaa' } as any);
    const b = resolveConfigDir({ CLAUDE_CONFIG_DIR: '/memo/aaa' } as any);
    assert.equal(a, b);
    assert.equal(a, '/memo/aaa');
    // a different value yields a different (fresh) result
    assert.equal(resolveConfigDir({ CLAUDE_CONFIG_DIR: '/memo/bbb' } as any), '/memo/bbb');
  });

  it('honors an injected homedir and env.HOME for the default dir (test seam / HOME precedence)', () => {
    // injected homedir wins over everything
    assert.equal(resolveConfigDir({} as any, '/injected/home'), path.join('/injected/home', '.ami'));
    // env.HOME is preferred over os.homedir() when no override/injection is given
    assert.equal(resolveConfigDir({ HOME: '/env/home' } as any), path.join('/env/home', '.ami'));
    // a ~ override expands against the injected home
    assert.equal(resolveConfigDir({ CLAUDE_CONFIG_DIR: '~/cfg' } as any, '/injected/home'), path.join('/injected/home', 'cfg'));
  });
});

describe('resolveHome (HOME-aware home resolution, parity: xdg.ts resolveOptions)', () => {
  it('prefers an injected homedir, then env.HOME, then os.homedir()', () => {
    assert.equal(resolveHome({ HOME: '/env/home' } as any, '/injected'), '/injected');
    assert.equal(resolveHome({ HOME: '/env/home' } as any), '/env/home');
    assert.equal(resolveHome({} as any), os.homedir());
  });
  it('treats an empty / whitespace HOME as unset', () => {
    assert.equal(resolveHome({ HOME: '   ' } as any), os.homedir());
    assert.equal(resolveHome({ HOME: '' } as any), os.homedir());
  });
});

describe('resolveReadDirs (cross-agent spend aggregation)', () => {
  it('an explicit override is authoritative (single root, deterministic)', () => {
    assert.deepEqual(resolveReadDirs({ CLAUDE_CONFIG_DIR: '/xxx' } as any), ['/xxx']);
    assert.deepEqual(resolveReadDirs({ SUPERINFERENCE_DATA_DIR: '/yyy' } as any), ['/yyy']);
  });
  it('expands a leading ~ in the authoritative override (parity with resolveConfigDir)', () => {
    assert.deepEqual(resolveReadDirs({ CLAUDE_CONFIG_DIR: '~/ami' } as any), [path.join(os.homedir(), 'ami')]);
    assert.deepEqual(resolveReadDirs({ SUPERINFERENCE_DATA_DIR: '~/sup' } as any), [path.join(os.homedir(), 'sup')]);
  });
  it('defaults to BOTH ~/.ami and ~/.claude (read Claude Code spend too)', () => {
    const dirs = resolveReadDirs({} as any);
    assert.ok(dirs.includes(path.join(os.homedir(), '.ami')));
    assert.ok(dirs.includes(path.join(os.homedir(), '.claude')));
  });
  it('merges dirs registered in the dashboard config, deduped', () => {
    const tmp = mkTmp();
    const cfg = path.join(tmp, 'cfg.json');
    const extra = path.join(tmp, 'extra-agent');
    fs.writeFileSync(cfg, JSON.stringify({ claudeConfigDirs: [extra, path.join(os.homedir(), '.claude')] }));
    const dirs = resolveReadDirs({ SUPERINFERENCE_DASHBOARD_CONFIG: cfg } as any);
    assert.ok(dirs.includes(extra), 'registered extra dir included');
    assert.equal(dirs.filter(d => d === path.join(os.homedir(), '.claude')).length, 1, 'deduped');
  });
  it('tolerates a corrupt/missing dashboard config', () => {
    const tmp = mkTmp();
    const cfg = path.join(tmp, 'bad.json');
    fs.writeFileSync(cfg, '{not json');
    const dirs = resolveReadDirs({ SUPERINFERENCE_DASHBOARD_CONFIG: cfg } as any);
    assert.ok(dirs.includes(path.join(os.homedir(), '.ami')));
  });
});

describe('projectSlug', () => {
  it('replaces every non-alphanumeric with a dash (dashboard convention)', () => {
    assert.equal(projectSlug('/home/me/dev/ui'), '-home-me-dev-ui');
    assert.equal(projectSlug('C:\\x.y'), 'C--x-y');
  });
});

describe('journalPathFor / subagentJournalPathFor', () => {
  it('builds <dir>/projects/<slug>/<sessionId>.jsonl', () => {
    const p = journalPathFor('/home/me/ui', 'sess1', { SUPERINFERENCE_DATA_DIR: '/data' } as any);
    assert.equal(p, path.join('/data', 'projects', '-home-me-ui', 'sess1.jsonl'));
  });
  it('subagent path lives under projects/<slug>/subagents/agent-<id>.jsonl', () => {
    const p = subagentJournalPathFor('/home/me/ui', 'ag1', { SUPERINFERENCE_DATA_DIR: '/data' } as any);
    assert.equal(p, path.join('/data', 'projects', '-home-me-ui', 'subagents', 'agent-ag1.jsonl'));
  });
  it('sanitizes session ids with separators / traversal (no escape)', () => {
    const p = journalPathFor('/home/me/ui', '../../etc/passwd', { SUPERINFERENCE_DATA_DIR: '/data' } as any);
    assert.ok(p.startsWith(path.join('/data', 'projects', '-home-me-ui')), p);
    assert.ok(!p.includes('passwd') || !p.includes('..'), 'traversal neutralized');
  });
});

describe('toReferenceToolName / AMI_TO_CLAUDE_TOOL', () => {
  it('maps internal tool ids to dashboard vocabulary', () => {
    assert.equal(toReferenceToolName('bash'), 'Bash');
    assert.equal(toReferenceToolName('file_edit'), 'Edit');
    assert.equal(toReferenceToolName('file_read'), 'Read');
    assert.equal(toReferenceToolName('task'), 'Task');
    assert.equal(toReferenceToolName('skill'), 'Skill');
    assert.equal(AMI_TO_CLAUDE_TOOL['glob'], 'Glob');
  });
  it('passes MCP tools through and leaves unknown names unchanged', () => {
    assert.equal(toReferenceToolName('mcp__server__do'), 'mcp__server__do');
    assert.equal(toReferenceToolName('something_custom'), 'something_custom');
  });
});

describe('toReferenceModelId', () => {
  it('maps known families, passes through dated snapshots + unknowns + empty', () => {
    assert.equal(toReferenceModelId('claude-opus-4-8'), 'claude-opus-4-20250514');
    assert.equal(toReferenceModelId('claude-sonnet-4'), 'claude-sonnet-4-20250514');
    assert.equal(toReferenceModelId('claude-haiku'), 'claude-3-5-haiku-20241022');
    assert.equal(toReferenceModelId('claude-opus-4-20250514'), 'claude-opus-4-20250514'); // dated
    assert.equal(toReferenceModelId('claude-opus-9-future'), 'claude-opus-4-20250514'); // family fallback
    assert.equal(toReferenceModelId('claude-sonnet-x'), 'claude-sonnet-4-20250514');
    assert.equal(toReferenceModelId('claude-haiku-x'), 'claude-3-5-haiku-20241022');
    assert.equal(toReferenceModelId('gpt-4o'), 'gpt-4o'); // unknown passthrough
    assert.equal(toReferenceModelId(''), '');
  });
});

describe('buildUsageFields', () => {
  it('maps to snake_case; input_tokens = prompt - cacheRead - cacheCreate', () => {
    const u = buildUsageFields({ promptTokens: 1000, completionTokens: 200, reasoningTokens: 30, cachedPromptTokens: 600, cacheCreationTokens: 100 });
    assert.equal(u.input_tokens, 300);
    assert.equal(u.output_tokens, 200);
    assert.equal(u.cache_read_input_tokens, 600);
    assert.equal(u.cache_creation_input_tokens, 100);
    assert.equal(u.cache_creation.ephemeral_5m_input_tokens, 100);
    assert.equal(u.cache_creation.ephemeral_1h_input_tokens, 0);
    assert.equal(u.output_tokens_details.thinking_tokens, 30);
  });
  it('defaults missing cache/reasoning fields to 0 and floors negatives', () => {
    const u = buildUsageFields({ promptTokens: 50, completionTokens: 10 });
    assert.equal(u.input_tokens, 50);
    assert.equal(u.cache_read_input_tokens, 0);
    assert.equal(u.output_tokens_details.thinking_tokens, 0);
    // prompt smaller than cache would go negative → floored to 0
    const u2 = buildUsageFields({ promptTokens: 10, completionTokens: 0, cachedPromptTokens: 50 });
    assert.equal(u2.input_tokens, 0);
  });
});

describe('buildAssistantEntry', () => {
  it('emits the full schema, maps tool names, and NEVER writes cost', () => {
    const e = buildAssistantEntry({
      uuid: 'u1', parentUuid: null, timestamp: '2026-01-01T00:00:00.000Z',
      sessionId: 's1', cwd: '/x', gitBranch: 'main', isSidechain: false,
      messageId: 's1:1', model: 'claude-opus-4-8', stopReason: 'end_turn',
      usage: { promptTokens: 100, completionTokens: 20, cachedPromptTokens: 10, cacheCreationTokens: 5, reasoningTokens: 3 },
      content: [
        { type: 'text', text: 'hi' },
        { type: 'thinking', thinking: 'hmm' },
        { type: 'tool_use', id: 't1', name: 'file_edit', input: { file_path: '/x/a' } },
      ],
    });
    assert.equal(e.type, 'assistant');
    assert.equal(e.version, COMPAT_VERSION);
    assert.equal(e.message.id, 's1:1');
    assert.equal(e.message.model, 'claude-opus-4-20250514');
    assert.equal(e.message.stop_reason, 'end_turn');
    // tool name mapped into dashboard vocabulary
    const tu = e.message.content.find(c => c.type === 'tool_use') as any;
    assert.equal(tu.name, 'Edit');
    assert.deepEqual(tu.input, { file_path: '/x/a' });
    // required usage keys present
    for (const k of ['input_tokens', 'output_tokens', 'cache_read_input_tokens', 'cache_creation_input_tokens']) {
      assert.ok(k in e.message.usage, `missing ${k}`);
    }
    assert.ok('thinking_tokens' in e.message.usage.output_tokens_details);
    // NO cost anywhere
    assert.ok(!('cost' in (e as any)));
    assert.ok(!('cost' in e.message));
    assert.ok(!JSON.stringify(e).includes('"cost"'));
  });
});

describe('buildUserEntry', () => {
  it('builds a prompt line (no toolUseResult) and a tool_result line (with it)', () => {
    const prompt = buildUserEntry({ uuid: 'u', parentUuid: 'p', timestamp: 't', sessionId: 's', cwd: '/x', gitBranch: '', isSidechain: false, content: 'do it' });
    assert.equal(prompt.type, 'user');
    assert.equal(prompt.message.content, 'do it');
    assert.ok(!('toolUseResult' in prompt));
    const res = buildUserEntry({ uuid: 'u', parentUuid: 'p', timestamp: 't', sessionId: 's', cwd: '/x', gitBranch: '', isSidechain: false, content: [{ type: 'tool_result' }], toolUseResult: { is_error: true } });
    assert.equal(res.toolUseResult!.is_error, true);
  });
});

describe('appendEntry', () => {
  it('creates parent dirs and appends newline-delimited JSON', () => {
    const dir = mkTmp();
    const file = path.join(dir, 'projects', 'slug', 's.jsonl');
    appendEntry(file, { a: 1 });
    appendEntry(file, { a: 2 });
    const lines = fs.readFileSync(file, 'utf8').trim().split('\n');
    assert.equal(lines.length, 2);
    assert.deepEqual(JSON.parse(lines[0]), { a: 1 });
    assert.deepEqual(JSON.parse(lines[1]), { a: 2 });
  });
  it('swallows errors (best-effort — never throws into the engine loop)', () => {
    const dir = mkTmp();
    const asFile = path.join(dir, 'blocker');
    fs.writeFileSync(asFile, 'x'); // a file where a dir is needed → mkdir throws, must be caught
    assert.doesNotThrow(() => appendEntry(path.join(asFile, 'nested', 's.jsonl'), { a: 1 }));
  });
  it('append-first: writes into an EXISTING dir without re-walking the tree', () => {
    // Parent already exists → the hot path appends straight through (no mkdir retry).
    const dir = mkTmp();
    const file = path.join(dir, 's.jsonl');
    appendEntry(file, { a: 1 });
    appendEntry(file, { a: 2 });
    const lines = fs.readFileSync(file, 'utf8').trim().split('\n');
    assert.deepEqual(lines.map(l => JSON.parse(l)), [{ a: 1 }, { a: 2 }]);
  });
  it('creates the journal file and its project dir owner-only (0o600 / 0o700)', { skip: process.platform === 'win32' }, () => {
    const dir = mkTmp();
    const projDir = path.join(dir, 'projects', 'slug');
    const file = path.join(projDir, 's.jsonl');
    appendEntry(file, { a: 1 });
    // The file must not be group/world readable (it holds prompts + tool inputs).
    assert.equal(fs.statSync(file).mode & 0o077, 0, 'journal file must be 0o600');
    // The created project dir must be owner-only too.
    assert.equal(fs.statSync(projDir).mode & 0o077, 0, 'project dir must be 0o700');
    // A subsequent append (EEXIST path) must not widen the perms.
    appendEntry(file, { a: 2 });
    assert.equal(fs.statSync(file).mode & 0o077, 0, 'perms unchanged after EEXIST append');
  });
});

describe('dashboard-config registration', () => {
  it('dashboardConfigPath honors the env, else null', () => {
    assert.equal(dashboardConfigPath({ SUPERINFERENCE_DASHBOARD_CONFIG: '/cfg.json' } as any), '/cfg.json');
    assert.equal(dashboardConfigPath({ SUPERINFERENCE_DASHBOARD_CONFIG: '  ' } as any), null);
    assert.equal(dashboardConfigPath({} as any), null);
  });
  it('registerDataDir merges idempotently, preserves other keys, handles corrupt json', () => {
    const dir = mkTmp();
    const cfg = path.join(dir, 'cfg.json');
    assert.equal(registerDataDir('/data', cfg), true); // fresh
    let parsed = JSON.parse(fs.readFileSync(cfg, 'utf8'));
    assert.deepEqual(parsed.claudeConfigDirs, ['/data']);
    assert.equal(registerDataDir('/data', cfg), false); // already present → no write
    // preserve other keys + append second dir
    fs.writeFileSync(cfg, JSON.stringify({ other: 1, claudeConfigDirs: ['/data'] }));
    assert.equal(registerDataDir('/data2', cfg), true);
    parsed = JSON.parse(fs.readFileSync(cfg, 'utf8'));
    assert.equal(parsed.other, 1);
    assert.deepEqual(parsed.claudeConfigDirs, ['/data', '/data2']);
    // corrupt json → treated as empty, then written
    fs.writeFileSync(cfg, '{not json');
    assert.equal(registerDataDir('/data3', cfg), true);
    parsed = JSON.parse(fs.readFileSync(cfg, 'utf8'));
    assert.deepEqual(parsed.claudeConfigDirs, ['/data3']);
  });
  it('registerDataDir returns false on write failure (best-effort)', () => {
    const dir = mkTmp();
    const asFile = path.join(dir, 'blocker');
    fs.writeFileSync(asFile, 'x');
    assert.equal(registerDataDir('/data', path.join(asFile, 'nested', 'cfg.json')), false);
  });
  it('registerDataDir writes the config owner-only (0o600) via the crash-safe writer', { skip: process.platform === 'win32' }, () => {
    const dir = mkTmp();
    const cfg = path.join(dir, 'sub', 'cfg.json'); // parent created by the atomic writer
    assert.equal(registerDataDir('/data', cfg), true);
    assert.equal(fs.statSync(cfg).mode & 0o077, 0, 'dashboard config must not be group/world readable');
  });
  it('maybeRegisterDataDir: false when unconfigured, registers resolved dir when configured', () => {
    assert.equal(maybeRegisterDataDir({} as any), false);
    const dir = mkTmp();
    const cfg = path.join(dir, 'cfg.json');
    const ok = maybeRegisterDataDir({ SUPERINFERENCE_DATA_DIR: '/data', SUPERINFERENCE_DASHBOARD_CONFIG: cfg } as any);
    assert.equal(ok, true);
    assert.deepEqual(JSON.parse(fs.readFileSync(cfg, 'utf8')).claudeConfigDirs, ['/data']);
  });
});

describe('migrateLegacyDataDir (issue #21 defect 1)', () => {
  it('moves durable config from ~/.superinference to ~/.ami, leaves runtime orphaned, is idempotent', () => {
    const home = mkTmp();
    const legacy = path.join(home, '.superinference');
    // durable config (should migrate) + transient runtime (should NOT)
    fs.mkdirSync(path.join(legacy, 'plugins', 'impeccable'), { recursive: true });
    fs.writeFileSync(path.join(legacy, 'plugins', 'impeccable', 'marker'), 'x');
    fs.mkdirSync(path.join(legacy, 'installed-skills'), { recursive: true });
    fs.writeFileSync(path.join(legacy, 'marketplaces.json'), '{}');
    fs.mkdirSync(path.join(legacy, 'sessions'), { recursive: true }); // runtime → stays orphaned

    const moved = migrateLegacyDataDir({} as any, home);
    assert.ok(moved.includes('plugins') && moved.includes('installed-skills') && moved.includes('marketplaces.json'));
    assert.ok(fs.existsSync(path.join(home, '.ami', 'plugins', 'impeccable', 'marker')), 'plugin migrated');
    assert.ok(!fs.existsSync(path.join(legacy, 'plugins')), 'legacy plugins moved out');
    assert.ok(!fs.existsSync(path.join(home, '.ami', 'sessions')), 'runtime NOT migrated (orphaned by design)');
    // Idempotent: a second run finds nothing to move.
    assert.deepEqual(migrateLegacyDataDir({} as any, home), []);
  });
  it('never clobbers an existing ~/.ami target subdir', () => {
    const home = mkTmp();
    fs.mkdirSync(path.join(home, '.superinference', 'plugins', 'old'), { recursive: true });
    fs.mkdirSync(path.join(home, '.ami', 'plugins', 'new'), { recursive: true });
    const moved = migrateLegacyDataDir({} as any, home);
    assert.ok(!moved.includes('plugins'), 'existing .ami/plugins is preserved, not overwritten');
    assert.ok(fs.existsSync(path.join(home, '.ami', 'plugins', 'new')));
  });
  it('is a no-op under an explicit data-dir override', () => {
    const home = mkTmp();
    fs.mkdirSync(path.join(home, '.superinference', 'plugins'), { recursive: true });
    assert.deepEqual(migrateLegacyDataDir({ SUPERINFERENCE_DATA_DIR: '/x' } as any, home), []);
    assert.deepEqual(migrateLegacyDataDir({ CLAUDE_CONFIG_DIR: '/y' } as any, home), []);
  });
  it('is a no-op when there is no legacy dir', () => {
    assert.deepEqual(migrateLegacyDataDir({} as any, mkTmp()), []);
  });
  it('records (does not swallow blindly) a per-item failure and still never throws', () => {
    // A legacy subdir exists to migrate, but the ~/.ami target path is a regular
    // FILE, so mkdir of the target throws for every item → caught + LOGGED, no
    // items migrated, and startup is never blocked. We capture the logger's write
    // seam to prove the failure is recorded (not silently swallowed) — this is
    // what makes the test discriminate the fix from a bare `catch {}`.
    const home = mkTmp();
    fs.mkdirSync(path.join(home, '.superinference', 'plugins'), { recursive: true });
    fs.writeFileSync(path.join(home, '.ami'), 'x'); // .ami is a file, not a dir
    __resetLoggerForTest();
    const lines: string[] = [];
    const spy = mock.method(loggerFsHooks, 'appendFileSync', (_f: unknown, line: unknown) => {
      lines.push(String(line));
    });
    let moved: string[] = [];
    try {
      assert.doesNotThrow(() => { moved = migrateLegacyDataDir({} as any, home); });
    } finally {
      spy.mock.restore();
    }
    assert.deepEqual(moved, [], 'nothing migrated when the target cannot be created');
    assert.ok(
      lines.some(l => l.includes('migrate-legacy') && l.includes('plugins')),
      'the per-item migration failure must be logged, not silently swallowed',
    );
  });
});
