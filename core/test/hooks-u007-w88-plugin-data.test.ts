/**
 * U007 / w88 — Plugin command-hook PERSISTENT data directory (Claude Code parity:
 * CLAUDE_PLUGIN_DATA / getPluginDataDir, utils/hooks.ts).
 *
 * A plugin-contributed command hook already resolves its read-only bundle root via
 * `${CLAUDE_PLUGIN_ROOT}`. Claude Code ALSO gives such a hook a stable, writable
 * per-plugin data directory exported as CLAUDE_PLUGIN_DATA and substitutable as
 * `${CLAUDE_PLUGIN_DATA}`, so a hook can cache state that survives across sessions
 * separately from the bundle. AMI previously exposed NO data dir — `${CLAUDE_PLUGIN_DATA}`
 * passed through verbatim and no env var was set. This adds it, keyed deterministically
 * off the bundle id so the same plugin always maps to the same directory.
 *
 * RED discriminators (fail without the feature):
 *  - resolvePluginDataDir returns undefined (no helper).
 *  - substitutePluginData leaves `${CLAUDE_PLUGIN_DATA}` verbatim.
 *  - the command hook writes to a literal '${CLAUDE_PLUGIN_DATA}' dir (shell error,
 *    no file) and $CLAUDE_PLUGIN_DATA expands to empty.
 */
import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  HookManager,
  substitutePluginData,
  substitutePluginRoot,
  resolvePluginDataDir,
} from '../src/hooks';

describe('substitutePluginData (pure helper)', () => {
  it('replaces ${CLAUDE_PLUGIN_DATA} and the ${AMI_PLUGIN_DATA} alias', () => {
    assert.equal(
      substitutePluginData('${CLAUDE_PLUGIN_DATA}/cache.db', '/data/p'),
      '/data/p/cache.db',
    );
    assert.equal(substitutePluginData('${AMI_PLUGIN_DATA}/x', '/data/p'), '/data/p/x');
  });

  it('replaces the bare $CLAUDE_PLUGIN_DATA form (word-bounded)', () => {
    assert.equal(substitutePluginData('$CLAUDE_PLUGIN_DATA/a', '/d'), '/d/a');
    // word boundary: $CLAUDE_PLUGIN_DATAX is NOT the var and stays untouched.
    assert.equal(substitutePluginData('$CLAUDE_PLUGIN_DATAX', '/d'), '$CLAUDE_PLUGIN_DATAX');
  });

  it('inserts a value containing $-patterns literally (no back-reference mangling)', () => {
    assert.equal(
      substitutePluginData('${CLAUDE_PLUGIN_DATA}/f', '/weird/$1$&dir'),
      '/weird/$1$&dir/f',
    );
  });

  it('returns the input unchanged when dataDir is empty/undefined', () => {
    assert.equal(substitutePluginData('${CLAUDE_PLUGIN_DATA}/a', undefined), '${CLAUDE_PLUGIN_DATA}/a');
    assert.equal(substitutePluginData('${CLAUDE_PLUGIN_DATA}/a', ''), '${CLAUDE_PLUGIN_DATA}/a');
    assert.equal(substitutePluginData('', '/d'), '');
  });

  it('does not disturb ${CLAUDE_PLUGIN_ROOT} (separate variable class)', () => {
    assert.equal(
      substitutePluginData('${CLAUDE_PLUGIN_ROOT}/x', '/d'),
      '${CLAUDE_PLUGIN_ROOT}/x',
    );
    // and substitutePluginRoot leaves DATA alone — the two are orthogonal.
    assert.equal(
      substitutePluginRoot('${CLAUDE_PLUGIN_DATA}/x', '/r'),
      '${CLAUDE_PLUGIN_DATA}/x',
    );
  });
});

describe('resolvePluginDataDir (pure helper)', () => {
  const origHome = process.env.HOME;
  afterEach(() => { process.env.HOME = origHome; });

  it('maps a bundle root to a deterministic ~/.ami/plugin-data/<id> dir', () => {
    const home = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u007-home-'));
    process.env.HOME = home;
    const a = resolvePluginDataDir('/some/where/.ami/plugins/MyPlugin');
    const b = resolvePluginDataDir('/other/path/.ami/plugins/MyPlugin');
    // Keyed off the sanitized basename — same plugin id → same data dir regardless
    // of where the bundle is unpacked.
    assert.equal(a, path.join(home, '.ami', 'plugin-data', 'myplugin'));
    assert.equal(a, b);
    fs.rmSync(home, { recursive: true, force: true });
  });

  it('returns undefined for an empty / basename-only-garbage root', () => {
    assert.equal(resolvePluginDataDir(undefined), undefined);
    assert.equal(resolvePluginDataDir(''), undefined);
    assert.equal(resolvePluginDataDir('   '), undefined);
  });
});

describe('Plugin command-hook CLAUDE_PLUGIN_DATA env + substitution (U007/w88)', () => {
  const origHome = process.env.HOME;
  afterEach(() => { process.env.HOME = origHome; });

  it('exports CLAUDE_PLUGIN_DATA, creates the dir, and substitutes ${CLAUDE_PLUGIN_DATA}', async () => {
    const home = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u007-pdhome-'));
    process.env.HOME = home;
    const cwd = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u007-plugindata-'));
    const bundleRoot = path.join(cwd, '.ami', 'plugins', 'dataplugin');
    fs.mkdirSync(path.join(bundleRoot, '.ami-plugin'), { recursive: true });
    fs.writeFileSync(
      path.join(bundleRoot, '.ami-plugin', 'plugin.json'),
      JSON.stringify({ name: 'dataplugin', version: '1.0.0' }),
    );
    // The hook BOTH writes into the ${CLAUDE_PLUGIN_DATA} placeholder dir (AMI
    // substitutes it before exec) AND records the $CLAUDE_PLUGIN_DATA env the shell
    // sees. If either path is broken, the files are absent/empty.
    fs.writeFileSync(
      path.join(bundleRoot, 'hooks.json'),
      JSON.stringify({
        hooks: [
          {
            event: 'preToolUse',
            hook: {
              type: 'command',
              command: 'printf "%s" "$CLAUDE_PLUGIN_DATA" > ${CLAUDE_PLUGIN_DATA}/seen.txt',
            },
          },
        ],
      }),
    );

    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(cwd);

    const decision = await hm.executePreToolUse({
      messages: [], turnCount: 0, toolName: 'bash', toolInput: { command: 'ls' },
    });
    assert.equal(decision.action, 'allow');

    const expectedDataDir = path.join(home, '.ami', 'plugin-data', 'dataplugin');
    const seenPath = path.join(expectedDataDir, 'seen.txt');
    // RED without the feature: the dir does not exist (no mkdir), the write targets a
    // literal '${CLAUDE_PLUGIN_DATA}' dir (shell error), and $CLAUDE_PLUGIN_DATA is empty.
    assert.ok(fs.existsSync(expectedDataDir), 'the per-plugin data dir must be created');
    assert.ok(fs.existsSync(seenPath), 'the ${CLAUDE_PLUGIN_DATA} path must resolve to the data dir');
    assert.equal(
      fs.readFileSync(seenPath, 'utf-8').trim(),
      expectedDataDir,
      'CLAUDE_PLUGIN_DATA env must equal the resolved data dir',
    );

    fs.rmSync(cwd, { recursive: true, force: true });
    fs.rmSync(home, { recursive: true, force: true });
  });
});
