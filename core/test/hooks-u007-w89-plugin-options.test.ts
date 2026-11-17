/**
 * U007 / w89 — Plugin command-hook userConfig OPTION values (Claude Code parity:
 * CLAUDE_PLUGIN_OPTION_<KEY> env vars + `${user_config.X}` command substitution,
 * utils/hooks.ts).
 *
 * A plugin-contributed command hook already resolves its read-only bundle root
 * (`${CLAUDE_PLUGIN_ROOT}`) and a persistent per-plugin data dir (`${CLAUDE_PLUGIN_DATA}`).
 * Claude Code ALSO threads the plugin's user-configured option VALUES to the hook:
 *   - each saved `userConfig` value is exported as `CLAUDE_PLUGIN_OPTION_<KEY>`, and
 *   - `${user_config.<key>}` placeholders in the command + args are substituted.
 * AMI previously exposed NEITHER — `${user_config.X}` passed through verbatim and no
 * option env var was set. This wires both, reading the values from the bundle's
 * `.options.json` sidecar via the canonical core reader.
 *
 * RED discriminators (fail without the feature):
 *  - readPluginOptionValues does not exist / is not exported from core.
 *  - substituteUserConfigValues leaves `${user_config.X}` verbatim.
 *  - buildPluginOptionEnv does not exist / sets no CLAUDE_PLUGIN_OPTION_<KEY>.
 *  - the command hook sees an empty $CLAUDE_PLUGIN_OPTION_TOKEN and a literal
 *    '${user_config.token}' on the command line.
 */
import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { HookManager } from '../src/hooks';
import {
  readPluginOptionValues,
  substituteUserConfigValues,
  buildPluginOptionEnv,
  PLUGIN_OPTIONS_FILE,
} from '../src/plugins';

describe('readPluginOptionValues (canonical sidecar reader)', () => {
  it('reads `.options.json` keeping only primitive values', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u007-opts-'));
    fs.writeFileSync(
      path.join(dir, PLUGIN_OPTIONS_FILE),
      JSON.stringify({ token: 'abc', count: 3, flag: true, nested: { x: 1 }, list: ['a'] }),
    );
    const v = readPluginOptionValues(dir);
    assert.deepEqual(v, { token: 'abc', count: 3, flag: true });
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('returns {} for an absent sidecar', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u007-opts-'));
    assert.deepEqual(readPluginOptionValues(dir), {});
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('returns {} for corrupt JSON and for a non-object top level', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u007-opts-'));
    fs.writeFileSync(path.join(dir, PLUGIN_OPTIONS_FILE), '{not json');
    assert.deepEqual(readPluginOptionValues(dir), {});
    fs.writeFileSync(path.join(dir, PLUGIN_OPTIONS_FILE), '[1,2,3]');
    assert.deepEqual(readPluginOptionValues(dir), {});
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('refuses to read a non-regular file at the sidecar path (directory guard)', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u007-opts-'));
    // A directory planted where the sidecar would be: never read, returns {}.
    fs.mkdirSync(path.join(dir, PLUGIN_OPTIONS_FILE));
    assert.deepEqual(readPluginOptionValues(dir), {});
    fs.rmSync(dir, { recursive: true, force: true });
  });
});

describe('substituteUserConfigValues (pure helper)', () => {
  it('substitutes `${user_config.<key>}` with stringified values', () => {
    assert.equal(
      substituteUserConfigValues('--token ${user_config.token} -n ${user_config.count}', { token: 'xy', count: 7 }),
      '--token xy -n 7',
    );
  });

  it('resolves an unknown/absent key to the empty string (no literal leak)', () => {
    assert.equal(substituteUserConfigValues('a ${user_config.missing} b', { token: 'z' }), 'a  b');
  });

  it('returns the input unchanged when there are no values', () => {
    assert.equal(substituteUserConfigValues('${user_config.token}', undefined), '${user_config.token}');
    assert.equal(substituteUserConfigValues('', { token: 'z' }), '');
  });

  it('does not touch other ${...} classes (CLAUDE_PLUGIN_ROOT / shell env)', () => {
    assert.equal(
      substituteUserConfigValues('${CLAUDE_PLUGIN_ROOT}/x $HOME', { token: 'z' }),
      '${CLAUDE_PLUGIN_ROOT}/x $HOME',
    );
  });
});

describe('buildPluginOptionEnv (pure helper)', () => {
  it('builds CLAUDE_PLUGIN_OPTION_<KEY> + AMI_PLUGIN_OPTION_<KEY> for each value', () => {
    assert.deepEqual(buildPluginOptionEnv({ token: 'abc', count: 3, flag: true }), {
      CLAUDE_PLUGIN_OPTION_TOKEN: 'abc',
      AMI_PLUGIN_OPTION_TOKEN: 'abc',
      CLAUDE_PLUGIN_OPTION_COUNT: '3',
      AMI_PLUGIN_OPTION_COUNT: '3',
      CLAUDE_PLUGIN_OPTION_FLAG: 'true',
      AMI_PLUGIN_OPTION_FLAG: 'true',
    });
  });

  it('upper-cases keys and replaces non-identifier chars so the var is shell-referenceable', () => {
    const env = buildPluginOptionEnv({ 'api-key': 'v' });
    assert.equal(env.CLAUDE_PLUGIN_OPTION_API_KEY, 'v');
    assert.equal(env.AMI_PLUGIN_OPTION_API_KEY, 'v');
  });

  it('returns {} when there are no values', () => {
    assert.deepEqual(buildPluginOptionEnv(undefined), {});
    assert.deepEqual(buildPluginOptionEnv({}), {});
  });
});

describe('Plugin command-hook CLAUDE_PLUGIN_OPTION env + ${user_config.X} substitution (U007/w89)', () => {
  const origHome = process.env.HOME;
  afterEach(() => { process.env.HOME = origHome; });

  it('exports CLAUDE_PLUGIN_OPTION_<KEY> and substitutes ${user_config.X} in a plugin command hook', async () => {
    const home = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u007-opthome-'));
    process.env.HOME = home;
    const cwd = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u007-pluginopts-'));
    const bundleRoot = path.join(cwd, '.ami', 'plugins', 'optplugin');
    fs.mkdirSync(path.join(bundleRoot, '.ami-plugin'), { recursive: true });
    fs.writeFileSync(
      path.join(bundleRoot, '.ami-plugin', 'plugin.json'),
      JSON.stringify({
        name: 'optplugin',
        version: '1.0.0',
        userConfig: { token: { type: 'string', title: 'Token', description: 'API token' } },
      }),
    );
    // The user's saved option values, as the plugin installer persists them.
    fs.writeFileSync(
      path.join(bundleRoot, PLUGIN_OPTIONS_FILE),
      JSON.stringify({ token: 'sekret-42' }),
    );
    // The hook records BOTH the $CLAUDE_PLUGIN_OPTION_TOKEN env the shell sees AND
    // the ${user_config.token} the command line was substituted with. The output
    // path itself uses ${CLAUDE_PLUGIN_DATA} (orthogonal, already supported).
    fs.writeFileSync(
      path.join(bundleRoot, 'hooks.json'),
      JSON.stringify({
        hooks: [
          {
            event: 'preToolUse',
            hook: {
              type: 'command',
              command:
                'printf "env=%s arg=%s" "$CLAUDE_PLUGIN_OPTION_TOKEN" "${user_config.token}" > ${CLAUDE_PLUGIN_DATA}/opt.txt',
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

    const dataDir = path.join(home, '.ami', 'plugin-data', 'optplugin');
    const outPath = path.join(dataDir, 'opt.txt');
    assert.ok(fs.existsSync(outPath), 'the hook must have run and written its output');
    // RED without the feature: env is empty and arg is the literal '${user_config.token}'.
    assert.equal(
      fs.readFileSync(outPath, 'utf-8').trim(),
      'env=sekret-42 arg=sekret-42',
      'CLAUDE_PLUGIN_OPTION_TOKEN env and ${user_config.token} substitution must both resolve to the saved value',
    );

    fs.rmSync(cwd, { recursive: true, force: true });
    fs.rmSync(home, { recursive: true, force: true });
  });
});
