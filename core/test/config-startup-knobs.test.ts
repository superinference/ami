import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  applyConfigCaCerts,
  applyConfigGitignore,
  applyConfigAutoCompact,
  applyConfigPlansDirectory,
  applyConfigAnthropicBetas,
  applyConfigStartupKnobs,
  resolveAdditionalDirectories,
  FAST_MODE_BETA,
  REDACT_THINKING_BETA,
  mergeConfigs,
  validateProjectConfigValues,
  findUnknownConfigKeys,
  type ProjectConfig,
} from '../src/config';

// Helper: a fresh, isolated env object so no test mutates the real process.env.
function env(init: Record<string, string> = {}): NodeJS.ProcessEnv {
  return { ...init } as NodeJS.ProcessEnv;
}

// ---------------------------------------------------------------------------
// applyConfigCaCerts  (extraCaCerts -> NODE_EXTRA_CA_CERTS)
// ---------------------------------------------------------------------------
describe('applyConfigCaCerts', () => {
  let dir: string;
  let certFile: string;
  beforeEach(() => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-ca-'));
    certFile = path.join(dir, 'corp-ca.pem');
    fs.writeFileSync(certFile, '-----BEGIN CERTIFICATE-----\n');
  });
  afterEach(() => fs.rmSync(dir, { recursive: true, force: true }));

  it('seeds NODE_EXTRA_CA_CERTS from a valid readable file', () => {
    const t = env();
    const result = applyConfigCaCerts({ extraCaCerts: certFile }, t);
    assert.equal(result, certFile);
    assert.equal(t.NODE_EXTRA_CA_CERTS, certFile);
  });

  it('an explicit env var wins and is returned unchanged (one-shot override)', () => {
    const t = env({ NODE_EXTRA_CA_CERTS: '/already/set.pem' });
    const result = applyConfigCaCerts({ extraCaCerts: certFile }, t);
    assert.equal(result, '/already/set.pem');
    assert.equal(t.NODE_EXTRA_CA_CERTS, '/already/set.pem');
  });

  it('ignores a missing path (fail-safe: never point Node at a bad bundle)', () => {
    const t = env();
    const result = applyConfigCaCerts({ extraCaCerts: path.join(dir, 'nope.pem') }, t);
    assert.equal(result, undefined);
    assert.equal(t.NODE_EXTRA_CA_CERTS, undefined);
  });

  it('ignores a non-regular-file path (a directory)', () => {
    const t = env();
    const result = applyConfigCaCerts({ extraCaCerts: dir }, t);
    assert.equal(result, undefined);
    assert.equal(t.NODE_EXTRA_CA_CERTS, undefined);
  });

  it('is a no-op for a blank or absent value', () => {
    const t = env();
    assert.equal(applyConfigCaCerts({ extraCaCerts: '   ' }, t), undefined);
    assert.equal(applyConfigCaCerts(null, t), undefined);
    assert.equal(applyConfigCaCerts({}, t), undefined);
    assert.equal(t.NODE_EXTRA_CA_CERTS, undefined);
  });

  it('trims surrounding whitespace before applying', () => {
    const t = env();
    assert.equal(applyConfigCaCerts({ extraCaCerts: `  ${certFile}  ` }, t), certFile);
    assert.equal(t.NODE_EXTRA_CA_CERTS, certFile);
  });
});

// ---------------------------------------------------------------------------
// applyConfigGitignore  (respectGitignore -> AMI_GLOB_NO_IGNORE)
// ---------------------------------------------------------------------------
describe('applyConfigGitignore', () => {
  it('respectGitignore:true sets the inverse NO_IGNORE knob to "false" (honor gitignore)', () => {
    const t = env();
    applyConfigGitignore({ respectGitignore: true }, t);
    assert.equal(t.AMI_GLOB_NO_IGNORE, 'false');
  });

  it('respectGitignore:false sets the NO_IGNORE knob to "true" (ignore gitignore)', () => {
    const t = env();
    applyConfigGitignore({ respectGitignore: false }, t);
    assert.equal(t.AMI_GLOB_NO_IGNORE, 'true');
  });

  it('an explicit env value wins (one-shot override)', () => {
    const t = env({ AMI_GLOB_NO_IGNORE: 'false' });
    applyConfigGitignore({ respectGitignore: false }, t);
    assert.equal(t.AMI_GLOB_NO_IGNORE, 'false');
  });

  it('is a no-op when unset', () => {
    const t = env();
    applyConfigGitignore({}, t);
    assert.equal(t.AMI_GLOB_NO_IGNORE, undefined);
  });
});

// ---------------------------------------------------------------------------
// applyConfigAutoCompact  (autoCompactEnabled -> SUPERINFERENCE_DISABLE_AUTO_COMPACT)
// ---------------------------------------------------------------------------
describe('applyConfigAutoCompact', () => {
  it('autoCompactEnabled:false arms the disable env knob', () => {
    const t = env();
    applyConfigAutoCompact({ autoCompactEnabled: false }, t);
    assert.equal(t.SUPERINFERENCE_DISABLE_AUTO_COMPACT, '1');
  });

  it('autoCompactEnabled:true leaves auto-compaction on (no env set)', () => {
    const t = env();
    applyConfigAutoCompact({ autoCompactEnabled: true }, t);
    assert.equal(t.SUPERINFERENCE_DISABLE_AUTO_COMPACT, undefined);
  });

  it('an explicit SUPERINFERENCE_ disable env wins', () => {
    const t = env({ SUPERINFERENCE_DISABLE_AUTO_COMPACT: '0' });
    applyConfigAutoCompact({ autoCompactEnabled: false }, t);
    assert.equal(t.SUPERINFERENCE_DISABLE_AUTO_COMPACT, '0');
  });

  it('the Claude-compatible DISABLE_AUTO_COMPACT alias also short-circuits', () => {
    const t = env({ DISABLE_AUTO_COMPACT: '1' });
    applyConfigAutoCompact({ autoCompactEnabled: false }, t);
    assert.equal(t.SUPERINFERENCE_DISABLE_AUTO_COMPACT, undefined);
  });

  it('is a no-op when unset', () => {
    const t = env();
    applyConfigAutoCompact({}, t);
    assert.equal(t.SUPERINFERENCE_DISABLE_AUTO_COMPACT, undefined);
  });
});

// ---------------------------------------------------------------------------
// applyConfigPlansDirectory  (plansDirectory -> AMI_PLANS_DIR)
// ---------------------------------------------------------------------------
describe('applyConfigPlansDirectory', () => {
  it('seeds AMI_PLANS_DIR from a non-blank value (trimmed)', () => {
    const t = env();
    applyConfigPlansDirectory({ plansDirectory: '  docs/plans  ' }, t);
    assert.equal(t.AMI_PLANS_DIR, 'docs/plans');
  });

  it('an explicit env value wins', () => {
    const t = env({ AMI_PLANS_DIR: 'existing' });
    applyConfigPlansDirectory({ plansDirectory: 'docs/plans' }, t);
    assert.equal(t.AMI_PLANS_DIR, 'existing');
  });

  it('is a no-op for a blank/absent value', () => {
    const t = env();
    applyConfigPlansDirectory({ plansDirectory: '   ' }, t);
    applyConfigPlansDirectory({}, t);
    assert.equal(t.AMI_PLANS_DIR, undefined);
  });
});

// ---------------------------------------------------------------------------
// applyConfigAnthropicBetas  (fastMode / redactThinking -> ANTHROPIC_BETAS)
// ---------------------------------------------------------------------------
describe('applyConfigAnthropicBetas', () => {
  it('fastMode appends the fast-mode beta token', () => {
    const t = env();
    applyConfigAnthropicBetas({ fastMode: true }, t);
    assert.equal(t.ANTHROPIC_BETAS, FAST_MODE_BETA);
  });

  it('redactThinking appends the redact-thinking beta token', () => {
    const t = env();
    applyConfigAnthropicBetas({ redactThinking: true }, t);
    assert.equal(t.ANTHROPIC_BETAS, REDACT_THINKING_BETA);
  });

  it('both knobs append both tokens, comma-joined', () => {
    const t = env();
    applyConfigAnthropicBetas({ fastMode: true, redactThinking: true }, t);
    assert.deepEqual((t.ANTHROPIC_BETAS ?? '').split(','), [FAST_MODE_BETA, REDACT_THINKING_BETA]);
  });

  it('preserves user-set tokens and does not duplicate', () => {
    const t = env({ ANTHROPIC_BETAS: `custom-beta, ${FAST_MODE_BETA}` });
    applyConfigAnthropicBetas({ fastMode: true, redactThinking: true }, t);
    assert.deepEqual((t.ANTHROPIC_BETAS ?? '').split(','), ['custom-beta', FAST_MODE_BETA, REDACT_THINKING_BETA]);
  });

  it('is a no-op when neither knob is enabled', () => {
    const t = env();
    applyConfigAnthropicBetas({ fastMode: false }, t);
    assert.equal(t.ANTHROPIC_BETAS, undefined);
  });
});

// ---------------------------------------------------------------------------
// applyConfigStartupKnobs  (aggregator)
// ---------------------------------------------------------------------------
describe('applyConfigStartupKnobs', () => {
  it('applies every env-targeting bridge in one call', () => {
    const t = env();
    applyConfigStartupKnobs(
      { respectGitignore: true, autoCompactEnabled: false, plansDirectory: 'p', fastMode: true },
      t,
    );
    assert.equal(t.AMI_GLOB_NO_IGNORE, 'false');
    assert.equal(t.SUPERINFERENCE_DISABLE_AUTO_COMPACT, '1');
    assert.equal(t.AMI_PLANS_DIR, 'p');
    assert.equal(t.ANTHROPIC_BETAS, FAST_MODE_BETA);
  });

  it('is a no-op for an empty config', () => {
    const t = env();
    applyConfigStartupKnobs({}, t);
    assert.deepEqual(t, {});
  });
});

// ---------------------------------------------------------------------------
// resolveAdditionalDirectories  (additionalDirectories)
// ---------------------------------------------------------------------------
describe('resolveAdditionalDirectories', () => {
  const cwd = '/home/user/project';

  it('resolves relative entries against cwd and keeps absolute entries', () => {
    const out = resolveAdditionalDirectories({ additionalDirectories: ['src', '/opt/data'] }, cwd);
    assert.deepEqual(out, [path.join(cwd, 'src'), '/opt/data']);
  });

  it('de-dupes and drops blank/non-string entries', () => {
    const out = resolveAdditionalDirectories(
      { additionalDirectories: ['src', 'src', '  ', 42 as unknown as string] },
      cwd,
    );
    assert.deepEqual(out, [path.join(cwd, 'src')]);
  });

  it('returns [] when the key is absent or not an array', () => {
    assert.deepEqual(resolveAdditionalDirectories({}, cwd), []);
    assert.deepEqual(resolveAdditionalDirectories(null, cwd), []);
    assert.deepEqual(
      resolveAdditionalDirectories({ additionalDirectories: 'nope' as unknown as string[] }, cwd),
      [],
    );
  });
});

// ---------------------------------------------------------------------------
// mergeConfigs — new scalar knobs copied, additionalDirectories accumulates
// ---------------------------------------------------------------------------
describe('mergeConfigs — startup knobs', () => {
  it('copies the new scalar knobs with last-layer-wins', () => {
    const merged = mergeConfigs(
      { fastMode: true },
      {},
      { respectGitignore: true, plansDirectory: 'global-plans' },
      { plansDirectory: 'lower', autoCompactEnabled: false, extraCaCerts: '/ca.pem', redactThinking: true },
    );
    assert.equal(merged.fastMode, true);
    assert.equal(merged.respectGitignore, true);
    assert.equal(merged.autoCompactEnabled, false);
    assert.equal(merged.extraCaCerts, '/ca.pem');
    assert.equal(merged.redactThinking, true);
    // project ('global-plans') is higher priority than global ('lower').
    assert.equal(merged.plansDirectory, 'global-plans');
  });

  it('accumulates additionalDirectories across layers (dedupe)', () => {
    const merged = mergeConfigs(
      {},
      {},
      { additionalDirectories: ['a', 'b'] },
      { additionalDirectories: ['b', 'c'] },
    );
    assert.deepEqual(merged.additionalDirectories, ['b', 'c', 'a']);
  });
});

// ---------------------------------------------------------------------------
// schema recognition + value validation
// ---------------------------------------------------------------------------
describe('startup knobs — schema recognition + validation', () => {
  it('are recognized config keys (no spurious unknown-key warning)', () => {
    const cfg: ProjectConfig = {
      extraCaCerts: '/ca.pem',
      additionalDirectories: ['x'],
      respectGitignore: true,
      autoCompactEnabled: false,
      plansDirectory: 'p',
      fastMode: true,
      redactThinking: true,
    };
    assert.deepEqual(findUnknownConfigKeys(cfg), []);
  });

  it('flag wrong value types', () => {
    const issues = validateProjectConfigValues({
      respectGitignore: 'yes' as unknown as boolean,
      extraCaCerts: 5 as unknown as string,
      additionalDirectories: 'x' as unknown as string[],
    });
    const fields = issues.map(i => i.field).sort();
    assert.deepEqual(fields, ['additionalDirectories', 'extraCaCerts', 'respectGitignore']);
  });

  it('accept well-typed values', () => {
    assert.deepEqual(
      validateProjectConfigValues({
        extraCaCerts: '/ca.pem',
        additionalDirectories: ['a', 'b'],
        respectGitignore: true,
        autoCompactEnabled: false,
        plansDirectory: 'p',
        fastMode: true,
        redactThinking: false,
      }),
      [],
    );
  });
});
