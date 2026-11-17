import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  loadConfigFileChecked,
  collectConfigDiagnostics,
  validateProjectConfigValues,
  findUnknownConfigKeys,
  KNOWN_CONFIG_KEYS,
  type ProjectConfig,
} from '../src/config';

// ---------------------------------------------------------------------------
// Claude-parity additive-superset gaps for config loading:
//   Gap 1 — unrecognized top-level config keys are surfaced (not silently
//            ignored), so a typo like "modle" doesn't take no effect.
//   Gap 2 — nested `superinference` flags are value-validated (a string where a
//            boolean is expected is caught, not flowed truthily into the gate).
// Each is RED-GREEN validated: the asserts discriminate the new behavior from
// the old (pre-fix) behavior.
// ---------------------------------------------------------------------------

describe('config parity — Gap 1: unknown top-level keys are surfaced', () => {
  let dir: string;
  beforeEach(() => { dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cfg-parity-unk-')); });
  afterEach(() => { try { fs.rmSync(dir, { recursive: true, force: true }); } catch { /* ignore */ } });

  it('findUnknownConfigKeys returns only unrecognized keys', () => {
    // RED (pre-fix): findUnknownConfigKeys did not exist. GREEN: typos are listed,
    // known keys (incl. managed-only + enum keys) are not.
    const unknown = findUnknownConfigKeys({
      model: 'x',
      permissionMode: 'ask',
      cacheTtl: '5m',
      // @ts-expect-error — deliberate unknown keys (typos / stale)
      modle: 'typo',
      // @ts-expect-error
      permisionMode: 'typo',
    });
    assert.deepEqual(unknown.sort(), ['modle', 'permisionMode']);
  });

  it('every known key is recognized (no false positives for a full valid config)', () => {
    // The known-key set is derived from the two validation tables; their union must
    // equal the full ProjectConfig surface or valid configs would spuriously warn.
    const full: ProjectConfig = {
      model: 'm', provider: 'p', baseUrl: 'u', permissionMode: 'ask',
      thinkingLevel: 't', maxEffortLevel: 'e', fallbackModel: 'f', compactionModel: 'c',
      maxTurns: 1, tokenBudget: 1, maxBudgetUsd: 1, superinference: { enforcing: true },
      skill: 's', enforceAvailableModels: ['a'], bashOutputMaxChars: 1, taskOutputMaxChars: 1, toolOutputMaxChars: 1,
      cacheTtl: '5m', safeMode: true, hasTrustDialogAccepted: true, trustedProjectPaths: ['/srv/trusted'], maxWebSearchesPerSession: 1, deferTools: true,
      alwaysLoadTools: ['a'], fallbackModels: ['a'], disableBypassPermissionsMode: true,
      disableAllHooks: true, allowManagedHooksOnly: true, enabledPlugins: {}, allowManagedModsOnly: true,
      allowedMcpServers: ['a'], deniedMcpServers: ['a'], managedMcpServers: {}, statusLine: 'l', sandboxExcludedCommands: ['a'],
      env: {}, tipsEnabled: true, tipsOverride: {}, composerHintsEnabled: true, composerHintsOverride: {},
      migrationVersion: 1, worktree: { symlinkDirectories: ['a'] }, cleanupPeriodDays: 1,
      attribution: { commit: 'x', pr: 'y' }, disabledMcpServers: ['a'], enabledMcpServers: ['a'],
      language: 'japanese', subagentDefaultModel: 'haiku', memoryExcludes: ['**/AGENTS.md'],
      externalMemoryImportsApproved: true, externalMemoryImportRoots: ['/srv/shared'],
      allowManagedSandboxDomainsOnly: true, skillUsage: { demo: { usageCount: 1, lastUsedAt: 1 } },
      // Persisted one-time preference flags + count-limited notice seen-counts (U010 w87):
      // global-only internal state, declared/validated like skillUsage.
      preferencesShown: { ideAutoConnectDialog: true }, noticeSeenCounts: { welcome: 2 },
      strictPluginOnlyCustomization: ['skills'],
      // Engine-behavior knobs (U010 w78): read off the merged config by the engine
      // factory, now declared/validated/merged so a config-file value takes effect.
      architectEnabled: false, architectModel: 'arch', embeddedSearchTools: true,
      teammateExecutionMode: 'in-process', unattendedRetry: true, fileCheckpointingEnabled: false,
      heapDumpOnMemoryWarning: true,
      features: { task_tracker: true }, max529RetriesBeforeFallback: 2, openrouterZdr: true,
      strictMcpConfig: true, mcpRequireApproval: true, maxUsageLimitWaitMs: 1000,
      sessionMemoryInterval: 5, alwaysLoadMcpServers: ['srv'], continueOnInvalidConfig: true,
      outputStyle: 'concise', includeGitInstructions: false,
      pluginHintsDisabled: true,
      // Startup runtime-knob bridges (w79): CA certs, plans dir, gitignore,
      // auto-compact, anthropic betas, extra directories.
      extraCaCerts: '/etc/ca.pem', plansDirectory: 'plans', respectGitignore: true,
      autoCompactEnabled: false, fastMode: true, redactThinking: true,
      additionalDirectories: ['/srv/data'],
      // Round-2 engine-behavior knobs (w80): memory/dream/session-memory toggles,
      // title generation, worktree tools, OSC 9;4 progress bar, turn-duration.
      autoMemoryEnabled: true, autoDreamEnabled: false, sessionMemoryEnabled: true,
      enableTitleGeneration: true, worktreeToolsEnabled: false,
      autoConnectIde: true,
      terminalProgressBarEnabled: true, showTurnDuration: false,
      // Cosmetic / output-detail startup knobs (U010 w81): bridged to their cli env
      // knobs by applyConfigStartupKnobs; declared/validated/merged like the above.
      verbose: true, copyOnSelect: false, syntaxHighlightingDisabled: true,
      showThinkingSummaries: false, messageIdleNotifThresholdMs: 60000,
      spinnerVerbs: { mode: 'append', verbs: ['Churning'] },
    };
    assert.deepEqual(findUnknownConfigKeys(full), []);
    // Every declared key is in the known-key set (and vice-versa — no extras).
    for (const k of Object.keys(full)) assert.ok(KNOWN_CONFIG_KEYS.has(k), `missing known key: ${k}`);
    // KNOWN_CONFIG_KEYS == the full ProjectConfig surface PLUS the editor-only `$schema`
    // reference-key (meta, not a typed knob — it carries a JSON-Schema URL for editor
    // autocomplete and is recognized so it never warns). Account for it explicitly.
    assert.ok(KNOWN_CONFIG_KEYS.has('$schema'), '`$schema` reference key is recognized');
    assert.equal(KNOWN_CONFIG_KEYS.size, Object.keys(full).length + 1);
  });

  it('loadConfigFileChecked emits an unknown-key diagnostic but still loads the config', () => {
    const p = path.join(dir, 'config.json');
    fs.writeFileSync(p, JSON.stringify({ model: 'gpt-4o', modle: 'typo', provder: 'oops' }));
    const r = loadConfigFileChecked(p);
    // GREEN: config still loads (advisory, non-fatal).
    assert.equal(r.config!.model, 'gpt-4o');
    // RED (pre-fix): no diagnostic at all for an otherwise-valid object.
    assert.ok(r.diagnostic, 'expected an unknown-key diagnostic');
    assert.equal(r.diagnostic!.kind, 'unknown-key');
    assert.deepEqual(r.diagnostic!.unknownKeys!.sort(), ['modle', 'provder']);
    assert.match(r.diagnostic!.message, /unknown config key/);
  });

  it('a fully-valid config with only known keys yields NO diagnostic', () => {
    // Discriminator: proves unknown-key detection does not false-positive.
    const p = path.join(dir, 'config.json');
    fs.writeFileSync(p, JSON.stringify({ model: 'gpt-4o', permissionMode: 'ask' }));
    const r = loadConfigFileChecked(p);
    assert.equal(r.diagnostic, undefined);
  });

  it('invalid-value takes precedence over unknown-key (values reported first)', () => {
    const p = path.join(dir, 'config.json');
    fs.writeFileSync(p, JSON.stringify({ permissionMode: 'wide-open', bogus: 1 }));
    const r = loadConfigFileChecked(p);
    assert.equal(r.diagnostic!.kind, 'invalid-value');
  });

  it('collectConfigDiagnostics surfaces unknown-key diagnostics across tiers', () => {
    fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(dir, '.ami', 'config.json'), JSON.stringify({ nonsense: true }));
    const diags = collectConfigDiagnostics(dir);
    const d = diags.find(x => x.path.endsWith(path.join('.ami', 'config.json')));
    assert.ok(d);
    assert.equal(d!.kind, 'unknown-key');
    assert.deepEqual(d!.unknownKeys, ['nonsense']);
  });
});

describe('config parity — Gap 2: nested superinference flags are value-validated', () => {
  it('flags a non-boolean superinference.enforcing', () => {
    // RED (pre-fix): only the outer object type was checked, so `"true"` passed and
    // flowed truthily into the reflection gate. GREEN: it is reported per nested field.
    const issues = validateProjectConfigValues({
      superinference: { enforcing: 'true' as unknown as boolean },
    });
    const i = issues.find(x => x.field === 'superinference.enforcing');
    assert.ok(i, 'expected a superinference.enforcing issue');
    assert.match(i!.message, /must be a boolean/);
    assert.equal(i!.value, 'true');
  });

  it('flags a non-boolean superinference.useLLMCritic', () => {
    const issues = validateProjectConfigValues({
      superinference: { useLLMCritic: 1 as unknown as boolean },
    });
    assert.ok(issues.some(x => x.field === 'superinference.useLLMCritic'));
  });

  it('accepts valid boolean flags (no false positive)', () => {
    // Discriminator: valid nested booleans produce no issue.
    const issues = validateProjectConfigValues({
      superinference: { enforcing: true, useLLMCritic: false },
    });
    assert.deepEqual(issues, []);
  });

  it('does not double-report when superinference itself is the wrong type', () => {
    // When the outer value is not an object, only the base-type issue is reported;
    // nested validation is skipped (no crash reading flags off a non-object).
    const issues = validateProjectConfigValues({
      superinference: 'nope' as unknown as ProjectConfig['superinference'],
    });
    assert.equal(issues.length, 1);
    assert.equal(issues[0]!.field, 'superinference');
  });

  it('surfaces nested issues through the file loader', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cfg-parity-si-'));
    try {
      const p = path.join(dir, 'config.json');
      fs.writeFileSync(p, JSON.stringify({ superinference: { enforcing: 'yes' } }));
      const r = loadConfigFileChecked(p);
      assert.equal(r.diagnostic!.kind, 'invalid-value');
      assert.ok(r.diagnostic!.issues!.some(i => i.field === 'superinference.enforcing'));
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});
