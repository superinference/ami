/**
 * U010 — `hasTrustDialogAccepted` persisted per-project FOLDER-TRUST config knob.
 *
 * Parity: the reference's `hasTrustDialogAccepted` project-config knob
 * (checkHasTrustDialogAccepted / saveCurrentProjectConfig). AMI previously had only
 * the INVERSE `safeMode` opt-in distrust and no config-backed way to GRANT trust, so
 * HookManager.setWorkspaceTrusted had no production caller and project hooks stayed
 * permanently skipped.
 *
 * Covers:
 *  - the ProjectConfig knob is a recognized, validated boolean key;
 *  - it merges across layers (last layer wins; managed/global/project/local);
 *  - resolveWorkspaceTrustAccepted() reads the config waterfall;
 *  - END-TO-END: HookManager.loadFromFile honors the persisted knob so a project hook
 *    loads ONLY when the workspace is trusted via config (RED-GREEN reproducer).
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  mergeConfigs,
  validateProjectConfigValues,
  KNOWN_CONFIG_KEYS,
  resolveWorkspaceTrustAccepted,
  type ProjectConfig,
} from '../src/config';
import { HookManager } from '../src/hooks';

describe('U010 hasTrustDialogAccepted — config knob', () => {
  it('is a recognized top-level config key', () => {
    assert.ok(KNOWN_CONFIG_KEYS.has('hasTrustDialogAccepted'),
      'the trust knob must be a known key so it is not flagged as an unknown/typo key');
  });

  it('flags a non-boolean value as an invalid-value issue', () => {
    const issues = validateProjectConfigValues({ hasTrustDialogAccepted: 'yes' as unknown as boolean });
    const issue = issues.find(i => i.field === 'hasTrustDialogAccepted');
    assert.ok(issue, 'a string value must be reported as invalid');
    assert.match(issue!.message, /boolean/);
  });

  it('accepts a boolean value without issue', () => {
    assert.equal(
      validateProjectConfigValues({ hasTrustDialogAccepted: true }).some(i => i.field === 'hasTrustDialogAccepted'),
      false);
  });

  it('merges across layers (project over global; higher layer wins)', () => {
    // global=false, project=true → trusted.
    const a = mergeConfigs({}, {}, { hasTrustDialogAccepted: true }, { hasTrustDialogAccepted: false });
    assert.equal(a.hasTrustDialogAccepted, true);
    // project=false explicitly overrides a global=true.
    const b = mergeConfigs({}, {}, { hasTrustDialogAccepted: false }, { hasTrustDialogAccepted: true });
    assert.equal(b.hasTrustDialogAccepted, false);
    // absent everywhere → undefined (untrusted default).
    assert.equal(mergeConfigs({}, {}, {}, {}).hasTrustDialogAccepted, undefined);
  });
});

describe('U010 resolveWorkspaceTrustAccepted + HookManager wiring', () => {
  let tmp: string;
  let home: string;
  let origHome: string | undefined;
  let origManaged: string | undefined;

  beforeEach(() => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-trust-'));
    home = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-trust-home-'));
    origHome = process.env.HOME;
    origManaged = process.env.SUPERINFERENCE_MANAGED_CONFIG;
    process.env.HOME = home;
    // Point the managed-config path at a guaranteed-absent file so a host /etc policy
    // can never influence the trust resolution under test.
    process.env.SUPERINFERENCE_MANAGED_CONFIG = path.join(home, 'no-such-managed.json');
  });

  afterEach(() => {
    if (origHome === undefined) delete process.env.HOME; else process.env.HOME = origHome;
    if (origManaged === undefined) delete process.env.SUPERINFERENCE_MANAGED_CONFIG; else process.env.SUPERINFERENCE_MANAGED_CONFIG = origManaged;
    for (const d of [tmp, home]) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ } }
  });

  function writeProjectConfig(cfg: ProjectConfig): void {
    fs.mkdirSync(path.join(tmp, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(tmp, '.ami', 'config.json'), JSON.stringify(cfg));
  }
  function writeGlobalConfig(cfg: ProjectConfig): void {
    fs.mkdirSync(path.join(home, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(home, '.ami', 'config.json'), JSON.stringify(cfg));
  }
  function writeProjectHook(): void {
    fs.mkdirSync(path.join(tmp, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(tmp, '.ami', 'hooks.json'),
      JSON.stringify({ hooks: [{ event: 'preToolUse', hook: { type: 'command', command: 'echo project' } }] }));
  }

  it('returns false when no config grants trust', () => {
    assert.equal(resolveWorkspaceTrustAccepted(tmp), false);
  });

  it('returns true when the PROJECT config sets the knob', () => {
    writeProjectConfig({ hasTrustDialogAccepted: true });
    assert.equal(resolveWorkspaceTrustAccepted(tmp), true);
  });

  it('returns true when the GLOBAL config sets the knob', () => {
    writeGlobalConfig({ hasTrustDialogAccepted: true });
    assert.equal(resolveWorkspaceTrustAccepted(tmp), true);
  });

  it('an explicit project false overrides a global true', () => {
    writeGlobalConfig({ hasTrustDialogAccepted: true });
    writeProjectConfig({ hasTrustDialogAccepted: false });
    assert.equal(resolveWorkspaceTrustAccepted(tmp), false);
  });

  // END-TO-END RED-GREEN: project hook loads ONLY when trust is granted via config.
  it('HookManager loads a project hook when the config grants trust', () => {
    writeProjectHook();
    writeProjectConfig({ hasTrustDialogAccepted: true });
    const hm = new HookManager();     // NOT calling setWorkspaceTrusted — trust must come from config
    hm.setCwd(tmp);
    hm.loadFromFile(tmp);
    // RED (revert the hooks.ts wiring): workspaceTrusted stays false → this is 0.
    assert.equal(hm.getSummary().preToolUse, 1, 'config-granted trust must let the project hook load');
    assert.equal(hm.isWorkspaceTrusted(), true, 'the knob promotes workspace trust');
  });

  it('HookManager SKIPS a project hook when the config does NOT grant trust (control)', () => {
    writeProjectHook();
    writeProjectConfig({ hasTrustDialogAccepted: false });
    const hm = new HookManager();
    hm.setCwd(tmp);
    hm.loadFromFile(tmp);
    assert.equal(hm.getSummary().preToolUse, 0, 'without a trust grant the project hook stays skipped');
    assert.equal(hm.isWorkspaceTrusted(), false);
  });

  it('does not DEMOTE an already-trusted manager (promote-only)', () => {
    writeProjectHook();
    writeProjectConfig({ hasTrustDialogAccepted: false });
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);   // already trusted by an explicit caller
    hm.setCwd(tmp);
    hm.loadFromFile(tmp);
    assert.equal(hm.isWorkspaceTrusted(), true, 'an explicit trust grant is never demoted by a false knob');
    assert.equal(hm.getSummary().preToolUse, 1);
  });
});
