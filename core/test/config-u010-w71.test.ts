import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  validateProjectConfigValues,
  getConfigValidationDocLink,
  formatConfigValueIssues,
  loadConfigFileChecked,
  mergeConfigs,
  applyConfigLanguage,
  CONFIG_DOC_BASE,
  KNOWN_CONFIG_KEYS,
  type ConfigValueIssue,
  type ProjectConfig,
} from '../src/config';

const tmpDirs: string[] = [];
function makeTmp(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-cfg-w71-'));
  tmpDirs.push(d);
  return d;
}
function writeConfig(dir: string, raw: string): string {
  const p = path.join(dir, 'config.json');
  fs.writeFileSync(p, raw);
  return p;
}
after(() => {
  for (const d of tmpDirs) {
    try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* best effort */ }
  }
});

// ---------------------------------------------------------------------------
// U010 (w71) — ValidationErrorsList parity: per-error docLink + deduped
// "Learn more:" affordance on config validation errors.
// ---------------------------------------------------------------------------
describe('config validation docLink (ValidationErrorsList parity)', () => {
  it('resolves a top-level field to its own anchor', () => {
    assert.equal(getConfigValidationDocLink('maxTurns'), `${CONFIG_DOC_BASE}#maxturns`);
    assert.equal(getConfigValidationDocLink('permissionMode'), `${CONFIG_DOC_BASE}#permissionmode`);
  });

  it('collapses a nested field to its TOP-LEVEL anchor (so sibling sub-field errors share one link)', () => {
    assert.equal(getConfigValidationDocLink('superinference.enforcing'), `${CONFIG_DOC_BASE}#superinference`);
    assert.equal(getConfigValidationDocLink('worktree.sparsePaths'), `${CONFIG_DOC_BASE}#worktree`);
    assert.equal(getConfigValidationDocLink('env.DEBUG'), `${CONFIG_DOC_BASE}#env`);
  });

  it('attaches a docLink to EVERY issue returned by validateProjectConfigValues', () => {
    const issues = validateProjectConfigValues({
      maxTurns: 'nope' as unknown as number,
      permissionMode: 'wrong' as unknown as ProjectConfig['permissionMode'],
    });
    assert.ok(issues.length >= 2);
    for (const i of issues) {
      assert.equal(typeof i.docLink, 'string');
      assert.ok((i.docLink as string).startsWith(CONFIG_DOC_BASE));
    }
  });

  it('formatConfigValueIssues renders per-issue tips AND a single deduped "Learn more:" line', () => {
    // Two issues on nested siblings of the SAME top-level object → one shared docLink.
    const issues: ConfigValueIssue[] = [
      { field: 'superinference.enforcing', message: 'must be a boolean (got string)', tip: 'Use true or false without quotes.', docLink: getConfigValidationDocLink('superinference.enforcing') },
      { field: 'superinference.useLLMCritic', message: 'must be a boolean (got number)', tip: 'Use true or false without quotes.', docLink: getConfigValidationDocLink('superinference.useLLMCritic') },
    ];
    const out = formatConfigValueIssues(issues);
    assert.match(out, /superinference\.enforcing: must be a boolean/);
    assert.match(out, /superinference\.useLLMCritic: must be a boolean/);
    // "Learn more:" appears exactly once and the shared anchor is not duplicated.
    const learnMore = out.match(/Learn more:/g) ?? [];
    assert.equal(learnMore.length, 1);
    const anchor = `${CONFIG_DOC_BASE}#superinference`;
    const occurrences = out.split(anchor).length - 1;
    assert.equal(occurrences, 1, 'the shared docLink must be deduplicated');
  });

  it('formatConfigValueIssues omits the "Learn more:" line when no docLink is present', () => {
    const out = formatConfigValueIssues([{ field: 'x', message: 'bad' }]);
    assert.equal(out, 'x: bad');
    assert.doesNotMatch(out, /Learn more/);
  });

  it('surfaces the deduped "Learn more:" affordance through loadConfigFileChecked', () => {
    const dir = makeTmp();
    const p = writeConfig(dir, JSON.stringify({ maxTurns: 'ten', permissionMode: 'nope' }));
    const { config, diagnostic } = loadConfigFileChecked(p);
    // Invalid values are advisory — the file still loads.
    assert.ok(config);
    assert.ok(diagnostic);
    assert.equal(diagnostic!.kind, 'invalid-value');
    assert.match(diagnostic!.message, /Learn more: /);
    // Each issue carries its docLink for a structured renderer.
    assert.ok(diagnostic!.issues && diagnostic!.issues.every(i => typeof i.docLink === 'string'));
  });
});

// ---------------------------------------------------------------------------
// U010 (w71) — LanguagePicker parity: persisted `language` config knob.
// ---------------------------------------------------------------------------
describe('persisted language config knob (LanguagePicker parity)', () => {
  it('is a recognized config key and type-validated as a string', () => {
    assert.ok(KNOWN_CONFIG_KEYS.has('language'));
    const issues = validateProjectConfigValues({ language: 123 as unknown as string });
    const langIssue = issues.find(i => i.field === 'language');
    assert.ok(langIssue, 'a non-string language must be flagged');
    assert.match(langIssue!.message, /must be a string/);
  });

  it('accepts a valid string language with no issue', () => {
    const issues = validateProjectConfigValues({ language: 'japanese' });
    assert.equal(issues.find(i => i.field === 'language'), undefined);
  });

  it('merges across config layers with higher layers winning', () => {
    const merged = mergeConfigs(
      {},
      {},
      { language: 'french' },          // project
      { language: 'spanish' },         // global (lower priority)
    );
    assert.equal(merged.language, 'french');
  });

  it('applyConfigLanguage seeds SUPERINFERENCE_LANGUAGE from config when env is unset', () => {
    const env: NodeJS.ProcessEnv = {};
    const result = applyConfigLanguage({ language: 'japanese' }, env);
    assert.equal(result, 'japanese');
    assert.equal(env.SUPERINFERENCE_LANGUAGE, 'japanese');
  });

  it('applyConfigLanguage trims a padded config value', () => {
    const env: NodeJS.ProcessEnv = {};
    applyConfigLanguage({ language: '  german  ' }, env);
    assert.equal(env.SUPERINFERENCE_LANGUAGE, 'german');
  });

  it('applyConfigLanguage lets an explicit env var WIN over the config field (one-shot override)', () => {
    const env: NodeJS.ProcessEnv = { SUPERINFERENCE_LANGUAGE: 'korean' };
    const result = applyConfigLanguage({ language: 'japanese' }, env);
    assert.equal(result, 'korean');
    assert.equal(env.SUPERINFERENCE_LANGUAGE, 'korean');
  });

  it('applyConfigLanguage is a no-op for a blank/absent value on both sides', () => {
    const env: NodeJS.ProcessEnv = {};
    assert.equal(applyConfigLanguage({ language: '   ' }, env), undefined);
    assert.equal(env.SUPERINFERENCE_LANGUAGE, undefined);
    assert.equal(applyConfigLanguage(null, env), undefined);
    assert.equal(applyConfigLanguage(undefined, env), undefined);
    assert.equal(env.SUPERINFERENCE_LANGUAGE, undefined);
  });

  it('applyConfigLanguage treats a blank env var as unset and falls through to config', () => {
    const env: NodeJS.ProcessEnv = { SUPERINFERENCE_LANGUAGE: '   ' };
    const result = applyConfigLanguage({ language: 'italian' }, env);
    assert.equal(result, 'italian');
    assert.equal(env.SUPERINFERENCE_LANGUAGE, 'italian');
  });
});
