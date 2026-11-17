import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  applyConfigVerbose,
  applyConfigCopyOnSelect,
  applyConfigSyntaxHighlighting,
  applyConfigIdleNotifThreshold,
  applyConfigSpinnerVerbs,
  applyConfigAnthropicBetas,
  applyConfigStartupKnobs,
  mergeConfigs,
  validateProjectConfigValues,
  findUnknownConfigKeys,
  REDACT_THINKING_BETA,
  VERBOSE_ENV,
  NO_COPY_ON_SELECT_ENV,
  SYNTAX_HIGHLIGHT_ENV,
  NOTIFY_THRESHOLD_ENV,
  SPINNER_VERBS_ENV,
  type ProjectConfig,
} from '../src/config';

// A fresh, isolated env object so no test mutates the real process.env.
function env(init: Record<string, string> = {}): NodeJS.ProcessEnv {
  return { ...init } as NodeJS.ProcessEnv;
}

// ---------------------------------------------------------------------------
// applyConfigVerbose  (verbose -> AMI_VERBOSE)
// ---------------------------------------------------------------------------
describe('applyConfigVerbose', () => {
  it('seeds AMI_VERBOSE=1 when verbose === true', () => {
    const t = env();
    applyConfigVerbose({ verbose: true }, t);
    assert.equal(t[VERBOSE_ENV], '1');
  });

  it('is a no-op when verbose is unset/false', () => {
    const t1 = env();
    applyConfigVerbose({}, t1);
    assert.equal(t1[VERBOSE_ENV], undefined);
    const t2 = env();
    applyConfigVerbose({ verbose: false }, t2);
    assert.equal(t2[VERBOSE_ENV], undefined);
  });

  it('an explicit env var wins (flag/--config bridge preserved)', () => {
    const t = env({ [VERBOSE_ENV]: '0' });
    applyConfigVerbose({ verbose: true }, t);
    assert.equal(t[VERBOSE_ENV], '0');
  });
});

// ---------------------------------------------------------------------------
// applyConfigCopyOnSelect  (copyOnSelect -> AMI_NO_COPY_ON_SELECT)
// ---------------------------------------------------------------------------
describe('applyConfigCopyOnSelect', () => {
  it('arms AMI_NO_COPY_ON_SELECT when copyOnSelect === false', () => {
    const t = env();
    applyConfigCopyOnSelect({ copyOnSelect: false }, t);
    assert.equal(t[NO_COPY_ON_SELECT_ENV], '1');
  });

  it('is a no-op when copyOnSelect is unset/true (auto-copy stays on)', () => {
    const t1 = env();
    applyConfigCopyOnSelect({}, t1);
    assert.equal(t1[NO_COPY_ON_SELECT_ENV], undefined);
    const t2 = env();
    applyConfigCopyOnSelect({ copyOnSelect: true }, t2);
    assert.equal(t2[NO_COPY_ON_SELECT_ENV], undefined);
  });

  it('an explicit env opt-out value wins', () => {
    const t = env({ [NO_COPY_ON_SELECT_ENV]: '' });
    applyConfigCopyOnSelect({ copyOnSelect: false }, t);
    assert.equal(t[NO_COPY_ON_SELECT_ENV], '');
  });
});

// ---------------------------------------------------------------------------
// applyConfigSyntaxHighlighting  (syntaxHighlightingDisabled -> AMI_SYNTAX_HIGHLIGHT)
// ---------------------------------------------------------------------------
describe('applyConfigSyntaxHighlighting', () => {
  it('arms AMI_SYNTAX_HIGHLIGHT=0 (defined-falsy) when disabled', () => {
    const t = env();
    applyConfigSyntaxHighlighting({ syntaxHighlightingDisabled: true }, t);
    assert.equal(t[SYNTAX_HIGHLIGHT_ENV], '0');
  });

  it('is a no-op when unset/false (highlighting stays on)', () => {
    const t = env();
    applyConfigSyntaxHighlighting({ syntaxHighlightingDisabled: false }, t);
    assert.equal(t[SYNTAX_HIGHLIGHT_ENV], undefined);
  });

  it('an explicit env value wins', () => {
    const t = env({ [SYNTAX_HIGHLIGHT_ENV]: '1' });
    applyConfigSyntaxHighlighting({ syntaxHighlightingDisabled: true }, t);
    assert.equal(t[SYNTAX_HIGHLIGHT_ENV], '1');
  });
});

// ---------------------------------------------------------------------------
// applyConfigIdleNotifThreshold  (messageIdleNotifThresholdMs -> AMI_DESKTOP_NOTIFY_THRESHOLD_MS)
// ---------------------------------------------------------------------------
describe('applyConfigIdleNotifThreshold', () => {
  it('seeds the threshold from a positive finite value (floored, stringified)', () => {
    const t = env();
    applyConfigIdleNotifThreshold({ messageIdleNotifThresholdMs: 60000 }, t);
    assert.equal(t[NOTIFY_THRESHOLD_ENV], '60000');
    const t2 = env();
    applyConfigIdleNotifThreshold({ messageIdleNotifThresholdMs: 1234.9 }, t2);
    assert.equal(t2[NOTIFY_THRESHOLD_ENV], '1234');
  });

  it('ignores non-positive / non-finite values (keeps the default)', () => {
    for (const bad of [0, -5, NaN, Infinity]) {
      const t = env();
      applyConfigIdleNotifThreshold({ messageIdleNotifThresholdMs: bad }, t);
      assert.equal(t[NOTIFY_THRESHOLD_ENV], undefined, `bad=${bad}`);
    }
  });

  it('an explicit env value wins', () => {
    const t = env({ [NOTIFY_THRESHOLD_ENV]: '5000' });
    applyConfigIdleNotifThreshold({ messageIdleNotifThresholdMs: 60000 }, t);
    assert.equal(t[NOTIFY_THRESHOLD_ENV], '5000');
  });
});

// ---------------------------------------------------------------------------
// applyConfigSpinnerVerbs  (spinnerVerbs -> AMI_SPINNER_VERBS JSON)
// ---------------------------------------------------------------------------
describe('applyConfigSpinnerVerbs', () => {
  it('serializes the override to the JSON env shape (mode defaults to append)', () => {
    const t = env();
    applyConfigSpinnerVerbs({ spinnerVerbs: { verbs: ['Transmuting', 'Conjuring'] } }, t);
    assert.deepEqual(JSON.parse(t[SPINNER_VERBS_ENV] as string), {
      mode: 'append',
      verbs: ['Transmuting', 'Conjuring'],
    });
  });

  it('honors mode: replace and drops blank/non-string verbs', () => {
    const t = env();
    applyConfigSpinnerVerbs(
      { spinnerVerbs: { mode: 'replace', verbs: ['Good', '  ', '', 'Great'] } },
      t,
    );
    assert.deepEqual(JSON.parse(t[SPINNER_VERBS_ENV] as string), {
      mode: 'replace',
      verbs: ['Good', 'Great'],
    });
  });

  it('is a no-op when verbs is missing / empty / all-blank', () => {
    for (const sv of [undefined, {}, { verbs: [] }, { verbs: ['', '   '] }] as ProjectConfig['spinnerVerbs'][]) {
      const t = env();
      applyConfigSpinnerVerbs({ spinnerVerbs: sv }, t);
      assert.equal(t[SPINNER_VERBS_ENV], undefined);
    }
  });

  it('an explicit env value wins', () => {
    const t = env({ [SPINNER_VERBS_ENV]: '{"mode":"append","verbs":["X"]}' });
    applyConfigSpinnerVerbs({ spinnerVerbs: { verbs: ['Y'] } }, t);
    assert.equal(t[SPINNER_VERBS_ENV], '{"mode":"append","verbs":["X"]}');
  });
});

// ---------------------------------------------------------------------------
// applyConfigAnthropicBetas  (showThinkingSummaries inverse -> redact-thinking)
// ---------------------------------------------------------------------------
describe('applyConfigAnthropicBetas + showThinkingSummaries', () => {
  it('arms redact-thinking when showThinkingSummaries === false', () => {
    const t = env();
    applyConfigAnthropicBetas({ showThinkingSummaries: false }, t);
    assert.equal(t.ANTHROPIC_BETAS, REDACT_THINKING_BETA);
  });

  it('is a no-op when showThinkingSummaries is unset/true', () => {
    const t1 = env();
    applyConfigAnthropicBetas({ showThinkingSummaries: true }, t1);
    assert.equal(t1.ANTHROPIC_BETAS, undefined);
    const t2 = env();
    applyConfigAnthropicBetas({}, t2);
    assert.equal(t2.ANTHROPIC_BETAS, undefined);
  });

  it('does not double-append when both redactThinking and showThinkingSummaries fire', () => {
    const t = env();
    applyConfigAnthropicBetas({ redactThinking: true, showThinkingSummaries: false }, t);
    assert.equal(t.ANTHROPIC_BETAS, REDACT_THINKING_BETA);
  });
});

// ---------------------------------------------------------------------------
// mergeConfigs + validation + unknown-key recognition (end-to-end waterfall)
// ---------------------------------------------------------------------------
describe('w81 cosmetic knobs through the config waterfall', () => {
  it('mergeConfigs carries the new knobs with project > global priority', () => {
    const merged = mergeConfigs(
      {}, // cliArgs
      {}, // envVars
      { verbose: true, syntaxHighlightingDisabled: true, messageIdleNotifThresholdMs: 30000 }, // project
      {
        verbose: false, // overridden by the higher-priority project layer
        copyOnSelect: true,
        spinnerVerbs: { mode: 'replace', verbs: ['Pondering'] },
        showThinkingSummaries: false,
      }, // global
    );
    assert.equal(merged.verbose, true, 'project layer wins over global');
    assert.equal(merged.copyOnSelect, true);
    assert.equal(merged.syntaxHighlightingDisabled, true);
    assert.equal(merged.messageIdleNotifThresholdMs, 30000);
    assert.equal(merged.showThinkingSummaries, false);
    assert.deepEqual(merged.spinnerVerbs, { mode: 'replace', verbs: ['Pondering'] });
  });

  it('the new keys are recognized (no spurious unknown-key warning)', () => {
    const cfg: ProjectConfig = {
      verbose: true,
      copyOnSelect: false,
      syntaxHighlightingDisabled: true,
      showThinkingSummaries: false,
      messageIdleNotifThresholdMs: 60000,
      spinnerVerbs: { verbs: ['X'] },
    };
    assert.deepEqual(findUnknownConfigKeys(cfg), []);
  });

  it('base-type validation rejects wrong-typed values', () => {
    const issues = validateProjectConfigValues({
      verbose: 'yes',
      messageIdleNotifThresholdMs: 'soon',
      spinnerVerbs: 'nope',
    } as unknown as ProjectConfig);
    const fields = issues.map(i => i.field);
    assert.ok(fields.includes('verbose'), 'verbose type issue');
    assert.ok(fields.includes('messageIdleNotifThresholdMs'), 'threshold type issue');
    assert.ok(fields.includes('spinnerVerbs'), 'spinnerVerbs type issue');
  });

  it('applyConfigStartupKnobs applies ALL new bridges from one merged config', () => {
    const t = env();
    applyConfigStartupKnobs(
      {
        verbose: true,
        copyOnSelect: false,
        syntaxHighlightingDisabled: true,
        messageIdleNotifThresholdMs: 45000,
        showThinkingSummaries: false,
        spinnerVerbs: { verbs: ['Churning'] },
      },
      t,
    );
    assert.equal(t[VERBOSE_ENV], '1');
    assert.equal(t[NO_COPY_ON_SELECT_ENV], '1');
    assert.equal(t[SYNTAX_HIGHLIGHT_ENV], '0');
    assert.equal(t[NOTIFY_THRESHOLD_ENV], '45000');
    assert.equal(t.ANTHROPIC_BETAS, REDACT_THINKING_BETA);
    assert.deepEqual(JSON.parse(t[SPINNER_VERBS_ENV] as string), { mode: 'append', verbs: ['Churning'] });
  });
});
