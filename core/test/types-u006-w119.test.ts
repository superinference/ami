/**
 * U006 (w119) — two additive type-level parity capabilities:
 *
 * 1. `THEME_LABELS` / `describeTheme` — descriptive labels for each {@link Theme}
 *    value (parity: Claude Code's `ThemePicker` option labels, e.g.
 *    `auto` → "Auto (match terminal)"). The `/theme` listing surfaces them so a
 *    user sees what each option does, not just its terse machine id.
 *
 * 2. `memory_warning` EngineEvent gains a progressive `level` (1 high / 2 critical /
 *    3 severe) so a frontend can render DISTINCT severity wording and escalate the
 *    notification kind at the top tier (parity: Claude's useMemoryUsage status).
 *
 * These lock the label map ↔ THEMES 1:1 invariant, the describeTheme pass-through,
 * and the memory_warning level discriminant.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { THEMES, THEME_LABELS, describeTheme } from '../src/types';
import type { EngineEvent, Theme } from '../src/types';

describe('THEME_LABELS / describeTheme (U006 w119)', () => {
  it('has a non-empty label for EVERY theme in THEMES (map ↔ enum 1:1, no drift)', () => {
    for (const t of THEMES) {
      const label = THEME_LABELS[t];
      assert.ok(typeof label === 'string' && label.length > 0, `missing label for ${t}`);
    }
    // No stray keys beyond the enum members.
    assert.deepEqual(
      Object.keys(THEME_LABELS).sort(),
      [...THEMES].sort(),
      'THEME_LABELS keys must match THEMES exactly',
    );
  });

  it('matches Claude ThemePicker labels for the descriptive entries', () => {
    assert.equal(THEME_LABELS['auto'], 'Auto (match terminal)');
    assert.equal(THEME_LABELS['dark'], 'Dark mode');
    assert.equal(THEME_LABELS['light'], 'Light mode');
    assert.equal(THEME_LABELS['dark-daltonized'], 'Dark mode (colorblind-friendly)');
    assert.equal(THEME_LABELS['light-ansi'], 'Light mode (ANSI colors only)');
  });

  it('describeTheme resolves a known theme to its label', () => {
    assert.equal(describeTheme('auto'), 'Auto (match terminal)');
    const t: Theme = 'dark-ansi';
    assert.equal(describeTheme(t), 'Dark mode (ANSI colors only)');
  });

  it('describeTheme passes an unknown name through unchanged', () => {
    assert.equal(describeTheme('neon-custom'), 'neon-custom');
    assert.equal(describeTheme(''), '');
  });
});

describe('memory_warning level tier (U006 w119)', () => {
  it('accepts each escalation tier on the event', () => {
    for (const level of [1, 2, 3] as const) {
      const ev: EngineEvent = {
        type: 'memory_warning',
        level,
        rssBytes: 2_000_000_000,
        thresholdBytes: 1_500_000_000,
        recoverySteps: ['/compact to summarize context'],
      };
      assert.equal(ev.type === 'memory_warning' && ev.level, level);
    }
  });

  it('narrows the union on the type discriminant to expose level', () => {
    const ev: EngineEvent = {
      type: 'memory_warning',
      level: 3,
      rssBytes: 3_000_000_000,
      thresholdBytes: 2_500_000_000,
      recoverySteps: ['/clear to reset conversation'],
    };
    if (ev.type === 'memory_warning') {
      assert.equal(ev.level, 3);
      assert.ok(ev.recoverySteps.length > 0);
    } else {
      assert.fail('discriminant did not narrow');
    }
  });
});
