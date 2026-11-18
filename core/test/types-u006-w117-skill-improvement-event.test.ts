/**
 * U006 (w117) — `skill_improvement` EngineEvent (introspection surface for AMI's
 * write-back-to-SKILL.md feature).
 *
 * Parity: Claude Code's structured `skill_improvement_detected` signal
 * (utils/hooks/skillImprovement.ts) and its survey appeared/responded lifecycle
 * (hooks/useSkillImprovementSurvey.ts), minus the vendor telemetry routing. AMI had
 * the skill-improvement FEATURE (`/skill improve`) but no observable event, so the TUI,
 * a diagnostic, and `ami --introspect` could not see WHEN it fired. These tests lock
 * the event's discriminant + payload shape and its presence in ENGINE_EVENT_TYPES
 * (which flows into `ami --introspect`).
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { ENGINE_EVENT_TYPES } from '../src/types';
import type { EngineEvent } from '../src/types';

describe('skill_improvement EngineEvent (U006 w117)', () => {
  it('is advertised in ENGINE_EVENT_TYPES (discoverable via ami --introspect)', () => {
    assert.ok(
      (ENGINE_EVENT_TYPES as readonly string[]).includes('skill_improvement'),
      'ENGINE_EVENT_TYPES must advertise skill_improvement',
    );
  });

  it("accepts a 'detected' event carrying skillName + updateCount (no applied)", () => {
    const ev: EngineEvent = {
      type: 'skill_improvement',
      phase: 'detected',
      skillName: 'my-skill',
      updateCount: 3,
    };
    assert.equal(ev.type, 'skill_improvement');
    assert.equal(ev.phase, 'detected');
    assert.equal(ev.skillName, 'my-skill');
    assert.equal(ev.updateCount, 3);
  });

  it("accepts a 'responded' event with applied true/false", () => {
    const applied: EngineEvent = {
      type: 'skill_improvement',
      phase: 'responded',
      skillName: 's',
      updateCount: 2,
      applied: true,
    };
    const dismissed: EngineEvent = {
      type: 'skill_improvement',
      phase: 'responded',
      skillName: 's',
      updateCount: 2,
      applied: false,
    };
    assert.equal(applied.type === 'skill_improvement' && applied.applied, true);
    assert.equal(dismissed.type === 'skill_improvement' && dismissed.applied, false);
  });

  it('narrows the union on the type discriminant', () => {
    const ev: EngineEvent = { type: 'skill_improvement', phase: 'detected', skillName: 'x', updateCount: 1 };
    if (ev.type === 'skill_improvement') {
      // compile-time: these fields exist only on this variant
      assert.equal(typeof ev.phase, 'string');
      assert.equal(typeof ev.updateCount, 'number');
    } else {
      assert.fail('discriminant did not narrow');
    }
  });
});
