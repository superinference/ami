import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  verdictRank,
  holdsOver,
  mostRestrictiveVerdict,
  PermissionVerdict,
} from '../src/permissions';

describe('verdictRank', () => {
  it('ranks deny as 3', () => {
    assert.equal(verdictRank('deny'), 3);
  });

  it('ranks ask as 2', () => {
    assert.equal(verdictRank('ask'), 2);
  });

  it('ranks allow as 1', () => {
    assert.equal(verdictRank('allow'), 1);
  });

  it('ranks unknown as 0', () => {
    assert.equal(verdictRank('unknown'), 0);
    assert.equal(verdictRank(''), 0);
  });
});

describe('holdsOver', () => {
  it('deny holds over allow', () => {
    assert.equal(holdsOver({ decision: 'deny' }, { decision: 'allow' }), true);
  });

  it('deny holds over ask', () => {
    assert.equal(holdsOver({ decision: 'deny' }, { decision: 'ask' }), true);
  });

  it('ask holds over allow', () => {
    assert.equal(holdsOver({ decision: 'ask' }, { decision: 'allow' }), true);
  });

  it('allow does NOT hold over deny', () => {
    assert.equal(holdsOver({ decision: 'allow' }, { decision: 'deny' }), false);
  });

  it('allow does NOT hold over ask', () => {
    assert.equal(holdsOver({ decision: 'allow' }, { decision: 'ask' }), false);
  });

  it('ask does NOT hold over deny', () => {
    assert.equal(holdsOver({ decision: 'ask' }, { decision: 'deny' }), false);
  });

  it('same rank: held has rule, answer does not → holds', () => {
    assert.equal(
      holdsOver({ decision: 'ask', rule: 'org-policy' }, { decision: 'ask' }),
      true,
    );
  });

  it('same rank: held has source, answer does not → holds', () => {
    assert.equal(
      holdsOver({ decision: 'deny', source: 'security' }, { decision: 'deny' }),
      true,
    );
  });

  it('same rank: neither has rule/source → does not hold', () => {
    assert.equal(
      holdsOver({ decision: 'ask' }, { decision: 'ask' }),
      false,
    );
  });

  it('same rank: answer has rule → does not hold', () => {
    assert.equal(
      holdsOver({ decision: 'deny' }, { decision: 'deny', rule: 'policy' }),
      false,
    );
  });

  it('same rank: both have rule → does not hold (no redundant override)', () => {
    assert.equal(
      holdsOver(
        { decision: 'ask', rule: 'a' },
        { decision: 'ask', rule: 'b' },
      ),
      false,
    );
  });

  it('carries reason through without affecting hold logic', () => {
    const held: PermissionVerdict = { decision: 'deny', reason: 'blocked' };
    const answer: PermissionVerdict = { decision: 'allow', reason: 'default' };
    assert.equal(holdsOver(held, answer), true);
    assert.equal(held.reason, 'blocked');
  });
});

describe('mostRestrictiveVerdict', () => {
  it('returns allow for empty array', () => {
    const result = mostRestrictiveVerdict([]);
    assert.equal(result.decision, 'allow');
  });

  it('returns single verdict unchanged', () => {
    const v: PermissionVerdict = { decision: 'ask', rule: 'test' };
    assert.deepEqual(mostRestrictiveVerdict([v]), v);
  });

  it('picks deny over allow and ask', () => {
    const result = mostRestrictiveVerdict([
      { decision: 'allow' },
      { decision: 'ask' },
      { decision: 'deny' },
    ]);
    assert.equal(result.decision, 'deny');
  });

  it('picks ask over allow', () => {
    const result = mostRestrictiveVerdict([
      { decision: 'allow' },
      { decision: 'ask' },
    ]);
    assert.equal(result.decision, 'ask');
  });

  it('picks the one with rule when ranks tie', () => {
    const result = mostRestrictiveVerdict([
      { decision: 'deny' },
      { decision: 'deny', rule: 'security-policy' },
    ]);
    assert.equal(result.rule, 'security-policy');
  });

  it('picks the one with source when ranks tie and no rule', () => {
    const result = mostRestrictiveVerdict([
      { decision: 'ask' },
      { decision: 'ask', source: 'org' },
    ]);
    assert.equal(result.source, 'org');
  });

  it('preserves reason on winning verdict', () => {
    const result = mostRestrictiveVerdict([
      { decision: 'allow', reason: 'default' },
      { decision: 'deny', reason: 'blocked by admin' },
    ]);
    assert.equal(result.decision, 'deny');
    assert.equal(result.reason, 'blocked by admin');
  });

  it('first verdict wins when all identical with no attribution', () => {
    const a: PermissionVerdict = { decision: 'ask' };
    const b: PermissionVerdict = { decision: 'ask' };
    const result = mostRestrictiveVerdict([a, b]);
    assert.strictEqual(result, a);
  });
});
