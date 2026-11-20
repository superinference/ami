import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { resolveCriticModelLock } from '../src/superinference/types';

// Gap (parity: the reference's advisor `canUserConfigure`): a reviewer-model override must be
// LOCKABLE by a managed/experiment policy. When the policy pins the critic model
// (canUserConfigureCriticModel === false) a user-supplied override is dropped in favour
// of the pinned value; when user-configurable, the user's choice wins.
describe('resolveCriticModelLock', () => {
  it('returns undefined when nothing configures a critic model anywhere', () => {
    assert.equal(resolveCriticModelLock(undefined, undefined), undefined);
    assert.equal(resolveCriticModelLock({}, {}), undefined);
  });

  it('uses the user critic model when nothing is pinned', () => {
    assert.equal(
      resolveCriticModelLock({ criticModel: 'gpt-reviewer' }, undefined),
      'gpt-reviewer',
    );
  });

  it('honours an explicit user-configurable flag (true) and keeps the user model', () => {
    assert.equal(
      resolveCriticModelLock({ criticModel: 'gpt-reviewer', canUserConfigureCriticModel: true }, undefined),
      'gpt-reviewer',
    );
  });

  it('DROPS a user override to the pinned managed model when managed locks it', () => {
    assert.equal(
      resolveCriticModelLock(
        { criticModel: 'user-expensive' },
        { criticModel: 'policy-reviewer', canUserConfigureCriticModel: false },
      ),
      'policy-reviewer',
    );
  });

  it('DROPS a user override to inherit (empty) when managed locks without its own model', () => {
    assert.equal(
      resolveCriticModelLock(
        { criticModel: 'user-expensive' },
        { canUserConfigureCriticModel: false },
      ),
      '',
    );
  });

  it('honours a self-declared lock in the user config (no managed opinion)', () => {
    assert.equal(
      resolveCriticModelLock(
        { criticModel: 'user-expensive', canUserConfigureCriticModel: false },
        undefined,
      ),
      '',
    );
  });

  it('managed unlock (true) overrides a user self-lock and keeps the user model', () => {
    assert.equal(
      resolveCriticModelLock(
        { criticModel: 'user-pick', canUserConfigureCriticModel: false },
        { canUserConfigureCriticModel: true },
      ),
      'user-pick',
    );
  });

  it('falls back to the managed model when the user set none and it is user-configurable', () => {
    assert.equal(
      resolveCriticModelLock({}, { criticModel: 'policy-default' }),
      'policy-default',
    );
  });
});
